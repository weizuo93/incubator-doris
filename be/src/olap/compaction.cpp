// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "gutil/strings/substitute.h"
#include "olap/compaction.h"
#include "olap/rowset/rowset_factory.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {

Semaphore Compaction::_concurrency_sem;

Compaction::Compaction(TabletSharedPtr tablet)
    : _tablet(tablet),
      _input_rowsets_size(0),
      _input_row_num(0),
      _state(CompactionState::INITED)
{ }

Compaction::~Compaction() {}

OLAPStatus Compaction::init(int concurreny) {
    _concurrency_sem.set_count(concurreny);
    return OLAP_SUCCESS;
}

OLAPStatus Compaction::do_compaction() {
    _concurrency_sem.wait();
    TRACE("got concurrency lock and start to do compaction");
    OLAPStatus st = do_compaction_impl();
    _concurrency_sem.signal();
    return st;
}

/*执行rowset的compaction操作*/
OLAPStatus Compaction::do_compaction_impl() {
    OlapStopWatch watch;

    // 1. prepare input and output parameters
    int64_t segments_num = 0;
    for (auto& rowset : _input_rowsets) {
        _input_rowsets_size += rowset->data_disk_size();
        _input_row_num += rowset->num_rows();
        segments_num += rowset->num_segments();
    }
    TRACE_COUNTER_INCREMENT("input_rowsets_data_size", _input_rowsets_size);
    TRACE_COUNTER_INCREMENT("input_row_num", _input_row_num);
    TRACE_COUNTER_INCREMENT("input_segments_num", segments_num);

    _output_version = Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());//front()返回当前vector容器中起始元素的引用,back()返回当前vector容器中末尾元素的引用。
    _tablet->compute_version_hash_from_rowsets(_input_rowsets, &_output_version_hash);//计算版本hash

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->full_name()
            << ", output version is=" << _output_version.first << "-" << _output_version.second;

    RETURN_NOT_OK(construct_output_rowset_writer()); //构建compaction生成的rowset的writer
    RETURN_NOT_OK(construct_input_rowset_readers()); //构建compaction输入的rowset的reader
    TRACE("prepare finished");

    // 2. write merged rows to output rowset
    Merger::Statistics stats;
    auto res = Merger::merge_rowsets(_tablet, compaction_type(), _input_rs_readers, _output_rs_writer.get(), &stats);//将tablet中的多个rowset合并成一个rowset
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to do " << compaction_name()
                     << ". res=" << res
                     << ", tablet=" << _tablet->full_name()
                     << ", output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return res;
    }
    TRACE("merge rowsets finished");
    TRACE_COUNTER_INCREMENT("merged_rows", stats.merged_rows);
    TRACE_COUNTER_INCREMENT("filtered_rows", stats.filtered_rows);

    _output_rowset = _output_rs_writer->build();//通过RowsetWriter构建compaction生成的rowset
    if (_output_rowset == nullptr) {
        LOG(WARNING) << "rowset writer build failed. writer version:"
                     << ", output_version=" << _output_version.first
                     << "-" << _output_version.second;
        return OLAP_ERR_MALLOC_ERROR;
    }
    TRACE_COUNTER_INCREMENT("output_rowset_data_size", _output_rowset->data_disk_size());
    TRACE_COUNTER_INCREMENT("output_row_num", _output_rowset->num_rows());
    TRACE_COUNTER_INCREMENT("output_segments_num", _output_rowset->num_segments());
    TRACE("output rowset built");

    // 3. check correctness
    RETURN_NOT_OK(check_correctness(stats));//根据合并前后rowset中行数的对应关系检查rowset合并的正确性
    TRACE("check correctness finished");

    // 4. modify rowsets in memory
    modify_rowsets();//compaction后将多个rowset合并成了一个大的rowset，需要在tablet的meta信息中更新rowset meta信息
    TRACE("modify rowsets finished");

    // 5. update last success compaction time
    int64_t now = UnixMillis();
    if (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {//判断compaction的类型
        _tablet->set_last_cumu_compaction_success_time(now);//更新cumulative compaction的时间
    } else {
        _tablet->set_last_base_compaction_success_time(now);//更新base compaction的时间
    }

    LOG(INFO) << "succeed to do " << compaction_name()
              << ". tablet=" << _tablet->full_name()
              << ", output_version=" << _output_version.first
              << "-" << _output_version.second
              << ", segments=" << segments_num
              << ". elapsed time=" << watch.get_elapse_second() << "s.";

    return OLAP_SUCCESS;
}

/*为compaction输出的rowset创建writer，保存在成员变量_output_rs_writer中*/
OLAPStatus Compaction::construct_output_rowset_writer() {
    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet->tablet_uid();
    context.tablet_id = _tablet->tablet_id();
    context.partition_id = _tablet->partition_id();
    context.tablet_schema_hash = _tablet->schema_hash();
    context.rowset_type = StorageEngine::instance()->default_rowset_type();
    if (_tablet->tablet_meta()->preferred_rowset_type() == BETA_ROWSET) {
        context.rowset_type = BETA_ROWSET;
    }
    context.rowset_path_prefix = _tablet->tablet_path();
    context.tablet_schema = &(_tablet->tablet_schema());
    context.rowset_state = VISIBLE;
    context.version = _output_version;
    context.version_hash = _output_version_hash;
    context.segments_overlap = NONOVERLAPPING;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, &_output_rs_writer));//通过RowsetFactory创建rowset writer
    return OLAP_SUCCESS;
}

/*为tablet中需要compaction的每一个rowset创建reader，并添加到vector类型的成员变量_input_rs_readers中*/
OLAPStatus Compaction::construct_input_rowset_readers() {
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_NOT_OK(rowset->create_reader(&rs_reader));
        _input_rs_readers.push_back(std::move(rs_reader));
    }
    return OLAP_SUCCESS;
}

/*将compaction生成的rowset在tablet的meta信息中进行更新*/
void Compaction::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);
    
    WriteLock wrlock(_tablet->get_header_lock_ptr());
    _tablet->modify_rowsets(output_rowsets, _input_rowsets);//将compaction生成的rowset在tablet的meta信息中进行更新
    _tablet->save_meta();//保存tablet的meta信息
}

/*回收不可用的rowset，将不可用的rowset添加到StorageEngine类的成员变量_unused_rowsets中进行管理*/
OLAPStatus Compaction::gc_unused_rowsets() {
    StorageEngine* storage_engine = StorageEngine::instance();
    //如果compaction没有成功，则将compaction生成的output rowset设为不可用
    if (_state != CompactionState::SUCCESS) {
        storage_engine->add_unused_rowset(_output_rowset);
        return OLAP_SUCCESS;
    }
    //如果compaction成功了，则将compaction之前的那些已经被合并的rowset设为不可用
    for (auto& rowset : _input_rowsets) {
        storage_engine->add_unused_rowset(rowset);
    }
    _input_rowsets.clear();
    return OLAP_SUCCESS;
}

/*检查参数传入的多个rowset的版本连续性*/
OLAPStatus Compaction::check_version_continuity(const vector<RowsetSharedPtr>& rowsets) {
    RowsetSharedPtr prev_rowset = rowsets.front();
    for (size_t i = 1; i < rowsets.size(); ++i) {
        RowsetSharedPtr rowset = rowsets[i];
        if (rowset->start_version() != prev_rowset->end_version() + 1) {
            LOG(WARNING) << "There are missed versions among rowsets. "
                << "prev_rowset verison=" << prev_rowset->start_version()
                << "-" << prev_rowset->end_version()
                << ", rowset version=" << rowset->start_version()
                << "-" << rowset->end_version();
            return OLAP_ERR_CUMULATIVE_MISS_VERSION;
        }
        prev_rowset = rowset;
    }

    return OLAP_SUCCESS;
}

/*检查rowset合并的正确性*/
OLAPStatus Compaction::check_correctness(const Merger::Statistics& stats) {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
        LOG(WARNING) << "row_num does not match between cumulative input and output! "
                   << "input_row_num=" << _input_row_num
                   << ", merged_row_num=" << stats.merged_rows
                   << ", filted_row_num=" << stats.filtered_rows
                   << ", output_row_num=" << _output_rowset->num_rows();

        // ATTN(cmy): We found that the num_rows in some rowset meta may be set to the wrong value,
        // but it is not known which version of the code has the problem. So when the compaction
        // result is inconsistent, we then try to verify by num_rows recorded in segment_groups.
        // If the check passes, ignore the error and set the correct value in the output rowset meta
        // to fix this problem.
        // Only handle alpha rowset because we only find this bug in alpha rowset
        int64_t num_rows = _get_input_num_rows_from_seg_grps(); //从segment group中读取 alpha类型的rowset的行数
        if (num_rows == -1) {
            return OLAP_ERR_CHECK_LINES_ERROR;
        }
        if (num_rows != _output_rowset->num_rows() + stats.merged_rows + stats.filtered_rows) {
            // If it is still incorrect, it may be another problem
            LOG(WARNING) << "row_num got from seg groups does not match between cumulative input and output! "
                << "input_row_num=" << num_rows
                << ", merged_row_num=" << stats.merged_rows
                << ", filted_row_num=" << stats.filtered_rows
                << ", output_row_num=" << _output_rowset->num_rows();

            return OLAP_ERR_CHECK_LINES_ERROR;
        }
    }
    return OLAP_SUCCESS;
}

/*从segment group中读取 alpha类型的rowset的行数*/
int64_t Compaction::_get_input_num_rows_from_seg_grps() {
    int64_t num_rows = 0;
    for (auto& rowset : _input_rowsets) {
        if (rowset->rowset_meta()->rowset_type() != RowsetTypePB::ALPHA_ROWSET) {
            return -1;
        }
        for (auto& seg_grp : rowset->rowset_meta()->alpha_rowset_extra_meta_pb().segment_groups()) {
            num_rows += seg_grp.num_rows();
        }
    }
    return num_rows;
}

}  // namespace doris
