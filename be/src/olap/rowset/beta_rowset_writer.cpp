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

#include "olap/rowset/beta_rowset_writer.h"

#include <ctime> // time

#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "olap/olap_define.h"
#include "olap/fs/fs_util.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/row.h" // ContiguousRow
#include "olap/row_cursor.h" // RowCursor
#include "olap/storage_engine.h"
#include "runtime/exec_env.h"

namespace doris {

// TODO(lingbin): Should be a conf that can be dynamically adjusted, or a member in the context
const uint32_t MAX_SEGMENT_SIZE = static_cast<uint32_t>(
        OLAP_MAX_COLUMN_SEGMENT_FILE_SIZE * OLAP_COLUMN_FILE_SEGMENT_SIZE_SCALE);

BetaRowsetWriter::BetaRowsetWriter() :
        _rowset_meta(nullptr),
        _num_segment(0),
        _segment_writer(nullptr),
        _num_rows_written(0),
        _total_data_size(0),
        _total_index_size(0) {}

BetaRowsetWriter::~BetaRowsetWriter() {
    // TODO(lingbin): Should wrapper exception logic, no need to know file ops directly.
    if (!_already_built) { // abnormal exit, remove all files generated
        _segment_writer.reset(); // ensure all files are closed
        Status st;
        for (int i = 0; i < _num_segment; ++i) {
            auto path = BetaRowset::segment_file_path(
                    _context.rowset_path_prefix, _context.rowset_id, i);
            // Even if an error is encountered, these files that have not been cleaned up
            // will be cleaned up by the GC background. So here we only print the error
            // message when we encounter an error.
            WARN_IF_ERROR(Env::Default()->delete_file(path),
                          strings::Substitute("Failed to delete file=$0", path));
        }
    }
}

/*初始化BetaRowsetWriter对象*/
OLAPStatus BetaRowsetWriter::init(const RowsetWriterContext& rowset_writer_context) {
    _context = rowset_writer_context;
    _rowset_meta.reset(new RowsetMeta);
    _rowset_meta->set_rowset_id(_context.rowset_id);
    _rowset_meta->set_partition_id(_context.partition_id);
    _rowset_meta->set_tablet_id(_context.tablet_id);
    _rowset_meta->set_tablet_schema_hash(_context.tablet_schema_hash);
    _rowset_meta->set_rowset_type(_context.rowset_type);
    _rowset_meta->set_rowset_state(_context.rowset_state);
    _rowset_meta->set_segments_overlap(_context.segments_overlap);
    if (_context.rowset_state == PREPARED || _context.rowset_state == COMMITTED) {
        _is_pending = true;
        _rowset_meta->set_txn_id(_context.txn_id);
        _rowset_meta->set_load_id(_context.load_id);
    } else {
        _rowset_meta->set_version(_context.version);
        _rowset_meta->set_version_hash(_context.version_hash);
    }
    _rowset_meta->set_tablet_uid(_context.tablet_uid);

    return OLAP_SUCCESS;
}

/*将memtable中的一行数据添加到segment中*/
template<typename RowType>
OLAPStatus BetaRowsetWriter::_add_row(const RowType& row) {
    if (PREDICT_FALSE(_segment_writer == nullptr)) {
        RETURN_NOT_OK(_create_segment_writer()); //创建一个SegmentWriter对象用来初始化成员变量_segment_writer
    }
    // TODO update rowset's zonemap
    auto s = _segment_writer->append_row(row); //向_segment_writer中添加一行数据
    if (PREDICT_FALSE(!s.ok())) {
        LOG(WARNING) << "failed to append row: " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    // 当前segment的大小到达某个设定的阈值或segment中的行数到达某个设定的阈值，则执行segment flush,因此，一个memtable可能会刷写出多个segment文件
    if (PREDICT_FALSE(_segment_writer->estimate_segment_size() >= MAX_SEGMENT_SIZE
            || _segment_writer->num_rows_written() >= _context.max_rows_per_segment)) {
        RETURN_NOT_OK(_flush_segment_writer()); //刷写一个segment文件
    }
    ++_num_rows_written; //写入行数增1
    return OLAP_SUCCESS;
}

template OLAPStatus BetaRowsetWriter::_add_row(const RowCursor& row);
template OLAPStatus BetaRowsetWriter::_add_row(const ContiguousRow& row);

OLAPStatus BetaRowsetWriter::add_rowset(RowsetSharedPtr rowset) {
    assert(rowset->rowset_meta()->rowset_type() == BETA_ROWSET);
    RETURN_NOT_OK(rowset->link_files_to(_context.rowset_path_prefix, _context.rowset_id));
    _num_rows_written += rowset->num_rows();
    _total_data_size += rowset->rowset_meta()->data_disk_size();
    _total_index_size += rowset->rowset_meta()->index_disk_size();
    _num_segment += rowset->num_segments();
    // TODO update zonemap
    if (rowset->rowset_meta()->has_delete_predicate()) {
        _rowset_meta->set_delete_predicate(rowset->rowset_meta()->delete_predicate());
    }
    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetWriter::add_rowset_for_linked_schema_change(RowsetSharedPtr rowset,
                                                                 const SchemaMapping& schema_mapping) {
    // TODO use schema_mapping to transfer zonemap
    return add_rowset(rowset);
}

/*刷写segment文件*/
OLAPStatus BetaRowsetWriter::flush() {
    if (_segment_writer != nullptr) {
        RETURN_NOT_OK(_flush_segment_writer()); //刷写segment文件
    }
    return OLAP_SUCCESS;
}

/*更新rowset的meta信息*/
RowsetSharedPtr BetaRowsetWriter::build() {
    // TODO(lingbin): move to more better place, or in a CreateBlockBatch?
    for (auto& wblock : _wblocks) {
        wblock->close();
    }
    // When building a rowset, we must ensure that the current _segment_writer has been
    // flushed, that is, the current _segment_wirter is nullptr
    DCHECK(_segment_writer == nullptr) << "segment must be null when build rowset";
    _rowset_meta->set_num_rows(_num_rows_written);       // 设置rowset中的行数
    _rowset_meta->set_total_disk_size(_total_data_size); // 设置rowset的数据大小
    _rowset_meta->set_data_disk_size(_total_data_size);
    _rowset_meta->set_index_disk_size(_total_index_size);
    // TODO write zonemap to meta
    _rowset_meta->set_empty(_num_rows_written == 0);
    _rowset_meta->set_creation_time(time(nullptr));
    _rowset_meta->set_num_segments(_num_segment);         // 设置rowset中的segment文件数目
    if (_num_segment <= 1) {
        _rowset_meta->set_segments_overlap(NONOVERLAPPING); // 如果文件数目小于或等于1，则segment之间是NONOVERLAPPING，否则是OVERLAPPING
    }
    if (_is_pending) {
        _rowset_meta->set_rowset_state(COMMITTED); // 设置rowset的状态为COMMITTED
    } else {
        _rowset_meta->set_rowset_state(VISIBLE);   // 设置rowset的状态为VISIBLE
    }

    RowsetSharedPtr rowset;
    auto status = RowsetFactory::create_rowset(_context.tablet_schema,
                                               _context.rowset_path_prefix,
                                               _rowset_meta,
                                               &rowset); // 根据rowset meta创建一个rowset，通过参数rowset传回
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "rowset init failed when build new rowset, res=" << status;
        return nullptr;
    }
    _already_built = true;
    return rowset;
}

/*创建SegmentWriter对象*/
OLAPStatus BetaRowsetWriter::_create_segment_writer() {
    auto path = BetaRowset::segment_file_path(_context.rowset_path_prefix,
                                              _context.rowset_id,
                                              _num_segment);
    // TODO(lingbin): should use a more general way to get BlockManager object
    // and tablets with the same type should share one BlockManager object;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(); //获取文件块管理器block_mgr
    std::unique_ptr<fs::WritableBlock> wblock;
    fs::CreateBlockOptions opts({path});
    DCHECK(block_mgr != nullptr);
    Status st = block_mgr->create_block(opts, &wblock); //通过block_mgr创建一个可写入的文件块wblock，每一个文件块对应一个segment文件
    if (!st.ok()) {
        LOG(WARNING) << "failed to create writable block. path=" << path;
        return OLAP_ERR_INIT_FAILED;
    }

    DCHECK(wblock != nullptr);
    segment_v2::SegmentWriterOptions writer_options;
    _segment_writer.reset(new segment_v2::SegmentWriter(
            wblock.get(), _num_segment, _context.tablet_schema, writer_options)); //针对新创建的文件块wblock创建一个SegmentWriter对象，并让成员变量_segment_writer指向它
    _wblocks.push_back(std::move(wblock)); //将新创建的文件块添加到vector类型的成员变量_wblocks中进行维护
    // TODO set write_mbytes_per_sec based on writer type (load/base compaction/cumulative compaction)
    auto s = _segment_writer->init(config::push_write_mbytes_per_sec); //初始化成员变量_segment_writer
    if (!s.ok()) {
        LOG(WARNING) << "failed to init segment writer: " << s.to_string();
        _segment_writer.reset(nullptr);
        return OLAP_ERR_INIT_FAILED;
    }
    ++_num_segment; //当前rowset中的segment数目增1
    return OLAP_SUCCESS;
}

/*执行segment flush，通过segment writer向segment文件中刷写数据和索引*/
OLAPStatus BetaRowsetWriter::_flush_segment_writer() {
    uint64_t segment_size;
    uint64_t index_size;
    Status s = _segment_writer->finalize(&segment_size, &index_size);//通过segment writer向segment文件中刷写数据和索引，并获取segment文件大小和索引数据大小
    if (!s.ok()) {
        LOG(WARNING) << "failed to finalize segment: " << s.to_string();
        return OLAP_ERR_WRITER_DATA_WRITE_ERROR;
    }
    _total_data_size += segment_size;
    _total_index_size += index_size;
    _segment_writer.reset(); //segment文件刷写完成，智能指针unique_ptr包装的_segment_writer置为空指针
    return OLAP_SUCCESS;
}

} // namespace doris
