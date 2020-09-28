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

#include "olap/base_compaction.h"
#include "util/doris_metrics.h"
#include "util/trace.h"

namespace doris {

BaseCompaction::BaseCompaction(TabletSharedPtr tablet)
    : Compaction(tablet)
{ }

BaseCompaction::~BaseCompaction() { }

/*执行base compaction*/
OLAPStatus BaseCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    MutexLock lock(_tablet->get_base_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(WARNING) << "another base compaction is running. tablet=" << _tablet->full_name();
        return OLAP_ERR_BE_TRY_BE_LOCK_ERROR;
    }
    TRACE("got base compaction lock");

    // 1. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());//选取需要合并的rowset，保存在向量_input_rowsets中。函数返回OLAP_SUCCESS才会继续执行base compaction，否则停止base compaction的执行
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());

    // 2. do base compaction, merge rowsets
    RETURN_NOT_OK(do_compaction()); //调用父类Compaction中定义的函数do_compaction()来执行base compaction(其中会通过信号量控制并发执行的线程数)
    TRACE("compaction finished");

    // 3. set state to success
    _state = CompactionState::SUCCESS;//设置compaction状态

    // 4. add metric to base compaction
    DorisMetrics::instance()->base_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::instance()->base_compaction_bytes_total.increment(_input_rowsets_size);
    TRACE("save base compaction metrics");

    // 5. garbage collect input rowsets after base compaction 
    RETURN_NOT_OK(gc_unused_rowsets());//base compaction结束之后，回收不可用的rowset（compaction时的那些候选rowset）
    TRACE("unused rowsets have been moved to GC queue");

    return OLAP_SUCCESS;
}

/*选取需要合并的候选rowset*/
OLAPStatus BaseCompaction::pick_rowsets_to_compact() {
    _input_rowsets.clear();//清空向量_input_rowsets
    _tablet->pick_candicate_rowsets_to_base_compaction(&_input_rowsets);//选择tablet中_cumulative_point之前的所有rowset作为base compaction的候选rowset
    if (_input_rowsets.size() <= 1) {
        return OLAP_ERR_BE_NO_SUITABLE_VERSION;
    }

    std::sort(_input_rowsets.begin(), _input_rowsets.end(), Rowset::comparator);//对所有的候选rowset进行排序
    RETURN_NOT_OK(check_version_continuity(_input_rowsets));//检查候选rowset的版本连续性
    RETURN_NOT_OK(_check_rowset_overlapping(_input_rowsets));//检查所有的候选rowset中是否有重叠

    if (_input_rowsets.size() == 2 && _input_rowsets[0]->end_version() == 1) {
        // the tablet is with rowset: [0-1], [2-y]
        // and [0-1] has no data. in this situation, no need to do base compaction.
        return OLAP_ERR_BE_NO_SUITABLE_VERSION;
    }

    // 1. cumulative rowset must reach base_compaction_num_cumulative_deltas threshold
    //当候选的rowset数量超过config::base_compaction_num_cumulative_deltas（默认为5）时，函数直接返回OLAP_SUCCESS，执行base compaction
    if (_input_rowsets.size() > config::base_compaction_num_cumulative_deltas) {
        LOG(INFO) << "satisfy the base compaction policy. tablet="<< _tablet->full_name()
                  << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
                  << ", base_compaction_num_cumulative_rowsets=" << config::base_compaction_num_cumulative_deltas;
        return OLAP_SUCCESS;
    }

    // 2. the ratio between base rowset and all input cumulative rowsets reachs the threshold
    //在所有候选rowset中，当其他候选rowset与base rowset（start_version为0）的大小比率超过config::base_cumulative_delta_ratio（默认为0.3）时，函数直接返回OLAP_SUCCESS，执行base compaction
    int64_t base_size = 0;
    int64_t cumulative_total_size = 0;
    for (auto& rowset : _input_rowsets) {
        if (rowset->start_version() != 0) {
            cumulative_total_size += rowset->data_disk_size();//计算除base rowset之外的其他候选rowset的大小之和
        } else {
            base_size = rowset->data_disk_size();//获取base rowset的大小
        }
    }

    double base_cumulative_delta_ratio = config::base_cumulative_delta_ratio;
    if (base_size == 0) {
        // base_size == 0 means this may be a base version [0-1], which has no data.
        // set to 1 to void devide by zero
        base_size = 1;
    }
    double cumulative_base_ratio = static_cast<double>(cumulative_total_size) / base_size;

    if (cumulative_base_ratio > base_cumulative_delta_ratio) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", cumualtive_total_size=" << cumulative_total_size
                  << ", base_size=" << base_size
                  << ", cumulative_base_ratio=" << cumulative_base_ratio
                  << ", policy_ratio=" << base_cumulative_delta_ratio;
        return OLAP_SUCCESS;
    }

    // 3. the interval since last base compaction reachs the threshold
    //如果当前时间距离上次执行base compaction的时间(即base compaction的创建时间)间隔超过config::base_compaction_interval_seconds_since_last_operation（默认为1天，即86400秒），函数直接返回OLAP_SUCCESS，执行base compaction
    int64_t base_creation_time = _input_rowsets[0]->creation_time();//获取base rowset的创建时间
    int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation;
    int64_t interval_since_last_base_compaction = time(NULL) - base_creation_time;
    if (interval_since_last_base_compaction > interval_threshold) {
        LOG(INFO) << "satisfy the base compaction policy. tablet=" << _tablet->full_name()
                  << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction 
                   << ", interval_threshold=" << interval_threshold;
        return OLAP_SUCCESS;
    }

    //满足以上3种情况之一，就可以执行base compaction的，其余情况下函数返回OLAP_ERR_BE_NO_SUITABLE_VERSION，不会继续执行base compaction
    LOG(INFO) << "don't satisfy the base compaction policy. tablet=" << _tablet->full_name()
              << ", num_cumulative_rowsets=" << _input_rowsets.size() - 1
              << ", cumulative_base_ratio=" << cumulative_base_ratio
              << ", interval_since_last_base_compaction=" << interval_since_last_base_compaction;
    return OLAP_ERR_BE_NO_SUITABLE_VERSION;
}

/*检查rowset是否重叠*/
OLAPStatus BaseCompaction::_check_rowset_overlapping(const vector<RowsetSharedPtr>& rowsets) {
    for (auto& rs : rowsets) {
        if (rs->rowset_meta()->is_segments_overlapping()) {
            return OLAP_ERR_BE_SEGMENTS_OVERLAPPING;
        }
    }
    return OLAP_SUCCESS;
}

}  // namespace doris
