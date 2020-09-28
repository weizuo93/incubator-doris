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

#include "olap/cumulative_compaction.h"
#include "util/doris_metrics.h"
#include "util/time.h"
#include "util/trace.h"

namespace doris {

CumulativeCompaction::CumulativeCompaction(TabletSharedPtr tablet)
    : Compaction(tablet),
      _cumulative_rowset_size_threshold(config::cumulative_compaction_budgeted_bytes)
{ }

CumulativeCompaction::~CumulativeCompaction() { }

/*执行cumulative compaction*/
OLAPStatus CumulativeCompaction::compact() {
    if (!_tablet->init_succeeded()) {
        return OLAP_ERR_CUMULATIVE_INVALID_PARAMETERS;
    }

    MutexLock lock(_tablet->get_cumulative_lock(), TRY_LOCK);
    if (!lock.own_lock()) {
        LOG(INFO) << "The tablet is under cumulative compaction. tablet=" << _tablet->full_name();
        return OLAP_ERR_CE_TRY_CE_LOCK_ERROR;
    }
    TRACE("got cumulative compaction lock");

    // 1.calculate cumulative point 
    _tablet->calculate_cumulative_point();//计算tablet中的cumulative point
    TRACE("calculated cumulative point");

    // 2. pick rowsets to compact
    RETURN_NOT_OK(pick_rowsets_to_compact());//选取需要合并的rowset，保存在向量_input_rowsets中
    TRACE("rowsets picked");
    TRACE_COUNTER_INCREMENT("input_rowsets_count", _input_rowsets.size());

    // 3. do cumulative compaction, merge rowsets
    RETURN_NOT_OK(do_compaction());//调用父类Compaction中定义的函数do_compaction()来执行cumulative compaction(其中会通过信号量控制并发执行的线程数)
    TRACE("compaction finished");

    // 4. set state to success
    _state = CompactionState::SUCCESS;//设置compaction状态

    // 5. set cumulative point
    //cumulative compaction之后更新当前tablet的cumulative point值为最后一个候选rowset的end_version+1
    _tablet->set_cumulative_layer_point(_input_rowsets.back()->end_version() + 1);//更新当前tablet的cumulative point值
    
    // 6. add metric to cumulative compaction
    DorisMetrics::instance()->cumulative_compaction_deltas_total.increment(_input_rowsets.size());
    DorisMetrics::instance()->cumulative_compaction_bytes_total.increment(_input_rowsets_size);
    TRACE("save cumulative compaction metrics");

    // 7. garbage collect input rowsets after cumulative compaction 
    RETURN_NOT_OK(gc_unused_rowsets());//cumulative compaction结束之后，回收不可用的rowset（compaction时的那些候选rowset）
    TRACE("unused rowsets have been moved to GC queue");

    return OLAP_SUCCESS;
}

/*选取需要合并的候选rowset*/
OLAPStatus CumulativeCompaction::pick_rowsets_to_compact() {
    std::vector<RowsetSharedPtr> candidate_rowsets;
    //选择tablet中_cumulative_point之后，同时满足创建时间距离当前时间大于某一个时间间隔（skip_window_sec）的所有rowset作为cumulative compaction的候选rowset
    _tablet->pick_candicate_rowsets_to_cumulative_compaction(config::cumulative_compaction_skip_window_seconds, &candidate_rowsets);

    if (candidate_rowsets.empty()) {
        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    std::sort(candidate_rowsets.begin(), candidate_rowsets.end(), Rowset::comparator);//对所有的候选rowset进行排序
    RETURN_NOT_OK(check_version_continuity(candidate_rowsets));//检查候选rowset的版本连续性

    std::vector<RowsetSharedPtr> transient_rowsets;//用来保存需要执行cumulative compactiom的候选rowset
    size_t compaction_score = 0;
    // the last delete version we meet when traversing candidate_rowsets
    Version last_delete_version { -1, -1 };

    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        //判断当前rowset是否已经被删除
        if (_tablet->version_for_delete_predicate(rowset->version())) {
            last_delete_version = rowset->version();
            // 当前rowset已经被删除，并且该删除版本之前存在rowset，需要执行cumulative compactiom，循环退出
            if (!transient_rowsets.empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                _input_rowsets = transient_rowsets; //transient_rowsets中的rowset就是需要执行cumulative compactiom的rowset
                break;
            }
            // transient_rowsets为空，表示在该删除版本之前不存在其他的rowset，直接跳过该删除版本的rowset
            // we meet a delete version, and no other versions before, skip it and continue
            transient_rowsets.clear(); //清空transient_rowsets
            compaction_score = 0;
            continue;
        }

        //当前compaction score到达config::max_cumulative_compaction_num_singleton_deltas（默认为1000），则需要执行cumulative compactiom
        if (compaction_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            // got enough segments
            break;
        }

        //该版本的rowset没有被删除，并且当前的compaction score没有到达阈值（1000）
        compaction_score += rowset->rowset_meta()->get_compaction_score();//修改compaction_score，增加当前rowset的compaction score
        transient_rowsets.push_back(rowset); //将当前的候选rowset添加到transient_rowsets中
    }

    // if we have a sufficient number of segments,
    // or have other versions before encountering the delete version, we should process the compaction.
    //前面for循环退出时，如果compaction score到达阈值，或存在rowset已经被删除并且被删除的rowset之前存在候选rowset，则需要对transient_rowsets中的rowset执行cumulative compaction
    if (compaction_score >= config::min_cumulative_compaction_num_singleton_deltas || (last_delete_version.first != -1 && !transient_rowsets.empty())) {
        _input_rowsets = transient_rowsets;
    }

    // Cumulative compaction will process with at least 1 rowset.
    // So when there is no rowset being chosen, we should return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS:
    if (_input_rowsets.empty()) {
        if (last_delete_version.first != -1) { // 存在rowset已经被删除
            // we meet a delete version, should increase the cumulative point to let base compaction handle the delete version.
            // plus 1 to skip the delete version.
            // NOTICE: after that, the cumulative point may be larger than max version of this tablet, but it doen't matter.
            // 存在rowset已经被删除，并且被删除的rowset之前不存在其他的候选rowset，则需要更新cumulative point
            _tablet->set_cumulative_layer_point(last_delete_version.first + 1);//更新cumulative point
            return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
        }

        // we did not meet any delete version. which means compaction_score is not enough to do cumulative compaction.
        // We should wait until there are more rowsets to come, and keep the cumulative point unchanged.
        // But in order to avoid the stall(拖延) of compaction because no new rowset arrives later, we should increase
        // the cumulative point after waiting for a long time, to ensure that the base compaction can continue.

        // check both last success time of base and cumulative compaction
        // 候选rowset中没有被删除的rowset，并且所有候选rowset的compaction score没有达到做compaction的阈值
        int64_t now = UnixMillis();
        int64_t last_cumu = _tablet->last_cumu_compaction_success_time();
        int64_t last_base = _tablet->last_base_compaction_success_time();
        if (last_cumu != 0 || last_base != 0) {
            int64_t interval_threshold = config::base_compaction_interval_seconds_since_last_operation * 1000;
            int64_t cumu_interval = now - last_cumu;
            int64_t base_interval = now - last_base;
            // 如果cumulative compaction和base compaction距离上次成功执行均超过特定的时间间隔，则需要执行此次cumulative compaction，尽管compaction score没有达到阈值
            if (cumu_interval > interval_threshold && base_interval > interval_threshold) {
                // before increasing cumulative point, we should make sure all rowsets are non-overlapping.
                // if at least one rowset is overlapping, we should compact them first.
                CHECK(candidate_rowsets.size() == transient_rowsets.size())
                    << "tablet: " << _tablet->full_name() << ", "<<  candidate_rowsets.size() << " vs. " << transient_rowsets.size();
                for (auto& rs : candidate_rowsets) {
                    if (rs->rowset_meta()->is_segments_overlapping()) {
                        _input_rowsets = candidate_rowsets; //将所有候选rowset执行cumulative compaction
                        return OLAP_SUCCESS;
                    }
                }

                // all candicate rowsets are non-overlapping, increase the cumulative point
                _tablet->set_cumulative_layer_point(candidate_rowsets.back()->start_version() + 1);//更新cumulative point
            }
        } else {
            // 使用当前时间初始化compaction成功的时间  init the compaction success time for first time
            if (last_cumu == 0) {
                _tablet->set_last_cumu_compaction_success_time(now);
            }

            if (last_base == 0) {
                _tablet->set_last_base_compaction_success_time(now);
            }
        }

        return OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS;
    }

    return OLAP_SUCCESS;
}

}  // namespace doris

