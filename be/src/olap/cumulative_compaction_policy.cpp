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

#include "olap/cumulative_compaction_policy.h"

#include <boost/algorithm/string.hpp>
#include <string>

#include "util/time.h"

namespace doris {

/*SizeBasedCumulativeCompactionPolicy的构造函数*/
SizeBasedCumulativeCompactionPolicy::SizeBasedCumulativeCompactionPolicy(
        int64_t size_based_promotion_size, double size_based_promotion_ratio,
        int64_t size_based_promotion_min_size, int64_t size_based_compaction_lower_bound_size)
        : CumulativeCompactionPolicy(),
          _size_based_promotion_size(size_based_promotion_size),
          _size_based_promotion_ratio(size_based_promotion_ratio),
          _size_based_promotion_min_size(size_based_promotion_min_size),
          _size_based_compaction_lower_bound_size(size_based_compaction_lower_bound_size) {
    // init _levels by divide 2 between size_based_promotion_size and size_based_compaction_lower_bound_size
    int64_t i_size = size_based_promotion_size / 2; // 最高level等级为size_based_promotion_size/2 （默认设置为 1024MB / 2）

    while (i_size >= size_based_compaction_lower_bound_size) {
        _levels.push_back(i_size); // 从最高level等级开始，大小减半为下一个level等级，直到大小小于size_based_compaction_lower_bound_size （默认设置为 64MB）
        i_size /= 2;
    }
}

/*计算tablet的cumulative point*/
void SizeBasedCumulativeCompactionPolicy::calculate_cumulative_point(
        Tablet* tablet, const std::vector<RowsetMetaSharedPtr>& all_metas,
        int64_t current_cumulative_point, int64_t* ret_cumulative_point) {
    *ret_cumulative_point = Tablet::K_INVALID_CUMULATIVE_POINT;
    if (current_cumulative_point != Tablet::K_INVALID_CUMULATIVE_POINT) {
        // only calculate the point once.
        // after that, cumulative point will be updated along with compaction process.
        return;
    }
    // empty return
    if (all_metas.empty()) {
        return;
    }

    std::list<RowsetMetaSharedPtr> existing_rss;
    for (auto& rs : all_metas) { // 遍历tablet下所有的rowset meta
        existing_rss.emplace_back(rs);
    }

    // sort the existing rowsets by version in ascending order
    existing_rss.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // simple because 2 versions are certainly not overlapping
        return a->version().first < b->version().first;
    }); // 按照版本对rowset进行排序

    // calculate promotion size
    auto base_rowset_meta = existing_rss.begin(); // 获取base rowset（即version.first为0的rowset）
    // check base rowset first version must be zero
    CHECK((*base_rowset_meta)->start_version() == 0);

    int64_t promotion_size = 0;
    _calc_promotion_size(*base_rowset_meta, &promotion_size); // 根据base rowset的大小计算promotion_size

    int64_t prev_version = -1;
    for (const RowsetMetaSharedPtr& rs : existing_rss) { // 依次遍历tablet下的每一个rowset
        if (rs->version().first > prev_version + 1) { // 判断是否存在版本缺失
            // There is a hole, do not continue
            break;
        }

        bool is_delete = tablet->version_for_delete_predicate(rs->version()); // 判断当前rowset是否为已经删除的版本

        // break the loop if segments in this rowset is overlapping, or is a singleton.
        if (!is_delete && (rs->is_segments_overlapping() || rs->is_singleton_delta())) { // 当前rowset含有多个segment文件或没有发生过版本合并（当前rowset不是数据删除的版本并且）
            *ret_cumulative_point = rs->version().first; // cumulative point不再后移，本次计算的cumulative point为当前版本的first
            break;
        }

        // check the rowset is whether less than promotion size
        if (!is_delete && rs->version().first != 0 && rs->total_disk_size() < promotion_size) { // 当前rowset不是base rowset并且rowset大小小于promotion_size（当前rowset不是数据删除的版本）
            *ret_cumulative_point = rs->version().first; // cumulative point不再后移，本次计算的cumulative point为当前版本的first
            break;
        }

        prev_version = rs->version().second;
        *ret_cumulative_point = prev_version + 1;
    }
    VLOG_NOTICE << "cumulative compaction size_based policy, calculate cumulative point value = "
            << *ret_cumulative_point << ", calc promotion size value = " << promotion_size
            << " tablet = " << tablet->full_name();
}

/*根据base rowset计算promotion_size*/
void SizeBasedCumulativeCompactionPolicy::_calc_promotion_size(RowsetMetaSharedPtr base_rowset_meta,
                                                               int64_t* promotion_size) {
    int64_t base_size = base_rowset_meta->total_disk_size(); // 获取base rowset的大小
    *promotion_size = base_size * _size_based_promotion_ratio; // 根据base rowset计算promotion_size（_size_based_promotion_ratio的默认配置为0.05）

    // promotion_size is between _size_based_promotion_size and _size_based_promotion_min_size
    // promotion_size需要设置在_size_based_promotion_size到_size_based_promotion_min_size之间
    if (*promotion_size >= _size_based_promotion_size) {
        *promotion_size = _size_based_promotion_size; // _size_based_promotion_size的默认值为1024MB
    } else if (*promotion_size <= _size_based_promotion_min_size) {
        *promotion_size = _size_based_promotion_min_size; // _size_based_promotion_min_size默认值为64MB
    }
    _refresh_tablet_size_based_promotion_size(*promotion_size); // 更新成员变量_tablet_size_based_promotion_size
}

/*根据参数传入的promotion_size更新成员变量_tablet_size_based_promotion_size*/
void SizeBasedCumulativeCompactionPolicy::_refresh_tablet_size_based_promotion_size(
        int64_t promotion_size) {
    _tablet_size_based_promotion_size = promotion_size;
}

/*更新tablet的cumulative point*/
void SizeBasedCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr output_rowset, Version& last_delete_version) {
    // if rowsets have delete version, move to the last directly
    if (last_delete_version.first != -1) {
        tablet->set_cumulative_layer_point(output_rowset->end_version() + 1); // 如果存在删除数据的版本，则更新cumulative point
    } else {
        // if rowsets have not delete version, check output_rowset total disk size
        // satisfies promotion size.
        size_t total_size = output_rowset->rowset_meta()->total_disk_size(); // 如果不存在删除数据的版本，获取输出rowset的大小
        if (total_size >= _tablet_size_based_promotion_size) { // 如果输出rowset大小超过_tablet_size_based_promotion_size，则更新cumulative point
            tablet->set_cumulative_layer_point(output_rowset->end_version() + 1);
        }
    }
}

/*计算cumulative compaction score*/
void SizeBasedCumulativeCompactionPolicy::calc_cumulative_compaction_score(
        const std::vector<RowsetMetaSharedPtr>& all_metas, int64_t current_cumulative_point,
        uint32_t* score) {
    bool base_rowset_exist = false;
    const int64_t point = current_cumulative_point;
    int64_t promotion_size = 0;

    std::vector<RowsetMetaSharedPtr> rowset_to_compact;
    int64_t total_size = 0;

    // check the base rowset and collect the rowsets of cumulative part
    auto rs_meta_iter = all_metas.begin();
    for (; rs_meta_iter != all_metas.end(); rs_meta_iter++) { // 从第一个rowset开始，遍历tablet下的每一个rowset
        auto rs_meta = *rs_meta_iter;
        // check base rowset
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
            _calc_promotion_size(rs_meta, &promotion_size); // 根据base rowset计算promotion_size
        }
        if (rs_meta->end_version() < point) { // rowset没有排序，因此需要逐个判断
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        } else {
            // collect the rowsets of cumulative part
            total_size += rs_meta->total_disk_size();  // 获取rowset的大小
            *score += rs_meta->get_compaction_score(); // 获取rowset的score
            rowset_to_compact.push_back(rs_meta);      // 保存当前rowset
        }
    }

    // If base version does not exist, it may be that tablet is doing alter table.
    // Do not select it and set *score = 0
    if (!base_rowset_exist) { // base rowset不存在，当前tablet可能正在进行alter操作，score设置为0
        *score = 0;
        return;
    }

    // if total_size is greater than promotion_size, return total score
    if (total_size >= promotion_size) { // 如果cumulative point之后的rowset的大小大于promotion_size，返回的score为这些rowset的score之和
        return;
    }

    // sort the rowsets of cumulative part
    std::sort(rowset_to_compact.begin(), rowset_to_compact.end(), RowsetMeta::comparator); // 按照版本大小对cumulative point之后的rowset进行排序

    // calculate the rowsets to do cumulative compaction
    for (auto& rs_meta : rowset_to_compact) {
        int current_level = _level_size(rs_meta->total_disk_size()); // 计算当前rowset的大小的等级
        int remain_level = _level_size(total_size - rs_meta->total_disk_size()); // 计算cumulative point之后，除当前rowset之外，其他rowset大小之和的等级
        // if current level less then remain level, score contains current rowset
        // and process return; otherwise, score does not contains current rowset.
        if (current_level <= remain_level) { // 如果当前rowset的等级小于其他rowset大小之和的等级，则compaction score包含当前rowset的score
            return;
        }
        total_size -= rs_meta->total_disk_size();
        *score -= rs_meta->get_compaction_score();
    }
}

/*选择执行cumulative compaction的rowset*/
int SizeBasedCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score) {
    size_t promotion_size = _tablet_size_based_promotion_size;
    int transient_size = 0;
    *compaction_score = 0;
    int64_t total_size = 0;
    for (size_t i = 0; i < candidate_rowsets.size(); ++i) { // 依次遍历每一个候选rowset（循环退出的三种情况：遇到删除版本、选出的rowset的score之和到达上限或候选rowset遍历结束）
        RowsetSharedPtr rowset = candidate_rowsets[i];
        // check whether this rowset is delete version
        if (tablet->version_for_delete_predicate(rowset->version())) { // 判断当前rowset是否为数据删除版本
            *last_delete_version = rowset->version(); // 记录上一个数据删除版本
            if (!input_rowsets->empty()) { // 当前数据删除版本之前还有其他版本，则对当前数据删除版本之前的其他rowset执行cumulative compaction
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                break;
            } else {                       // 当前数据删除版本之前没有其他版本，则跳过当前数据删除版本
                // we meet a delete version, and no other versions before, skip it and continue
                input_rowsets->clear();
                *compaction_score = 0;
                transient_size = 0;
                continue;
            }
        }
        if (*compaction_score >= max_compaction_score) { // 如果score已经到达上限(config::max_cumulative_compaction_num_singleton_deltas默认设置为1000)
            // got enough segments
            break;
        }
        *compaction_score += rowset->rowset_meta()->get_compaction_score(); // 获取当前rowset的score
        total_size += rowset->rowset_meta()->total_disk_size();             // 获取当前rowset的大小

        transient_size += 1;
        input_rowsets->push_back(rowset); // 将当前rowset添加到input_rowsets中
    }

    if (total_size >= promotion_size) { // 如果选出的所有rowset大小之和达到promotion_size，函数退出
        return transient_size;
    }

    // 选出的所有rowset大小之和未达到promotion_size
    // if there is delete version, do compaction directly
    if (last_delete_version->first != -1) { // 如果存在删除数据的版本（因为遇到删除的数据版本而导致遍历候选rowset结束）
        if (input_rowsets->size() == 1) { // 判断是否只选出了一个rowset
            auto rs_meta = input_rowsets->front()->rowset_meta();
            // if there is only one rowset and not overlapping,
            // we do not need to do cumulative compaction
            if (!rs_meta->is_segments_overlapping()) { // 选出的rowset是非overlapping的，则不需要做cumulative compaction
                input_rowsets->clear();
                *compaction_score = 0;
            }
        }
        return transient_size;
    }

    // 不存在删除数据的版本
    auto rs_iter = input_rowsets->begin();
    while (rs_iter != input_rowsets->end()) { // 依次遍历每一个选出的rowset
        auto rs_meta = (*rs_iter)->rowset_meta();
        int current_level = _level_size(rs_meta->total_disk_size()); // 计算当前rowset的大小的等级
        int remain_level = _level_size(total_size - rs_meta->total_disk_size()); // 计算input_rowsets中其他rowset的大小之和的等级
        // if current level less then remain level, input rowsets contain current rowset
        // and process return; otherwise, input rowsets do not contain current rowset.
        if (current_level <= remain_level) { // 如果当前rowset的等级小于input_rowsets中其他rowset大小之和的等级，则input_rowsets中包含当前rowset
            break;
        }
        // 当前rowset的等级大于input_rowsets中其他rowset大小之和的等级，则从input_rowsets中删除当前rowset
        total_size -= rs_meta->total_disk_size();
        *compaction_score -= rs_meta->get_compaction_score();

        rs_iter = input_rowsets->erase(rs_iter); // 从input_rowsets中删除当前rowset
    }

    VLOG_CRITICAL << "cumulative compaction size_based policy, compaction_score = " << *compaction_score
            << ", total_size = " << total_size << ", calc promotion size value = " << promotion_size
            << ", tablet = " << tablet->full_name() << ", input_rowset size "
            << input_rowsets->size();

    // empty return
    if (input_rowsets->empty()) { // 判断input_rowsets是否为空
        return transient_size;
    }

    // if we have a sufficient number of segments, we should process the compaction.
    // otherwise, we check number of segments and total_size whether can do compaction.
    if (total_size < _size_based_compaction_lower_bound_size && // 判断input_rowsets中选出的rowset大小（64MB）和数量（5）是否同时小于最小阈值
        *compaction_score < min_compaction_score) {
        input_rowsets->clear();
        *compaction_score = 0;
    } else if (total_size >= _size_based_compaction_lower_bound_size &&
               input_rowsets->size() == 1) { // input_rowsets中只有一个rowset，并且大小超过最小阈值（64MB）
        auto rs_meta = input_rowsets->front()->rowset_meta();
        // if there is only one rowset and not overlapping,
        // we do not need to do compaction
        if (!rs_meta->is_segments_overlapping()) { // 选出的rowset是非overlapping的，则不需要做cumulative compaction
            input_rowsets->clear();
            *compaction_score = 0;
        }
    }
    return transient_size;
}

/*根据rowset的大小计算参数传入的rowset大小的level*/
int SizeBasedCumulativeCompactionPolicy::_level_size(const int64_t size) {
    for (auto& i : _levels) {
        if (size >= i) {
            return i;
        }
    }
    return 0;
}

void NumBasedCumulativeCompactionPolicy::update_cumulative_point(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& input_rowsets,
        RowsetSharedPtr _output_rowset, Version& last_delete_version) {
    // use the version after end version of the last input rowsets to update cumulative point
    int64_t cumulative_point = input_rowsets.back()->end_version() + 1;
    tablet->set_cumulative_layer_point(cumulative_point);
}

int NumBasedCumulativeCompactionPolicy::pick_input_rowsets(
        Tablet* tablet, const std::vector<RowsetSharedPtr>& candidate_rowsets,
        const int64_t max_compaction_score, const int64_t min_compaction_score,
        std::vector<RowsetSharedPtr>* input_rowsets, Version* last_delete_version,
        size_t* compaction_score) {
    *compaction_score = 0;
    int transient_size = 0;
    for (size_t i = 0; i < candidate_rowsets.size(); ++i) {
        RowsetSharedPtr rowset = candidate_rowsets[i];
        // check whether this rowset is delete version
        if (tablet->version_for_delete_predicate(rowset->version())) {
            *last_delete_version = rowset->version();
            if (!input_rowsets->empty()) {
                // we meet a delete version, and there were other versions before.
                // we should compact those version before handling them over to base compaction
                break;
            } else {
                // we meet a delete version, and no other versions before, skip it and continue
                input_rowsets->clear();
                transient_size = 0;
                *compaction_score = 0;
                continue;
            }
        }
        if (*compaction_score >= max_compaction_score) {
            // got enough segments
            break;
        }
        *compaction_score += rowset->rowset_meta()->get_compaction_score();
        input_rowsets->push_back(rowset);
        transient_size += 1;
    }

    if (input_rowsets->empty()) {
        return transient_size;
    }

    // if we have a sufficient number of segments,
    // or have other versions before encountering the delete version, we should process the compaction.
    if (last_delete_version->first == -1 && *compaction_score < min_compaction_score) {
        input_rowsets->clear();
    }
    return transient_size;
}

void NumBasedCumulativeCompactionPolicy::calc_cumulative_compaction_score(
        const std::vector<RowsetMetaSharedPtr>& all_rowsets, const int64_t current_cumulative_point,
        uint32_t* score) {
    bool base_rowset_exist = false;
    const int64_t point = current_cumulative_point;
    for (auto& rs_meta : all_rowsets) {
        if (rs_meta->start_version() == 0) {
            base_rowset_exist = true;
        }
        if (rs_meta->start_version() < point) {
            // all_rs_metas() is not sorted, so we use _continue_ other than _break_ here.
            continue;
        }
        *score += rs_meta->get_compaction_score();
    }

    // If base version does not exist, it may be that tablet is doing alter table.
    // Do not select it and set *score = 0
    if (!base_rowset_exist) {
        *score = 0;
    }
}

void NumBasedCumulativeCompactionPolicy::calculate_cumulative_point(
        Tablet* tablet, const std::vector<RowsetMetaSharedPtr>& all_metas,
        int64_t current_cumulative_point, int64_t* ret_cumulative_point) {
    *ret_cumulative_point = Tablet::K_INVALID_CUMULATIVE_POINT;
    if (current_cumulative_point != Tablet::K_INVALID_CUMULATIVE_POINT) {
        // only calculate the point once.
        // after that, cumulative point will be updated along with compaction process.
        return;
    }

    std::list<RowsetMetaSharedPtr> existing_rss;
    for (auto& rs : all_metas) {
        existing_rss.emplace_back(rs);
    }

    // sort the existing rowsets by version in ascending order
    existing_rss.sort([](const RowsetMetaSharedPtr& a, const RowsetMetaSharedPtr& b) {
        // simple because 2 versions are certainly not overlapping
        return a->version().first < b->version().first;
    });

    int64_t prev_version = -1;
    for (const RowsetMetaSharedPtr& rs : existing_rss) {
        if (rs->version().first > prev_version + 1) {
            // There is a hole, do not continue
            break;
        }
        // break the loop if segments in this rowset is overlapping, or is a singleton.
        if (rs->is_segments_overlapping() || rs->is_singleton_delta()) {
            *ret_cumulative_point = rs->version().first;
            break;
        }

        prev_version = rs->version().second;
        *ret_cumulative_point = prev_version + 1;
    }
}

void CumulativeCompactionPolicy::pick_candidate_rowsets(
        int64_t skip_window_sec,
        const std::unordered_map<Version, RowsetSharedPtr, HashOfVersion>& rs_version_map,
        int64_t cumulative_point, std::vector<RowsetSharedPtr>* candidate_rowsets) {
    int64_t now = UnixSeconds();
    for (auto& it : rs_version_map) {
        // find all rowset version greater than cumulative_point and skip the create time in skip_window_sec
        if (it.first.first >= cumulative_point
            && ((it.second->creation_time() + skip_window_sec < now)
            // this case means a rowset has been compacted before which is not a new published rowset, so it should participate compaction
            || (it.first.first != it.first.second))) {
            candidate_rowsets->push_back(it.second);
        }
    }
    std::sort(candidate_rowsets->begin(), candidate_rowsets->end(), Rowset::comparator);
}

std::unique_ptr<CumulativeCompactionPolicy>
CumulativeCompactionPolicyFactory::create_cumulative_compaction_policy(std::string type) {
    CompactionPolicy policy_type;
    _parse_cumulative_compaction_policy(type, &policy_type);

    if (policy_type == NUM_BASED_POLICY) {
        return std::unique_ptr<CumulativeCompactionPolicy>(
                new NumBasedCumulativeCompactionPolicy());
    } else if (policy_type == SIZE_BASED_POLICY) {
        return std::unique_ptr<CumulativeCompactionPolicy>(
                new SizeBasedCumulativeCompactionPolicy());
    }

    return std::unique_ptr<CumulativeCompactionPolicy>(new NumBasedCumulativeCompactionPolicy());
}

void CumulativeCompactionPolicyFactory::_parse_cumulative_compaction_policy(
        std::string type, CompactionPolicy* policy_type) {
    boost::to_upper(type);
    if (type == CUMULATIVE_NUM_BASED_POLICY) {
        *policy_type = NUM_BASED_POLICY;
    } else if (type == CUMULATIVE_SIZE_BASED_POLICY) {
        *policy_type = SIZE_BASED_POLICY;
    } else {
        LOG(WARNING) << "parse cumulative compaction policy error " << type << ", default use "
                     << CUMULATIVE_NUM_BASED_POLICY;
        *policy_type = NUM_BASED_POLICY;
    }
}
} // namespace doris
