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

#include "beta_rowset_reader.h"

#include "olap/generic_iterators.h"
#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/schema.h"
#include "olap/delete_handler.h"

namespace doris {

/*BetaRowsetReader构造函数*/
BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset)
    : _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->aquire();
}

/*rowset reader初始化*/
OLAPStatus BetaRowsetReader::init(RowsetReaderContext* read_context) {
    RETURN_NOT_OK(_rowset->load()); // 加载rowset，rowset的状态变为ROWSET_LOADED
    _context = read_context;
    if (_context->stats != nullptr) {
        // schema change/compaction should use owned_stats
        // When doing schema change/compaction,
        // only statistics of this RowsetReader is necessary.
        _stats = _context->stats;
    }
    // SegmentIterator will load seek columns on demand
    Schema schema(_context->tablet_schema->columns(), *(_context->return_columns)); // 创建schema，用于记录需要从segment文件中读取的字段（不需要从segment文件中读取所有的字段，只读取需要的字段数据就可以）

    // convert RowsetReaderContext to StorageReadOptions
    // 创建并初始化read options
    StorageReadOptions read_options;
    read_options.stats = _stats;
    read_options.conditions = read_context->conditions;
    if (read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
            read_options.key_ranges.emplace_back(
                read_context->lower_bound_keys->at(i),
                read_context->is_lower_keys_included->at(i),
                read_context->upper_bound_keys->at(i),
                read_context->is_upper_keys_included->at(i));
        }
    }
    if (read_context->delete_handler != nullptr) {
        read_context->delete_handler->get_delete_conditions_after_version(_rowset->end_version(),
                &read_options.delete_conditions);
    }
    read_options.column_predicates = read_context->predicates;
    read_options.use_page_cache = read_context->use_page_cache;

    // create iterator for each segment
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : _rowset->_segments) { // 依次遍历rowset下的每一个segment
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(schema, read_options, &iter); // 针对segment创建SegmentIterator(RowwiseIterator的子类对象)
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return OLAP_ERR_ROWSET_READER_INIT;
        }
        seg_iterators.push_back(std::move(iter)); // 将创建的RowwiseIterator添加到seg_iterators
    }
    std::vector<RowwiseIterator*> iterators;
    for (auto& owned_it : seg_iterators) { // 依次遍历seg_iterators下的每一个RowwiseIterator
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release()); // 将RowwiseIterator添加到iterators
    }

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
        final_iterator = new_merge_iterator(iterators); // 需要聚合，根据iterators创建merge_iterator
    } else {
        final_iterator = new_union_iterator(iterators); // 不需要聚合，根据iterators创建union_iterator
    }
    auto s = final_iterator->init(read_options); // 初始化final_iterator
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }
    _iterator.reset(final_iterator); // 使用final_iterator初始化成员变量_iterator

    // init input block
    _input_block.reset(new RowBlockV2(schema, 1024)); // 初始化成员变量_input_block

    // init output block and row
    _output_block.reset(new RowBlock(read_context->tablet_schema));
    RowBlockInfo output_block_info;
    output_block_info.row_num = 1024;
    output_block_info.null_supported = true;
    // the output block's schema should be seek_columns to comform to v1
    // TODO(hkp): this should be optimized to use return_columns
    output_block_info.column_ids = *(_context->seek_columns);
    RETURN_NOT_OK(_output_block->init(output_block_info)); // 初始化成员变量_output_block
    _row.reset(new RowCursor());
    RETURN_NOT_OK(_row->init(*(read_context->tablet_schema), *(_context->seek_columns))); // 初始化成员变量_row

    return OLAP_SUCCESS;
}

/*从当前rowset中获取next block，通过参数block传回*/
OLAPStatus BetaRowsetReader::next_block(RowBlock** block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // read next input block
    _input_block->clear();
    {
        auto s = _iterator->next_batch(_input_block.get()); // 获取当前rowset的下一个block，通过参数_input_block传回
        if (!s.ok()) {
            if (s.is_end_of_file()) {
                *block = nullptr;
                return OLAP_ERR_DATA_EOF;
            }
            LOG(WARNING) << "failed to read next block: " << s.to_string();
            return OLAP_ERR_ROWSET_READ_FAILED;
        }
    }

    // convert to output block
    _output_block->clear();
    {
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        _input_block->convert_to_row_block(_row.get(), _output_block.get()); // 将_input_block转换为row block，通过参数_output_block传回
    }
    *block = _output_block.get(); // 将_output_block赋值给block
    return OLAP_SUCCESS;
}

} // namespace doris
