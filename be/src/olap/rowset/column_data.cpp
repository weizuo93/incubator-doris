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

#include "olap/rowset/column_data.h"

#include "olap/rowset/segment_reader.h"
#include "olap/olap_cond.h"
#include "olap/row_block.h"
#include "olap/storage_engine.h"

namespace doris {

/*根据segment_group创建ColumnData对象，每一个ColumnData对象对应一个segment_group*/
ColumnData* ColumnData::create(SegmentGroup* segment_group) {
    ColumnData* data = new(std::nothrow) ColumnData(segment_group); // 根据segment_group创建ColumnData对象
    return data;
}

/*构造函数*/
ColumnData::ColumnData(SegmentGroup* segment_group)
      : _segment_group(segment_group),
        _eof(false),
        _conditions(nullptr),
        _col_predicates(nullptr),
        _delete_status(DEL_NOT_SATISFIED),
        _runtime_state(nullptr),
        _schema(segment_group->get_tablet_schema()),
        _is_using_cache(false),
        _segment_reader(nullptr),
        _lru_cache(nullptr) {
    if (StorageEngine::instance() != nullptr) {
        _lru_cache = StorageEngine::instance()->index_stream_lru_cache();
    } else {
        // for independent usage, eg: unit test/segment tool
        _lru_cache = FileHandler::get_fd_cache();
    }
    _num_rows_per_block = _segment_group->get_num_rows_per_row_block(); // 获取segment group中每个row block中的数据行数
}

/*析构函数*/
ColumnData::~ColumnData() {
    _segment_group->release(); // 当前_segment_group的读引用计数减1
    SAFE_DELETE(_segment_reader); // 释放指针_segment_reader所指向的内存空间
}

/*初始化当前ColumnData对象*/
OLAPStatus ColumnData::init() {
    _segment_group->acquire(); // 当前_segment_group的读引用计数增1

    auto res = _short_key_cursor.init(_segment_group->short_key_columns()); // 根据前缀索引列（short_key）初始化成员变量_short_key_cursor
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "key cursor init failed, res:" << res;
        return res;
    }
    return res;
}

/*获取下一个row block*/
OLAPStatus ColumnData::get_next_block(RowBlock** row_block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    _is_normal_read = true;
    auto res = _get_block(false); // 获取下一个row block，保存在成员变量_read_block中
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Get next block failed.";
        }
        *row_block = nullptr;
        return res;
    }
    *row_block = _read_block.get(); // 将获取到的row block的指针赋值给参数row_block
    return OLAP_SUCCESS;
}

/*从当前segment group对应的row block中读取一行数据*/
OLAPStatus ColumnData::_next_row(const RowCursor** row, bool without_filter) {
    _read_block->pos_inc(); // _read_block的位置指针后移一位，获取要读取的行位置
    do {
        if (_read_block->has_remaining()) { // 判断当前的row block是否还有数据
            // 1. get one row for vectorized_row_batch
            size_t pos = _read_block->pos(); // 获取row block的当前位置
            _read_block->get_row(pos, &_cursor); // 读取位置为pos的一行，通过参数_cursor返回
            if (without_filter) { // 判断是否不需要进行数据过滤
                *row = &_cursor; // 如果不需要进行数据过滤则直接将该行数据通过参数传出
                return OLAP_SUCCESS;
            }

            // 需要数据过滤
            // when without_filter is true, _include_blocks is nullptr
            if (_read_block->block_status() == DEL_NOT_SATISFIED) { // 判断当前block的删除状态是否为DEL_NOT_SATISFIED
                *row = &_cursor; // 将该行数据通过参数传出
                return OLAP_SUCCESS;
            } else {
                DCHECK(_read_block->block_status() == DEL_PARTIAL_SATISFIED); // 当前block的删除状态是否为DEL_PARTIAL_SATISFIED
                bool row_del_filter = _delete_handler->is_filter_data(
                    _segment_group->version().second, _cursor); // 判断block中当前行是否被删除
                if (!row_del_filter) {
                    *row = &_cursor; // block中当前行没有被删除，则将该行数据通过参数传出
                    return OLAP_SUCCESS;
                }
                // This row is filtered, continue to process next row
                _stats->rows_del_filtered++;
                _read_block->pos_inc(); // 当前行被删除过滤了，_read_block的位置指针后移一位，继续读取下一行
            }
        } else { // 当前的row block中已经没有数据了
            // get_next_block
            auto res = _get_block(without_filter); // 获取下一个block
            if (res != OLAP_SUCCESS) {
                return res;
            }
        }
    } while (true);

    return OLAP_SUCCESS;
}

/*按照参数block_pos传入的位置寻找block*/
OLAPStatus ColumnData::_seek_to_block(const RowBlockPosition& block_pos, bool without_filter) {
    // TODO(zc): _segment_readers???
    // open segment reader if needed
    if (_segment_reader == nullptr || block_pos.segment != _current_segment) {
        if (block_pos.segment >= _segment_group->num_segments() ||
            (_end_key_is_set && block_pos.segment > _end_segment)) {
            _eof = true;
            return OLAP_ERR_DATA_EOF;
        }
        SAFE_DELETE(_segment_reader); // 删除前一个segment的segment reader
        std::string file_name;
        file_name = segment_group()->construct_data_file_path(block_pos.segment); // 获取当前segment的文件路径
        _segment_reader = new(std::nothrow) SegmentReader(
                file_name, segment_group(),  block_pos.segment,
                _seek_columns, _load_bf_columns, _conditions,
                _delete_handler, _delete_status, _lru_cache, _runtime_state, _stats); // 针对当前segment创建segment reader
        if (_segment_reader == nullptr) {
            OLAP_LOG_WARNING("fail to malloc segment reader.");
            return OLAP_ERR_MALLOC_ERROR;
        }

        _current_segment = block_pos.segment; // 记录当前的segment
        auto res = _segment_reader->init(_is_using_cache); // 初始化创建的segment reader
        if (OLAP_SUCCESS != res) {
            OLAP_LOG_WARNING("fail to init segment reader. [res=%d]", res);
            return res;
        }
    }

    uint32_t end_block;
    if (_end_key_is_set && block_pos.segment == _end_segment) {
        end_block = _end_block; // 计算end block
    } else {
        end_block = _segment_reader->block_count() - 1;
    }

    VLOG(3) << "seek from " << block_pos.data_offset << " to " << end_block;
    return _segment_reader->seek_to_block(
        block_pos.data_offset, end_block, without_filter, &_next_block, &_segment_eof); // 通过segment reader寻找block
}

OLAPStatus ColumnData::_find_position_by_short_key(
        const RowCursor& key, bool find_last_key, RowBlockPosition *position) {
    RowBlockPosition tmp_pos;
    auto res = _segment_group->find_short_key(key, &_short_key_cursor, find_last_key, &tmp_pos);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }
    res = segment_group()->find_prev_point(tmp_pos, position);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_find_position_by_full_key(
        const RowCursor& key, bool find_last_key, RowBlockPosition *position) {
    RowBlockPosition tmp_pos;
    auto res = _segment_group->find_short_key(key, &_short_key_cursor, false, &tmp_pos);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }
    RowBlockPosition start_position;
    res = segment_group()->find_prev_point(tmp_pos, &start_position);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("find prev row block failed. [res=%d]", res);
        return res;
    }

    RowBlockPosition end_position;
    res = _segment_group->find_short_key(key, &_short_key_cursor, true, &end_position);
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            res = OLAP_ERR_DATA_EOF;
        } else {
            OLAP_LOG_WARNING("find row block failed. [res=%d]", res);
        }
        return res;
    }

    // choose min value of end_position and m_end_key_block_position as real end_position
    if (_end_key_is_set) {
        RowBlockPosition end_key_position;
        end_key_position.segment = _end_segment;
        end_key_position.data_offset = _end_block;
        if (end_position > end_key_position) {
            OLAPIndexOffset index_offset;
            index_offset.segment = _end_segment;
            index_offset.offset = _end_block;
            res = segment_group()->get_row_block_position(index_offset, &end_position);
            if (res != OLAP_SUCCESS) {
                OLAP_LOG_WARNING("fail to get row block position. [res=%d]", res);
                return res;
            }
        }
    }

    // ????end_position
    uint32_t distance = segment_group()->compute_distance(start_position, end_position);

    BinarySearchIterator it_start(0u);
    BinarySearchIterator it_end(distance + 1);
    BinarySearchIterator it_result(0u);
    ColumnDataComparator comparator(
            start_position,
            this,
            segment_group());
    try {
        if (!find_last_key) {
            it_result = std::lower_bound(it_start, it_end, key, comparator);
        } else {
            it_result = std::upper_bound(it_start, it_end, key, comparator);
        }
        VLOG(3) << "get result iterator. offset=" << *it_result
                << ", start_pos=" << start_position.to_string();
    } catch (std::exception& e) {
        LOG(WARNING) << "exception happens when doing seek. exception=" << e.what();
        return OLAP_ERR_STL_ERROR;
    }

    if (*it_result != *it_start) {
        it_result -= 1;
    }

    if (OLAP_SUCCESS != (res = segment_group()->advance_row_block(*it_result, 
                &start_position))) {
        OLAP_LOG_WARNING("fail to advance row_block. [res=%d it_offset=%u "
                "start_pos='%s']", res, *it_result, 
                start_position.to_string().c_str());
        return res;
    }

    if (_end_key_is_set) {
        RowBlockPosition end_key_position;
        end_key_position.segment = _end_segment;
        end_key_position.data_offset = _end_block;
        if (end_position > end_key_position) {
            return OLAP_ERR_DATA_EOF;
        }
    }

    *position = start_position;
    return OLAP_SUCCESS;
}

OLAPStatus ColumnData::_seek_to_row(const RowCursor& key, bool find_last_key, bool is_end_key) {
    RowBlockPosition position;
    OLAPStatus res = OLAP_SUCCESS;
    const TabletSchema& tablet_schema = _segment_group->get_tablet_schema();
    FieldType type = tablet_schema.column(key.field_count() - 1).type();
    if (key.field_count() > _segment_group->get_num_short_key_columns() || OLAP_FIELD_TYPE_VARCHAR == type) {
        res = _find_position_by_full_key(key, find_last_key, &position);
    } else {
        res = _find_position_by_short_key(key, find_last_key, &position);
    }
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Fail to find the key.[res=" << res << " key=" << key.to_string()
                         << " find_last_key=" << find_last_key << "]";
        }
        return res;
    }
    bool without_filter = is_end_key;
    res = _seek_to_block(position, without_filter);
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to get row block. "
                         "[res=%d segment=%d block_size=%d data_offset=%d index_offset=%d]",
                         res,
                         position.segment, position.block_size,
                         position.data_offset, position.index_offset);
        return res;
    }
    res = _get_block(without_filter);
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Fail to find the key.[res=" << res
                         << " key=" << key.to_string() << " find_last_key=" << find_last_key << "]";
        }
        return res;
    }

    const RowCursor* row_cursor = _current_row();
    if (!find_last_key) {
        // 不找last key。 那么应该返回大于等于这个key的第一个，也就是
        // row_cursor >= key
        // 此处比较2个block的行数，是存在一种极限情况：若未找到满足的block，
        // Index模块会返回倒数第二个block，此时key可能是最后一个block的最后一行
        while (res == OLAP_SUCCESS && compare_row_key(*row_cursor, key) < 0) {
            res = _next_row(&row_cursor, without_filter);
        }
    } else {
        // 找last key。返回大于这个key的第一个。也就是
        // row_cursor > key
        while (res == OLAP_SUCCESS && compare_row_key(*row_cursor,key) <= 0) {
            res = _next_row(&row_cursor, without_filter);
        }
    }

    return res;
}

const RowCursor* ColumnData::seek_and_get_current_row(const RowBlockPosition& position) {
    auto res = _seek_to_block(position, true);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to seek to block in seek_and_get_current_row, res=" << res
            << ", segment:" << position.segment << ", block:" << position.data_offset;
        return nullptr;
    }
    res = _get_block(true, 1);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "Fail to get block in seek_and_get_current_row, res=" << res
            << ", segment:" << position.segment << ", block:" << position.data_offset;
        return nullptr;
    }
    return _current_row();
}

OLAPStatus ColumnData::prepare_block_read(
        const RowCursor* start_key, bool find_start_key,
        const RowCursor* end_key, bool find_end_key,
        RowBlock** first_block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    set_eof(false);
    _end_key_is_set = false;
    _is_normal_read = false;
    // set end position
    if (end_key != nullptr) { // 判断参数传入的end_key是否为空
        auto res = _seek_to_row(*end_key, find_end_key, true); // 寻找参数传入的end key
        if (res == OLAP_SUCCESS) {
            // we find a
            _end_segment = _current_segment;
            _end_block = _current_block;
            _end_row_index = _read_block->pos();
            _end_key_is_set = true;
        } else if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "Find end key failed.key=" << end_key->to_string();
            return res;
        }
        // res == OLAP_ERR_DATA_EOF means there is no end key, then we read to
        // the end of this ColumnData
    }
    set_eof(false);
    if (start_key != nullptr) { // 判断参数传入的start_key是否为空
        auto res = _seek_to_row(*start_key, !find_start_key, false); // 寻找参数传入的start key
        if (res == OLAP_SUCCESS) {
            *first_block = _read_block.get(); // 获取第一个block
        } else if (res == OLAP_ERR_DATA_EOF) {
            _eof = true;
            *first_block = nullptr;
            return res;
        } else {
            LOG(WARNING) << "start_key can't be found.key=" << start_key->to_string();
            return res;
        }
    } else { // 参数传入的start_key为空
        // This is used to 
        _is_normal_read = true;

        RowBlockPosition pos;
        pos.segment = 0u;
        pos.data_offset = 0u;
        auto res = _seek_to_block(pos, false); // 寻找segment group中的第一个block
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to seek to block in, res=" << res
                << ", segment:" << pos.segment << ", block:" << pos.data_offset;
            return res;
        }
        res = _get_block(false); // 获取block
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to get block in , res=" << res
                << ", segment:" << pos.segment << ", block:" << pos.data_offset;
            return res;
        }
        *first_block = _read_block.get();
    }
    return OLAP_SUCCESS;
}

// ColumnData向上返回的列至少由几部分组成:
// 1. return_columns中要求返回的列,即Fetch命令中指定要查询的列.
// 2. condition中涉及的列, 绝大多数情况下这些列都已经在return_columns中.
// 在这个函数里,合并上述几种情况
void ColumnData::set_read_params(
        const std::vector<uint32_t>& return_columns,
        const std::vector<uint32_t>& seek_columns,
        const std::set<uint32_t>& load_bf_columns,
        const Conditions& conditions,
        const std::vector<ColumnPredicate*>& col_predicates,
        bool is_using_cache,
        RuntimeState* runtime_state) {
    _conditions = &conditions;
    _col_predicates = &col_predicates;
    _need_eval_predicates = !col_predicates.empty();
    _is_using_cache = is_using_cache;
    _runtime_state = runtime_state;
    _return_columns = return_columns;
    _seek_columns = seek_columns;
    _load_bf_columns = load_bf_columns;

    auto res = _cursor.init(_segment_group->get_tablet_schema(), _seek_columns);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init row_cursor";
    }

    _read_vector_batch.reset(new VectorizedRowBatch(
            &(_segment_group->get_tablet_schema()), _return_columns, _num_rows_per_block));

    _seek_vector_batch.reset(new VectorizedRowBatch(
            &(_segment_group->get_tablet_schema()), _seek_columns, _num_rows_per_block));

    _read_block.reset(new RowBlock(&(_segment_group->get_tablet_schema())));
    RowBlockInfo block_info;
    block_info.row_num = _num_rows_per_block;
    block_info.null_supported = true;
    block_info.column_ids = _seek_columns;
    _read_block->init(block_info);
}

/*获取第一个row block*/
OLAPStatus ColumnData::get_first_row_block(RowBlock** row_block) {
    DCHECK(!_end_key_is_set) << "end key is set while use block interface.";
    _is_normal_read = true;
    _eof = false;

    // to be same with OLAPData, we use segment_group.
    RowBlockPosition block_pos;
    OLAPStatus res = segment_group()->find_first_row_block(&block_pos); // 寻找当前ColumnData对应的SegmentGroup中第一个block的位置，通过参数block_pos返回
    if (res != OLAP_SUCCESS) {
        if (res == OLAP_ERR_INDEX_EOF) {
            *row_block = nullptr;
            _eof = true;
            return res;
        }
        OLAP_LOG_WARNING("fail to find first row block with SegmentGroup.");
        return res;
    }

    res = _seek_to_block(block_pos, false); // 按照参数block_pos传入的block位置，寻找block
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            OLAP_LOG_WARNING("seek to block fail. [res=%d]", res);
        }
        *row_block = nullptr;
        return res;
    }

    res = _get_block(false); // 获取row block，保存在成员变量_read_block中
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "fail to load data to row block. res=" << res
                         << ", version=" << version().first
                         << "-" << version().second;
        }
        *row_block = nullptr;
        return res;
    }

    *row_block = _read_block.get(); // 将获取到的row block赋值给返回参数row_block
    return OLAP_SUCCESS;
}

/*判断当前ColumnData对象对应的segment group是否被查询条件过滤掉了*/
bool ColumnData::rowset_pruning_filter() {
    if (empty() || zero_num_rows()) { // 当前ColumnData对象对应的segment group中没有数据行
        return true;
    }

    if (!_segment_group->has_zone_maps()) { // 当前ColumnData对象对应的segment group中没有zone map
        return false;
    }

    return _conditions->rowset_pruning_filter(_segment_group->get_zone_maps());
}

/*判断当前ColumnData对象对应的segment group是否因为delete操作而被过滤掉了*/
int ColumnData::delete_pruning_filter() {
    if (empty() || zero_num_rows()) {
        // should return DEL_NOT_SATISFIED, because that when creating rollup tablet,
        // the delete version file should preserved for filter data.
        return DEL_NOT_SATISFIED;
    }

    int num_zone_maps = _schema.keys_type() == KeysType::DUP_KEYS ? _schema.num_columns() : _schema.num_key_columns(); // 获取zone map列的数目，当keys_type为DUP_KEYS时，zone map列的数目为整个schema列的数目，否则，zone map列的数目为schema中key列的数目
    // _segment_group->get_zone_maps().size() < num_zone_maps for a table is schema changed from older version that not support
    // generate zone map for duplicated mode value column, using DEL_PARTIAL_SATISFIED
    if (!_segment_group->has_zone_maps() || _segment_group->get_zone_maps().size() < num_zone_maps)  {
        /*
         * if segment_group has no column statistics, we cannot judge whether the data can be filtered or not
         */
        return DEL_PARTIAL_SATISFIED;
    }

    /*
     * the relationship between delete condition A and B is A || B.
     * if any delete condition is satisfied, the data can be filtered.
     * elseif all delete condition is not satifsified, the data can't be filtered.
     * else is the partial satisfied.
    */
    int ret = DEL_PARTIAL_SATISFIED;
    bool del_partial_stastified = false;
    bool del_stastified = false;
    for (auto& delete_condtion : _delete_handler->get_delete_conditions()) { // 遍历_delete_handler中的每一个delete_condition
        if (delete_condtion.filter_version <= _segment_group->version().first) {
            continue; // delete_condition的删除版本小于或等于当前_segment_group的起始版本，则该条件下不会删除，继续判断下一个delete_condition
        }

        Conditions* del_cond = delete_condtion.del_cond;
        int del_ret = del_cond->delete_pruning_filter(_segment_group->get_zone_maps()); // 通过zone map判断当前segment group是否因为delete操作而被过滤掉了
        if (DEL_SATISFIED == del_ret) {
            del_stastified = true;
            break;
        } else if (DEL_PARTIAL_SATISFIED == del_ret) {
            del_partial_stastified = true;
        } else {
            continue;
        }
    }

    if (del_stastified) {
        ret = DEL_SATISFIED;
    } else if (del_partial_stastified) {
        ret = DEL_PARTIAL_SATISFIED;
    } else {
        ret = DEL_NOT_SATISFIED;
    }

    return ret;
}

/*获取因为delete操作而被删除的行*/
uint64_t ColumnData::get_filted_rows() {
    return _stats->rows_del_filtered;
}

OLAPStatus ColumnData::schema_change_init() {
    _is_using_cache = false;

    for (int i = 0; i < _segment_group->get_tablet_schema().num_columns(); ++i) {
        _return_columns.push_back(i);
        _seek_columns.push_back(i);
    }

    auto res = _cursor.init(_segment_group->get_tablet_schema());
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row_cursor");
        return res;
    }

    _read_vector_batch.reset(new VectorizedRowBatch(
            &(_segment_group->get_tablet_schema()), _return_columns, _num_rows_per_block));

    _read_block.reset(new RowBlock(&(_segment_group->get_tablet_schema())));

    RowBlockInfo block_info;
    block_info.row_num = _num_rows_per_block;
    block_info.null_supported = true;
    _read_block->init(block_info);
    return OLAP_SUCCESS;
}

/*读取一个batch的数据，通过参数vec_batch传回*/
OLAPStatus ColumnData::_get_block_from_reader(
        VectorizedRowBatch** got_batch, bool without_filter, int rows_read) {
    VectorizedRowBatch* vec_batch = nullptr;
    if (_is_normal_read) {
        vec_batch = _read_vector_batch.get(); // 获取成员变量_read_vector_batch的地址
    } else {
        vec_batch = _seek_vector_batch.get(); // 获取成员变量_seek_vector_batch的地址
    }
    // If this is normal read
    do {
#if 0
        LOG(INFO) << "_current_segment is " << _current_segment
            << ", _next_block:" << _next_block
            << ", _end_segment::"  << _end_segment
            << ", _end_block:" << _end_block
            << ", _end_row_index:" << _end_row_index
            << ", _segment_eof:" << _segment_eof;
#endif
        vec_batch->clear(); // 清空 vec_batch
        if (rows_read > 0) {
            vec_batch->set_limit(rows_read);
        }
        // If we are going to read last block, we need to set batch limit to the end of key
        // if without_filter is true and _end_key_is_set is true, this must seek to start row's
        // block, we must load the entire block.
        if (OLAP_UNLIKELY(!without_filter &&
                          _end_key_is_set &&
                          _next_block == _end_block &&
                          _current_segment == _end_segment)) {
            vec_batch->set_limit(_end_row_index);
            if (_end_row_index == 0) {
                _segment_eof = true; // 当前segment结束
            }
        }

        if (!_segment_eof) { // 当前segment没有结束
            _current_block = _next_block;
            auto res = _segment_reader->get_block(vec_batch, &_next_block, &_segment_eof); // 通过segment reader获取一个block
            if (res != OLAP_SUCCESS) {
                return res;
            }
            // Normal case
            *got_batch = vec_batch; // 将读到的batch数据指针赋值给参数got_batch
            return OLAP_SUCCESS;
        }
        // When this segment is read over, we reach here.
        // Seek to next segment
        RowBlockPosition block_pos;
        block_pos.segment = _current_segment + 1; // 一个segment数据读取结束，读取下一个segment
        block_pos.data_offset = 0;
        auto res = _seek_to_block(block_pos, without_filter); // 寻找下一个segment的第一个block
        if (res != OLAP_SUCCESS) {
            return res;
        }
    } while (true);

    return OLAP_SUCCESS;
}

/*获取row block*/
OLAPStatus ColumnData::_get_block(bool without_filter, int rows_read) {
    do {
        VectorizedRowBatch* vec_batch = nullptr;
        auto res = _get_block_from_reader(&vec_batch, without_filter, rows_read); // 通过segment reader读取一个batch的数据，通过参数vec_batch传回
        if (res != OLAP_SUCCESS) {
            return res;
        }
        // evaluate predicates
        if (!without_filter && _need_eval_predicates) { // 判断是否需要进行数据过滤
            SCOPED_RAW_TIMER(&_stats->vec_cond_ns);
            size_t old_size = vec_batch->size();
            for (auto pred : *_col_predicates) { // 依次使用每一个列条件对batch数据进行判断过滤
                pred->evaluate(vec_batch);
            }
            _stats->rows_vec_cond_filtered += old_size - vec_batch->size();
        }
        // if vector is empty after predicate evaluate, get next block
        if (vec_batch->size() == 0) {
            continue;
        }
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        // when reach here, we have already read a block successfully
        _read_block->clear(); // 清空成员变量_read_block
        vec_batch->dump_to_row_block(_read_block.get()); // 将vec_batch中的batch数据dump到_read_block中
        return OLAP_SUCCESS;
    } while (true);
    return OLAP_SUCCESS;
}

}  // namespace doris
