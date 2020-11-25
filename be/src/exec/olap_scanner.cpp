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

#include <cstring>
#include <string>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap_scanner.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "olap/field.h"
#include "service/backend_options.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/mem_util.hpp"
#include "util/network_util.h"
#include "util/doris_metrics.h"

namespace doris {

static const std::string SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
static const std::string MATERIALIZE_TUPLE_TIMER =
    "MaterializeTupleTime(*)";

/*构造函数*/
OlapScanner::OlapScanner(
        RuntimeState* runtime_state,
        OlapScanNode* parent,
        bool aggregation,
        bool need_agg_finalize,
        const TPaloScanRange& scan_range,
        const std::vector<OlapScanRange*>& key_ranges)
            : _runtime_state(runtime_state),
            _parent(parent),
            _tuple_desc(parent->_tuple_desc),
            _profile(parent->runtime_profile()),
            _string_slots(parent->_string_slots),
            _is_open(false),
            _aggregation(aggregation),
            _need_agg_finalize(need_agg_finalize),
            _tuple_idx(parent->_tuple_idx),
            _direct_conjunct_size(parent->_direct_conjunct_size) {
    _reader.reset(new Reader());
    DCHECK(_reader.get() != NULL);

    _rows_read_counter = parent->rows_read_counter();
    _rows_pushed_cond_filtered_counter = parent->_rows_pushed_cond_filtered_counter;
}

OlapScanner::~OlapScanner() {
}

/*对一个tablet的数据读取前的准备*/
Status OlapScanner::prepare(
        const TPaloScanRange& scan_range, const std::vector<OlapScanRange*>& key_ranges,
        const std::vector<TCondition>& filters, const std::vector<TCondition>& is_nulls) {
    // Get olap table
    TTabletId tablet_id = scan_range.tablet_id; //获取要读取的tablet id
    SchemaHash schema_hash =
        strtoul(scan_range.schema_hash.c_str(), nullptr, 10); //获取要读取的tablet的schema hash
    _version =
        strtoul(scan_range.version.c_str(), nullptr, 10); //获取要读取的tablet中rowset的版本号
    {
        std::string err;
        _tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash, true, &err); //根据tablet id和schema hash获取tablet
        if (_tablet.get() == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet. tablet_id=" << tablet_id
               << ", with schema_hash=" << schema_hash
               << ", reason=" << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        {
            ReadLock rdlock(_tablet->get_header_lock_ptr());
            const RowsetSharedPtr rowset = _tablet->rowset_with_max_version(); //获取tablet中版本最新的rowset
            if (rowset == nullptr) {
                std::stringstream ss;
                ss << "fail to get latest version of tablet: " << tablet_id;
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }

            // acquire tablet rowset readers at the beginning of the scan node
            // to prevent this case: when there are lots of olap scanners to run for example 10000
            // the rowsets maybe compacted when the last olap scanner starts
            Version rd_version(0, _version);
            OLAPStatus acquire_reader_st = _tablet->capture_rs_readers(rd_version, &_params.rs_readers); //获取参数传入的版本范围rd_version在tablet中包含的所有真实rowset,并为每一个rowset分别创建rowset reader，通过参数传回
            if (acquire_reader_st != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to init reader.res=" << acquire_reader_st;
                std::stringstream ss;
                ss << "failed to initialize storage reader. tablet=" << _tablet->full_name()
                << ", res=" << acquire_reader_st << ", backend=" << BackendOptions::get_localhost();
                return Status::InternalError(ss.str().c_str());
            }
        }
    }

    {
        // Initialize _params
        RETURN_IF_ERROR(_init_params(key_ranges, filters, is_nulls)); //参数初始化
    }

    return Status::OK();
}

/*打开并初始化reader*/
Status OlapScanner::open() {
    SCOPED_TIMER(_parent->_reader_init_timer);

    if (_conjunct_ctxs.size() > _direct_conjunct_size) {
        _use_pushdown_conjuncts = true;
    }

    auto res = _reader->init(_params); //使用参数_params初始化reader
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init reader.[res=%d]", res);
        std::stringstream ss;
        ss << "failed to initialize storage reader. tablet=" << _params.tablet->full_name()
           << ", res=" << res << ", backend=" << BackendOptions::get_localhost();
        return Status::InternalError(ss.str().c_str());
    }
    return Status::OK();
}

// it will be called under tablet read lock because capture rs readers need
/*初始化ReaderParams参数*/
Status OlapScanner::_init_params(
        const std::vector<OlapScanRange*>& key_ranges,
        const std::vector<TCondition>& filters,
        const std::vector<TCondition>& is_nulls) {
    RETURN_IF_ERROR(_init_return_columns()); //初始化要返回的列，即要返回哪些列(保存在成员变量_return_columns和_query_slots中)

    _params.tablet = _tablet;
    _params.reader_type = READER_QUERY;
    _params.aggregation = _aggregation;
    _params.version = Version(0, _version);

    // Condition
    for (auto& filter : filters) {
        _params.conditions.push_back(filter);
    }
    for (auto& is_null_str : is_nulls) {
        _params.conditions.push_back(is_null_str);
    }
    // Range
    for (auto key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 &&
                key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _params.range = (key_range->begin_include ? "ge" : "gt");
        _params.end_range = (key_range->end_include ? "le" : "lt");

        _params.start_key.push_back(key_range->begin_scan_range);
        _params.end_key.push_back(key_range->end_scan_range);
    }

    // TODO(zc)
    _params.profile = _profile;
    _params.runtime_state = _runtime_state;

    if (_aggregation) {
        _params.return_columns = _return_columns;
    } else {
        for (size_t i = 0; i < _tablet->num_key_columns(); ++i) {
            _params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (_tablet->tablet_schema().column(index).is_key()) {
                continue;
            } else {
                _params.return_columns.push_back(index);
            }
        }
    }

    // use _params.return_columns, because reader use this to merge sort
    OLAPStatus res = _read_row_cursor.init(_tablet->tablet_schema(), _params.return_columns); //根据tablet schema和返回的列数初始化_read_row_cursor
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init row cursor.[res=%d]", res);
        return Status::InternalError("failed to initialize storage read row cursor");
    }
    _read_row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema());

    // If a agg node is this scan node direct parent
    // we will not call agg object finalize method in scan node,
    // to avoid the unnecessary SerDe and improve query performance
    _params.need_agg_finalize = _need_agg_finalize;

    if (!config::disable_storage_page_cache) {
        _params.use_page_cache = true;
    }

    return Status::OK();
}

/*初始化要返回的列，即要返回哪些列*/
Status OlapScanner::_init_return_columns() {
    for (auto slot : _tuple_desc->slots()) { //依次遍历每一个列
        if (!slot->is_materialized()) {
            continue;
        }
        int32_t index = _tablet->field_index(slot->col_name()); //获取当前列在schema中的顺序
        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalied. field="  << slot->col_name();
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _return_columns.push_back(index); //将当前列在schema中的顺序添加到成员变量_return_columns中
        _query_slots.push_back(slot);     //将当前列添加到成员变量_query_slots中
    }
    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

/*读取一个batch的数据*/
Status OlapScanner::get_batch(
        RuntimeState* state, RowBatch* batch, bool* eof) {
    // 2. Allocate Row's Tuple buf
    uint8_t *tuple_buf = batch->tuple_data_pool()->allocate(
        state->batch_size() * _tuple_desc->byte_size()); //根据batch的数据行数以及每行的字节数为一个batch（多个行）数据分配内存
    bzero(tuple_buf, state->batch_size() * _tuple_desc->byte_size()); //将分配的内存的全部字节设为0值
    Tuple *tuple = reinterpret_cast<Tuple*>(tuple_buf); //将uint8_t类型的指针转换为Tuple类型的指针

    std::unique_ptr<MemTracker> tracker(new MemTracker(state->fragment_mem_tracker()->limit()));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get())); //创建内存池

    int64_t raw_rows_threshold = raw_rows_read() + config::doris_scanner_row_num;
    {
        SCOPED_TIMER(_parent->_scan_timer);
        while (true) {
            // Batch is full, break
            if (batch->is_full()) { //判断batch是否已经写满
                _update_realtime_counter();
                break;
            }
            // Read one row from reader
            auto res = _reader->next_row_with_aggregation(&_read_row_cursor, mem_pool.get(), batch->agg_object_pool(), eof); //通过_reader读取一行数据，通过参数_read_row_cursor返回
            if (res != OLAP_SUCCESS) {
                std::stringstream ss;
                ss << "Internal Error: read storage fail. res=" << res;
                return Status::InternalError(ss.str());
            }
            // If we reach end of this scanner, break
            if (UNLIKELY(*eof)) {         // 读到结尾
                break;
            }

            _num_rows_read++;

            _convert_row_to_tuple(tuple); //将行数据转化为tuple
            if (VLOG_ROW_IS_ON) {
                VLOG_ROW << "OlapScanner input row: " << Tuple::to_string(tuple, *_tuple_desc);
            }

            // 3.4 Set tuple to RowBatch(not commited)
            int row_idx = batch->add_row(); //向batch中添加一行
            TupleRow* row = batch->get_row(row_idx);
            row->set_tuple(_tuple_idx, tuple);

            do {
                // 3.5.1 Using direct conjuncts to filter data
                if (_eval_conjuncts_fn != nullptr) {
                    if (!_eval_conjuncts_fn(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                } else {
                    if (!ExecNode::eval_conjuncts(&_conjunct_ctxs[0], _direct_conjunct_size, row)) {
                        // check direct conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        break;
                    }
                }

                // 3.5.2 Using pushdown conjuncts to filter data
                if (_use_pushdown_conjuncts) {
                    if (!ExecNode::eval_conjuncts(
                            &_conjunct_ctxs[_direct_conjunct_size],
                            _conjunct_ctxs.size() - _direct_conjunct_size, row)) {
                        // check pushdown conjuncts fail then clear tuple for reuse
                        // make sure to reset null indicators since we're overwriting
                        // the tuple assembled for the previous row
                        tuple->init(_tuple_desc->byte_size());
                        _num_rows_pushed_cond_filtered++;
                        break;
                    }
                }

                // Copy string slot
                for (auto desc : _string_slots) {
                    StringValue* slot = tuple->get_string_slot(desc->tuple_offset());
                    if (slot->len != 0) {
                        uint8_t* v = batch->tuple_data_pool()->allocate(slot->len);
                        memory_copy(v, slot->ptr, slot->len);
                        slot->ptr = reinterpret_cast<char*>(v);
                    }
                }

                // the memory allocate by mem pool has been copied,
                // so we should release these memory immediately
                mem_pool->clear();

                if (VLOG_ROW_IS_ON) {
                    VLOG_ROW << "OlapScanner output row: " << Tuple::to_string(tuple, *_tuple_desc);
                }

                // check direct && pushdown conjuncts success then commit tuple
                batch->commit_last_row();
                char* new_tuple = reinterpret_cast<char*>(tuple);
                new_tuple += _tuple_desc->byte_size();
                tuple = reinterpret_cast<Tuple*>(new_tuple);

                // compute pushdown conjuncts filter rate
                if (_use_pushdown_conjuncts) {
                    // check this rate after
                    if (_num_rows_read > 32768) {
                        int32_t pushdown_return_rate
                            = _num_rows_read * 100 / (_num_rows_read + _num_rows_pushed_cond_filtered);
                        if (pushdown_return_rate > config::doris_max_pushdown_conjuncts_return_rate) {
                            _use_pushdown_conjuncts = false;
                            VLOG(2) << "Stop Using PushDown Conjuncts. "
                                << "PushDownReturnRate: " << pushdown_return_rate << "%"
                                << " MaxPushDownReturnRate: "
                                << config::doris_max_pushdown_conjuncts_return_rate << "%";
                        }
                    }
                }
            } while (false);

            if (raw_rows_read() >= raw_rows_threshold) {
                break;
            }
        }
    }

    return Status::OK();
}

void OlapScanner::_convert_row_to_tuple(Tuple* tuple) {
    size_t slots_size = _query_slots.size();
    for (int i = 0; i < slots_size; ++i) { //依次遍历一行数据中的每一列
        SlotDescriptor* slot_desc = _query_slots[i]; //每个slot对应一行数据中的1列
        auto cid = _return_columns[i]; //获取列id
        if (_read_row_cursor.is_null(cid)) { //判断当前列是否为空
            tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        char* ptr = (char*)_read_row_cursor.cell_ptr(cid); //获取当前列的指针
        size_t len = _read_row_cursor.column_size(cid); //获取当前列的数据长度
        switch (slot_desc->type().type) { //获取当前列数据的类型
        case TYPE_CHAR: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue *slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = strnlen(slot->ptr, slice->size);
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_OBJECT:
        case TYPE_HLL: {
            Slice* slice = reinterpret_cast<Slice*>(ptr);
            StringValue *slot = tuple->get_string_slot(slot_desc->tuple_offset());
            slot->ptr = slice->data;
            slot->len = slice->size;
            break;
        }
        case TYPE_DECIMAL: {
            DecimalValue *slot = tuple->get_decimal_slot(slot_desc->tuple_offset());

            // TODO(lingbin): should remove this assign, use set member function
            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            *slot = DecimalValue(int_value, frac_value);
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value *slot = tuple->get_decimalv2_slot(slot_desc->tuple_offset());

            int64_t int_value = *(int64_t*)(ptr);
            int32_t frac_value = *(int32_t*)(ptr + sizeof(int64_t));
            if (!slot->from_olap_decimal(int_value, frac_value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATETIME: {
            DateTimeValue *slot = tuple->get_datetime_slot(slot_desc->tuple_offset());
            uint64_t value = *reinterpret_cast<uint64_t*>(ptr);
            if (!slot->from_olap_datetime(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        case TYPE_DATE: {
            DateTimeValue *slot = tuple->get_datetime_slot(slot_desc->tuple_offset());
            uint64_t value = 0;
            value = *(unsigned char*)(ptr + 2);
            value <<= 8;
            value |= *(unsigned char*)(ptr + 1);
            value <<= 8;
            value |= *(unsigned char*)(ptr);
            if (!slot->from_olap_date(value)) {
                tuple->set_null(slot_desc->null_indicator_offset());
            }
            break;
        }
        default: {
            void *slot = tuple->get_slot(slot_desc->tuple_offset());
            memory_copy(slot, ptr, len);
            break;
        }
        }
    }
}

/*更新计数器*/
void OlapScanner::update_counter() {
    if (_has_update_counter) { //判断是否已经更新过计数器了
        return;
    }
    COUNTER_UPDATE(_rows_read_counter, _num_rows_read);
    COUNTER_UPDATE(_rows_pushed_cond_filtered_counter, _num_rows_pushed_cond_filtered);

    COUNTER_UPDATE(_parent->_io_timer, _reader->stats().io_ns);
    COUNTER_UPDATE(_parent->_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    COUNTER_UPDATE(_parent->_decompressor_timer, _reader->stats().decompress_ns);
    COUNTER_UPDATE(_parent->_read_uncompressed_counter, _reader->stats().uncompressed_bytes_read);
    COUNTER_UPDATE(_parent->bytes_read_counter(), _reader->stats().bytes_read);

    COUNTER_UPDATE(_parent->_block_load_timer, _reader->stats().block_load_ns);
    COUNTER_UPDATE(_parent->_block_load_counter, _reader->stats().blocks_load);
    COUNTER_UPDATE(_parent->_block_fetch_timer, _reader->stats().block_fetch_ns);
    COUNTER_UPDATE(_parent->_block_seek_timer, _reader->stats().block_seek_ns);
    COUNTER_UPDATE(_parent->_block_convert_timer, _reader->stats().block_convert_ns);

    COUNTER_UPDATE(_parent->_raw_rows_counter, _reader->stats().raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += _reader->mutable_stats()->raw_rows_read;
    // COUNTER_UPDATE(_parent->_filtered_rows_counter, _reader->stats().num_rows_filtered);
    COUNTER_UPDATE(_parent->_vec_cond_timer, _reader->stats().vec_cond_ns);
    COUNTER_UPDATE(_parent->_rows_vec_cond_counter, _reader->stats().rows_vec_cond_filtered);

    COUNTER_UPDATE(_parent->_stats_filtered_counter, _reader->stats().rows_stats_filtered);
    COUNTER_UPDATE(_parent->_bf_filtered_counter, _reader->stats().rows_bf_filtered);
    COUNTER_UPDATE(_parent->_del_filtered_counter, _reader->stats().rows_del_filtered);             // 因为被删除而过滤的行数
    COUNTER_UPDATE(_parent->_key_range_filtered_counter, _reader->stats().rows_key_range_filtered);

    COUNTER_UPDATE(_parent->_index_load_timer, _reader->stats().index_load_ns);

    COUNTER_UPDATE(_parent->_total_pages_num_counter, _reader->stats().total_pages_num);
    COUNTER_UPDATE(_parent->_cached_pages_num_counter, _reader->stats().cached_pages_num);

    COUNTER_UPDATE(_parent->_bitmap_index_filter_counter, _reader->stats().rows_bitmap_index_filtered);
    COUNTER_UPDATE(_parent->_bitmap_index_filter_timer, _reader->stats().bitmap_index_filter_timer);
    COUNTER_UPDATE(_parent->_block_seek_counter, _reader->stats().block_seek_num);

    DorisMetrics::instance()->query_scan_bytes.increment(_compressed_bytes_read); //获取当前tablet的累计查询字节数
    DorisMetrics::instance()->query_scan_rows.increment(_raw_rows_read);          //获取当前tablet的累计查询行数

    _has_update_counter = true;
}

void OlapScanner::_update_realtime_counter() {
    COUNTER_UPDATE(_parent->_read_compressed_counter, _reader->stats().compressed_bytes_read);
    _compressed_bytes_read += _reader->stats().compressed_bytes_read;
    _reader->mutable_stats()->compressed_bytes_read = 0;

    COUNTER_UPDATE(_parent->_raw_rows_counter, _reader->stats().raw_rows_read);
    // if raw_rows_read is reset, scanNode will scan all table rows which may cause BE crash
    _raw_rows_read += _reader->stats().raw_rows_read;
    _reader->mutable_stats()->raw_rows_read = 0;
}

/*关闭Reader*/
Status OlapScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    // olap scan node will call scanner.close() when finished
    // will release resources here
    // if not clear rowset readers in read_params here
    // readers will be release when runtime state deconstructed but
    // deconstructor in reader references runtime state
    // so that it will core
    _params.rs_readers.clear(); //清除rowset reader
    update_counter(); //更新计数器
    _reader.reset(); //关闭Reader
    Expr::close(_conjunct_ctxs, state);
    _is_closed = true;
    return Status::OK();
}

} // namespace doris
