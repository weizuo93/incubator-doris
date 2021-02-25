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

#include "olap/rowset/segment_v2/segment_iterator.h"

#include <set>

#include "gutil/strings/substitute.h"
#include "util/doris_metrics.h"
#include "olap/fs/fs_util.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/row_block2.h"
#include "olap/row_cursor.h"
#include "olap/short_key_index.h"
#include "olap/column_predicate.h"
#include "olap/row.h"

using strings::Substitute;

namespace doris {
namespace segment_v2 {

// A fast range iterator for roaring bitmap. Output ranges use closed-open form, like [from, to).
// Example:
//   input bitmap:  [0 1 4 5 6 7 10 15 16 17 18 19]
//   output ranges: [0,2), [4,8), [10,11), [15,20) (when max_range_size=10)
//   output ranges: [0,2), [4,8), [10,11), [15,18), [18,20) (when max_range_size=3)
class SegmentIterator::BitmapRangeIterator {
public:
    explicit BitmapRangeIterator(const Roaring& bitmap)
        : _last_val(0),
          _buf(new uint32_t[256]),
          _buf_pos(0),
          _buf_size(0),
          _eof(false) {
        roaring_init_iterator(&bitmap.roaring, &_iter); // 初始化iterator
        _read_next_batch(); // 读取第一个batch的bitmap
    }

    ~BitmapRangeIterator() {
        delete[] _buf;
    }

    bool has_more_range() const { return !_eof; }

    // read next range into [*from, *to) whose size <= max_range_size.
    // return false when there is no more range.
    /*获取下一个bitmap范围，范围边界通过参数from和to传回，获取的是一个连续的bitmap范围*/
    bool next_range(uint32_t max_range_size, uint32_t* from, uint32_t* to) {
        if (_eof) {
            return false;
        }
        *from = _buf[_buf_pos]; // 获取起始位置，_buf_pos记录bitmap中的当前位置
        uint32_t range_size = 0;
        do {
            _last_val = _buf[_buf_pos];
            _buf_pos++;
            range_size++;
            if (_buf_pos == _buf_size) { // read next batch
                _read_next_batch(); //读取下一个batch的bitmap到成员变量_buf中
            }
        } while (range_size < max_range_size && !_eof && _buf[_buf_pos] == _last_val + 1); // 如果获取的range超过特定大小、或当前bitmap值与前一个bitmap值不连续，则循环退出
        *to = *from + range_size;
        return true;
    }

private:
    /*读取下一个batch的bitmap到成员变量_buf中*/
    void _read_next_batch() {
        uint32_t n = roaring_read_uint32_iterator(&_iter, _buf, kBatchSize); // 读取下一个batch的bitmap到成员变量_buf中，kBatchSize表示需要读取的bitmap位数，函数返回值是真实读取的bitmap位数（因为iterator中bitmap的位数可能不足kBatchSize）
        _buf_pos = 0;  // 复位batch中当前访问的位置为0
        _buf_size = n; // 设置bitmap batch的大小
        _eof = n == 0;
    }

    static const uint32_t kBatchSize = 256; // 设置batch大小的上限
    roaring_uint32_iterator_t _iter;
    uint32_t _last_val;       // 记录访问的上一个bitmap的值
    uint32_t* _buf = nullptr; // 保存一个batch的bitmap
    uint32_t _buf_pos;        // 记录batch中当前访问的位置
    uint32_t _buf_size;       // 保存当前batch的实际大小
    bool _eof;
};

/*SegmentIterator的构造函数*/
SegmentIterator::SegmentIterator(std::shared_ptr<Segment> segment,
                                 const Schema& schema)
    : _segment(std::move(segment)),
      _schema(schema),
      _column_iterators(_schema.num_columns(), nullptr),
      _bitmap_index_iterators(_schema.num_columns(), nullptr),
      _cur_rowid(0),
      _lazy_materialization_read(false),
      _inited(false) {
}

/*SegmentIterator的析构函数*/
SegmentIterator::~SegmentIterator() {
    for (auto iter : _column_iterators) {
        delete iter;
    }
    for (auto iter : _bitmap_index_iterators) {
        delete iter;
    }
}

/*初始化SegmentIterator，使用参数传入的StorageReadOptions初始化成员变量_opts*/
Status SegmentIterator::init(const StorageReadOptions& opts) {
    _opts = opts;
    if (opts.column_predicates != nullptr) {
        _col_predicates = *(opts.column_predicates); // 初始化成员变量_col_predicates
    }
    return Status::OK();
}

/*初始化SegmentIterator（依次使用short key索引、bitmap索引、bloom filter索引、zone map索引和delete condition对segment的数据行进行过滤，最终需要读取的数据行保存在成员变量_row_bitmap中）*/
Status SegmentIterator::_init() {
    DorisMetrics::instance()->segment_read_total.increment(1);
    // get file handle from file descriptor of segment
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    RETURN_IF_ERROR(block_mgr->open_block(_segment->_fname, &_rblock)); // 通过block mgr打开segment文件
    _row_bitmap.addRange(0, _segment->num_rows()); // 初始化成员变量_row_bitmap的范围为整个segment文件的所有行，成员变量_row_bitmap中记录了当前segment中需要读取的所有数据行的row id
    RETURN_IF_ERROR(_init_return_column_iterators()); // 初始化返回列的iterator
    RETURN_IF_ERROR(_init_bitmap_index_iterators()); // 初始化每一列的bitmap index iterator
    RETURN_IF_ERROR(_get_row_ranges_by_keys()); // 根据short key（前缀索引列）的范围获取row id的范围
    RETURN_IF_ERROR(_get_row_ranges_by_column_conditions()); // 根据各个column condition的范围获取row id的范围（依次使用bitmap索引、bloom filter索引、zone map索引和delete condition对segment的数据行进行过滤）
    _init_lazy_materialization(); // 初始化lazy materialization（延迟物化）
    _range_iter.reset(new BitmapRangeIterator(_row_bitmap)); // 创建BitmapRangeIterator对象，并初始化成员变量_range_iter
    return Status::OK();
}

/*根据前缀索引（short key）的范围获取segment中row id的范围，结果会更新在成员变量_row_bitmap中*/
Status SegmentIterator::_get_row_ranges_by_keys() {
    DorisMetrics::instance()->segment_row_total.increment(num_rows());

    // fast path for empty segment or empty key ranges
    if (_row_bitmap.isEmpty() || _opts.key_ranges.empty()) { // 如果不存在short key range（查询条件中没有按前缀索引规则使用key），则函数返回
        return Status::OK();
    }

    RowRanges result_ranges;
    for (auto& key_range : _opts.key_ranges) { // 依次遍历前缀索引（short key，包含多个列）的每一个范围
        rowid_t lower_rowid = 0;
        rowid_t upper_rowid = num_rows();
        RETURN_IF_ERROR(_prepare_seek(key_range)); // 创建并初始化short key中各个列对应的column iterator，初始化成员变量_seek_schema和_seek_block
        if (key_range.upper_key != nullptr) {
            // If client want to read upper_bound, the include_upper is true. So we
            // should get the first ordinal at which key is larger than upper_bound.
            // So we call _lookup_ordinal with include_upper's negate
            RETURN_IF_ERROR(_lookup_ordinal( // 查找当前前缀索引（short key）范围的upper_key在segment中的的row id
                *key_range.upper_key, !key_range.include_upper, num_rows(), &upper_rowid));
        }
        if (upper_rowid > 0 && key_range.lower_key != nullptr) {
            RETURN_IF_ERROR(
                _lookup_ordinal(*key_range.lower_key, key_range.include_lower, upper_rowid, &lower_rowid)); // 查找当前前缀索引（short key）范围的lower_key在segment中的的row id
        }
        auto row_range = RowRanges::create_single(lower_rowid, upper_rowid); // 获取当前short key的range对应的row id的范围
        RowRanges::ranges_union(result_ranges, row_range, &result_ranges); // 执行row id范围的合并，将当前short key range所在的row id范围与其他short key range所在的row id范围求并集
    }
    // pre-condition: _row_ranges == [0, num_rows)
    size_t pre_size = _row_bitmap.cardinality(); // 获取_row_bitmap中连续range的个数
    _row_bitmap = RowRanges::ranges_to_roaring(result_ranges); // 使用所有的short key range对应的row id的范围更新成员变量_row_bitmap
    _opts.stats->rows_key_range_filtered += (pre_size - _row_bitmap.cardinality());
    DorisMetrics::instance()->segment_rows_by_short_key.increment(_row_bitmap.cardinality());

    return Status::OK();
}

// Set up environment for the following seek.
/*创建并初始化short key中各个列对应的column iterator，初始化成员变量_seek_schema和_seek_block*/
Status SegmentIterator::_prepare_seek(const StorageReadOptions::KeyRange& key_range) {
    std::vector<const Field*> key_fields;
    std::set<uint32_t> column_set;
    if (key_range.lower_key != nullptr) {
        for (auto cid : key_range.lower_key->schema()->column_ids()) { // 依次遍历short key中的每一个列
            column_set.emplace(cid);
            key_fields.emplace_back(key_range.lower_key->schema()->column(cid)); // 将当前列添加到key_fields中
        }
    }
    if (key_range.upper_key != nullptr) {
        for (auto cid : key_range.upper_key->schema()->column_ids()) { // 依次遍历short key中的每一个列
            if (column_set.count(cid) == 0) { // 判断当前列是否已经在key_fields中存在
                key_fields.emplace_back(key_range.upper_key->schema()->column(cid));
                column_set.emplace(cid);
            }
        }
    }
    _seek_schema.reset(new Schema(key_fields, key_fields.size())); // 根据key_fields创建Schema对象，并初始化成员变量_seek_schema
    _seek_block.reset(new RowBlockV2(*_seek_schema, 1)); // 根据_seek_schema创建RowBlockV2对象，并初始化成员变量_seek_block

    // create used column iterator
    for (auto cid : _seek_schema->column_ids()) { // 依次遍历_seek_schema中的每一列
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid])); // 针对当前列创建column iterator
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.rblock = _rblock.get();
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts)); // 初始化当前列的column iterator
        }
    }

    return Status::OK();
}

/*根据各个column condition的范围获取row id的范围，最终结果会更新在成员变量_row_bitmap中*/
Status SegmentIterator::_get_row_ranges_by_column_conditions() {
    if (_row_bitmap.isEmpty()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(_apply_bitmap_index()); // 使用位图索引（bitmap index）过滤数据行，需要读取的数据行保存在成员变量_row_bitmap

    if (!_row_bitmap.isEmpty() && (_opts.conditions != nullptr || _opts.delete_conditions.size() > 0)) {
        RowRanges condition_row_ranges = RowRanges::create_single(_segment->num_rows()); // 初始化condition_row_ranges为segment中所有的数据行
        RETURN_IF_ERROR(_get_row_ranges_from_conditions(&condition_row_ranges)); // 依次使用布隆过滤器（bloom filter）索引、zone map索引和delete condition过滤数据行，获取的row id的范围通过参数condition_row_ranges传回
        size_t pre_size = _row_bitmap.cardinality();
        _row_bitmap &= RowRanges::ranges_to_roaring(condition_row_ranges);  // 将使用布隆过滤器（bloom filter）索引、zone map索引和delete condition过滤之后需要读取的row id的范围（保存在condition_row_ranges中）与（前一步）使用bitmap索引过滤之后需要读取的row id的范围取交集，结果就是最终需要从segment文件中读取的数据行的范围，保存在成员变量_row_bitmap中
        _opts.stats->rows_del_filtered += (pre_size - _row_bitmap.cardinality());
    }

    // TODO(hkp): calculate filter rate to decide whether to
    // use zone map/bloom filter/secondary index or not.
    return Status::OK();
}

/*依次使用布隆过滤器（bloom filter）索引、zone map索引和delete condition过滤数据行，获取的row id的范围通过参数condition_row_ranges传回*/
Status SegmentIterator::_get_row_ranges_from_conditions(RowRanges* condition_row_ranges) {
    std::set<int32_t> cids;
    if (_opts.conditions != nullptr) {
        for (auto& column_condition : _opts.conditions->columns()) { // 依次遍历每一个condition列
            cids.insert(column_condition.first); //将column id添加到cids
        }
    }
    // first filter data by bloom filter index
    // bloom filter index only use CondColumn
    // 首先，根据布隆过滤器索引过滤数据
    RowRanges bf_row_ranges = RowRanges::create_single(num_rows());
    for (auto& cid : cids) { // 依次遍历每一个condition列
        // get row ranges by bf index of this column,
        RowRanges column_bf_row_ranges = RowRanges::create_single(num_rows()); // 初始化column_bf_row_ranges为segment中所有的数据行
        CondColumn* column_cond = _opts.conditions->get_column(cid); // 获取当前列的condition
        RETURN_IF_ERROR(
            _column_iterators[cid]->get_row_ranges_by_bloom_filter( // 通过当前列的column iterator，根据bloom filter过滤当前列的数据行，需要读取的row范围通过参数column_bf_row_ranges传回
                column_cond,
                &column_bf_row_ranges));
        RowRanges::ranges_intersection(bf_row_ranges, column_bf_row_ranges, &bf_row_ranges); // 将当前列bloom filter索引计算的row范围与其他列bloom filter索引计算的row范围求交集
    }
    size_t pre_size = condition_row_ranges->count();
    RowRanges::ranges_intersection(*condition_row_ranges, bf_row_ranges, condition_row_ranges); // 将bloom filter索引计算的row范围与其他condition计算的row范围求交集
    _opts.stats->rows_bf_filtered += (pre_size - condition_row_ranges->count());

    RowRanges zone_map_row_ranges = RowRanges::create_single(num_rows()); // 初始化zone_map_row_ranges为segment中所有的数据行
    // second filter data by zone map
    // 然后，根据zone map索引过滤数据
    for (auto& cid : cids) { // 依次遍历每一个condition列
        // get row ranges by zone map of this column,
        RowRanges column_row_ranges = RowRanges::create_single(num_rows()); // 初始化column_row_ranges为segment中所有的数据行
        CondColumn* column_cond = nullptr;
        if (_opts.conditions != nullptr) {
            column_cond = _opts.conditions->get_column(cid);
        }
        RETURN_IF_ERROR(
                _column_iterators[cid]->get_row_ranges_by_zone_map( // 通过当前列的column iterator，根据zone map索引过滤当前列的数据行，需要读取的row范围通过参数column_row_ranges传回
                        column_cond,
                        nullptr,
                        &column_row_ranges));
        // intersect different columns's row ranges to get final row ranges by zone map
        RowRanges::ranges_intersection(zone_map_row_ranges, column_row_ranges, &zone_map_row_ranges);// 将当前列zone map索引计算的row范围与其他列zone map索引计算的row范围求交集
    }

    // final filter data with delete conditions
    // 最后，根据delete condition过滤数据
    for (auto& delete_condition : _opts.delete_conditions) { // 依次遍历每一个delete condition
        RowRanges delete_condition_row_ranges = RowRanges::create_single(0); // 初始化delete_condition_row_ranges为0行
        for (auto& delete_column_condition : delete_condition->columns()) { // 依次遍历当前delete condition涉及到的每一列
            const int32_t cid = delete_column_condition.first; // 获取列id
            CondColumn* column_cond = nullptr;
            if (_opts.conditions != nullptr) {
                column_cond = _opts.conditions->get_column(cid);
            }
            RowRanges single_delete_condition_row_ranges = RowRanges::create_single(num_rows()); // 初始化single_delete_condition_row_ranges为segment中所有的数据行
            RETURN_IF_ERROR(
                    _column_iterators[cid]->get_row_ranges_by_zone_map( // 通过当前列的column iterator，根据zone map索引过滤当前列被删除的数据行，需要读取的row范围通过参数single_delete_condition_row_ranges传回
                            column_cond,
                            delete_column_condition.second,
                            &single_delete_condition_row_ranges));
            RowRanges::ranges_union(delete_condition_row_ranges, single_delete_condition_row_ranges, &delete_condition_row_ranges); // 将当前列delete condition过滤的row范围与其他列delete condition过滤的row范围求并集
        }
        RowRanges::ranges_intersection(zone_map_row_ranges, delete_condition_row_ranges, &zone_map_row_ranges);
    }

    DorisMetrics::instance()->segment_rows_read_by_zone_map.increment(zone_map_row_ranges.count());
    pre_size = condition_row_ranges->count();
    RowRanges::ranges_intersection(*condition_row_ranges, zone_map_row_ranges, condition_row_ranges);
    _opts.stats->rows_stats_filtered += (pre_size - condition_row_ranges->count());
    return Status::OK();
}

// filter rows by evaluating column predicates using bitmap indexes.
// upon return, predicates that've been evaluated by bitmap indexes are removed from _col_predicates.
/*使用位图索引（bitmap index）过滤数据行，并更新成员变量_row_bitmap*/
Status SegmentIterator::_apply_bitmap_index() {
    SCOPED_RAW_TIMER(&_opts.stats->bitmap_index_filter_timer);
    size_t input_rows = _row_bitmap.cardinality(); // 获取_row_bitmap中记录的需要读取的行数
    std::vector<ColumnPredicate*> remaining_predicates;

    for (auto pred : _col_predicates) { // 依次遍历每一个谓词列
        if (_bitmap_index_iterators[pred->column_id()] == nullptr) { // 判断当前列是否没有bitmap索引
            // no bitmap index for this column
            remaining_predicates.push_back(pred); // 当前列没有bitmap索引，将当前列保存在remaining_predicates中
        } else {
            RETURN_IF_ERROR(pred->evaluate(_schema, _bitmap_index_iterators, _segment->num_rows(), &_row_bitmap)); // 针对当前谓词列，使用位图索引（bitmap index）过滤数据行，并更新成员变量_row_bitmap
            if (_row_bitmap.isEmpty()) {
                break; // all rows have been pruned, no need to process further predicates
            }
        }
    }
    _col_predicates = std::move(remaining_predicates); // 使用remaining_predicates更新成员变量_col_predicates
    _opts.stats->rows_bitmap_index_filtered += (input_rows - _row_bitmap.cardinality());
    return Status::OK();
}

/*初始化返回列的iterator*/
Status SegmentIterator::_init_return_column_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid : _schema.column_ids()) { // 依次遍历schema中的每一列（schema中记录了需要返回的列字段）
        if (_column_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_column_iterator(cid, &_column_iterators[cid])); // 针对当前列创建ColumnIterator
            ColumnIteratorOptions iter_opts;
            iter_opts.stats = _opts.stats;
            iter_opts.use_page_cache = _opts.use_page_cache;
            iter_opts.rblock = _rblock.get();
            RETURN_IF_ERROR(_column_iterators[cid]->init(iter_opts)); // 初始化新创建的ColumnIterator
        }
    }
    return Status::OK();
}

/*初始化每一列的bitmap index iterator*/
Status SegmentIterator::_init_bitmap_index_iterators() {
    if (_cur_rowid >= num_rows()) {
        return Status::OK();
    }
    for (auto cid: _schema.column_ids()) { // 依次遍历schema中的每一列（schema中记录了需要返回的列字段）
        if (_bitmap_index_iterators[cid] == nullptr) {
            RETURN_IF_ERROR(_segment->new_bitmap_index_iterator(cid, &_bitmap_index_iterators[cid])); // 针对当前列创建BitmapIndexIterator
        }
    }
    return Status::OK();
}

// Schema of lhs and rhs are different.
// callers should assure that rhs' schema has all columns in lhs schema
template<typename LhsRowType, typename RhsRowType>
int compare_row_with_lhs_columns(const LhsRowType& lhs, const RhsRowType& rhs) {
    for (auto cid : lhs.schema()->column_ids()) {
        auto res = lhs.schema()->column(cid)->compare_cell(lhs.cell(cid), rhs.cell(cid));
        if (res != 0) {
            return res;
        }
    }
    return 0;
}

// look up one key to get its ordinal at which can get data.
// 'upper_bound' is defined the max ordinal the function will search.
// We use upper_bound to reduce search times.
// If we find a valid ordinal, it will be set in rowid and with Status::OK()
// If we can not find a valid key in this segment, we will set rowid to upper_bound
// Otherwise return error.
// 1. get [start, end) ordinal through short key index
// 2. binary search to find exact ordinal that match the input condition
// Make is_include template to reduce branch
/*查找参数传入的前缀索引（short key）值key在segment中的row id*/
Status SegmentIterator::_lookup_ordinal(const RowCursor& key, bool is_include,
                                        rowid_t upper_bound, rowid_t* rowid) {
    std::string index_key;
    encode_key_with_padding(&index_key, key, _segment->num_short_keys(), is_include); // 对前缀索引（short key）进行编码，通过参数index_key传回

    uint32_t start_block_id = 0;
    auto start_iter = _segment->lower_bound(index_key); // 获取segment中第一个等于（short key为index_key的行存在）或大于（short key为index_key的行不存在）index_key的行，ShortKeyIndexIterator类型的返回值start_iter指向这一行
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = _segment->last_block();
    }
    rowid_t start = start_block_id * _segment->num_rows_per_block(); // 获取参数传入的short key所在范围的起始行号

    rowid_t end = upper_bound;                                       // 获取参数传入的short key所在范围的结束行号
    auto end_iter = _segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * _segment->num_rows_per_block();
    }

    // binary search to find the exact key
    /*通过二分查找算法在start与end之间寻找参数传入的short key所在的行号*/
    while (start < end) {
        rowid_t mid = (start + end) / 2;
        RETURN_IF_ERROR(_seek_and_peek(mid));
        int cmp = compare_row_with_lhs_columns(key, _seek_block->row(0));
        if (cmp > 0) {
            start = mid + 1;
        } else if (cmp == 0) {
            if (is_include) {
                // lower bound
                end = mid;
            } else {
                // upper bound
                start = mid + 1;
            }
        } else {
            end = mid;
        }
    }

    *rowid = start;
    return Status::OK();
}

// seek to the row and load that row to _key_cursor
/*寻找_seek_schema的每一列中行号为参数传入的rowid的数据，并将其读取到成员变量_seek_block中*/
Status SegmentIterator::_seek_and_peek(rowid_t rowid) {
    RETURN_IF_ERROR(_seek_columns(_seek_schema->column_ids(), rowid)); // 通过列的iterator寻找参数column_ids中每一列中id为rowid的数据
    size_t num_rows = 1; // 设置读取数据的行数为1
    // please note that usually RowBlockV2.clear() is called to free MemPool memory before reading the next block,
    // but here since there won't be too many keys to seek, we don't call RowBlockV2.clear() so that we can use
    // a single MemPool for all seeked keys.
    RETURN_IF_ERROR(_read_columns(_seek_schema->column_ids(), _seek_block.get(), 0, num_rows)); // 读取参数column_ids中每一列的1行数据到block中
    _seek_block->set_num_rows(num_rows);
    return Status::OK();
}

/*初始化lazy materialization（延迟物化）*/
void SegmentIterator::_init_lazy_materialization() {
    if (!_col_predicates.empty()) {
        std::set<ColumnId> predicate_columns; // 保存谓词列的id
        for (auto predicate : _col_predicates) { // 遍历每一个谓词列
            predicate_columns.insert(predicate->column_id()); // 保存谓词列的id到变量predicate_columns
        }
        // when all return columns have predicates, disable lazy materialization to avoid its overhead
        if (_schema.column_ids().size() > predicate_columns.size()) {
            _lazy_materialization_read = true;
            _predicate_columns.assign(predicate_columns.cbegin(), predicate_columns.cend());
            for (auto cid : _schema.column_ids()) { // 遍历schema中的每一列
                if (predicate_columns.find(cid) == predicate_columns.end()) { // 判断当前列是否为谓词列
                    _non_predicate_columns.push_back(cid); // 将非谓词列添加到成员变量_non_predicate_columns中
                }
            }
        }
    }
}

/*获取参数column_ids中每一列的第pos行数据的ordinal*/
Status SegmentIterator::_seek_columns(const std::vector<ColumnId>& column_ids, rowid_t pos) {
    _opts.stats->block_seek_num += 1;
    SCOPED_RAW_TIMER(&_opts.stats->block_seek_ns);
    for (auto cid : column_ids) { // 依次遍历需要读取的每一列
        RETURN_IF_ERROR(_column_iterators[cid]->seek_to_ordinal(pos)); // 通过当前列的iterator寻找当前列中行号为pos的数据
    }
    return Status::OK();
}

/*读取参数column_ids中每一列的多行数据到block中*/
Status SegmentIterator::_read_columns(const std::vector<ColumnId>& column_ids,
                                      RowBlockV2* block,
                                      size_t row_offset,
                                      size_t nrows) {
    for (auto cid : column_ids) { // 依次遍历需要读取的每一列
        auto column_block = block->column_block(cid);   // 获取当前列在block中的位置
        ColumnBlockView dst(&column_block, row_offset); // 获取当前列的第row_offset在block中的位置
        size_t rows_read = nrows; // 获取需要读取的行数
        RETURN_IF_ERROR(_column_iterators[cid]->next_batch(&rows_read, &dst)); // 读取当前列的多行数据到block中
        block->set_delete_state(column_block.delete_state()); // 设置block的数据删除状态
        DCHECK_EQ(nrows, rows_read);
    }
    return Status::OK();
}

/*从segment中获取一个block*/
Status SegmentIterator::next_batch(RowBlockV2* block) {
    SCOPED_RAW_TIMER(&_opts.stats->block_load_ns);
    if (UNLIKELY(!_inited)) { // 判断SegmentIterator是否已经被初始化
        RETURN_IF_ERROR(_init()); // 会在第一次执行next_batch()函数时初始化SegmentIterator
        if (_lazy_materialization_read) { // 判断是否需要延迟物化
            _block_rowids.reserve(block->capacity());
        }
        _inited = true;
    }

    uint32_t nrows_read = 0;
    uint32_t nrows_read_limit = block->capacity(); // 设置读数据行数上限为block的容量
    _block_rowids.resize(nrows_read_limit);        // _block_rowids中保存读到block中的数据行的id
    const auto& read_columns = _lazy_materialization_read ? _predicate_columns : block->schema()->column_ids(); // 当需要延迟物化时，read_columns中只保存谓词列

    // phase 1: read rows selected by various index (indicated by _row_bitmap) into block
    // when using lazy-materialization-read, only columns with predicates are read
    // 读取数据行到block中，读取的数据行依赖于bitmap
    do {
        uint32_t range_from;
        uint32_t range_to;
        bool has_next_range = _range_iter->next_range(nrows_read_limit - nrows_read, &range_from, &range_to); // 获取下一个连续的bitmap范围
        if (!has_next_range) {
            break; //如果没有获取到bitmap范围，则循环结束
        }
        if (_cur_rowid == 0 || _cur_rowid != range_from) {
            _cur_rowid = range_from; // 更新成员变量
            RETURN_IF_ERROR(_seek_columns(read_columns, _cur_rowid)); // 获取参数read_columns中每一列的第pos行数据的ordinal
        }
        size_t rows_to_read = range_to - range_from; // 计算要读的数据行数
        RETURN_IF_ERROR(_read_columns(read_columns, block, nrows_read, rows_to_read)); // 读取参数read_columns中每一列的多行数据到block中
        _cur_rowid += rows_to_read; // 读取多行数据到block之后，更新成员变量_cur_rowid
        if (_lazy_materialization_read) {
            for (uint32_t rid = range_from; rid < range_to; rid++) {
                _block_rowids[nrows_read++] = rid;
            }
        } else {
            nrows_read += rows_to_read;
        }
    } while (nrows_read < nrows_read_limit); // 如果block的数据行数达到或超过上限，则循环退出

    block->set_num_rows(nrows_read); // 设置block的数据行数
    block->set_selected_size(nrows_read);
    if (nrows_read == 0) {
        return Status::EndOfFile("no more data in segment");
    }
    _opts.stats->raw_rows_read += nrows_read;
    _opts.stats->blocks_load += 1; // 更新block的加载次数

    // phase 2: run vectorization evaluation on remaining predicates to prune rows.
    // block's selection vector will be set to indicate which rows have passed predicates.
    // TODO(hkp): optimize column predicate to check column block once for one column
    // 根据谓词列对block中的数据行进行过滤
    if (!_col_predicates.empty()) {
        // init selection position index
        uint16_t selected_size = block->selected_size();
        uint16_t original_size = selected_size;
        SCOPED_RAW_TIMER(&_opts.stats->vec_cond_ns);
        for (auto column_predicate : _col_predicates) { // 依次遍历每一个谓词列
            auto column_block = block->column_block(column_predicate->column_id()); // 在block中获取当前谓词列的数据块
            column_predicate->evaluate(&column_block, block->selection_vector(), &selected_size); // 根据谓词列对block中的数据行进行过滤
        }
        block->set_selected_size(selected_size);
        block->set_num_rows(selected_size); // 更新block的数据行数
        _opts.stats->rows_vec_cond_filtered += original_size - selected_size;
    }

    // phase 3: read non-predicate columns of rows that have passed predicates
    // 如果需要延迟物化，则前面block中只读取了谓词列数据
    if (_lazy_materialization_read) {
        uint16_t i = 0;
        const uint16_t* sv = block->selection_vector();
        const uint16_t sv_size = block->selected_size();
        while (i < sv_size) {
            // i: start offset the current range
            // j: past the last offset of the current range
            uint16_t j = i + 1;
            while (j < sv_size && _block_rowids[sv[j]] == _block_rowids[sv[j - 1]] + 1) {
                ++j;
            }
            uint16_t range_size = j - i;
            RETURN_IF_ERROR(_seek_columns(_non_predicate_columns, _block_rowids[sv[i]]));// 获取参数_non_predicate_columns中每一列的第pos行数据的ordinal
            RETURN_IF_ERROR(_read_columns(_non_predicate_columns, block, sv[i], range_size));// 读取参数_non_predicate_columns中每一列的多行数据到block中
            i += range_size;
        }
    }
    return Status::OK();
}

}
}
