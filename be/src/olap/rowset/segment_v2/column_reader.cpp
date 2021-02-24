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

#include "olap/rowset/segment_v2/column_reader.h"

#include "common/logging.h"
#include "gutil/strings/substitute.h" // for Substitute
#include "olap/rowset/segment_v2/encoding_info.h" // for EncodingInfo
#include "olap/rowset/segment_v2/page_handle.h" // for PageHandle
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h" // for PagePointer
#include "olap/types.h" // for TypeInfo
#include "olap/column_block.h" // for ColumnBlockView
#include "util/coding.h" // for get_varint32
#include "util/rle_encoding.h" // for RleDecoder
#include "util/block_compression.h"
#include "olap/rowset/segment_v2/binary_dict_page.h" // for BinaryDictPageDecoder
#include "olap/rowset/segment_v2/bloom_filter_index_reader.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

/*创建ColumnReader对象，通过参数reader传回*/
Status ColumnReader::create(const ColumnReaderOptions& opts,
                            const ColumnMetaPB& meta,
                            uint64_t num_rows,
                            const std::string& file_name,
                            std::unique_ptr<ColumnReader>* reader) {
    std::unique_ptr<ColumnReader> reader_local(
        new ColumnReader(opts, meta, num_rows, file_name)); // 创建ColumnReader对象
    RETURN_IF_ERROR(reader_local->init()); // 初始化ColumnReader对象
    *reader = std::move(reader_local);
    return Status::OK();
}

/*ColumnReader的构造函数*/
ColumnReader::ColumnReader(const ColumnReaderOptions& opts,
                           const ColumnMetaPB& meta,
                           uint64_t num_rows,
                           const std::string& file_name)
        : _opts(opts), _meta(meta), _num_rows(num_rows), _file_name(file_name) {
}

ColumnReader::~ColumnReader() = default;

/*ColumnReader初始化*/
Status ColumnReader::init() {
    _type_info = get_type_info((FieldType)_meta.type()); // 获取当前列的类型
    if (_type_info == nullptr) {
        return Status::NotSupported(Substitute("unsupported typeinfo, type=$0", _meta.type()));
    }
    RETURN_IF_ERROR(EncodingInfo::get(_type_info, _meta.encoding(), &_encoding_info)); // 根据该列的TypeInfo和EncodingTypePB获取编码信息，并保存在成员变量_encoding_info中
    RETURN_IF_ERROR(get_block_compression_codec(_meta.compression(), &_compress_codec)); // 根据该列的压缩类型获取压缩编码，并保存在成员变量_compress_codec中

    for (int i = 0; i < _meta.indexes_size(); i++) { // 依次访问该列的每一种索引类型
        auto& index_meta = _meta.indexes(i); // 获取索引元数据
        switch (index_meta.type()) {
        case ORDINAL_INDEX:
            _ordinal_index_meta = &index_meta.ordinal_index();   // 获取该列的ordinal index的元数据
            break;
        case ZONE_MAP_INDEX:
            _zone_map_index_meta = &index_meta.zone_map_index(); // 获取该列的zone map index的元数据
            break;
        case BITMAP_INDEX:
            _bitmap_index_meta = &index_meta.bitmap_index();     // 获取该列的bitmap index的元数据
            break;
        case BLOOM_FILTER_INDEX:
            _bf_index_meta = &index_meta.bloom_filter_index();   // 获取该列的bloom filter index的元数据
            break;
        default:
            return Status::Corruption(Substitute(
                    "Bad file $0: invalid column index type $1", _file_name, index_meta.type()));
        }
    }
    if (_ordinal_index_meta == nullptr) { // 判断是否ordinal index的元数据缺失
        return Status::Corruption(Substitute(
                "Bad file $0: missing ordinal index for column $1", _file_name, _meta.column_id()));
    }
    return Status::OK();
}

/*创建FileColumnIterator对象，通过参数iterator传回*/
Status ColumnReader::new_iterator(ColumnIterator** iterator) {
    *iterator = new FileColumnIterator(this); // 创建FileColumnIterator对象，FileColumnIterator被用来从segment文件中读取列数据
    return Status::OK();
}

/*创建BitmapIndexIterator，通过参数iterator传回*/
Status ColumnReader::new_bitmap_index_iterator(BitmapIndexIterator** iterator) {
    RETURN_IF_ERROR(_ensure_index_loaded()); // 加载索引信息，如果索引信息已经被加载，则不会重复加载
    RETURN_IF_ERROR(_bitmap_index->new_iterator(iterator)); // 创建BitmapIndexIterator
    return Status::OK();
}

/*读取并解压一个page的数据*/
Status ColumnReader::read_page(const ColumnIteratorOptions& iter_opts, const PagePointer& pp,
                               PageHandle* handle, Slice* page_body, PageFooterPB* footer) {
    iter_opts.sanity_check();
    PageReadOptions opts;
    opts.rblock = iter_opts.rblock;
    opts.page_pointer = pp;
    opts.codec = _compress_codec;
    opts.stats = iter_opts.stats;
    opts.verify_checksum = _opts.verify_checksum;
    opts.use_page_cache = iter_opts.use_page_cache;
    opts.kept_in_memory = _opts.kept_in_memory;

    return PageIO::read_and_decompress_page(opts, handle, page_body, footer); // 读取并解压一个page的数据
}

/*根据zone map获取row范围*/
Status ColumnReader::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                CondColumn* delete_condition,
                                                std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                                RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded()); // 加载索引信息，如果索引信息已经被加载，则不会重复加载

    std::vector<uint32_t> page_indexes;
    RETURN_IF_ERROR(_get_filtered_pages(cond_column, delete_condition, delete_partial_filtered_pages, &page_indexes)); // 使用condition和delete_condition对当前列的page进行过滤，留下需要读取的page（通过参数page_indexes传回，并将存在部分数据被删除的page通过参数delete_partial_filtered_pages传回）
    RETURN_IF_ERROR(_calculate_row_ranges(page_indexes, row_ranges)); // 根据参数传入的需要读取的page，计算需要读取的行范围，通过参数row_ranges返回
    return Status::OK();
}

/*判断segment当前列的zone map范围是否满足condition*/
bool ColumnReader::match_condition(CondColumn* cond) const {
    if (_zone_map_index_meta == nullptr || cond == nullptr) {
        return true;
    }
    FieldType type = _type_info->type(); // 获取当前列的数据类型
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length())); // 根据当前列的数据类型创建变量，用于保存zone map的最小值
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length())); // 根据当前列的数据类型创建变量，用于保存zone map的最大值
    _parse_zone_map(_zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get()); // 解析zone map，根据当前page的zone map获取该page的最小值和最大值
    return _zone_map_match_condition(
            _zone_map_index_meta->segment_zone_map(), min_value.get(), max_value.get(), cond); // 判断整个segment当前列的zone map范围是否满足condition
}

/*解析zone map，根据当前page的zone map获取该page的最小值和最大值*/
void ColumnReader::_parse_zone_map(const ZoneMapPB& zone_map,
                         WrapperField* min_value_container,
                         WrapperField* max_value_container) const {
    // min value and max value are valid if has_not_null is true
    if (zone_map.has_not_null()) {
        min_value_container->from_string(zone_map.min()); // 获取zone map的最小值
        max_value_container->from_string(zone_map.max()); // 获取zone map的最大值
    }
    // for compatible original Cond eval logic
    // TODO(hkp): optimize OlapCond
    if (zone_map.has_null()) {
        // for compatible, if exist null, original logic treat null as min
        min_value_container->set_null(); // 如果存在null值，则将zone map的最小值设置为null
        if (!zone_map.has_not_null()) {
            // for compatible OlapCond's 'is not null'
            max_value_container->set_null(); // 如果存在null值，同时存在非null值，则将zone map的最大值设置为null
        }
    }
}

/*判断当前的zone map范围是否满足condition*/
bool ColumnReader::_zone_map_match_condition(const ZoneMapPB& zone_map,
                                             WrapperField* min_value_container,
                                             WrapperField* max_value_container,
                                             CondColumn* cond) const {
    if (!zone_map.has_not_null() && !zone_map.has_null()) { // 判断当前的zone map中没有数据
        return false; // no data in this zone
    }

    if (cond == nullptr) { // 判断condition是否为空
        return true;
    }

    return cond->eval({min_value_container, max_value_container}); // 判断当前的zone map范围是否满足condition
}

/*使用condition和delete_condition对当前列的page进行过滤，留下需要读取的page（通过参数page_indexes传回，并将存在部分数据被删除的page通过参数delete_partial_filtered_pages传回）*/
Status ColumnReader::_get_filtered_pages(CondColumn* cond_column,
                                         CondColumn* delete_condition,
                                         std::unordered_set<uint32_t>* delete_partial_filtered_pages,
                                         std::vector<uint32_t>* page_indexes) {
    FieldType type = _type_info->type(); // 获取该列的数据类型
    const std::vector<ZoneMapPB>& zone_maps = _zone_map_index->page_zone_maps(); // 根据zone map索引获取所有page的zone map
    int32_t page_size = _zone_map_index->num_pages(); // 获取含有zone map索引的page数目
    std::unique_ptr<WrapperField> min_value(WrapperField::create_by_type(type, _meta.length())); // 根据当前列的数据类型创建变量，用于保存zone map的最小值
    std::unique_ptr<WrapperField> max_value(WrapperField::create_by_type(type, _meta.length())); // 根据当前列的数据类型创建变量，用于保存zone map的最大值
    for (int32_t i = 0; i < page_size; ++i) { // 依次遍历每一个含有zone map索引的page
        _parse_zone_map(zone_maps[i], min_value.get(), max_value.get()); // 解析zone map，根据当前page的zone map获取该page的最小值和最大值
        if (_zone_map_match_condition(zone_maps[i], min_value.get(), max_value.get(), cond_column)) { // 判断当前的zone map范围是否满足condition
            bool should_read = true;
            if (delete_condition != nullptr) {
                int state = delete_condition->del_eval({min_value.get(), max_value.get()}); // 判断min_value与max_value之间的数据是否满足delete_condition
                if (state == DEL_SATISFIED) {
                    should_read = false; // 如果min_value与max_value之间的数据均被删除了，则该page可以不用读，直接跳过
                } else if (state == DEL_PARTIAL_SATISFIED) {
                    delete_partial_filtered_pages->insert(i); // 如果min_value与max_value之间的数据部分被删除了，则该page需要被读取，将该page添加到delete_partial_filtered_pages中，通过函数参数返回
                }
            }
            if (should_read) { // 如果该page需要读取，则将该page添加到page_indexes中，通过函数参数返回
                page_indexes->push_back(i);
            }
        }
    }
    return Status::OK();
}

/*根据参数传入的需要读取的page，计算需要读取的行范围，通过参数row_ranges返回*/
Status ColumnReader::_calculate_row_ranges(const std::vector<uint32_t>& page_indexes, RowRanges* row_ranges) {
    row_ranges->clear();
    for (auto i : page_indexes) { // 依次遍历page_indexes中每一个需要读取的page
        ordinal_t page_first_id = _ordinal_index->get_first_ordinal(i); // 获取当前page中第一行的id
        ordinal_t page_last_id = _ordinal_index->get_last_ordinal(i);   // 获取当前page中最后一行的id
        RowRanges page_row_ranges(RowRanges::create_single(page_first_id, page_last_id + 1)); // 根据当前page中第一行的id和最后一行的id创建行范围
        RowRanges::ranges_union(*row_ranges, page_row_ranges, row_ranges); // 将当前page的行范围与其他page的行范围进行合并
    }
    return Status::OK();
}

/*根据bloom filter获取row范围*/
// BloomFilter按Page粒度生成，在数据写入一个完整的Page时，Doris会根据Hash策略同时生成这个Page的BloomFilter索引数据。
Status ColumnReader::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    RETURN_IF_ERROR(_ensure_index_loaded()); // 加载索引信息，如果索引信息已经被加载，则不会重复加载
    RowRanges bf_row_ranges;
    std::unique_ptr<BloomFilterIndexIterator> bf_iter;
    RETURN_IF_ERROR(_bloom_filter_index->new_iterator(&bf_iter)); // 创建BloomFilterIndexIterator对象
    size_t range_size = row_ranges->range_size(); // 获取row_ranges中连续row范围的个数
    // get covered page ids
    std::set<uint32_t> page_ids;
    for (int i = 0; i < range_size; ++i) { // 依次遍历row_ranges中连续row范围的个数
        int64_t from = row_ranges->get_range_from(i); // 获取当前row范围的起始值
        int64_t idx = from;
        int64_t to = row_ranges->get_range_to(i);     // 获取当前row范围的结束值
        auto iter = _ordinal_index->seek_at_or_before(from); // 寻找当前row范围的起始值所在的page，每个page都对应一个ordinal索引项。ordinal index记录了每个page的位置offset、大小size和第一个数据项行号信息，即ordinal
        while (idx < to) { // page的最后一行超过当前row范围的结束值
            page_ids.insert(iter.page_index()); // 将当前page添加到page_ids中
            idx = iter.last_ordinal() + 1;
            iter.next(); // 访问下一个page
        }
    }
    for (auto& pid : page_ids) { // 依次遍历中的每一个page
        std::unique_ptr<BloomFilter> bf;
        RETURN_IF_ERROR(bf_iter->read_bloom_filter(pid, &bf)); // 读取当前page的bloom filter
        if (cond_column->eval(bf.get())) { // 判断当前page的bloom filter是否满足condition
            bf_row_ranges.add(RowRange(_ordinal_index->get_first_ordinal(pid), // 根据ordinal index获取当前page的row范围，并将获取到的row范围添加到bf_row_ranges
                    _ordinal_index->get_last_ordinal(pid) + 1)); // _ordinal_index->get_first_ordinal(pid)表示page的第一行，_ordinal_index->get_last_ordinal(pid)表示page的最后一行
        }
    }
    RowRanges::ranges_intersection(*row_ranges, bf_row_ranges, row_ranges); // 将bloom filter生成的row范围和参数传入的row范围取交集
    return Status::OK();
}

/*加载ordinal index*/
Status ColumnReader::_load_ordinal_index(bool use_page_cache, bool kept_in_memory) {
    DCHECK(_ordinal_index_meta != nullptr);
    _ordinal_index.reset(new OrdinalIndexReader(_file_name, _ordinal_index_meta, _num_rows)); // 创建OrdinalIndexReader对象
    return _ordinal_index->load(use_page_cache, kept_in_memory); // 加载ordinal索引
}

/*加载zone map index*/
Status ColumnReader::_load_zone_map_index(bool use_page_cache, bool kept_in_memory) {
    if (_zone_map_index_meta != nullptr) {
        _zone_map_index.reset(new ZoneMapIndexReader(_file_name, _zone_map_index_meta)); // 创建ZoneMapIndexReader对象
        return _zone_map_index->load(use_page_cache, kept_in_memory); // 加载zone map索引
    }
    return Status::OK();
}

/*加载bitmap index*/
Status ColumnReader::_load_bitmap_index(bool use_page_cache, bool kept_in_memory) {
    if (_bitmap_index_meta != nullptr) {
        _bitmap_index.reset(new BitmapIndexReader(_file_name, _bitmap_index_meta)); // 创建BitmapIndexReader对象
        return _bitmap_index->load(use_page_cache, kept_in_memory); // 加载bitmap map索引
    }
    return Status::OK();
}

/*加载bloom filter index*/
Status ColumnReader::_load_bloom_filter_index(bool use_page_cache, bool kept_in_memory) {
    if (_bf_index_meta != nullptr) {
        _bloom_filter_index.reset(new BloomFilterIndexReader(_file_name, _bf_index_meta)); // 创建BloomFilterIndexReader对象
        return _bloom_filter_index->load(use_page_cache, kept_in_memory); // 加载bloom filter索引
    }
    return Status::OK();
}

/*获取当前列的第一个page的OrdinalPageIndexIterator对象*/
Status ColumnReader::seek_to_first(OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded()); // 加载索引信息，如果索引信息已经被加载，则不会重复加载
    *iter = _ordinal_index->begin(); // 获取当前列的第一个page的OrdinalPageIndexIterator对象
    if (!iter->valid()) {
        return Status::NotFound("Failed to seek to first rowid");
    }
    return Status::OK();
}

/*寻找参数ordinal所在的page,通过参数iter传回*/
Status ColumnReader::seek_at_or_before(ordinal_t ordinal, OrdinalPageIndexIterator* iter) {
    RETURN_IF_ERROR(_ensure_index_loaded()); // 加载索引信息，如果索引信息已经被加载，则不会重复加载
    *iter = _ordinal_index->seek_at_or_before(ordinal); // 寻找参数ordinal所在的page，OrdinalPageIndexIterator类型的iter会指向找到的page
    if (!iter->valid()) {
        return Status::NotFound(Substitute("Failed to seek to ordinal $0, ", ordinal));
    }
    return Status::OK();
}

/*FileColumnIterator构造函数*/
FileColumnIterator::FileColumnIterator(ColumnReader* reader) : _reader(reader) {
}

FileColumnIterator::~FileColumnIterator() = default;

/*寻找当前列的第一个page，并寻找page中的第一行数据*/
Status FileColumnIterator::seek_to_first() {
    RETURN_IF_ERROR(_reader->seek_to_first(&_page_iter)); // 通过column reader获取当前列的第一个page
    RETURN_IF_ERROR(_read_data_page(_page_iter)); // 读取当前列中_page_iter指向的page，保存在成员变量_page中

    _seek_to_pos_in_page(_page.get(), 0); // 更新_page中的offset_in_page值为0，寻找page中的第一行数据
    _current_ordinal = 0; // 更新成员变量_current_ordinal（记录当前列中当前访问的行id）为0
    return Status::OK();
}

/*寻找当前列中行号为参数传入的ord的数据*/
Status FileColumnIterator::seek_to_ordinal(ordinal_t ord) {
    // if current page contains this row, we don't need to seek
    if (_page == nullptr || !_page->contains(ord)) { // 判断当前page中是否包含参数传入的行号ord
        RETURN_IF_ERROR(_reader->seek_at_or_before(ord, &_page_iter)); // 通过column reader寻找行号为ord的数据行所在的page，通过参数_page_iter传回
        RETURN_IF_ERROR(_read_data_page(_page_iter)); // 读取_page_iter所指向的page，其中会更新当前page为_page_iter所指向的page（更新成员变量_page）
    }
    _seek_to_pos_in_page(_page.get(), ord - _page->first_ordinal); // 在当前page中寻找行号为ord的数据行
    _current_ordinal = ord; // 更新成员变量_current_ordinal（记录当前列中当前访问的行id）为ord
    return Status::OK();
}

/*寻找page中的第offset_in_page行数据，更新page的成员变量offset_in_page值为参数传入的offset_in_page*/
void FileColumnIterator::_seek_to_pos_in_page(ParsedPage* page, ordinal_t offset_in_page) {
    if (page->offset_in_page == offset_in_page) { // page的成员变量offset_in_page记录page中当前访问行的id
        // fast path, do nothing
        return;                // 如果page中的当前访问行的id与参数传入的offset_in_page相同，则直接返回
    }

    ordinal_t pos_in_data = offset_in_page;
    if (_page->has_null) {
        ordinal_t offset_in_data = 0;
        ordinal_t skips = offset_in_page;

        if (offset_in_page > page->offset_in_page) {
            // forward, reuse null bitmap
            skips = offset_in_page - page->offset_in_page;
            offset_in_data = page->data_decoder->current_index();
        } else {
            // rewind null bitmap, and
            page->null_decoder = RleDecoder<bool>((const uint8_t*)page->null_bitmap.data, page->null_bitmap.size, 1);
        }

        auto skip_nulls = page->null_decoder.Skip(skips);
        pos_in_data = offset_in_data + skips - skip_nulls;
    }

    page->data_decoder->seek_to_position_in_page(pos_in_data); // 通过page的data_decoder寻找page中的第pos_in_data行
    page->offset_in_page = offset_in_page; // 更新page的成员变量offset_in_page
}

/*读取当前列的多行(batch)数据到segment iterator的block（column block）中*/
Status FileColumnIterator::next_batch(size_t* n, ColumnBlockView* dst) {
    size_t remaining = *n;
    while (remaining > 0) {
        if (!_page->has_remaining()) { // 判断当前page中是否还有待读取的数据行
            bool eos = false;
            RETURN_IF_ERROR(_load_next_page(&eos)); // 加载下一个page
            if (eos) {
                break;
            }
        }

        auto iter = _delete_partial_statisfied_pages.find(_page->page_index); // 判断当前page是否存在部分数据被删除
        bool is_partial = iter != _delete_partial_statisfied_pages.end();
        if (is_partial) {
            dst->column_block()->set_delete_state(DEL_PARTIAL_SATISFIED); // 设置当前列本次数据读取的数据部分删除状态
        } else {
            dst->column_block()->set_delete_state(DEL_NOT_SATISFIED);
        }
        // number of rows to be read from this page
        size_t nrows_in_page = std::min(remaining, _page->remaining()); // 计算需要从当前page中读取的数据行数
        size_t nrows_to_read = nrows_in_page;
        if (_page->has_null) { // page中存在null值
            // when this page contains NULLs we read data in some runs
            // first we read null bits in the same value, if this is null, we
            // don't need to read value from page.
            // If this is not null, we read data from page in batch.
            // This would be bad in case that data is arranged one by one, which
            // will lead too many function calls to PageDecoder
            while (nrows_to_read > 0) {
                bool is_null = false;
                size_t this_run = _page->null_decoder.GetNextRun(&is_null, nrows_to_read); // 从当前page中读取一次数据，返回的this_run表示本次读取的行数，本次读取的this_run行数据是否为null值通过参数is_null传回，本次读取的最大数据行数通过参数nrows_to_read限定
                // we use num_rows only for CHECK
                size_t num_rows = this_run;
                if (!is_null) { // 如果本次读取的当前page中this_run行数据不是null值
                    RETURN_IF_ERROR(_page->data_decoder->next_batch(&num_rows, dst)); // 从当前page中读取num_rows行数据到column block中
                    DCHECK_EQ(this_run, num_rows);
                }

                // set null bits
                dst->set_null_bits(this_run, is_null); // 设置本次读取的当前page中this_run行数据是否为null

                nrows_to_read -= this_run;         // 本次读取当前page中this_run行数据之后，更新变量nrows_to_read（待读取的行数）
                _page->offset_in_page += this_run; // 本次读取当前page中this_run行数据之后，更新page的当前行
                dst->advance(this_run);            // 本次读取当前page中this_run行数据之后，将column block指针后移this_run行
                _current_ordinal += this_run;      // 本次读取当前page中this_run行数据之后，当前列的当前行后移nrows_to_read位
            }
        } else {
            RETURN_IF_ERROR(_page->data_decoder->next_batch(&nrows_to_read, dst)); // 从当前page中读取nrows_to_read行数据到column block中
            DCHECK_EQ(nrows_to_read, nrows_in_page);

            if (dst->is_nullable()) {
                dst->set_null_bits(nrows_to_read, false);
            }

            _page->offset_in_page += nrows_to_read; // 当前page的当前行后移nrows_to_read位
            dst->advance(nrows_to_read);            // column block的数据指针后移nrows_to_read位
            _current_ordinal += nrows_to_read;      // 当前列的当前行后移nrows_to_read位
        }
        remaining -= nrows_in_page; // 读完一个page的数据之后，更新变量remaining
    }
    *n -= remaining; // 更新本次从当前列真实读取的行数。有可能读完当前列中最后一个page数据，所读取的行数还不足参数传入的 *n
    // TODO(hkp): for string type, the bytes_read should be passed to page decoder
    // bytes_read = data size + null bitmap size
    _opts.stats->bytes_read += *n * dst->type_info()->size() + BitmapSize(*n);
    return Status::OK();
}

/*加载下一个page*/
Status FileColumnIterator::_load_next_page(bool* eos) {
    _page_iter.next(); // _page_iter指向下一个page
    if (!_page_iter.valid()) { // 判断当前列是否存在下一个page
        *eos = true;
        return Status::OK();
    }

    RETURN_IF_ERROR(_read_data_page(_page_iter)); // 读取当前列中_page_iter指向的page，并使用读取的page数据更新成员变量_page
    _seek_to_pos_in_page(_page.get(), 0); // 寻找当前page的第一行数据
    *eos = false;
    return Status::OK();
}

/*读取当前列中iter指向的page，保存在成员变量_page中*/
Status FileColumnIterator::_read_data_page(const OrdinalPageIndexIterator& iter) {
    PageHandle handle;
    Slice page_body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_reader->read_page(_opts, iter.page(), &handle, &page_body, &footer)); // 通过column reader读取并解压一个page的数据
    // parse data page
    RETURN_IF_ERROR(ParsedPage::create(
            std::move(handle), page_body, footer.data_page_footer(), _reader->encoding_info(),
            iter.page(), iter.page_index(), &_page));                                      // 解析读取的page数据，通过参数_page传回

    // dictionary page is read when the first data page that uses it is read,
    // this is to optimize the memory usage: when there is no query on one column, we could
    // release the memory of dictionary page.
    // note that concurrent iterators for the same column won't repeatedly read dictionary page
    // because of page cache.
    // 第一次读取当前列的page时，会读一次字典page。
    if (_reader->encoding_info()->encoding() == DICT_ENCODING) { // 判断当前列是否是字典编码
        auto dict_page_decoder = reinterpret_cast<BinaryDictPageDecoder*>(_page->data_decoder);
        if (dict_page_decoder->is_dict_encoding()) { // 判断当前page的的data decoder是否是字典编码
            if (_dict_decoder == nullptr) {
                // read dictionary page
                Slice dict_data;
                PageFooterPB dict_footer;
                RETURN_IF_ERROR(_reader->read_page(
                        _opts, _reader->get_dict_page_pointer(), // 通过column reader获取字典page的指针
                        &_dict_page_handle, &dict_data, &dict_footer)); // 通过column reader读取字典page
                // ignore dict_footer.dict_page_footer().encoding() due to only
                // PLAIN_ENCODING is supported for dict page right now
                _dict_decoder.reset(new BinaryPlainPageDecoder(dict_data)); // 创建字典page的decoder，并初始化成员变量_dict_decoder
                RETURN_IF_ERROR(_dict_decoder->init()); // 初始化字典page的decoder
            }
            dict_page_decoder->set_dict_decoder(_dict_decoder.get());
        }
    }
    return Status::OK();
}

/*根据zone map获取row范围*/
Status FileColumnIterator::get_row_ranges_by_zone_map(CondColumn* cond_column,
                                                      CondColumn* delete_condition,
                                                      RowRanges* row_ranges) {
    if (_reader->has_zone_map()) { // 根据column reader判断当前列是否存在zone map索引
        RETURN_IF_ERROR(_reader->get_row_ranges_by_zone_map(cond_column, delete_condition,
                &_delete_partial_statisfied_pages, row_ranges)); // 根据zone map获取row范围
    }
    return Status::OK();
}

/*根据bloom filter获取row范围*/
Status FileColumnIterator::get_row_ranges_by_bloom_filter(CondColumn* cond_column, RowRanges* row_ranges) {
    if (cond_column != nullptr &&
            cond_column->can_do_bloom_filter() && _reader->has_bloom_filter_index()) { // 判断是否存在condition可以使用bloom filter（只有运算符为=、in或is时，可以使用bloom filter），并且根据column reader判断当前列是否存在bloom filter索引
        RETURN_IF_ERROR(_reader->get_row_ranges_by_bloom_filter(cond_column, row_ranges)); // 根据bloom filter获取row范围
    }
    return Status::OK();
}

/*初始化DefaultValueColumnIterator*/
Status DefaultValueColumnIterator::init(const ColumnIteratorOptions& opts) {
    _opts = opts;
    // be consistent with segment v1
    // if _has_default_value, we should create default column iterator for this column, and
    // "NULL" is a special default value which means the default value is null.
    if (_has_default_value) { // 判断当前列是否有默认值
        if (_default_value == "NULL") {
            DCHECK(_is_nullable);
            _is_default_value_null = true; // 设置默认值为null的标志
        } else {
            // 如果默认值不为null，则根据成员变量_default_value以及当前列的数据类型为当前列设置默认值
            TypeInfo* type_info = get_type_info(_type); // 根据当前列的数据类型(_type)获取该数据类型的相关信息
            _type_size = type_info->size(); // 获取当前列的数据类型的长度
            _mem_value = reinterpret_cast<void*>(_pool->allocate(_type_size)); // 根据当前列的数据类型的长度分配内存空间，用于保存默认值
            OLAPStatus s = OLAP_SUCCESS;
            if (_type == OLAP_FIELD_TYPE_CHAR) {
                int32_t length = _schema_length; // 获取当前列的数据类型长度
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length)); // 根据当前列的数据类型的长度分配内存空间
                memset(string_buffer, 0, length);
                memory_copy(string_buffer, _default_value.c_str(), _default_value.length()); // 将默认值copy到变量string_buffer所在的内存空间
                ((Slice*)_mem_value)->size = length;        // 更新成员变量_mem_value的size域，用于保存当前列数据默认值的数据长度
                ((Slice*)_mem_value)->data = string_buffer; // 使用变量string_buffer更新成员变量_mem_value的data域，用于保存当前列数据默认值
            } else if ( _type == OLAP_FIELD_TYPE_VARCHAR ||
                _type == OLAP_FIELD_TYPE_HLL ||
                _type == OLAP_FIELD_TYPE_OBJECT ) {
                int32_t length = _default_value.length();
                char* string_buffer = reinterpret_cast<char*>(_pool->allocate(length));
                memory_copy(string_buffer, _default_value.c_str(), length);
                ((Slice*)_mem_value)->size = length;
                ((Slice*)_mem_value)->data = string_buffer;
            } else {
                s = type_info->from_string(_mem_value, _default_value);
            }
            if (s != OLAP_SUCCESS) {
                return Status::InternalError(
                        strings::Substitute("get value of type from default value failed. status:$0", s));
            }
        }
    } else if (_is_nullable) {
        // if _has_default_value is false but _is_nullable is true, we should return null as default value.
        // 如果当前列数据没有默认值，但是可以为null，则将null作为默认值
        _is_default_value_null = true;
    } else { // 当前列没有默认值
        return Status::InternalError("invalid default value column for no default value and not nullable");
    }
    return Status::OK();
}

/*读取当前列的多行(batch)数据到segment iterator的block（column block）中*/
Status DefaultValueColumnIterator::next_batch(size_t* n, ColumnBlockView* dst) {
    if (dst->is_nullable()) {
        dst->set_null_bits(*n, _is_default_value_null); // 将column block中的*n行数据设置为null
    }

    if (_is_default_value_null) {
        dst->advance(*n); // 如果默认值为null，column block的数据指针后移*n位（前一步已经将column block中的*n行数据设置为null）
    } else {
        for (int i = 0; i < *n; ++i) { // 如果默认值不为null
            memcpy(dst->data(), _mem_value, _type_size); // 将当前列的默认值copy到column block中
            dst->advance(1); // column block的数据指针后移1位
        }
    }
    return Status::OK();
}

}
}
