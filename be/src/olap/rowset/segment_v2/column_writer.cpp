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

#include "olap/rowset/segment_v2/column_writer.h"

#include <cstddef>

#include "common/logging.h"
#include "env/env.h"
#include "gutil/strings/substitute.h"
#include "olap/fs/block_manager.h"
#include "olap/rowset/segment_v2/bitmap_index_writer.h"
#include "olap/rowset/segment_v2/bloom_filter.h"
#include "olap/rowset/segment_v2/bloom_filter_index_writer.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/options.h"
#include "olap/rowset/segment_v2/ordinal_page_index.h"
#include "olap/rowset/segment_v2/page_builder.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/zone_map_index.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/rle_encoding.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

class NullBitmapBuilder {
public:
    NullBitmapBuilder() : _has_null(false), _bitmap_buf(512), _rle_encoder(&_bitmap_buf, 1) {
    }

    explicit NullBitmapBuilder(size_t reserve_bits)
        : _has_null(false), _bitmap_buf(BitmapSize(reserve_bits)), _rle_encoder(&_bitmap_buf, 1) {
    }

    void add_run(bool value, size_t run) {
        _has_null |= value;
        _rle_encoder.Put(value, run);
    }

    // Returns whether the building nullmap contains NULL
    bool has_null() const { return _has_null; }

    OwnedSlice finish() {
        _rle_encoder.Flush();
        return _bitmap_buf.build();
    }

    void reset() {
        _has_null = false;
        _rle_encoder.Clear();
    }

    uint64_t size() {
        return _bitmap_buf.size();
    }
private:
    bool _has_null;
    faststring _bitmap_buf;
    RleEncoder<bool> _rle_encoder;
};

/*ColumnWriter的构造函数*/
ColumnWriter::ColumnWriter(const ColumnWriterOptions& opts,
                           std::unique_ptr<Field> field,
                           fs::WritableBlock* wblock) :
        _opts(opts),
        _field(std::move(field)),
        _wblock(wblock),
        _is_nullable(_opts.meta->is_nullable()),
        _data_size(0) {
    // these opts.meta fields should be set by client
    DCHECK(opts.meta->has_column_id());
    DCHECK(opts.meta->has_unique_id());
    DCHECK(opts.meta->has_type());
    DCHECK(opts.meta->has_length());
    DCHECK(opts.meta->has_encoding());
    DCHECK(opts.meta->has_compression());
    DCHECK(opts.meta->has_is_nullable());
    DCHECK(wblock != nullptr);
}

/*ColumnWriter的析构函数*/
ColumnWriter::~ColumnWriter() {
    // delete all pages
    Page* page = _pages.head; // 获取第一个page
    while (page != nullptr) { // 依次遍历该列下的每一个page，并将其内存释放
        Page* next_page = page->next; // 获取下一个page
        delete page;
        page = next_page;
    }
}

/*初始化ColumnWriter*/
Status ColumnWriter::init() {
    RETURN_IF_ERROR(EncodingInfo::get(_field->type_info(), _opts.meta->encoding(), &_encoding_info)); // 获取编码信息
    _opts.meta->set_encoding(_encoding_info->encoding()); // 设置该列的编码
    // should store more concrete encoding type instead of DEFAULT_ENCODING
    // because the default encoding of a data type can be changed in the future
    DCHECK_NE(_opts.meta->encoding(), DEFAULT_ENCODING);

    RETURN_IF_ERROR(get_block_compression_codec(_opts.meta->compression(), &_compress_codec)); // 获取压缩类型

    // create page builder
    PageBuilder* page_builder = nullptr;
    PageBuilderOptions opts;
    opts.data_page_size = _opts.data_page_size;
    RETURN_IF_ERROR(_encoding_info->create_page_builder(opts, &page_builder)); // 创建page builder
    if (page_builder == nullptr) {
        return Status::NotSupported(
            Substitute("Failed to create page builder for type $0 and encoding $1",
                       _field->type(), _opts.meta->encoding()));
    }
    _page_builder.reset(page_builder); // 使用新创建的page builder初始化成员变量_page_builder
    // create ordinal builder
    _ordinal_index_builder.reset(new OrdinalIndexWriter()); // 创建OrdinalIndexWriter对象并初始化成员变量_ordinal_index_builder
    // create null bitmap builder
    if (_is_nullable) {
        _null_bitmap_builder.reset(new NullBitmapBuilder()); // 创建NullBitmapBuilder对象并初始化成员变量_null_bitmap_builder
    }
    if (_opts.need_zone_map) {
        _zone_map_index_builder.reset(new ZoneMapIndexWriter(_field.get())); // 创建ZoneMapIndexWriter对象并初始化成员变量_zone_map_index_builder
    }
    if (_opts.need_bitmap_index) {
        RETURN_IF_ERROR(BitmapIndexWriter::create(_field->type_info(), &_bitmap_index_builder));// 创建BitmapIndexWriter对象并初始化成员变量_bitmap_index_builder
    }
    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(BloomFilterIndexWriter::create(BloomFilterOptions(),
                _field->type_info(), &_bloom_filter_index_builder)); // 创建BloomFilterIndexWriter对象并初始化成员变量_bloom_filter_index_builder
    }
    return Status::OK();
}

/*在当前列中增加多行null值*/
Status ColumnWriter::append_nulls(size_t num_rows) {
    _null_bitmap_builder->add_run(true, num_rows); // 在当前列中增加多行null值
    _next_rowid += num_rows;  // 更新成员变量_next_rowid
    if (_opts.need_zone_map) {
        _zone_map_index_builder->add_nulls(num_rows); // 在zone map index中增加多行null值
    }
    if (_opts.need_bitmap_index) {
        _bitmap_index_builder->add_nulls(num_rows);   // 在bitmap index中增加多行null值
    }
    if (_opts.need_bloom_filter) {
        _bloom_filter_index_builder->add_nulls(num_rows); // 在bloom filter index中增加多行null值
    }
    return Status::OK();
}

/*向column writer中添加列数据，data为列数据的指针，num_rows为行数（多行中的同一列数据）*/
Status ColumnWriter::append(const void* data, size_t num_rows) {
    return _append_data((const uint8_t**)&data, num_rows);
}

// append data to page builder. this function will make sure that
// num_rows must be written before return. And ptr will be modified
// to next data should be written
/*向page builder和各个index builder中添加列数据*/
Status ColumnWriter::_append_data(const uint8_t** ptr, size_t num_rows) {
    size_t remaining = num_rows;
    while (remaining > 0) {
        size_t num_written = remaining;
        //向_page_builder中添加列数据，add()函数中会修改num_written值，表示当前page在这一次添加了多少行（当前page可能还没有添加完num_written行就已经满了，添加的是每一行中的一个列数据）
        RETURN_IF_ERROR(_page_builder->add(*ptr, &num_written));
        if (_opts.need_zone_map) {
            _zone_map_index_builder->add_values(*ptr, num_written); //向_zone_map_index_builder中添加列数据
        }
        if (_opts.need_bitmap_index) {
            _bitmap_index_builder->add_values(*ptr, num_written); //向_bitmap_index_builder中添加列数据
        }
        if (_opts.need_bloom_filter) {
            _bloom_filter_index_builder->add_values(*ptr, num_written); //向_bloom_filter_index_builder中添加列数据
        }

        bool is_page_full = (num_written < remaining); //如果num_written < remaining，则表示当前page已经满了（本次没有添加完所有的remaining行，当前page就已经满了，添加的是每一行中的一个列数据）
        remaining -= num_written;             //修改循环条件变量，同时也是下一次需要添加的行数
        _next_rowid += num_written;           //修正成员变量_next_rowid为下一次添加的数据的行id
        *ptr += _field->size() * num_written; //修正指针为下一次要写的数据的位置
        // we must write null bits after write data, because we don't
        // know how many rows can be written into current page
        if (_is_nullable) {
            _null_bitmap_builder->add_run(false, num_written);
        }

        if (is_page_full) {
            RETURN_IF_ERROR(_finish_current_page()); //如果当前page已经写满，对该page进行编码和压缩
        }
    }
    return Status::OK();
}

/*向column writer中追加列数据，数据可以为null，data为列数据的指针，num_rows为行数（多行中的同一列数据）*/
Status ColumnWriter::append_nullable(
        const uint8_t* is_null_bits, const void* data, size_t num_rows) {
    const uint8_t* ptr = (const uint8_t*)data;
    BitmapIterator null_iter(is_null_bits, num_rows);
    bool is_null = false;
    size_t this_run = 0;
    while ((this_run = null_iter.Next(&is_null)) > 0) {
        if (is_null) { // 数据为空
            _null_bitmap_builder->add_run(true, this_run); //向_null_bitmap_builder中添加多个null数据
            _next_rowid += this_run;
            if (_opts.need_zone_map) {
                _zone_map_index_builder->add_nulls(this_run); //向_zone_map_index_builder中添加多个null数据
            }
            if (_opts.need_bitmap_index) {
                _bitmap_index_builder->add_nulls(this_run); //向_bitmap_index_builder中添加多个null数据
            }
            if (_opts.need_bloom_filter) {
                _bloom_filter_index_builder->add_nulls(this_run); //向_bloom_filter_index_builder中添加多个null数据
            }
        } else {// 数据不为空
            RETURN_IF_ERROR(_append_data(&ptr, this_run)); //向page builder和各个index builder中添加列数据
        }
    }
    return Status::OK();
}

/*计算当前列的大小*/
uint64_t ColumnWriter::estimate_buffer_size() {
    uint64_t size = _data_size;
    size += _page_builder->size(); // 获取page数据大小
    if (_is_nullable) {
        size += _null_bitmap_builder->size(); // 获取 null bitmap index大小
    }
    size += _ordinal_index_builder->size(); // 获取ordinal index大小
    if (_opts.need_zone_map) {
        size += _zone_map_index_builder->size(); // 获取zone map index大小
    }
    if (_opts.need_bitmap_index) {
        size += _bitmap_index_builder->size(); // 获取bitmap index大小
    }
    if (_opts.need_bloom_filter) {
        size += _bloom_filter_index_builder->size(); // 获取bloom filter大小
    }
    return size;
}

/*当前列的数据已经写完，对最后一个page进行编码和压缩*/
Status ColumnWriter::finish() {
    return _finish_current_page(); // 当前列的最后一个page写入数据结束，对该page进行编码和压缩
}

/*将当前列的page数据写入文件*/
Status ColumnWriter::write_data() {
    Page* page = _pages.head; // 获取当前列的第一个page
    while (page != nullptr) { // 依次获取当前列的每一个page
        RETURN_IF_ERROR(_write_data_page(page)); // 将当前page写入文件
        page = page->next; // 获取当前列的下一个page
    }
    // write column dict
    if (_encoding_info->encoding() == DICT_ENCODING) { // 判断编码类型是否为字典编码
        OwnedSlice dict_body;
        RETURN_IF_ERROR(_page_builder->get_dictionary_page(&dict_body)); // 获取字典page

        PageFooterPB footer;
        footer.set_type(DICTIONARY_PAGE); // 设置page类型为字典页
        footer.set_uncompressed_size(dict_body.slice().get_size()); // 设置压缩之前的数据大小
        footer.mutable_dict_page_footer()->set_encoding(PLAIN_ENCODING); // 设置编码格式

        PagePointer dict_pp;
        RETURN_IF_ERROR(PageIO::compress_and_write_page(
                _compress_codec, _opts.compression_min_space_saving, _wblock,
                { dict_body.slice() }, footer, &dict_pp)); // 对字典page进行压缩，通过参数dict_pp返回
        dict_pp.to_proto(_opts.meta->mutable_dict_page()); // 将dict_pp转化为PB格式
    }
    return Status::OK();
}

/*向文件块中追加ordinal索引*/
Status ColumnWriter::write_ordinal_index() {
    return _ordinal_index_builder->finish(_wblock, _opts.meta->add_indexes());
}

/*向文件块中追加zonemap索引*/
Status ColumnWriter::write_zone_map() {
    if (_opts.need_zone_map) {
        return _zone_map_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

/*向文件块中追加bitmap索引*/
Status ColumnWriter::write_bitmap_index() {
    if (_opts.need_bitmap_index) {
        return _bitmap_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

/*向文件块中追加bloom filter索引*/
Status ColumnWriter::write_bloom_filter_index() {
    if (_opts.need_bloom_filter) {
        return _bloom_filter_index_builder->finish(_wblock, _opts.meta->add_indexes());
    }
    return Status::OK();
}

// write a data page into file and update ordinal index
/*将参数传入的page写入文件*/
Status ColumnWriter::_write_data_page(Page* page) {
    PagePointer pp;
    std::vector<Slice> compressed_body;
    for (auto& data : page->data) {
        compressed_body.push_back(data.slice()); // 获取page中压缩之后的数据
    }
    RETURN_IF_ERROR(PageIO::write_page(_wblock, compressed_body, page->footer, &pp)); // 将压缩之后的page数据写入文件
    _ordinal_index_builder->append_entry(page->footer.data_page_footer().first_ordinal(), pp); // 更新ordinal index
    return Status::OK();
}

/*当前列的一个page写入数据结束，对该page进行编码和压缩*/
Status ColumnWriter::_finish_current_page() {
    if (_next_rowid == _first_rowid) {
        return Status::OK();
    }

    if (_opts.need_zone_map) {
        RETURN_IF_ERROR(_zone_map_index_builder->flush()); // 将当前page的zone map index进行flush
    }

    if (_opts.need_bloom_filter) {
        RETURN_IF_ERROR(_bloom_filter_index_builder->flush()); // 将当前page的bloom filter index进行flush
    }

    // build data page body : encoded values + [nullmap]
    vector<Slice> body;
    OwnedSlice encoded_values = _page_builder->finish(); // 对当前page的数据进行编码
    _page_builder->reset(); // reset成员变量_page_builder
    body.push_back(encoded_values.slice()); // 将编码之后的page数据添加到body中

    OwnedSlice nullmap;
    if (_is_nullable && _null_bitmap_builder->has_null()) {
        nullmap = _null_bitmap_builder->finish();
        body.push_back(nullmap.slice()); // 将null bitmap添加到body中
    }
    if (_null_bitmap_builder != nullptr) {
        _null_bitmap_builder->reset(); // reset成员变量_null_bitmap_builder
    }

    // prepare data page footer
    std::unique_ptr<Page> page(new Page()); // 创建一个Page对象
    page->footer.set_type(DATA_PAGE); // 设置page footer的类型
    page->footer.set_uncompressed_size(Slice::compute_total_size(body)); // 设置当前page压缩之前的数据大小
    auto data_page_footer = page->footer.mutable_data_page_footer();
    data_page_footer->set_first_ordinal(_first_rowid); // 设置当前page的第一行的row id
    data_page_footer->set_num_values(_next_rowid - _first_rowid); // 设置当前page的行数
    data_page_footer->set_nullmap_size(nullmap.slice().size); // 设置当前page中null的数量

    // trying to compress page body
    OwnedSlice compressed_body;
    RETURN_IF_ERROR(PageIO::compress_page_body(
            _compress_codec, _opts.compression_min_space_saving, body, &compressed_body)); // 对当前page按照指定的压缩格式进行压缩
    if (compressed_body.slice().empty()) { // 判断数据是否未被压缩
        // page body is uncompressed
        page->data.emplace_back(std::move(encoded_values)); // 将编码之后的数据添加到page的data域中
        page->data.emplace_back(std::move(nullmap));        // 将nullmap添加到page的data域中
    } else {
        // page body is compressed
        page->data.emplace_back(std::move(compressed_body)); // 将压缩之后的数据添加到page的data域中
    }

    _push_back_page(page.release()); // 将当前page添加到成员变量_pages中
    _first_rowid = _next_rowid; // 将成员变量_first_rowid更新为_next_rowid的值，为下一个page作准备
    return Status::OK();
}

}
}
