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

#include "olap/rowset/segment_v2/segment.h"

#include "common/logging.h" // LOG
#include "gutil/strings/substitute.h"
#include "olap/fs/fs_util.h"
#include "olap/rowset/segment_v2/column_reader.h" // ColumnReader
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "util/slice.h" // Slice
#include "olap/tablet_schema.h"
#include "util/crc32c.h"

namespace doris {
namespace segment_v2 {

using strings::Substitute;

/*打开segment文件*/
Status Segment::open(std::string filename,
                     uint32_t segment_id,
                     const TabletSchema* tablet_schema,
                     std::shared_ptr<Segment>* output) {
    std::shared_ptr<Segment> segment(new Segment(std::move(filename), segment_id, tablet_schema));
    RETURN_IF_ERROR(segment->_open()); // 打开segment文件
    output->swap(segment);
    return Status::OK();
}

/*Segment的构造函数*/
Segment::Segment(
        std::string fname, uint32_t segment_id,
        const TabletSchema* tablet_schema)
        : _fname(std::move(fname)),
        _segment_id(segment_id),
        _tablet_schema(tablet_schema) {
}

Segment::~Segment() = default;

/*打开segment文件*/
Status Segment::_open() {
    RETURN_IF_ERROR(_parse_footer()); // 解析segment文件的footer信息
    RETURN_IF_ERROR(_create_column_readers()); // 创建 column reader
    return Status::OK();
}

/*针对当前segment文件创建SegmentIterator*/
Status Segment::new_iterator(const Schema& schema,
                             const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* iter) {
    DCHECK_NOTNULL(read_options.stats);
    // trying to prune the current segment by segment-level zone map
    if (read_options.conditions != nullptr) {
        for (auto& column_condition : read_options.conditions->columns()) {
            int32_t column_id = column_condition.first;
            if (_column_readers[column_id] == nullptr || !_column_readers[column_id]->has_zone_map()) {
                continue;
            }
            if (!_column_readers[column_id]->match_condition(column_condition.second)) {
                // any condition not satisfied, return.
                iter->reset(new EmptySegmentIterator(schema));
                return Status::OK();
            }
        }
    }

    RETURN_IF_ERROR(_load_index()); // 加载索引
    iter->reset(new SegmentIterator(this->shared_from_this(), schema)); // 创建SegmentIterator对象
    iter->get()->init(read_options); // 使用read_options初始化创建的SegmentIterator对象
    return Status::OK();
}

/*解析segment文件的footer信息*/
Status Segment::_parse_footer() {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::unique_ptr<fs::ReadableBlock> rblock;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager();
    RETURN_IF_ERROR(block_mgr->open_block(_fname, &rblock)); // 通过block mgr打开segment文件，并保存在rblock中

    uint64_t file_size;
    RETURN_IF_ERROR(rblock->size(&file_size)); // 计算segment文件大小

    if (file_size < 12) {
        return Status::Corruption(Substitute("Bad segment file $0: file size $1 < 12", _fname, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(rblock->read(file_size - 12, Slice(fixed_buf, 12))); // 读取segment文件中最后12字节的数据到fixed_buf中，segment文件中最后12字节的数据为footer

    // validate magic number
    // 验证Magic Number
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) { // 比较fixed_buf中的最后4个字节数据是否为"D0R1", 字符串k_segment_magic为"D0R1"，k_segment_magic_length值为4
        return Status::Corruption(Substitute("Bad segment file $0: magic number not match", _fname));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf); // 从segment文件中读取footer PB length
    if (file_size < 12 + footer_length) {
        return Status::Corruption(
            Substitute("Bad segment file $0: file size $1 < $2", _fname, file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(rblock->read(file_size - 12 - footer_length, footer_buf)); // 从segment文件中读取footer PB，保存在footer_buf中

    // validate footer PB's checksum
    // 验证footer PB的checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4); // 从segment文件中读取checksum
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size()); // 根据footer_buf中保存的footer PB计算真正的checksum
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
            Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                       _fname, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    // 反序列化footer PB
    if (!_footer.ParseFromString(footer_buf)) { // 反序列化footer PB，保存在成员变量_footer中
        return Status::Corruption(Substitute("Bad segment file $0: failed to parse SegmentFooterPB", _fname));
    }
    return Status::OK();
}

/*加载segment文件的前缀索引（short key index）*/
Status Segment::_load_index() {
    return _load_index_once.call([this] {
        // read and parse short key index page
        std::unique_ptr<fs::ReadableBlock> rblock;
        fs::BlockManager* block_mgr = fs::fs_util::block_manager();
        RETURN_IF_ERROR(block_mgr->open_block(_fname, &rblock)); // 通过block mgr打开segment文件，并保存在rblock中

        PageReadOptions opts;
        opts.rblock = rblock.get();
        opts.page_pointer = PagePointer(_footer.short_key_index_page()); // 从footerPB获取short key（前缀索引）
        opts.codec = nullptr; // short key index page uses NO_COMPRESSION for now
        OlapReaderStatistics tmp_stats;
        opts.stats = &tmp_stats;

        Slice body;
        PageFooterPB footer;
        RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer)); // 根据参数传入的opts读取并解压short key index page
        DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
        DCHECK(footer.has_short_key_page_footer());

        _sk_index_decoder.reset(new ShortKeyIndexDecoder); // 初始化成员变量_sk_index_decoder
        return _sk_index_decoder->parse(body, footer.short_key_page_footer()); // 解析short key page footer
    });
}

/*创建column reader*/
Status Segment::_create_column_readers() {
    for (uint32_t ordinal = 0; ordinal < _footer.columns().size(); ++ordinal) { // 依次遍历footerPB中的每一列
        auto& column_pb = _footer.columns(ordinal); // 从footerPB中获取当前列的PB
        _column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal); // 将当前列id到column ordinal的映射关系保存在成员变量_column_id_to_footer_ordinal中
    }

    _column_readers.resize(_tablet_schema->columns().size());
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) { // 依次遍历schema中的每一列
        auto& column = _tablet_schema->columns()[ordinal];
        auto iter = _column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == _column_id_to_footer_ordinal.end()) { // 判断schema中的当前列是否在footerPB中存在
            continue;
        }

        ColumnReaderOptions opts;
        opts.kept_in_memory = _tablet_schema->is_in_memory();
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(
            opts, _footer.columns(iter->second), _footer.num_rows(), _fname, &reader)); // 针对当前列创建column reader，通过参数reader传回
        _column_readers[ordinal] = std::move(reader); // 将当前列的reader保存在成员变量_column_readers中
    }
    return Status::OK();
}

/*针对参数传入的列cid创建ColumnIterator，通过参数iter传回*/
Status Segment::new_column_iterator(uint32_t cid, ColumnIterator** iter) {
    if (_column_readers[cid] == nullptr) { // 判断参数传入的列是否不在footerPB中存在
        const TabletColumn& tablet_column = _tablet_schema->column(cid); // 从schema中获取当前列
        if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) { // 判断当前列是否没有默认值并且不能为null
            return Status::InternalError("invalid nonexistent column without default value.");
        }
        std::unique_ptr<DefaultValueColumnIterator> default_value_iter(           // 创建ColumnIterator对象
                new DefaultValueColumnIterator(tablet_column.has_default_value(),
                tablet_column.default_value(),
                tablet_column.is_nullable(),
                tablet_column.type(),
                tablet_column.length()));
        ColumnIteratorOptions iter_opts;
        RETURN_IF_ERROR(default_value_iter->init(iter_opts)); // 初始化新创建的ColumnIterator对象
        *iter = default_value_iter.release();
        return Status::OK();
    }
    return _column_readers[cid]->new_iterator(iter); // 针对当前列创建ColumnIterator对象
}

/*针对参数传入的列cid创建BitmapIndexIterator，通过参数iter传回*/
Status Segment::new_bitmap_index_iterator(uint32_t cid, BitmapIndexIterator** iter) {
    if (_column_readers[cid] != nullptr && _column_readers[cid]->has_bitmap_index()) {
        return _column_readers[cid]->new_bitmap_index_iterator(iter); // 通过列cid的column reader创建BitmapIndexIterator
    }
    return Status::OK();
}

}
}
