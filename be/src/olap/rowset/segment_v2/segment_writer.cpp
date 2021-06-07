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

#include "olap/rowset/segment_v2/segment_writer.h"

#include "common/logging.h" // LOG
#include "env/env.h" // Env
#include "olap/fs/block_manager.h"
#include "olap/row.h" // ContiguousRow
#include "olap/row_cursor.h" // RowCursor
#include "olap/rowset/segment_v2/column_writer.h" // ColumnWriter
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/short_key_index.h"
#include "olap/schema.h"
#include "util/crc32c.h"
#include "util/faststring.h"

namespace doris {
namespace segment_v2 {

const char* k_segment_magic = "D0R1";
const uint32_t k_segment_magic_length = 4;

SegmentWriter::SegmentWriter(fs::WritableBlock* wblock,
                             uint32_t segment_id,
                             const TabletSchema* tablet_schema,
                             const SegmentWriterOptions& opts) :
        _segment_id(segment_id),
        _tablet_schema(tablet_schema),
        _opts(opts),
        _wblock(wblock) {
    CHECK_NOTNULL(_wblock);
}

SegmentWriter::~SegmentWriter() = default;

/*SegmentWriter对象初始化*/
Status SegmentWriter::init(uint32_t write_mbytes_per_sec __attribute__((unused))) {
    uint32_t column_id = 0;
    _column_writers.reserve(_tablet_schema->columns().size()); //根据schema中列的数目为各个列分配column writer
    for (auto& column : _tablet_schema->columns()) {
        std::unique_ptr<Field> field(FieldFactory::create(column));
        DCHECK(field.get() != nullptr);

        ColumnWriterOptions opts;      //创建ColumnWriterOptions对象
        opts.meta = _footer.add_columns();
        // TODO(zc): Do we need this column_id??
        opts.meta->set_column_id(column_id++);            //在segment中为当前列设置id，即当前列在schema中的序号
        opts.meta->set_unique_id(column.unique_id());     //在segment中为当前列设置全局唯一的id
        opts.meta->set_type(field->type());               //在segment中为当前列设置类型信息
        opts.meta->set_length(column.length());           //在segment中设置当前列字段的长度
        opts.meta->set_encoding(DEFAULT_ENCODING);        //在segment中为当前列设置编码类型
        opts.meta->set_compression(LZ4F);                 //在segment中为当前列设置数据压缩类型
        opts.meta->set_is_nullable(column.is_nullable()); //在segment中设置当前列字段是否可以为空

        // now we create zone map for key columns
        opts.need_zone_map = column.is_key() || _tablet_schema->keys_type() == KeysType::DUP_KEYS; //在segment中为当前列设置是否需要ZoneMap索引
        opts.need_bloom_filter = column.is_bf_column();                                            //在segment中为当前列设置是否需要BitMap索引
        opts.need_bitmap_index = column.has_bitmap_index();                                        //在segment中为当前列设置是否需要BloomFilter索引

        std::unique_ptr<ColumnWriter> writer(new ColumnWriter(opts, std::move(field), _wblock));   //为当前列创建ColumnWriter对象
        RETURN_IF_ERROR(writer->init());                                                           //初始化当前列的ColumnWriter对象
        _column_writers.push_back(std::move(writer));                                              //将当前列的ColumnWriter对象添加到成员变量_column_writers中进行管理
    }
    _index_builder.reset(new ShortKeyIndexBuilder(_segment_id, _opts.num_rows_per_block)); //创建ShortKeyIndexBuilder对象来初始化成员变量_index_builder（_opts.num_rows_per_block默认为1024，表示segment中每1024行添加一个前缀索引）
    return Status::OK();
}

/*向SegmentWriter对象中添加一行数据*/
template<typename RowType>
Status SegmentWriter::append_row(const RowType& row) {
    for (size_t cid = 0; cid < _column_writers.size(); ++cid) { //遍历该行数据中的每一列
        auto cell = row.cell(cid);                           //获取该行中的一列
        RETURN_IF_ERROR(_column_writers[cid]->append(cell)); //将当前列添加到该列对应的column writer中
    }

    // At the begin of one block, so add a short key index entry
    // 在数据写入过程中，每隔一定行数（默认为1024行），会生成一个前缀索引项（short key）
    if ((_row_count % _opts.num_rows_per_block) == 0) {
        std::string encoded_key;
        encode_key(&encoded_key, row, _tablet_schema->num_short_key_columns()); //根据前缀列的数目，将该行数据进行编码（olap_file.proto中TabletSchemaPB定义了num_short_key_columns默认为3）
        RETURN_IF_ERROR(_index_builder->add_item(encoded_key)); //添加一条前缀索引项(ShortKeyIndex)
    }
    ++_row_count; //当前segment中的行数目增1
    return Status::OK();
}

template Status SegmentWriter::append_row(const RowCursor& row);
template Status SegmentWriter::append_row(const ContiguousRow& row);

/*估计segment文件的大小*/
// TODO(lingbin): Currently this function does not include the size of various indexes,
// We should make this more precise.
// NOTE: This function will be called when any row of data is added, so we need to
// make this function efficient.
uint64_t SegmentWriter::estimate_segment_size() {
    // footer_size(4) + checksum(4) + segment_magic(4)
    uint64_t size = 12;
    for (auto& column_writer : _column_writers) { //依次遍历每一个列的column_writer，并估计buffer大小
        size += column_writer->estimate_buffer_size();
    }
    size += _index_builder->size(); //将索引大小增加到segment大小中
    return size;
}

/*向segment文件中刷写数据和索引*/
Status SegmentWriter::finalize(uint64_t* segment_file_size, uint64_t* index_size) {
    for (auto& column_writer : _column_writers) { // 依次遍历每一个列的column_writer
        RETURN_IF_ERROR(column_writer->finish()); // 对当前列的最后一个page进行编码和压缩
    }
    RETURN_IF_ERROR(_write_data()); //向文件块中追加列数据
    uint64_t index_offset = _wblock->bytes_appended(); //获取已经追加到文件块中的字节数，也就是列数据大小，同时也是索引数据的起始位置
    RETURN_IF_ERROR(_write_ordinal_index());       //向文件块中追加ordinal索引
    RETURN_IF_ERROR(_write_zone_map());            //向文件块中追加zonemap索引
    RETURN_IF_ERROR(_write_bitmap_index());        //向文件块中追加bitmap索引
    RETURN_IF_ERROR(_write_bloom_filter_index());  //向文件块中追加bloom filter索引
    RETURN_IF_ERROR(_write_short_key_index());     //向文件块中追加short key索引
    *index_size = _wblock->bytes_appended() - index_offset; //获取追加到文件块中的索引数据大小
    RETURN_IF_ERROR(_write_footer()); //向segment文件中写footer
    RETURN_IF_ERROR(_wblock->finalize()); //执行finalize()之后，文件块将不再接受数据写入，并开始将数据异步刷写到磁盘上，但是不能保证数据一定能持久化到磁盘上
    *segment_file_size = _wblock->bytes_appended(); //获取已经追加到文件块中的字节数,即整个segment文件大小
    return Status::OK();
}

/*向segment文件中写数据*/
// write column data to file one by one
Status SegmentWriter::_write_data() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_data());//通过column writer依次将每一列数据写入segment文件
    }
    return Status::OK();
}

/*向segment文件中写ordinal索引*/
// write ordinal index after data has been written
Status SegmentWriter::_write_ordinal_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_ordinal_index());//通过column writer依次将每一列ordinal索引写入segment文件
    }
    return Status::OK();
}

/*向segment文件中写zonemap索引*/
Status SegmentWriter::_write_zone_map() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_zone_map());//通过column writer依次将每一列zonemap索引写入segment文件
    }
    return Status::OK();
}

/*向segment文件中写bitmap索引*/
Status SegmentWriter::_write_bitmap_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bitmap_index());//通过column writer依次将每一列bitmap索引写入segment文件
    }
    return Status::OK();
}

/*向segment文件中写bloom filter索引*/
Status SegmentWriter::_write_bloom_filter_index() {
    for (auto& column_writer : _column_writers) {
        RETURN_IF_ERROR(column_writer->write_bloom_filter_index());//通过column writer依次将每一列bloom filter索引写入segment文件
    }
    return Status::OK();
}

/*向segment文件中写前缀short key索引*/
Status SegmentWriter::_write_short_key_index() {
    std::vector<Slice> body;
    PageFooterPB footer;
    RETURN_IF_ERROR(_index_builder->finalize(_row_count, &body, &footer));
    PagePointer pp;
    // short key index page is not compressed right now
    RETURN_IF_ERROR(PageIO::write_page(_wblock, body, footer, &pp));
    pp.to_proto(_footer.mutable_short_key_index_page());
    return Status::OK();
}

/*向segment文件中写footer*/
Status SegmentWriter::_write_footer() {
    _footer.set_num_rows(_row_count); //给footer设置segment中的行数

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!_footer.SerializeToString(&footer_buf)) { //将SegmentFooterPB类型的成员变量_footer序列化为字符串footer_buf
        return Status::InternalError("failed to serialize segment footer");
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size()); //向fixed_buf中追加4字节的PB length（le表示以小端(little_endian)格式追加）
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum); //向fixed_buf中追加4字节的PB checksum（le表示以小端(little_endian)格式追加）
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_segment_magic, k_segment_magic_length); //向fixed_buf中追加magic（字符数组k_segment_magic的值为"D0R1"，k_segment_magic_length的值为4）

    std::vector<Slice> slices{footer_buf, fixed_buf}; //定义并使用列表{footer_buf, fixed_buf}中的元素初始化向量slice，slice中的数据包含SegmentFooterPB类型的footer_buf和faststring类型的fixed_buf（fixed_buf中包含了PB length、PB checksum和magic code）
    return _write_raw_data(slices); //向文件块中写footer
}

/*向文件块中追加footer*/
Status SegmentWriter::_write_raw_data(const std::vector<Slice>& slices) {
    RETURN_IF_ERROR(_wblock->appendv(&slices[0], slices.size())); //向文件块中追加多个slice
    return Status::OK();
}

}
}
