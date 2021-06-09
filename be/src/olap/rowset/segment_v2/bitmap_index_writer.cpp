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

#include "olap/rowset/segment_v2/bitmap_index_writer.h"

#include <map>
#include <roaring/roaring.hh>

#include "env/env.h"
#include "olap/rowset/segment_v2/common.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris {
namespace segment_v2 {

namespace {

template<typename CppType>
struct BitmapIndexTraits {
    using MemoryIndexType = std::map<CppType, Roaring>;
};

template<>
struct BitmapIndexTraits<Slice> {
    using MemoryIndexType = std::map<Slice, Roaring, Slice::Comparator>;
};
// Bitmap Index和字典原理
// Builder for bitmap index. Bitmap index is comprised of two parts
// - an "ordered dictionary" which contains all distinct values of a column and maps each value to an id.
//   the smallest value mapped to 0, second value mapped to 1, ..
// - a posting list which stores one bitmap for each value in the dictionary. each bitmap is used to represent
//   the list of rowid where a particular value exists.
//
// E.g, if the column contains 10 rows ['x', 'x', 'x', 'b', 'b', 'b', 'x', 'b', 'b', 'b'],
// then the ordered dictionary would be ['b', 'x'] which maps 'b' to 0 and 'x' to 1,
// and the posting list would contain two bitmaps
//   bitmap for ID 0 : [0 0 0 1 1 1 0 1 1 1]
//   bitmap for ID 1 : [1 1 1 0 0 0 1 0 0 0]
//   the n-th bit is set to 1 if the n-th row equals to the corresponding value.
//
template <FieldType field_type>
class BitmapIndexWriterImpl : public BitmapIndexWriter {
public:
    using CppType = typename CppTypeTraits<field_type>::CppType;
    using MemoryIndexType = typename BitmapIndexTraits<CppType>::MemoryIndexType;

    explicit BitmapIndexWriterImpl(const TypeInfo* typeinfo)
        : _typeinfo(typeinfo), _reverted_index_size(0), _tracker(), _pool(&_tracker) {}

    ~BitmapIndexWriterImpl() = default;

    /*column中新增加多条值为values的数据，更新bitmap索引*/
    void add_values(const void* values, size_t count) override {
        auto p = reinterpret_cast<const CppType*>(values);
        for (size_t i = 0; i < count; ++i) {
            add_value(*p);
            p++;
        }
    }

    /*column中新增加一条数据，更新bitmap索引*/
    void add_value(const CppType& value) {
        auto it = _mem_index.find(value); // 从字典（std::map<CppType, Roaring>类型的成员变量_mem_index）中查找新增的数据是否存在
        uint64_t old_size = 0;
        if (it != _mem_index.end()) { // 新增的数据值在字典中已经存在
            // exiting value, update bitmap
            old_size = it->second.getSizeInBytes(false);
            it->second.add(_rid); // 更新该数据值对应的bitmap，在bitmap末尾新增一位1， （bitmap for ID * : [1 1 1 0 0 0 1 0 0 0]）
        } else { // 新增的数据值在字典中不存在
            // new value, copy value and insert new key->bitmap pair
            CppType new_value;
            _typeinfo->deep_copy(&new_value, &value, &_pool);
            _mem_index.insert({new_value, Roaring::bitmapOf(1, _rid)}); // 将新增加的数据值添加到字典（成员变量_mem_index）中
            it = _mem_index.find(new_value);
        }
        _reverted_index_size += it->second.getSizeInBytes(false) - old_size;
        _rid++;
    }

    /*column中新增加一条null值，更新null值对应的位图*/
    void add_nulls(uint32_t count) override {
        _null_bitmap.addRange(_rid, _rid + count);
        _rid += count;
    }

    /*通过WritableBlock将一个列的bitmap索引（包括字典和位图）追加到block中*/
    Status finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta) override {
        index_meta->set_type(BITMAP_INDEX);
        BitmapIndexPB* meta = index_meta->mutable_bitmap_index(); // 获取BitmapIndex的meta

        meta->set_bitmap_type(BitmapIndexPB::ROARING_BITMAP); // 设置bitmap的类型
        meta->set_has_null(!_null_bitmap.isEmpty());          // 设置当前列是否有null值的标志

        {   // 通过WritableBlock将bitmap索引中的字典添加到block中（write dictionary）
            IndexedColumnWriterOptions options;
            options.write_ordinal_index = false;
            options.write_value_index = true;
            options.encoding = EncodingInfo::get_default_encoding(_typeinfo, true); // 设置默认的编码格式
            options.compression = LZ4F; // 设置字典的压缩类型为LZ4F

            IndexedColumnWriter dict_column_writer(options, _typeinfo, wblock); // 创建当前列字典的IndexedColumnWriter对象
            RETURN_IF_ERROR(dict_column_writer.init()); // 初始化IndexedColumnWriter对象
            for (auto const& it : _mem_index) { // 依次遍历字典中的每一个元素
                RETURN_IF_ERROR(dict_column_writer.add(&(it.first))); // 将每一个字典的key值添加到dict_column_writer中
            }
            RETURN_IF_ERROR(dict_column_writer.finish(meta->mutable_dict_column())); // 将字典追加到block中
        }
        {   // 通过WritableBlock将bitmap索引中的位图添加到block中（write bitmaps）
            std::vector<Roaring*> bitmaps;
            for (auto& it : _mem_index) { // 依次遍历字典中的每一个元素
                bitmaps.push_back(&(it.second)); // 将字典中当前元素对应的位图添加到bitmaps中
            }
            if (!_null_bitmap.isEmpty()) { // 判断该列是否有null值
                bitmaps.push_back(&_null_bitmap); // 将null值对应的位图添加到bitmaps中
            }

            uint32_t max_bitmap_size = 0;
            std::vector<uint32_t> bitmap_sizes;
            for (auto& bitmap : bitmaps) { // 依次遍历bitmaps中的每一个位图
                bitmap->runOptimize();
                uint32_t bitmap_size = bitmap->getSizeInBytes(false); // 获取当前位图的长度
                if (max_bitmap_size < bitmap_size) {
                    max_bitmap_size = bitmap_size; // 记录bitmap索引中最大的位图长度
                }
                bitmap_sizes.push_back(bitmap_size); // 将当前位图长度添加到bitmap_sizes中
            }

            const TypeInfo* bitmap_typeinfo = get_type_info(OLAP_FIELD_TYPE_OBJECT);

            IndexedColumnWriterOptions options;
            options.write_ordinal_index = true;
            options.write_value_index = false;
            options.encoding = EncodingInfo::get_default_encoding(bitmap_typeinfo, false);// 设置默认的编码格式
            // we already store compressed bitmap, use NO_COMPRESSION to save some cpu
            options.compression = NO_COMPRESSION; // 设置不进行压缩

            IndexedColumnWriter bitmap_column_writer(options, bitmap_typeinfo, wblock); // 创建当前列bitmap索引的IndexedColumnWriter对象
            RETURN_IF_ERROR(bitmap_column_writer.init()); // 初始化IndexedColumnWriter对象

            faststring buf;
            buf.reserve(max_bitmap_size);
            for (size_t i = 0; i < bitmaps.size(); ++i) { // 依次遍历每一个位图
                buf.resize(bitmap_sizes[i]); // 使用当前位图的大小调整buf的长度 so that buf[0..size) can be read and written
                bitmaps[i]->write(reinterpret_cast<char*>(buf.data()), false); // 将位图写入buf中
                Slice buf_slice(buf);
                RETURN_IF_ERROR(bitmap_column_writer.add(&buf_slice)); // 向bitmap_column_writer中添加当前位图
            }
            RETURN_IF_ERROR(bitmap_column_writer.finish(meta->mutable_bitmap_column())); // 将所有位图追加到block中
        }
        return Status::OK();
    }

    /*获取bitmap索引的大小*/
    uint64_t size() const override {
        uint64_t size = 0;
        size += _null_bitmap.getSizeInBytes(false);
        size += _reverted_index_size;
        size += _mem_index.size() * sizeof(CppType);
        size += _pool.total_allocated_bytes();
        return size;
    }

private:
    const TypeInfo* _typeinfo;
    uint64_t _reverted_index_size;
    rowid_t _rid = 0;
    // row id list for null value
    Roaring _null_bitmap;
    // unique value to its row id list
    MemoryIndexType _mem_index;
    MemTracker _tracker;
    MemPool _pool;
};

} // namespace

/*根据字段类型，创建对应的BitmapIndexWriter，通过参数res传回*/
Status BitmapIndexWriter::create(const TypeInfo* typeinfo, std::unique_ptr<BitmapIndexWriter>* res) {
    FieldType type = typeinfo->type();
    switch (type) {
        case OLAP_FIELD_TYPE_TINYINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_TINYINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_SMALLINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_INT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_INT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_UNSIGNED_INT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_BIGINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_CHAR:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_CHAR>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_VARCHAR:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_VARCHAR>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_DATE:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_DATE>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_DATETIME:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_DATETIME>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_LARGEINT:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_LARGEINT>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_DECIMAL:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_DECIMAL>(typeinfo));
            break;
        case OLAP_FIELD_TYPE_BOOL:
            res->reset(new BitmapIndexWriterImpl<OLAP_FIELD_TYPE_BOOL>(typeinfo));
            break;
        default:
            return Status::NotSupported("unsupported type for bitmap index: " + std::to_string(type));
    }
    return Status::OK();
}

} // segment_v2
} // namespace doris
