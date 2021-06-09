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

#include "olap/rowset/segment_v2/zone_map_index.h"

#include "olap/column_block.h"
#include "olap/fs/block_manager.h"
#include "olap/olap_define.h"
#include "olap/rowset/segment_v2/encoding_info.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/indexed_column_writer.h"
#include "olap/types.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

namespace segment_v2 {

ZoneMapIndexWriter::ZoneMapIndexWriter(Field* field) : _field(field), _pool(&_tracker) {
    _page_zone_map.min_value = _field->allocate_value(&_pool);
    _page_zone_map.max_value = _field->allocate_value(&_pool);
    _reset_zone_map(&_page_zone_map);
    _segment_zone_map.min_value = _field->allocate_value(&_pool);
    _segment_zone_map.max_value = _field->allocate_value(&_pool);
    _reset_zone_map(&_segment_zone_map);
}

void ZoneMapIndexWriter::add_values(const void* values, size_t count) {
    if (count > 0) {
        _page_zone_map.has_not_null = true;
    }
    const char* vals = reinterpret_cast<const char*>(values);
    for (int i = 0; i < count; ++i) {
        if (_field->compare(_page_zone_map.min_value, vals) > 0) {
            _field->type_info()->direct_copy(_page_zone_map.min_value, vals);
        }
        if (_field->compare(_page_zone_map.max_value, vals) < 0) {
            _field->type_info()->direct_copy(_page_zone_map.max_value, vals);
        }
        vals +=  _field->size();
    }
}

/*使用当前page的zone map更新当前列的zone map，并将当前page的zone map添加到成员变量_values中，成员变量_values中保存当前列中所有data page的zone map*/
Status ZoneMapIndexWriter::flush() {
    // Update segment zone map.
    if (_field->compare(_segment_zone_map.min_value, _page_zone_map.min_value) > 0) { // 如果page zone map的最小值小于当前列的 zone map当前的最小值，则使用page zone map的最小值更新当前列的 zone map的最小值
        _field->type_info()->direct_copy(_segment_zone_map.min_value, _page_zone_map.min_value);
    }
    if (_field->compare(_segment_zone_map.max_value, _page_zone_map.max_value) < 0) { // 如果page zone map的最大值大于当前列的 zone map当前的最大值，则使用page zone map的最大值更新当前列的 zone map的最大值
        _field->type_info()->direct_copy(_segment_zone_map.max_value, _page_zone_map.max_value);
    }
    if (_page_zone_map.has_null) { // 如果page zone map中有null值，则更新当前列的 zone map的has_null为true
        _segment_zone_map.has_null = true;
    }
    if (_page_zone_map.has_not_null) { // 如果page zone map中有非null值，则更新当前列的 zone map的has_not_null为true
        _segment_zone_map.has_not_null = true;
    }

    ZoneMapPB zone_map_pb;
    _page_zone_map.to_proto(&zone_map_pb, _field); // 将page zone map转化化为PB
    _reset_zone_map(&_page_zone_map); // reset成员变量_page_zone_map

    std::string serialized_zone_map;
    bool ret = zone_map_pb.SerializeToString(&serialized_zone_map); // 将PB格式的page zone map序列化为字符串
    if (!ret) {
        return Status::InternalError("serialize zone map failed");
    }
    _estimated_size += serialized_zone_map.size() + sizeof(uint32_t); // 计算当前page zone map的大小
    _values.push_back(std::move(serialized_zone_map)); // 将当前page的zone map添加到成员变量_values中
    return Status::OK();
}

Status ZoneMapIndexWriter::finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* index_meta) {
    index_meta->set_type(ZONE_MAP_INDEX);
    ZoneMapIndexPB* meta = index_meta->mutable_zone_map_index();
    // store segment zone map
    _segment_zone_map.to_proto(meta->mutable_segment_zone_map(), _field);

    // write out zone map for each data pages
    const TypeInfo* typeinfo = get_type_info(OLAP_FIELD_TYPE_OBJECT);
    IndexedColumnWriterOptions options;
    options.write_ordinal_index = true;
    options.write_value_index = false;
    options.encoding = EncodingInfo::get_default_encoding(typeinfo, false);
    options.compression = NO_COMPRESSION; // currently not compressed

    IndexedColumnWriter writer(options, typeinfo, wblock);
    RETURN_IF_ERROR(writer.init());

    for (auto& value : _values) {
        Slice value_slice(value);
        RETURN_IF_ERROR(writer.add(&value_slice));
    }
    return writer.finish(meta->mutable_page_zone_maps());
}

Status ZoneMapIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    IndexedColumnReader reader(_filename, _index_meta->page_zone_maps());
    RETURN_IF_ERROR(reader.load(use_page_cache, kept_in_memory));
    IndexedColumnIterator iter(&reader);

    MemTracker tracker;
    MemPool pool(&tracker);
    _page_zone_maps.resize(reader.num_values());

    // read and cache all page zone maps
    for (int i = 0; i < reader.num_values(); ++i) {
        Slice value;
        uint8_t nullmap;
        size_t num_to_read = 1;
        ColumnBlock block(reader.type_info(), (uint8_t*) &value, &nullmap, num_to_read, &pool);
        ColumnBlockView column_block_view(&block);

        RETURN_IF_ERROR(iter.seek_to_ordinal(i));
        size_t num_read = num_to_read;
        RETURN_IF_ERROR(iter.next_batch(&num_read, &column_block_view));
        DCHECK(num_to_read == num_read);

        if (!_page_zone_maps[i].ParseFromArray(value.data, value.size)) {
            return Status::Corruption("Failed to parse zone map");
        }
        pool.clear();
    }
    return Status::OK();
}

} // namespace segment_v2
} // namespace doris
