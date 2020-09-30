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

#include "olap/memtable.h"

#include "common/logging.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/row_cursor.h"
#include "olap/row.h"
#include "olap/schema.h"
#include "runtime/tuple.h"
#include "util/debug_util.h"
#include "util/doris_metrics.h"

namespace doris {
/*
  memtable底层实质上是跳表SkipList数据结构：SkipList<char*, RowCursorComparator>
  SkipList以导入数据每一行的维度列为Key，并且所有节点依据Key有序
*/

MemTable::MemTable(int64_t tablet_id, Schema* schema, const TabletSchema* tablet_schema,
                   const std::vector<SlotDescriptor*>* slot_descs, TupleDescriptor* tuple_desc,
                   KeysType keys_type, RowsetWriter* rowset_writer, MemTracker* mem_tracker)
    : _tablet_id(tablet_id),
      _schema(schema),
      _tablet_schema(tablet_schema),
      _tuple_desc(tuple_desc),
      _slot_descs(slot_descs),
      _keys_type(keys_type),
      _row_comparator(_schema),
      _rowset_writer(rowset_writer) {

    _schema_size = _schema->schema_size();
    _mem_tracker.reset(new MemTracker(-1, "memtable", mem_tracker));
    _buffer_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _table_mem_pool.reset(new MemPool(_mem_tracker.get()));
    _skip_list = new Table(_row_comparator, _table_mem_pool.get(), _keys_type == KeysType::DUP_KEYS);
}

MemTable::~MemTable() {
    delete _skip_list;
}

MemTable::RowCursorComparator::RowCursorComparator(const Schema* schema) : _schema(schema) {}

int MemTable::RowCursorComparator::operator()(const char* left, const char* right) const {
    ContiguousRow lhs_row(_schema, left);
    ContiguousRow rhs_row(_schema, right);
    return compare_row(lhs_row, rhs_row);
}

/*向memtable中插入tuple,tuple对应导入数据的一行。向memtable中插入一行实质上是向SkipList中插入一个节点*/
void MemTable::insert(const Tuple* tuple) {
    bool overwritten = false;
    uint8_t* _tuple_buf = nullptr;
    if (_keys_type == KeysType::DUP_KEYS) { // 数据模型为Duplicate时，可以直接将tuple插入memtable
        // Will insert directly, so use memory from _table_mem_pool
        _tuple_buf = _table_mem_pool->allocate(_schema_size); //从memtable的内存池_table_mem_pool中为tuple分配内存空间，返回分配空间的首地址
        // 基于schema中列的偏移量（schema.column_offset()），ContiguousRow中包含了一行数据中所有column在内存中的布局
        ContiguousRow row(_schema, _tuple_buf); //根据schema和数据在内存池中存储的首地址创建ContiguousRow对象
        _tuple_to_row(tuple, &row, _table_mem_pool.get()); //将tuple中的数据写入row，实质上写入了_tuple_buf为首地址的内存
        _skip_list->Insert((TableKey)_tuple_buf, &overwritten); //将_tuple_buf插入跳表（memtable）,memtable底层实质上是跳表SkipList
        DCHECK(!overwritten) << "Duplicate key model meet overwrite in SkipList";
        return;
    }

    // 数据模型为Aggregate或Uniqe时
    // For non-DUP models, for the data rows passed from the upper layer, when copying the data,
    // we first allocate from _buffer_mem_pool, and then check whether it already exists in
    // _skiplist.  If it exists, we aggregate the new row into the row in skiplist.
    // otherwise, we need to copy it into _table_mem_pool before we can insert it.
    _tuple_buf = _buffer_mem_pool->allocate(_schema_size); //从内存池_buffer_mem_pool中为tuple分配内存空间
    // 基于schema中列的偏移量（schema.column_offset()），ContiguousRow中包含了一行数据中所有column在内存中的布局
    ContiguousRow src_row(_schema, _tuple_buf);
    _tuple_to_row(tuple, &src_row, _buffer_mem_pool.get()); //将tuple中的数据写入src_row，实质上写入了_tuple_buf为首地址的内存

    bool is_exist = _skip_list->Find((TableKey)_tuple_buf, &_hint); //从跳表中查找要插入的数据行的key是否存在
    if (is_exist) {
        _aggregate_two_row(src_row, _hint.curr->key); //如果存在，将需要插入的数据行与跳表中存在的具有相同key的数据行进行聚合
    } else {
        _tuple_buf = _table_mem_pool->allocate(_schema_size);//如果不存在，从memtable的内存池_table_mem_pool中为tuple分配内存空间，返回首地址
        // 基于schema中列的偏移量（schema.column_offset()），ContiguousRow中包含了一行数据中所有column在内存中的布局
        ContiguousRow dst_row(_schema, _tuple_buf);
        copy_row_in_memtable(&dst_row, src_row, _table_mem_pool.get()); //将src_row中的数据copy到dst_row，实质上写入了_tuple_buf为首地址的内存
        _skip_list->InsertWithHint((TableKey)_tuple_buf, is_exist, &_hint);//将_tuple_buf插入跳表（memtable）,memtable底层实质上是跳表SkipList
    }

    // Make MemPool to be reusable, but does not free its memory
    _buffer_mem_pool->clear();//清空内存池_buffer_mem_pool
}

/*将Tuple对象（一行数据）中的数据逐列（schema）写入ContiguousRow对象中*/
void MemTable::_tuple_to_row(const Tuple* tuple, ContiguousRow* row, MemPool* mem_pool) {
    for (size_t i = 0; i < _slot_descs->size(); ++i) {
        auto cell = row->cell(i);
        const SlotDescriptor* slot = (*_slot_descs)[i];

        bool is_null = tuple->is_null(slot->null_indicator_offset());
        const void* value = tuple->get_slot(slot->tuple_offset());
        _schema->column(i)->consume(
                &cell, (const char*)value, is_null, mem_pool, &_agg_object_pool);
    }
}

/*将src_row与跳表中存在的具有相同key的行进行聚合*/
void MemTable::_aggregate_two_row(const ContiguousRow& src_row, TableKey row_in_skiplist) {
    ContiguousRow dst_row(_schema, row_in_skiplist);
    agg_update_row(&dst_row, src_row, _table_mem_pool.get());
}

/*将memtable刷写到rowset中,memtable底层实质上是跳表SkipList*/
OLAPStatus MemTable::flush() {
    int64_t duration_ns = 0;
    {
        SCOPED_RAW_TIMER(&duration_ns);
        Table::Iterator it(_skip_list); //此处的Table指的是memtable，底层实质上是跳表SkipList<char*, RowCursorComparator>
        for (it.SeekToFirst(); it.Valid(); it.Next()) { //在memtable中遍历导入数据的每一行，实质上是从头开始遍历SkipList中的每一个节点
            char* row = (char*)it.key(); //获取每一行数据的维度列，实质上是SkipList的Key,此处获取到的应该是该行数据在内存池中的首地址
            ContiguousRow dst_row(_schema, row); //根据schema和该行数据在内存池中的首地址创建ContiguousRow对象
            agg_finalize_row(&dst_row, _table_mem_pool.get()); //???
            RETURN_NOT_OK(_rowset_writer->add_row(dst_row)); //将dst_row添加到RowsetWriter对象(在DeltaWriter对象初始化时创建)中进行flush
        }
        RETURN_NOT_OK(_rowset_writer->flush()); //memtable中的所有行都添加到了rowset writer中，通过RowsetWriter对象将memtable中的数据刷写到segment文件中
    }
    DorisMetrics::instance()->memtable_flush_total.increment(1);
    DorisMetrics::instance()->memtable_flush_duration_us.increment(duration_ns / 1000);
    return OLAP_SUCCESS;
}

OLAPStatus MemTable::close() {
    return flush();
}

} // namespace doris
