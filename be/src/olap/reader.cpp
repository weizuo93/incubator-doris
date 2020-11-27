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

#include "olap/reader.h"

#include "olap/rowset/column_data.h"
#include "olap/tablet.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "util/date_func.h"
#include "util/mem_util.hpp"
#include "runtime/mem_tracker.h"
#include "runtime/mem_pool.h"
#include <sstream>

#include "olap/comparison_predicate.h"
#include "olap/in_list_predicate.h"
#include "olap/null_predicate.h"
#include "olap/storage_engine.h"
#include "olap/row.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris {

class CollectIterator {
public:
    ~CollectIterator();

    // Hold reader point to get reader params
    void init(Reader* reader);

    OLAPStatus add_child(RowsetReaderSharedPtr rs_reader);

    // Get top row of the heap, nullptr if reach end.
    const RowCursor* current_row(bool* delete_flag) const {
        if (_cur_child != nullptr) {
            return _cur_child->current_row(delete_flag);
        }
        return nullptr;
    }

    // Read next row into *row.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
    //      Others when error happens
    OLAPStatus next(const RowCursor** row, bool* delete_flag) {
        DCHECK(_cur_child != nullptr);
        if (_merge) {
            return _merge_next(row, delete_flag);
        } else {
            return _normal_next(row, delete_flag);
        }
    }

    // Clear the MergeSet element and reset state.
    void clear();

private:
    class ChildCtx { // 每一个ChildCtx对象对应一个rowset reader
    public:
        /*ChildCtx的构造函数*/
        ChildCtx(RowsetReaderSharedPtr rs_reader, Reader* reader)
                : _rs_reader(rs_reader),
                  _is_delete(rs_reader->delete_flag()),
                  _reader(reader) { }

        /*初始化ChildCtx对象*/
        OLAPStatus init() {
            auto res = _row_cursor.init(_reader->_tablet->tablet_schema(), _reader->_seek_columns); // 根据传入的tablet schema和column id初始化RowCursor
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to init row cursor, res=" << res;
                return res;
            }
            RETURN_NOT_OK(_refresh_current_row());
            return OLAP_SUCCESS;
        }

        /*获取该ChildCtx对象对应rowset的当前行，并通过参数传回该行数据(当前rowset)是否已被删除的信息*/
        const RowCursor* current_row(bool* delete_flag) const {
            *delete_flag = _is_delete;  // *delete_flag = _is_delete || _current_row->is_delete(); // 获取当前rowset是否被删除的标志或当前行是否被删除
            return _current_row;
        }

        /*获取该ChildCtx对象对应rowset的当前行*/
        const RowCursor* current_row() const {
            return _current_row;
        }

        /*获取该ChildCtx对象对应rowset的version*/
        int32_t version() const {
            return _rs_reader->version().second;
        }

        /*从该ChildCtx对象对应rowset中获取一行数据，通过参数传回，同时通过参数传回该行是否被删除的标志*/
        OLAPStatus next(const RowCursor** row, bool* delete_flag) {
            _row_block->pos_inc(); // 将RowBlock中当前行的位置_pos后移一位
            auto res = _refresh_current_row(); // 更新当前行的位置，更新后的当前行保存在成员变量_current_row中
            *row = _current_row;               // 获取当前行的位置
            *delete_flag = _is_delete;         // 获取当前rowset是否被删除的标志
            /*
            if (_current_row!= nullptr) {
                delete_flag = _is_delete || _current_row->is_delete(); // 获取当前rowset是否被删除的标志或当前行是否被删除。当前行是否被删除可以通过该行schema的隐藏列_delete_sign_idx判断。
            };
             * */
            return res;
        }

    private:
        // refresh_current_row
        /*更新当前行的位置*/
        OLAPStatus _refresh_current_row() {
            do {
                if (_row_block != nullptr && _row_block->has_remaining()) { // 如果当前RowBlock不为空，并且当前行位置还未达到RowBlock内部buf的上限（RowBlock中有一个成员变量_pos来记录当前行的位置）
                    size_t pos = _row_block->pos();         // 获取RowBlock中当前行的位置_pos
                    _row_block->get_row(pos, &_row_cursor); // 将_row_cursor attach到RowBlock中pos位置，即从RowBlock中获取一行数据
                    if (_row_block->block_status() == DEL_PARTIAL_SATISFIED &&
                        _reader->_delete_handler.is_filter_data(_rs_reader->version().second, _row_cursor)) {
                        _reader->_stats.rows_del_filtered++;
                        _row_block->pos_inc(); // RowBlock中当前行的位置_pos后移一位
                        continue;
                    }
                    _current_row = &_row_cursor; // 获取一行数据，保存在成员变量_current_row中
                    return OLAP_SUCCESS;
                } else { // 如果当前RowBlock为空，或当前行位置达到了RowBlock的内部buf的上限
                    auto res = _rs_reader->next_block(&_row_block); // 获取下一个RowBlock，通过参数传回
                    if (res != OLAP_SUCCESS) {
                        _current_row = nullptr;
                        return res;
                    }
                }
            } while (_row_block != nullptr);
            _current_row = nullptr;
            return OLAP_ERR_DATA_EOF;
        }

        RowsetReaderSharedPtr _rs_reader;
        const RowCursor* _current_row = nullptr; //保存ChildCtx对象对应rowset的当前行
        bool _is_delete = false;
        Reader* _reader = nullptr;

        RowCursor _row_cursor; // point to rows inside `_row_block`
        RowBlock* _row_block = nullptr; // 一般由256或512行组成一个RowBlock，数据保存在RowBlock的内部buf中。因此，读数据时每个rowset reader会在内存中开辟一块儿特定大小的内存空间。
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class ChildCtxComparator { // 比较多个merge元素之间的row cursors，如果相等则比较数据版本
    public:
        ChildCtxComparator(const bool& revparam=false) {
            _reverse = revparam;
        }
        bool operator()(const ChildCtx* a, const ChildCtx* b);
    private:
        bool _reverse;
    };

    inline OLAPStatus _merge_next(const RowCursor** row, bool* delete_flag);
    inline OLAPStatus _normal_next(const RowCursor** row, bool* delete_flag);

    // each ChildCtx corresponds to a rowset reader
    std::vector<ChildCtx*> _children;                       // 每一个ChildCtx对应一个rowset reader
    // point to the ChildCtx containing the next output row.
    // null when CollectIterator hasn't been initialized or reaches EOF.
    ChildCtx* _cur_child = nullptr; //始终指向下一行所对应的ChildCtx对象，即下一行所对应的rowset reader

    // when `_merge == true`, rowset reader returns ordered rows and CollectIterator uses a priority queue to merge
    // sort them. The output of CollectIterator is also ordered.
    // When `_merge == false`, rowset reader returns *partial* ordered rows. CollectIterator simply returns all rows
    // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
    // *partially* ordered.
    bool _merge = true; // _merge为true时，会对读到的所有rowset的数据进行排序、合并之后返回；_merge为false时，按rowset顺序返回每一个rowset的数据，返回数据局部有序
    // used when `_merge == true`
    typedef std::priority_queue<ChildCtx*, std::vector<ChildCtx*>, ChildCtxComparator> MergeHeap; // priority_queue可以自定义其中数据的优先级,让优先级高的排在队列前面,优先出队，本质上是一个堆
    std::unique_ptr<MergeHeap> _heap; //定义执行compaction时的merge堆类型，每一个rowset reader是一个堆元素
    // used when `_merge == false`
    int _child_idx = 0;

    // Hold reader point to access read params, such as fetch conditions.
    Reader* _reader = nullptr;
};

/*CollectIterator的析构函数，依次释放每一个ChildCtx对象*/
CollectIterator::~CollectIterator() {
    for (auto child : _children) {
        delete child;
    }
}

/*初始化CollectIterator对象*/
void CollectIterator::init(Reader* reader) {
    _reader = reader;
    // when aggregate is enabled or key_type is DUP_KEYS, we don't merge
    // multiple data to aggregate for performance in user fetch
    if (_reader->_reader_type == READER_QUERY &&
            (_reader->_aggregation ||     // _reader->_aggregation为true，上层计算引擎会对数据进行聚合，此处读行数据的时不需要merge
             _reader->_tablet->keys_type() == KeysType::DUP_KEYS)) {
        _merge = false; // 当reader的类型是query、需要聚合或者tablet key的类型为DUP_KEYS时，_merge = false
        _heap.reset(nullptr);
    } else if (_reader->_tablet->keys_type() == KeysType::UNIQUE_KEYS) {
        _heap.reset(new MergeHeap(ChildCtxComparator(true))); // _merge默认为true
    } else {
        _heap.reset(new MergeHeap());
    }
}

/*创建rs_reader对应的ChildCtx对象，并添加到CollectIterator的成员变量_children中管理*/
OLAPStatus CollectIterator::add_child(RowsetReaderSharedPtr rs_reader) {
    std::unique_ptr<ChildCtx> child(new ChildCtx(rs_reader, _reader)); //使用rs_reader和_reader创建ChildCtx对象
    RETURN_NOT_OK(child->init());            //初始化创建的ChildCtx对象
    if (child->current_row() == nullptr) {   // 判断该rowset的当前行是否为空
        return OLAP_SUCCESS;
    }

    ChildCtx* child_ptr = child.release(); // unique_ptr的release()函数只是把智能指针赋值为空，获取裸指针，但是它原来指向的内存并没有被释放，相当于它只是释放了智能指针对资源的所有权。
    _children.push_back(child_ptr); // 将创建的ChildCtx对象添加到成员变量_children中管理
    if (_merge) {
        _heap->push(child_ptr); // 将创建的ChildCtx对象添加到堆中，每次添加一个元素，堆都有可能会发生调整，堆顶元素都可能会变化
        _cur_child = _heap->top(); // 更新_cur_child，_cur_child始终指向下一次要读取的行数据所在的rowset对应的ChildCtx对象
    } else {
        if (_cur_child == nullptr) {
            _cur_child = _children[_child_idx];  // 如果_cur_child为空，则初始化_cur_child，_child_idx初始值为0
        }
    }
    return OLAP_SUCCESS;
}

/*_merge为true，获取要读取的下一行数据，通过参数row返回，同时返回该行数据是否已被删除的标志（所有rowset对应的ChildCtx元素保存在heap结构中）*/
inline OLAPStatus CollectIterator::_merge_next(const RowCursor** row, bool* delete_flag) {
    /*
       堆顶ChildCtx对象的当前行已经在上次读行操作中被读出了，需要更新该ChildCtx对象下一行数据为_current_row。
       因为堆是按照堆中每一个ChildCtx元素的_current_row进行排序的，所以更新该ChildCtx对象的_current_row之后，
       堆的结构有可能会调整，堆顶元素有可能就不是该ChildCtx对象了。因此，将该ChildCtx对象先从堆结构中删除（删除堆
       顶元素后堆结构会重新调整，产生新的堆顶元素），更新该ChildCtx对象的_current_row之后（如果该ChildCtx对象的
       _current_row存在），再将该ChildCtx对象添加进堆。这样的话，堆始终是有序的。
     */
    _heap->pop(); // 从堆中删除堆顶的ChildCtx对象，_cur_child指向该对象。删除堆顶元素后堆结构会重新调整，产生新的堆顶元素。
    auto res = _cur_child->next(row, delete_flag); // _cur_child指向刚刚从堆中删除的ChildCtx对象，通过_cur_child从ChildCtx对象对应的rowset获取下一行数据，通过参数row传回，并将该行数据是否已经删除的标志通过参数delete_flag传回。该语句执行之后，该ChildCtx对象的_current_row更新为下一行数据。
    if (res == OLAP_SUCCESS) { // _cur_child指向的ChildCtx对象中存在下一行数据，并且_current_row更新为下一行数据
        _heap->push(_cur_child);   // 将_cur_child指向的ChildCtx对象重新添加到堆中，该ChildCtx对象添加到堆中之后，堆的结构有可能会调整，堆顶元素很有可能不是该ChildCtx对象了
        _cur_child = _heap->top(); // 使用新的堆顶ChildCtx对象更新_cur_child，这样读取的行数据就是按key列有序的，因为每一个rowset中的数据原本就是有序的
    } else if (res == OLAP_ERR_DATA_EOF) { // _cur_child指向的ChildCtx对象不存在下一行数据，即对应的rowset中的数据已经被读完
        if (!_heap->empty()) { // 如果堆不为空
            _cur_child = _heap->top(); // 使用新的堆顶ChildCtx对象更新_cur_child，这样读取的行数据就是按key列有序的，因为每一个rowset中的数据原本就是有序的
        } else {               // 如果堆为空
            _cur_child = nullptr; // 所有rowset中的数据都已被读完，_cur_child赋值nullptr
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
    *row = _cur_child->current_row(delete_flag); //读取新的堆顶ChildCtx对象对应的rowset中的_current_row，以及该行是否已被删除的标志
    return OLAP_SUCCESS;
}

/*_merge为false，获取要读取的下一行数据，通过参数row返回，同时返回该行数据是否已被删除的标志。（所有rowset对应的ChildCtx元素保存在vector结构中）*/
inline OLAPStatus CollectIterator::_normal_next(const RowCursor** row, bool* delete_flag) {
    auto res = _cur_child->next(row, delete_flag); // 通过rowset reader获取一行数据，通过参数row传回，并将该行数据是否已经删除的标志通过参数delete_flag传回
    if (LIKELY(res == OLAP_SUCCESS)) {
        return OLAP_SUCCESS; // 从rowset中成功读取一行数据，函数返回
    } else if (res == OLAP_ERR_DATA_EOF) {
        // 从rowset中读取一行数据失败，则从下一个rowset中读取一行数据
        // this child has been read, to read next
        _child_idx++;
        if (_child_idx < _children.size()) { // 还存在rowset没有读
            _cur_child = _children[_child_idx]; // 获取下一个rowset对应的ChildCtx对象
            *row = _cur_child->current_row(delete_flag); //读取ChildCtx对象对应rowset中的当前行，以及该行是否已被删除的标志
            return OLAP_SUCCESS;
        } else {                             // 所有rowset是否都已经读完
            _cur_child = nullptr;
            return OLAP_ERR_DATA_EOF;
        }
    } else {
        LOG(WARNING) << "failed to get next from child, res=" << res;
        return res;
    }
}

/*比较两个ChildCtx对象对应rowset当前行的key列数据大小，如果两行数据的key列都相同，则比较两行数据的版本*/
bool CollectIterator::ChildCtxComparator::operator()(const ChildCtx* a, const ChildCtx* b) {
    // First compare row cursor.
    const RowCursor* first = a->current_row(); // 获取当前行数据
    const RowCursor* second = b->current_row();// 获取当前行数据
    int cmp_res = compare_row(*first, *second); // 依次比较两行中的key列
    if (cmp_res != 0) {
        return cmp_res > 0;
    }
    // 如果两行数据的key列都相同，则比较两行数据的版本
    // if row cursors equal, compare data version.
    // read data from higher version to lower version.
    // for UNIQUE_KEYS just read the highest version and no need agg_update.
    // for AGG_KEYS if a version is deleted, the lower version no need to agg_update
    if (_reverse) {
        return a->version() < b->version();
    }
    return a->version() > b->version();
}

/*释放所有rowset reader对应的ChildCtx对象*/
void CollectIterator::clear() {
    while (_heap != nullptr && !_heap->empty()) { // 将堆中的所有ChildCtx对象依次删除
        _heap->pop();
    }
    for (auto child : _children) { // 依次释放所有rowset reader对应的ChildCtx对象
        delete child;
    }
    // _children.swap(std::vector<ChildCtx*>());
    _children.clear(); // 清空vector类型的成员变量_children
    _cur_child = nullptr;
    _child_idx = 0;
}

/*Reader构造函数*/
Reader::Reader() {
    _tracker.reset(new MemTracker(-1));
    _predicate_mem_pool.reset(new MemPool(_tracker.get()));
}

/*Reader析构函数*/
Reader::~Reader() {
    close(); // 关闭reader
}

/*根据参数初始化Reader对象*/
OLAPStatus Reader::init(const ReaderParams& read_params) {
    OLAPStatus res = _init_params(read_params); // 根据参数初始化Reader对象
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when init params. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    res = _capture_rs_readers(read_params);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    switch (_tablet->keys_type()) { // 根据tablet key的类型，选择不同的函数来读取一行数据
    case KeysType::DUP_KEYS:
        _next_row_func = &Reader::_dup_key_next_row;
        break;
    case KeysType::UNIQUE_KEYS:
        _next_row_func = &Reader::_unique_key_next_row;
        break;
    case KeysType::AGG_KEYS:
        _next_row_func = &Reader::_agg_key_next_row;
        break;
    default:
        break;
    }
    DCHECK(_next_row_func != nullptr) << "No next row function for type:"
        << _tablet->keys_type();

    return OLAP_SUCCESS;
}

/*读取一行数据，此时tablet key的类型为DUP_KEYS*/
OLAPStatus Reader::_dup_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return OLAP_SUCCESS;
    }
    direct_copy_row(row_cursor, *_next_key); // 将_next_key处的一行数据逐列copy到row_cursor指向的地址
    auto res = _collect_iter->next(&_next_key, &_next_delete_flag); //读取下一行数据到_next_key
    if (res != OLAP_SUCCESS) {
        if (res != OLAP_ERR_DATA_EOF) {
            return res;
        }
    }
    return OLAP_SUCCESS;
}

/*读取一行数据，此时tablet key的类型为AGG_KEYS*/
OLAPStatus Reader::_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return OLAP_SUCCESS;
    }
    init_row_with_others(row_cursor, *_next_key, mem_pool, agg_pool); // 将_next_key指向的一行数据copy到row_cursor
    int64_t merged_count = 0;
    do {
        auto res = _collect_iter->next(&_next_key, &_next_delete_flag); // 更新要读的下一行数据到_next_key，以及要读的下一行数据是否已被删除
        if (res != OLAP_SUCCESS) {
            if (res != OLAP_ERR_DATA_EOF) {
                LOG(WARNING) << "next failed:" << res;
                return res;
            }
            break; // 读完所有数据
        }

        if (_aggregation && merged_count > config::doris_scanner_row_num) {
            break; // _aggregation为true，同时已经merge的行数超过了设定的单次读行数上限
        }

        // break while can NOT doing aggregation
        if (!equal_row(_key_cids, *row_cursor, *_next_key)) { // 判断row_cursor与_next_key中key列数据是否都相等，如果相等，则循环读取下一行，直到读到不相等的下一行
            break; // 读到的当前行和下次读取的行key列数据不完全相等
        }
        agg_update_row(_value_cids, row_cursor, *_next_key); // 将当前行与key列相等的_next_key做聚合
        ++merged_count; // 更新本次读一行数据的操作中merge的行数
    } while (true);
    _merged_rows += merged_count; // 更新本次读tablet操作中merge的行数
    // For agg query, we don't need finalize agg object and directly pass agg object to agg node
    if (_need_agg_finalize) {
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
    }

    return OLAP_SUCCESS;
}

/*读取一行数据，此时tablet key的类型为AGG_KEYS*/
OLAPStatus Reader::_unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool, bool* eof) {
    *eof = false;
    bool cur_delete_flag = false;
    do {
        if (UNLIKELY(_next_key == nullptr)) {
            *eof = true;
            return OLAP_SUCCESS;
        }

        cur_delete_flag = _next_delete_flag; // 获取当前行是否被删除的标志
        // the verion is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        direct_copy_row(row_cursor, *_next_key); // 将_next_key处的一行数据逐列copy到row_cursor指向的地址
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
        // skip the lower version rows;
        while (nullptr != _next_key) { // 获取下一行要读取的数据时，跳过与当前行key列数据都相等的行，（key列数据都相等时，版本越新的数据行在堆中越靠前，会先被读出来）
            auto res = _collect_iter->next(&_next_key, &_next_delete_flag); // 更新要读的下一行数据到_next_key，以及要读的下一行数据是否已被删除
            if (res != OLAP_SUCCESS) {
                if (res != OLAP_ERR_DATA_EOF) {
                    return res;
                }
                break; // 读完所有数据
            }

            // break while can NOT doing aggregation
            if (!equal_row(_key_cids, *row_cursor, *_next_key)) { // 判断row_cursor与_next_key中key列数据是否都相等，如果相等，则循环读取下一行，直到读到不相等的下一行
                break; // 读到的当前行和下次读取的行key列数据不完全相等
            }
        }
        if (!cur_delete_flag) { // 判断当前行是否被删除，如果已被删除，则循环读取下一行
            return OLAP_SUCCESS;
        }

        _stats.rows_del_filtered++; // 更新因为被删除而过滤的行数
    } while (cur_delete_flag); // 如果当前行被删除，则继续读取下一行

    return OLAP_SUCCESS;
}

/*关闭reader*/
void Reader::close() {
    VLOG(3) << "merged rows:" << _merged_rows;
    _conditions.finalize();
    _delete_handler.finalize();

    for (auto pred : _col_predicates) {
        delete pred;
    }

    delete _collect_iter;
}

/*使用参数传入的read_params获取rs_readers*/
OLAPStatus Reader::_capture_rs_readers(const ReaderParams& read_params) {
    const std::vector<RowsetReaderSharedPtr>* rs_readers = &read_params.rs_readers; // 从read_params中获取所有rowset reader
    if (rs_readers->empty()) {
        LOG(WARNING) << "fail to acquire data sources. tablet=" << _tablet->full_name();
        return OLAP_ERR_VERSION_NOT_EXIST;
    }

    bool eof = false;
    for (int i = 0; i < _keys_param.start_keys.size(); ++i) {
        RowCursor* start_key = _keys_param.start_keys[i];
        RowCursor* end_key = _keys_param.end_keys[i];
        bool is_lower_key_included = false;
        bool is_upper_key_included = false;
        if (_keys_param.end_range == "lt") {
            is_upper_key_included = false;
        } else if (_keys_param.end_range == "le") {
            is_upper_key_included = true;
        } else {
            LOG(WARNING) << "reader params end_range is error. "
                         << "range=" << _keys_param.to_string();
            return OLAP_ERR_READER_GET_ITERATOR_ERROR;
        }

        if (_keys_param.range == "gt") {
            if (end_key != nullptr && compare_row_key(*start_key, *end_key) >= 0) {
                VLOG(3) << "return EOF when range=" << _keys_param.range
                        << ", start_key=" << start_key->to_string()
                        << ", end_key=" << end_key->to_string();
                eof = true;
                break;
            }
            is_lower_key_included = false;
        } else if (_keys_param.range == "ge") {
            if (end_key != nullptr && compare_row_key(*start_key, *end_key) > 0) {
                VLOG(3) << "return EOF when range=" << _keys_param.range
                        << ", start_key=" << start_key->to_string()
                        << ", end_key=" << end_key->to_string();
                eof = true;
                break;
            }
            is_lower_key_included = true;
        } else if (_keys_param.range == "eq") {
            is_lower_key_included = true;
            is_upper_key_included = true;
        } else {
            LOG(WARNING) << "reader params range is error. "
                         << "range=" << _keys_param.to_string();
            return OLAP_ERR_READER_GET_ITERATOR_ERROR;
        }

        _is_lower_keys_included.push_back(is_lower_key_included);
        _is_upper_keys_included.push_back(is_upper_key_included);
    }

    if (eof) { return OLAP_SUCCESS; }

    bool need_ordered_result = true;
    if (read_params.reader_type == READER_QUERY) {
        if (_tablet->tablet_schema().keys_type() == DUP_KEYS) {
            // duplicated keys are allowed, no need to merge sort keys in rowset
            need_ordered_result = false; // 如果tablet key的类型为DUP_KEYS时，读取的行数据不需要排序
        }
        if (_aggregation) {
            // compute engine will aggregate rows with the same key,
            // it's ok for rowset to return unordered result
            need_ordered_result = false; // 如果成员变量_aggregation的值为true，上层计算引擎会对数据进行聚合，从rowset中读到的数据可以是无需的
        }
    }

    _reader_context.reader_type = read_params.reader_type;
    _reader_context.tablet_schema = &_tablet->tablet_schema();
    _reader_context.need_ordered_result = need_ordered_result;
    _reader_context.return_columns = &_return_columns;
    _reader_context.seek_columns = &_seek_columns;
    _reader_context.load_bf_columns = &_load_bf_columns;
    _reader_context.conditions = &_conditions;
    _reader_context.predicates = &_col_predicates;
    _reader_context.lower_bound_keys = &_keys_param.start_keys;
    _reader_context.is_lower_keys_included = &_is_lower_keys_included;
    _reader_context.upper_bound_keys = &_keys_param.end_keys;
    _reader_context.is_upper_keys_included = &_is_upper_keys_included;
    _reader_context.delete_handler = &_delete_handler;
    _reader_context.stats = &_stats;
    _reader_context.runtime_state = read_params.runtime_state;
    _reader_context.use_page_cache = read_params.use_page_cache;
    for (auto& rs_reader : *rs_readers) {  // 依次遍历每一个rowset reader
        RETURN_NOT_OK(rs_reader->init(&_reader_context)); // 初始化rowset reader
        OLAPStatus res = _collect_iter->add_child(rs_reader); // 向成员变量中添加rs_reader对应的ChildCtx对象，此时会初始化_cur_child
        if (res != OLAP_SUCCESS && res != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to add child to iterator";
            return res;
        }
        _rs_readers.push_back(rs_reader); // 将rowset reader添加到成员变量_rs_readers中进行管理
    }

    _next_key = _collect_iter->current_row(&_next_delete_flag); // 获取堆顶ChildCtx对象（_cur_child）的当前行来初始化成员变量_next_key
    return OLAP_SUCCESS;
}

/*使用参数传入的read_params初始化Reader对象*/
OLAPStatus Reader::_init_params(const ReaderParams& read_params) {
    read_params.check_validation();

    _aggregation = read_params.aggregation;
    _need_agg_finalize = read_params.need_agg_finalize;
    _reader_type = read_params.reader_type;
    _tablet = read_params.tablet;

    _init_conditions_param(read_params);
    _init_load_bf_columns(read_params);

    OLAPStatus res = _init_delete_condition(read_params); // 根据参数传入的read_params初始化_delete_handler
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init delete param. [res=%d]", res);
        return res;
    }

    res = _init_return_columns(read_params);              // 根据参数传入的read_params初始化_return_columns
    if (res != OLAP_SUCCESS) {
        OLAP_LOG_WARNING("fail to init return columns. [res=%d]", res);
        return res;
    }

    res = _init_keys_param(read_params);                   // 根据参数传入的read_params初始化_keys_param
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to init keys param. res=" << res;
        return res;
    }

    _init_seek_columns();                                   // 初始化_seek_columns

    _collect_iter = new CollectIterator(); // 创建CollectIterator对象，可以通过该对象读取所有涉及到的rowset中的数据
    _collect_iter->init(this);             // 初始化创建的CollectIterator对象

    return res;
}

/*根据参数传入的read_params初始化_return_columns*/
OLAPStatus Reader::_init_return_columns(const ReaderParams& read_params) {
    if (read_params.reader_type == READER_QUERY) { // reader类型为READER_QUERY
        _return_columns = read_params.return_columns; // 获取_return_columns
        if (_delete_handler.conditions_num() != 0 && read_params.aggregation) { // 如果_delete_handler中删除条件的数目不为0，并且读操作需要聚合
            set<uint32_t> column_set(_return_columns.begin(), _return_columns.end());
            for (auto conds : _delete_handler.get_delete_conditions()) { // 依次遍历每一个删除条件
                for (auto cond_column : conds.del_cond->columns()) {
                    if (column_set.find(cond_column.first) == column_set.end()) { // 如果删除条件中涉及的column没有包含在成员变量_return_columns中
                        column_set.insert(cond_column.first);
                        _return_columns.push_back(cond_column.first); // 将删除条件中涉及的column添加到成员变量_return_columns中
                    }
                }
            }
        }
        for (auto id : read_params.return_columns) { // 依次遍历read_params.return_columns中的每一列
            if (_tablet->tablet_schema().column(id).is_key()) {
                _key_cids.push_back(id);    // 将key列的id添加到成员变量_key_cids中管理
            } else {
                _value_cids.push_back(id);  // 将value列的id添加到成员变量_value_cids中管理
            }
        }
    } else if (read_params.return_columns.empty()) { // 如果reader类型不是READER_QUERY，并且read_params.return_columns为空，则默认使用所有列作为_return_columns
        for (size_t i = 0; i < _tablet->tablet_schema().num_columns(); ++i) { // 依次遍历tablet中的每一列
            _return_columns.push_back(i);                       // 将每一列的id添加到成员变量_return_columns
            if (_tablet->tablet_schema().column(i).is_key()) {
                _key_cids.push_back(i);                         // 将key列的id添加到成员变量_key_cids中管理
            } else {
                _value_cids.push_back(i);                       // 将value列的id添加到成员变量_value_cids中管理
            }
        }
        VLOG(3) << "return column is empty, using full column as defaut.";
    } else if (read_params.reader_type == READER_CHECKSUM) { // 如果reader类型是READER_CHECKSUM，则使用read_params.return_columns初始化成员变量_return_columns
        _return_columns = read_params.return_columns;
        for (auto id : read_params.return_columns) {
            if (_tablet->tablet_schema().column(id).is_key()) {
                _key_cids.push_back(id);
            } else {
                _value_cids.push_back(id);
            }
        }
    } else {
        OLAP_LOG_WARNING("fail to init return columns. [reader_type=%d return_columns_size=%u]",
                         read_params.reader_type, read_params.return_columns.size());
        return OLAP_ERR_INPUT_PARAMETER_ERROR;
    }

    std::sort(_key_cids.begin(), _key_cids.end(), std::greater<uint32_t>()); // 对_key_cids中的column id元素进行排序

    return OLAP_SUCCESS;
}

/*初始化_seek_columns*/
void Reader::_init_seek_columns() {
    std::unordered_set<uint32_t> column_set(_return_columns.begin(), _return_columns.end()); // 使用_return_columns初始化column_set
    for (auto& it : _conditions.columns()) { // 遍历condition涉及的列，将这些列的id添加到column_set
        column_set.insert(it.first);
    }
    uint32_t max_key_column_count = 0;
    for (auto key : _keys_param.start_keys) {
        if (key->field_count() > max_key_column_count) {
            max_key_column_count = key->field_count();
        }
    }
    for (auto key : _keys_param.end_keys) {
        if (key->field_count() > max_key_column_count) {
            max_key_column_count = key->field_count();
        }
    }
    for (uint32_t i = 0; i < _tablet->tablet_schema().num_columns(); i++) {
        if (i < max_key_column_count || column_set.find(i) != column_set.end()) {
            _seek_columns.push_back(i);
        }
    }
}

/*根据参数传入的read_params初始化_keys_param*/
OLAPStatus Reader::_init_keys_param(const ReaderParams& read_params) {
    if (read_params.start_key.empty()) {
        return OLAP_SUCCESS;
    }

    _keys_param.range = read_params.range;
    _keys_param.end_range = read_params.end_range;

    size_t start_key_size = read_params.start_key.size();
    _keys_param.start_keys.resize(start_key_size, nullptr); // 为vector类型的_keys_param.start_keys分配大小，并使用nullptr初始化每一个元素
    for (size_t i = 0; i < start_key_size; ++i) { // 依次遍历vector类型的_keys_param.start_keys中的每一个元素
        if ((_keys_param.start_keys[i] = new(nothrow) RowCursor()) == nullptr) { // 创建RowCursor对象，并使用创建的RowCursor对象初始化_keys_param.start_keys[i]
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }

        OLAPStatus res = _keys_param.start_keys[i]->init_scan_key(_tablet->tablet_schema(), // 使用read_params.start_key[i]初始化_keys_param.start_keys[i]
                                                       read_params.start_key[i].values());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }

        res = _keys_param.start_keys[i]->from_tuple(read_params.start_key[i]); // 使用read_params.start_key[i]初始化_keys_param.start_keys[i]
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
    }

    size_t end_key_size = read_params.end_key.size();
    _keys_param.end_keys.resize(end_key_size, NULL); // 为vector类型的_keys_param.end_keys分配大小，并使用nullptr初始化每一个元素
    for (size_t i = 0; i < end_key_size; ++i) { // 依次遍历vector类型的_keys_param.end_keys中的每一个元素
        if ((_keys_param.end_keys[i] = new(nothrow) RowCursor()) == NULL) { // 创建RowCursor对象，并使用创建的RowCursor对象初始化_keys_param.end_keys[i]
            OLAP_LOG_WARNING("fail to new RowCursor!");
            return OLAP_ERR_MALLOC_ERROR;
        }

        OLAPStatus res = _keys_param.end_keys[i]->init_scan_key(_tablet->tablet_schema(),// 使用read_params.end_key[i]初始化_keys_param.end_keys[i]
                                                     read_params.end_key[i].values());
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor. [res=%d]", res);
            return res;
        }

        res = _keys_param.end_keys[i]->from_tuple(read_params.end_key[i]);// 使用read_params.end_key[i]初始化_keys_param.end_keys[i]
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to init row cursor from Keys. [res=%d key_index=%ld]", res, i);
            return res;
        }
    }

    //TODO:check the valid of start_key and end_key.(eg. start_key <= end_key)

    return OLAP_SUCCESS;
}

void Reader::_init_conditions_param(const ReaderParams& read_params) {
    _conditions.set_tablet_schema(&_tablet->tablet_schema());
    for (const auto& condition : read_params.conditions) {
        _conditions.append_condition(condition);
        ColumnPredicate* predicate = _parse_to_predicate(condition);
        if (predicate != nullptr) {
            _col_predicates.push_back(predicate);
        }
    }
}

#define COMPARISON_PREDICATE_CONDITION_VALUE(NAME, PREDICATE) \
ColumnPredicate* Reader::_new_##NAME##_pred(const TabletColumn& column, int index, const std::string& cond) { \
    ColumnPredicate* predicate = nullptr; \
    switch (column.type()) { \
        case OLAP_FIELD_TYPE_TINYINT: { \
            std::stringstream ss(cond); \
            int32_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int8_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_SMALLINT: { \
            std::stringstream ss(cond); \
            int16_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int16_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_INT: { \
            std::stringstream ss(cond); \
            int32_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int32_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_BIGINT: { \
            std::stringstream ss(cond); \
            int64_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int64_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_LARGEINT: { \
            std::stringstream ss(cond); \
            int128_t value = 0; \
            ss >> value; \
            predicate = new PREDICATE<int128_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DECIMAL: { \
            decimal12_t value(0, 0); \
            value.from_string(cond); \
            predicate = new PREDICATE<decimal12_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_CHAR: {\
            StringValue value; \
            size_t length = std::max(static_cast<size_t>(column.length()), cond.length());\
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length)); \
            memset(buffer, 0, length); \
            memory_copy(buffer, cond.c_str(), cond.length()); \
            value.len = length; \
            value.ptr = buffer; \
            predicate = new PREDICATE<StringValue>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_VARCHAR: { \
            StringValue value; \
            int32_t length = cond.length(); \
            char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length)); \
            memory_copy(buffer, cond.c_str(), length); \
            value.len = length; \
            value.ptr = buffer; \
            predicate = new PREDICATE<StringValue>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DATE: { \
            uint24_t value = timestamp_from_date(cond); \
            predicate = new PREDICATE<uint24_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_DATETIME: { \
            uint64_t value = timestamp_from_datetime(cond); \
            predicate = new PREDICATE<uint64_t>(index, value); \
            break; \
        } \
        case OLAP_FIELD_TYPE_BOOL: { \
            std::stringstream ss(cond); \
            bool value = false; \
            ss >> value; \
            predicate = new PREDICATE<bool>(index, value); \
            break; \
        } \
        default: break; \
    } \
 \
    return predicate; \
} \

COMPARISON_PREDICATE_CONDITION_VALUE(eq, EqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ne, NotEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(lt, LessPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(le, LessEqualPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(gt, GreaterPredicate)
COMPARISON_PREDICATE_CONDITION_VALUE(ge, GreaterEqualPredicate)

ColumnPredicate* Reader::_parse_to_predicate(const TCondition& condition) {
    // TODO: not equal and not in predicate is not pushed down
    int index = _tablet->field_index(condition.column_name);
    const TabletColumn& column = _tablet->tablet_schema().column(index);
    if (column.aggregation() != FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE) {
        return nullptr;
    }
    ColumnPredicate* predicate = nullptr;
    if (condition.condition_op == "*=" && condition.condition_values.size() == 1) {
        predicate = _new_eq_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<<") {
        predicate = _new_lt_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "<=") {
        predicate = _new_le_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">>") {
        predicate = _new_gt_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == ">=") {
        predicate = _new_ge_pred(column, index, condition.condition_values[0]);
    } else if (condition.condition_op == "*=" && condition.condition_values.size() > 1) {
        switch (column.type()) {
            case OLAP_FIELD_TYPE_TINYINT: {
                std::set<int8_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int32_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int8_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_SMALLINT: {
                std::set<int16_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int16_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int16_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_INT: {
                std::set<int32_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int32_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int32_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_BIGINT: {
                std::set<int64_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int64_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int64_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_LARGEINT: {
                std::set<int128_t> values;
                for (auto& cond_val : condition.condition_values) {
                    int128_t value = 0;
                    std::stringstream ss(cond_val);
                    ss >> value;
                    values.insert(value);
                }
                predicate = new InListPredicate<int128_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DECIMAL: {
                std::set<decimal12_t> values;
                for (auto& cond_val : condition.condition_values) {
                    decimal12_t value;
                    value.from_string(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<decimal12_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_CHAR: {
                std::set<StringValue> values;
                for (auto& cond_val : condition.condition_values) {
                    StringValue value;
                    size_t length = std::max(static_cast<size_t>(column.length()), cond_val.length());
                    char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                    memset(buffer, 0, length);
                    memory_copy(buffer, cond_val.c_str(), cond_val.length());
                    value.len = length;
                    value.ptr = buffer;
                    values.insert(value);
                }
                predicate = new InListPredicate<StringValue>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_VARCHAR: {
                std::set<StringValue> values;
                for (auto& cond_val : condition.condition_values) {
                    StringValue value;
                    int32_t length = cond_val.length();
                    char* buffer = reinterpret_cast<char*>(_predicate_mem_pool->allocate(length));
                    memory_copy(buffer, cond_val.c_str(), length);
                    value.len = length;
                    value.ptr = buffer;
                    values.insert(value);
                }
                predicate = new InListPredicate<StringValue>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DATE: {
                std::set<uint24_t> values;
                for (auto& cond_val : condition.condition_values) {
                    uint24_t value = timestamp_from_date(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<uint24_t>(index, std::move(values));
                break;
            }
            case OLAP_FIELD_TYPE_DATETIME: {
                std::set<uint64_t> values;
                for (auto& cond_val : condition.condition_values) {
                    uint64_t value = timestamp_from_datetime(cond_val);
                    values.insert(value);
                }
                predicate = new InListPredicate<uint64_t>(index, std::move(values));
                break;
            }
            // OLAP_FIELD_TYPE_BOOL is not valid in this case.
            default: break;
        }
    } else if (condition.condition_op == "is") {
        predicate = new NullPredicate(index, condition.condition_values[0] == "null");
    }
    return predicate;
}

void Reader::_init_load_bf_columns(const ReaderParams& read_params) {
    // add all columns with condition to _load_bf_columns
    for (const auto& cond_column : _conditions.columns()) {
        for (const Cond* cond : cond_column.second->conds()) {
            if (cond->op == OP_EQ
                    || (cond->op == OP_IN && cond->operand_set.size() < MAX_OP_IN_FIELD_NUM)) {
                _load_bf_columns.insert(cond_column.first);
            }
        }
    }

    // remove columns which have no bf stream
    for (int i = 0; i < _tablet->tablet_schema().num_columns(); ++i) {
        if (!_tablet->tablet_schema().column(i).is_bf_column()) {
            _load_bf_columns.erase(i);
        }
    }

    // remove columns which have same value between start_key and end_key
    int min_scan_key_len = _tablet->tablet_schema().num_columns();
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        if (read_params.start_key[i].size() < min_scan_key_len) {
            min_scan_key_len = read_params.start_key[i].size();
        }
    }

    for (int i = 0; i < read_params.end_key.size(); ++i) {
        if (read_params.end_key[i].size() < min_scan_key_len) {
            min_scan_key_len = read_params.end_key[i].size();
        }
    }

    int max_equal_index = -1;
    for (int i = 0; i < read_params.start_key.size(); ++i) {
        int j = 0;
        for (; j < min_scan_key_len; ++j) {
            if (read_params.start_key[i].get_value(j) != read_params.end_key[i].get_value(j)) {
                break;
            }
        }

        if (max_equal_index < j - 1) {
            max_equal_index = j - 1;
        }
    }

    for (int i = 0; i < max_equal_index; ++i) {
        _load_bf_columns.erase(i);
    }

    // remove the max_equal_index column when it's not varchar
    // or longer than number of short key fields
    if (max_equal_index == -1) {
        return;
    }
    FieldType type = _tablet->tablet_schema().column(max_equal_index).type();
    if (type != OLAP_FIELD_TYPE_VARCHAR || max_equal_index + 1 > _tablet->num_short_key_columns()) {
        _load_bf_columns.erase(max_equal_index);
    }
}

/*根据参数传入的read_params初始化_delete_handler*/
OLAPStatus Reader::_init_delete_condition(const ReaderParams& read_params) {
    if (read_params.reader_type != READER_CUMULATIVE_COMPACTION) { // 如果reader类型不是READER_CUMULATIVE_COMPACTION
        _tablet->obtain_header_rdlock();
        OLAPStatus ret = _delete_handler.init(_tablet->tablet_schema(), // 初始化成员变量_delete_handler
                                              _tablet->delete_predicates(),
                                              read_params.version.second);
        _tablet->release_header_lock();
        return ret;
    } else {
        return OLAP_SUCCESS;
    }
}

}  // namespace doris
