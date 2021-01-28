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

#include "runtime/tablets_channel.h"

#include "exec/tablet_info.h"
#include "gutil/strings/substitute.h"
#include "olap/delta_writer.h"
#include "olap/memtable.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "util/doris_metrics.h"

namespace doris {

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count; //本次数据加载涉及的tablet数目

/*构造函数，用来创建TabletsChannel对象*/
TabletsChannel::TabletsChannel(const TabletsChannelKey& key, MemTracker* mem_tracker):
        _key(key), _state(kInitialized), _closed_senders(64) {
    _mem_tracker.reset(new MemTracker(-1, "tablets channel", mem_tracker));
    // 在多线程编程中，有时某个任务只需要执行一次，可以用C++11中的std::call_once函数配合std::once_flag来实现。
    // 如果多个线程需要同时调用某个函数，std::call_once可以保证多个线程对该函数只调用一次。
    static std::once_flag once_flag;
    std::call_once(once_flag, [] {
        REGISTER_GAUGE_DORIS_METRIC(tablet_writer_count, [&]() {
            return _s_tablet_writer_count.load();
        });
    }); // 注册metric： tablet_writer_count
}

/*析构函数*/
TabletsChannel::~TabletsChannel() {
    _s_tablet_writer_count -= _tablet_writers.size();//更新static的全局变量_s_tablet_writer_count
    for (auto& it : _tablet_writers) {
        delete it.second;
    }
    delete _row_desc;
    delete _schema;
}

/*打开TabletsChannel*/
Status TabletsChannel::open(const PTabletWriterOpenRequest& params) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kOpened) { //判断TabletsChannel是否已经被其他sender开启
        // Normal case, already open by other sender
        return Status::OK();
    }
    LOG(INFO) << "open tablets channel: " << _key
              << ", tablets num: " << params.tablets().size()
              << ", timeout(s): " << params.load_channel_timeout_s();
    _txn_id = params.txn_id();
    _index_id = params.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(params.schema())); //使用参数传入的schema初始化成员变量_schema
    _tuple_desc = _schema->tuple_desc();
    _row_desc = new RowDescriptor(_tuple_desc, false);

    _num_remaining_senders = params.num_senders(); //获取本次数据加载的sender数量
    _next_seqs.resize(_num_remaining_senders, 0);  //使用0初始化vector类型的成员变量_next_seqs，即每一个sender下一个batch的期望序号
    _closed_senders.Reset(_num_remaining_senders);

    RETURN_IF_ERROR(_open_all_writers(params));//为PTabletWriterOpenRequest参数params中的每一个tablet创建DeltaWriter对象

    _state = kOpened;
    return Status::OK();
}

/*根据参数params加载一个batch的数据，其中包含多行数据，数据保存在参数params中*/
Status TabletsChannel::add_batch(const PTabletWriterAddBatchRequest& params) {
    DCHECK(params.tablet_ids_size() == params.row_batch().num_rows());
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    auto next_seq = _next_seqs[params.sender_id()]; //获取当前sender的本次batch数据期望的序号
    // check packet
    if (params.packet_seq() < next_seq) { //当前batch序号小于期望的batch序号，当前batch已经被加载过了
        LOG(INFO) << "packet has already recept before, expect_seq=" << next_seq
            << ", recept_seq=" << params.packet_seq();
        return Status::OK();
    } else if (params.packet_seq() > next_seq) { //当前batch序号大于期望的batch序号，中间存在的batch数据丢失
        LOG(WARNING) << "lost data packet, expect_seq=" << next_seq
            << ", recept_seq=" << params.packet_seq();
        return Status::InternalError("lost data packet");
    }

    //当前数据batch序号与期望的batch序号相同
    RowBatch row_batch(*_row_desc, params.row_batch(), _mem_tracker.get()); //获取一个多行的bach的数据，创建RowBatch对象

    // iterator all data
    for (int i = 0; i < params.tablet_ids_size(); ++i) { //遍历当前batch中的每一行数据
        auto tablet_id = params.tablet_ids(i); //获取tablet id
        auto it = _tablet_writers.find(tablet_id); //在成员变量_tablet_writers中查找tablet_id
        if (it == std::end(_tablet_writers)) {
            return Status::InternalError(strings::Substitute(
                    "unknown tablet to append data, tablet=$0", tablet_id));
        }
        auto st = it->second->write(row_batch.get_row(i)->get_tuple(0)); //通过DeltaWriter对象写一行数据到memtable中
        if (st != OLAP_SUCCESS) {
            const std::string& err_msg = strings::Substitute(
                    "tablet writer write failed, tablet_id=$0, txn_id=$1, err=$2",
                    it->first, _txn_id, st);
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
    }
    _next_seqs[params.sender_id()]++; //更新当前sender的下一次batch数据加载的序号
    return Status::OK();
}

/*关闭TabletsChannel*/
Status TabletsChannel::close(int sender_id, bool* finished,
        const google::protobuf::RepeatedField<int64_t>& partition_ids,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    if (_closed_senders.Get(sender_id)) { //判断当前sender的数据加载是否已经被关闭了
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }
    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id;
    for (auto pid : partition_ids) {
        _partition_ids.emplace(pid);
    }
    _closed_senders.Set(sender_id, true); //更新当前sender的关闭标志
    _num_remaining_senders--;             //更新本次数据加载还未关闭的sender数量
    *finished = (_num_remaining_senders == 0);
    if (*finished) { //判断本次数据加载所有的sender是否已经被关闭
        _state = kFinished; //更新tablet channel状态为kFinished
        // All senders are closed
        // 1. close all delta writers
        std::vector<DeltaWriter*> need_wait_writers;
        for (auto& it : _tablet_writers) { //遍历每一个DeltaWriter对象
            if (_partition_ids.count(it.second->partition_id()) > 0) { //set::count()是C++ STL中的内置函数，它返回元素在集合中出现的次数，由于set容器仅包含唯一元素，因此只能返回1或0。
                auto st = it.second->close(); //关闭DeltaWriter，实质上只是通过flush token将memtable提交给flush executor
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "close tablet writer failed, tablet_id=" << it.first
                        << ", transaction_id=" << _txn_id << ", err=" << st;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                need_wait_writers.push_back(it.second);
            } else {
                auto st = it.second->cancel(); //取消DeltaWriter,丢弃当前的memtable，等待所有flush期间的memtable被销毁
                if (st != OLAP_SUCCESS) {
                    LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << it.first
                        << ", transaction_id=" << _txn_id;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
            }
        }

        // 2. wait delta writers and build the tablet vector
        for (auto writer : need_wait_writers) {
            // close may return failed, but no need to handle it here.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            writer->close_wait(tablet_vec); //等待flush任务结束，更新rowset的meta信息，并向FE提交事务
        }
        // TODO(gaodayue) clear and destruct all delta writers to make sure all memory are freed
        // DCHECK_EQ(_mem_tracker->consumption(), 0);
    }
    return Status::OK();
}

/*降低内存消耗量（通过flush token将内存占用量最大的memtable提交给flush executor,并等待memtable完成刷写）*/
Status TabletsChannel::reduce_mem_usage() {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        // TabletsChannel is closed without LoadChannel's lock,
        // therefore it's possible for reduce_mem_usage() to be called right after close()
        return _close_status;
    }
    // find tablet writer with largest mem consumption
    // 获取内存占用量最大的DeltaWriter对象
    int64_t max_consume = 0L;
    DeltaWriter* writer = nullptr;
    for (auto& it : _tablet_writers) {
        if (it.second->mem_consumption() > max_consume) {
            max_consume = it.second->mem_consumption();
            writer = it.second;
        }
    }

    if (writer == nullptr || max_consume == 0) {
        // barely not happend, just return OK
        return Status::OK();
    }

    VLOG(3) << "pick the delte writer to flush, with mem consumption: " << max_consume
            << ", channel key: " << _key;
    OLAPStatus st = writer->flush_memtable_and_wait(); //通过flush token将内存占用量最大的memtable提交给flush executor,并等待memtable刷写
    if (st != OLAP_SUCCESS) {
        // flush failed, return error
        std::stringstream ss;
        ss << "failed to reduce mem consumption by flushing memtable. err: " << st;
        return Status::InternalError(ss.str());
    }
    return Status::OK();
}

/*为PTabletWriterOpenRequest参数中的每一个tablet创建DeltaWriter对象*/
Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& params) {
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        std::stringstream ss;
        ss << "unknown index id, key=" << _key;
        return Status::InternalError(ss.str());
    }
    for (auto& tablet : params.tablets()) { //依次遍历本次数据加载的每一个tablet
        WriteRequest request;
        request.tablet_id = tablet.tablet_id();
        request.schema_hash = schema_hash;
        request.write_type = WriteType::LOAD;
        request.txn_id = _txn_id;
        request.partition_id = tablet.partition_id();
        request.load_id = params.id();
        request.need_gen_rollup = params.need_gen_rollup();
        request.tuple_desc = _tuple_desc;
        request.slots = index_slots;

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&request, _mem_tracker.get(),  &writer); //为当前tablet创建DeltaWriter对象，通过参数writer返回
        if (st != OLAP_SUCCESS) {
            std::stringstream ss;
            ss << "open delta writer failed, tablet_id=" << tablet.tablet_id()
                << ", txn_id=" << _txn_id
                << ", partition_id=" << tablet.partition_id()
                << ", err=" << st;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }
        _tablet_writers.emplace(tablet.tablet_id(), writer); //将创建的DeltaWriter对象添加到std::unordered_map<int64_t, DeltaWriter*>类型的成员变量_tablet_writers中进行管理
    }
    _s_tablet_writer_count += _tablet_writers.size(); //更新static的全局变量_s_tablet_writer_count
    DCHECK_EQ(_tablet_writers.size(), params.tablets_size());
    return Status::OK();
}

/*取消TabletsChannel*/
Status TabletsChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    for (auto& it : _tablet_writers) {
        it.second->cancel(); //依次取消每一个DeltaWriter,丢弃memtable，等待所有flush期间的memtable被销毁
    }
    DCHECK_EQ(_mem_tracker->consumption(), 0);
    _state = kFinished; //更新tablet channel状态为kFinished
    return Status::OK();
}

std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

}
