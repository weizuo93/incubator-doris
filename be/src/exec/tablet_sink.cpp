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

#include "exec/tablet_sink.h"

#include <sstream>

#include "exprs/expr.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"

#include "olap/hll.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/monotime.h"
#include "util/uid_util.h"

namespace doris {
namespace stream_load {

/*NodeChannel的构造函数*/
NodeChannel::NodeChannel(OlapTableSink* parent, int64_t index_id, int64_t node_id,
                         int32_t schema_hash)
        : _parent(parent), _index_id(index_id), _node_id(node_id), _schema_hash(schema_hash) {}

/*NodeChannel的析构函数*/
NodeChannel::~NodeChannel() {
    if (_open_closure != nullptr) {
        if (_open_closure->unref()) {
            delete _open_closure;
        }
        _open_closure = nullptr;
    }
    if (_add_batch_closure != nullptr) {
        // it's safe to delete, but may take some time to wait until brpc joined
        delete _add_batch_closure;
        _add_batch_closure = nullptr;
    }
    _cur_add_batch_request.release_id();
}

/*初始化NodeChannel*/
// 每一个NodeChannel对象对应一个BE节点（NodeChannel与schema有关，table的不同rollup数据导入相同BE时对应的NodeChannel对象不同）
Status NodeChannel::init(RuntimeState* state) {
    _tuple_desc = _parent->_output_tuple_desc; // 获取tuple desc
    _node_info = _parent->_nodes_info->find_node(_node_id); // 获取BE节点id
    if (_node_info == nullptr) {
        std::stringstream ss;
        ss << "unknown node id, id=" << _node_id;
        _cancelled = true;
        return Status::InternalError(ss.str());
    }

    _row_desc.reset(new RowDescriptor(_tuple_desc, false)); // 根据tuple desc创建RowDescriptor
    _batch_size = state->batch_size(); // 获取batch size
    _cur_batch.reset(new RowBatch(*_row_desc, _batch_size, _parent->_mem_tracker)); // 根据row desc和batch size创建RowBatch对象

    _stub = state->exec_env()->brpc_stub_cache()->get_stub(_node_info->host, _node_info->brpc_port); // 针对当前NodeChannel对象对应的BE节点，获取brpc的stub
    if (_stub == nullptr) {
        LOG(WARNING) << "Get rpc stub failed, host=" << _node_info->host
                     << ", port=" << _node_info->brpc_port;
        _cancelled = true;
        return Status::InternalError("get rpc stub failed");
    }

    // Initialize _cur_add_batch_request
    _cur_add_batch_request.set_allocated_id(&_parent->_load_id); // 设置add batch的load id
    _cur_add_batch_request.set_index_id(_index_id);              // 设置add batch的index id
    _cur_add_batch_request.set_sender_id(_parent->_sender_id);   // 设置add batch的sender id
    _cur_add_batch_request.set_eos(false);

    _rpc_timeout_ms = state->query_options().query_timeout * 1000; // 设置brpc的超时时间

    _load_info = "load_id=" + print_id(_parent->_load_id) +
                 ", txn_id=" + std::to_string(_parent->_txn_id);
    _name = "NodeChannel[" + std::to_string(_index_id) + "-" + std::to_string(_node_id) + "]";
    return Status::OK();
}

/*打开NodeChannel*/
void NodeChannel::open() {
    PTabletWriterOpenRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_txn_id(_parent->_txn_id);
    request.set_allocated_schema(_parent->_schema->to_protobuf());
    for (auto& tablet : _all_tablets) { // 遍历当前NodeChannel涉及的所有tablet，添加到request中
        auto ptablet = request.add_tablets();
        ptablet->set_partition_id(tablet.partition_id);
        ptablet->set_tablet_id(tablet.tablet_id);
    }
    request.set_num_senders(_parent->_num_senders);
    request.set_need_gen_rollup(_parent->_need_gen_rollup);
    request.set_load_mem_limit(_parent->_load_mem_limit);
    request.set_load_channel_timeout_s(_parent->_load_channel_timeout_s);

    _open_closure = new RefCountClosure<PTabletWriterOpenResult>();
    _open_closure->ref();

    // This ref is for RPC's reference
    _open_closure->ref();
    _open_closure->cntl.set_timeout_ms(config::tablet_writer_open_rpc_timeout_sec * 1000);
    _stub->tablet_writer_open(&_open_closure->cntl, &request, &_open_closure->result,
                              _open_closure); // 通过RPC打开tablet_writer
    request.release_id();
    request.release_schema();
}

/*等待打开NodeChannel*/
Status NodeChannel::open_wait() {
    _open_closure->join();
    if (_open_closure->cntl.Failed()) {
        LOG(WARNING) << "failed to open tablet writer, error="
                     << berror(_open_closure->cntl.ErrorCode())
                     << ", error_text=" << _open_closure->cntl.ErrorText();
        _cancelled = true;
        return Status::InternalError("failed to open tablet writer");
    }
    Status status(_open_closure->result.status());
    if (_open_closure->unref()) {
        delete _open_closure;
    }
    _open_closure = nullptr;

    if (!status.ok()) {
        _cancelled = true;
        return status;
    }

    // add batch closure
    _add_batch_closure = ReusableClosure<PTabletWriterAddBatchResult>::create(); // 创建ReusableClosure对象
    _add_batch_closure->addFailedHandler([this]() { // 设置add batch失败的回调
        _cancelled = true;
        LOG(WARNING) << name() << " add batch req rpc failed, " << print_load_info()
                     << ", node=" << node_info()->host << ":" << node_info()->brpc_port;
    });

    _add_batch_closure->addSuccessHandler(           // 设置数据batch通过brpc发送到executor BE之后，并被成功写入tablet的回调
            [this](const PTabletWriterAddBatchResult& result, bool is_last_rpc) {
                Status status(result.status());
                if (status.ok()) {
                    if (is_last_rpc) {
                        for (auto& tablet : result.tablet_vec()) {
                            TTabletCommitInfo commit_info;
                            commit_info.tabletId = tablet.tablet_id();
                            commit_info.backendId = _node_id;
                            _tablet_commit_infos.emplace_back(std::move(commit_info));
                        }
                        _add_batches_finished = true; //当前NodeChannel的最后一个数据batch发送结束并在executor BE被处理完成之后，将成员变量_add_batches_finished置为true
                    }
                } else {
                    _cancelled = true;
                    LOG(WARNING) << name() << " add batch req success but status isn't ok, "
                                 << print_load_info() << ", node=" << node_info()->host << ":"
                                 << node_info()->brpc_port << ", errmsg=" << status.get_error_msg();
                }

                if (result.has_execution_time_us()) {
                    _add_batch_counter.add_batch_execution_time_us += result.execution_time_us();
                    _add_batch_counter.add_batch_wait_lock_time_us += result.wait_lock_time_us();
                    _add_batch_counter.add_batch_num++;
                }
            });

    return status;
}

/*添加一行数据到当前BE对应的NodeChannel*/
Status NodeChannel::add_row(Tuple* input_tuple, int64_t tablet_id) {
    // If add_row() when _eos_is_produced==true, there must be sth wrong, we can only mark this channel as failed.
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        return st.clone_and_prepend("already stopped, can't add_row. cancelled/eos: ");
    }

    // We use OlapTableSink mem_tracker which has the same ancestor of _plan node,
    // so in the ideal case, mem limit is a matter for _plan node.
    // But there is still some unfinished things, we do mem limit here temporarily.
    // _cancelled may be set by rpc callback, and it's possible that _cancelled might be set in any of the steps below.
    // It's fine to do a fake add_row() and return OK, because we will check _cancelled in next add_row() or mark_close().
    while (!_cancelled && _parent->_mem_tracker->any_limit_exceeded() && _pending_batches_num > 0) {
        SCOPED_RAW_TIMER(&_mem_exceeded_block_ns);
        SleepFor(MonoDelta::FromMilliseconds(10)); // 如果内存超过阈值，则当前过程被阻塞一段时间
    }

    auto row_no = _cur_batch->add_row(); // 添加一行数据，获取要添加行的行号
    if (row_no == RowBatch::INVALID_ROW_INDEX) {
        // _cur_batch中的数据行达到了batch size，一个batch数据已满
        {
            SCOPED_RAW_TIMER(&_queue_push_lock_ns);
            std::lock_guard<std::mutex> l(_pending_batches_lock);
            //To simplify the add_row logic, postpone adding batch into req until the time of sending req
            _pending_batches.emplace(std::move(_cur_batch), _cur_add_batch_request); // 生产出一个batch的数据，并将当前batch的数据添加到成员变量_pending_batches中供消费者使用
            _pending_batches_num++; // 更新成员变量，记录待发送的数据batch的数量
        }

        _cur_batch.reset(new RowBatch(*_row_desc, _batch_size, _parent->_mem_tracker)); // 清空数据batch，reset成员变量_cur_batch
        _cur_add_batch_request.clear_tablet_ids();

        row_no = _cur_batch->add_row(); // 将需要添加的数据行添加到新的数据batch中
    }
    DCHECK_NE(row_no, RowBatch::INVALID_ROW_INDEX);
    auto tuple = input_tuple->deep_copy(*_tuple_desc, _cur_batch->tuple_data_pool()); // 对要添加的行执行深拷贝
    _cur_batch->get_row(row_no)->set_tuple(0, tuple); // 设置行号为row_no的行数据为深拷贝得到的tuple
    _cur_batch->commit_last_row(); // 提交一行
    _cur_add_batch_request.add_tablet_ids(tablet_id);
    return Status::OK();
}

/*将NodeChannel标记为close*/
Status NodeChannel::mark_close() {
    auto st = none_of({_cancelled, _eos_is_produced});
    if (!st.ok()) {
        return st.clone_and_prepend("already stopped, can't mark as closed. cancelled/eos: ");
    }

    _cur_add_batch_request.set_eos(true);
    {
        std::lock_guard<std::mutex> l(_pending_batches_lock);
        _pending_batches.emplace(std::move(_cur_batch), _cur_add_batch_request); // 将NodeChannel标记为close，并将当前的数据batch添加到成员变量_pending_batches
        _pending_batches_num++;
        DCHECK(_pending_batches.back().second.eos());
    }

    _eos_is_produced = true; // 更新标志_eos_is_produced
    return Status::OK();
}

/*等待关闭NodeChannel*/
Status NodeChannel::close_wait(RuntimeState* state) {
    auto st = none_of({_cancelled, !_eos_is_produced});
    if (!st.ok()) {
        return st.clone_and_prepend("already stopped, skip waiting for close. cancelled/!eos: ");
    }

    // waiting for finished, it may take a long time, so we could't set a timeout
    MonotonicStopWatch timer;
    timer.start();
    while (!_add_batches_finished && !_cancelled) { // 等待当前NodeChannel的所有数据batch都通过brpc发送到了对应的BE节点或等待当前NodeChannel被cancel
        SleepFor(MonoDelta::FromMilliseconds(1));
    }
    timer.stop();
    VLOG(1) << name() << " close_wait cost: " << timer.elapsed_time() / 1000000 << " ms";

    if (_add_batches_finished) { // 当前NodeChannel的所有数据batch都通过brpc发送到了对应的BE节点
        {
            std::lock_guard<std::mutex> lg(_pending_batches_lock);
            CHECK(_pending_batches.empty()) << name();
            CHECK(_cur_batch == nullptr) << name();
        }
        state->tablet_commit_infos().insert(state->tablet_commit_infos().end(),
                                            std::make_move_iterator(_tablet_commit_infos.begin()),
                                            std::make_move_iterator(_tablet_commit_infos.end())); // 将当前NodeChannel涉及的tablet添加到runtimestate的_tablet_commit_infos成员变量中，用于stream load txn的commit
        return Status::OK();
    }

    return Status::InternalError("close wait failed coz rpc error");
}

/*关闭NodeChannel*/
void NodeChannel::cancel() {
    // we don't need to wait last rpc finished, cause closure's release/reset will join.
    // But do we need brpc::StartCancel(call_id)?
    _cancelled = true;

    PTabletWriterCancelRequest request;
    request.set_allocated_id(&_parent->_load_id);
    request.set_index_id(_index_id);
    request.set_sender_id(_parent->_sender_id);

    auto closure = new RefCountClosure<PTabletWriterCancelResult>();

    closure->ref();
    closure->cntl.set_timeout_ms(_rpc_timeout_ms);
    _stub->tablet_writer_cancel(&closure->cntl, &request, &closure->result, closure); // 通过RPC取消tablet_writer
    request.release_id();
}

/*发送一个batch的数据*/
int NodeChannel::try_send_and_fetch_status() {
    auto st = none_of({_cancelled, _send_finished}); // 数据发送没有完成，并且没有被取消
    if (!st.ok()) {
        return 0;
    }

    if (!_add_batch_closure->is_packet_in_flight() && _pending_batches_num > 0) {
        SCOPED_RAW_TIMER(&_actual_consume_ns);
        AddBatchReq send_batch;
        {
            std::lock_guard<std::mutex> lg(_pending_batches_lock);
            DCHECK(!_pending_batches.empty());
            send_batch = std::move(_pending_batches.front()); // 需要通过NodeChannel发送的数据batch都保存在成员变量_pending_batches中，由生产者生产，由数据发送线程消费
            _pending_batches.pop();
            _pending_batches_num--;
        }

        auto row_batch = std::move(send_batch.first); // 获取要发送的一个batch数据
        auto request = std::move(send_batch.second); // doesn't need to be saved in heap

        // tablet_ids has already set when add row

        request.set_packet_seq(_next_packet_seq);
        if (row_batch->num_rows() > 0) {
            SCOPED_RAW_TIMER(&_serialize_batch_ns);
            row_batch->serialize(request.mutable_row_batch()); // 将需要发送的一个batch数据序列化成PB格式
        }

        _add_batch_closure->reset();
        _add_batch_closure->cntl.set_timeout_ms(_rpc_timeout_ms);

        if (request.eos()) {
            for (auto pid : _parent->_partition_ids) {
                request.add_partition_ids(pid);
            }

            // eos request must be the last request
            _add_batch_closure->end_mark();
            _send_finished = true; // 当前batch为最后一个数据batch，数据发送完成
            DCHECK(_pending_batches_num == 0);
        }

        _add_batch_closure->set_in_flight();
        _stub->tablet_writer_add_batch(&_add_batch_closure->cntl, &request,
                                       &_add_batch_closure->result, _add_batch_closure); // 通过RPC发送一个batch的数据

        _next_packet_seq++;
    }

    return _send_finished ? 0 : 1; // 如果当前NodeChannel发送数据完成，则返回0；否则，返回1
}

Status NodeChannel::none_of(std::initializer_list<bool> vars) {
    bool none = std::none_of(vars.begin(), vars.end(), [](bool var) { return var; });
    Status st = Status::OK();
    if (!none) {
        std::string vars_str;
        std::for_each(vars.begin(), vars.end(),
                      [&vars_str](bool var) -> void { vars_str += (var ? "1/" : "0/"); });
        if (!vars_str.empty()) {
            vars_str.pop_back(); // 0/1/0/ -> 0/1/0
        }
        st = Status::InternalError(vars_str);
    }

    return st;
}

/*清空NodeChannel（成员变量_pending_batches）中的所有batch*/
void NodeChannel::clear_all_batches() {
    std::lock_guard<std::mutex> lg(_pending_batches_lock);
    std::queue<AddBatchReq> empty;
    std::swap(_pending_batches, empty);
    _cur_batch.reset();
}

IndexChannel::~IndexChannel() {}

/*初始化IndexChannel*/
// 每一个IndexChannel对象对应一个table的rollup
Status IndexChannel::init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets) {
    for (auto& tablet : tablets) { // 遍历当前rollup下的每一个tablet
        auto location = _parent->_location->find_tablet(tablet.tablet_id);
        if (location == nullptr) {
            LOG(WARNING) << "unknow tablet, tablet_id=" << tablet.tablet_id;
            return Status::InternalError("unknown tablet");
        }
        std::vector<NodeChannel*> channels;
        for (auto& node_id : location->node_ids) { // 遍历当前tablet分布的每一个BE节点
            NodeChannel* channel = nullptr;
            auto it = _node_channels.find(node_id); // 查找某一个BE节点对应的NodeChannel是否在成员变量_node_channels中
            if (it == std::end(_node_channels)) {
                channel = _parent->_pool->add(
                        new NodeChannel(_parent, _index_id, node_id, _schema_hash)); // 如果BE节点对应的NodeChannel不在成员变量_node_channels中，则创建NodeChannel
                _node_channels.emplace(node_id, channel); //将BE节点添加到成员变量_node_channels中管理
            } else {
                channel = it->second; // 如果BE节点对应的NodeChannel在成员变量_node_channels中，则获取BE节点对应的NodeChannel
            }
            channel->add_tablet(tablet); // 将当前tablet添加到BE节点对应的NodeChannel
            channels.push_back(channel); // channels中保存当前tablet分布的所有BE节点对应的NodeChannel
        }
        _channels_by_tablet.emplace(tablet.tablet_id, std::move(channels)); // 将当前tablet分布的所有BE节点对应的NodeChannel添加到成员变量_channels_by_tablet中管理
    }
    for (auto& it : _node_channels) { // 依次遍历并初始化当前rollup下所有tablet分布的所有BE对应的NodeChannel
        RETURN_IF_ERROR(it.second->init(state));
    }
    return Status::OK();
}

/*添加一行数据到当前rollup对应的IndexChannel*/
Status IndexChannel::add_row(Tuple* tuple, int64_t tablet_id) {
    auto it = _channels_by_tablet.find(tablet_id); // 根据该行数据所在的tablet id查找该tablet所分布的BE节点对应的NodeChannel
    DCHECK(it != std::end(_channels_by_tablet)) << "unknown tablet, tablet_id=" << tablet_id;
    for (auto channel : it->second) { // it->second对应的类型为std::vector<NodeChannel*>，表示该tablet所分布的所有BE节点对应的NodeChannel
        // if this node channel is already failed, this add_row will be skipped
        auto st = channel->add_row(tuple, tablet_id); // 将该行数据添加到当前BE节点所对应的NodeChannel，当累积到一个batch大小时，会将batch数据通过brpc发送到对应的BE
        if (!st.ok()) {
            mark_as_failed(channel); // 将当前NodeChannel标记为失败
        }
    }

    if (has_intolerable_failure()) { // 如果发生了不可容忍的失败，则返回
        return Status::InternalError("index channel has intoleralbe failure");
    }

    return Status::OK();
}

/*如果失败的NodeChannel数目超过特定阈值（对于3副本来说，超过两个副本写失败），则为不可容忍的失败*/
bool IndexChannel::has_intolerable_failure() {
    return _failed_channels.size() >= ((_parent->_num_repicas + 1) / 2);
}

/*OlapTableSink的构造函数*/
OlapTableSink::OlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc,
                             const std::vector<TExpr>& texprs, Status* status)
        : _pool(pool), _input_row_desc(row_desc), _filter_bitmap(1024) {
    if (!texprs.empty()) {
        *status = Expr::create_expr_trees(_pool, texprs, &_output_expr_ctxs);
    }
}

/*OlapTableSink的析构函数*/
OlapTableSink::~OlapTableSink() {
    // We clear NodeChannels' batches here, cuz NodeChannels' batches destruction will use
    // OlapTableSink::_mem_tracker and its parents.
    // But their destructions are after OlapTableSink's.
    // TODO: can be remove after all MemTrackers become shared.
    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel([](NodeChannel* ch) { ch->clear_all_batches(); });
    }
}

/*初始化OlapTableSink*/
Status OlapTableSink::init(const TDataSink& t_sink) {
    DCHECK(t_sink.__isset.olap_table_sink);
    auto& table_sink = t_sink.olap_table_sink;
    _load_id.set_hi(table_sink.load_id.hi);
    _load_id.set_lo(table_sink.load_id.lo);
    _txn_id = table_sink.txn_id;
    _db_id = table_sink.db_id;
    _table_id = table_sink.table_id;
    _num_repicas = table_sink.num_replicas;
    _need_gen_rollup = table_sink.need_gen_rollup;
    _db_name = table_sink.db_name;
    _table_name = table_sink.table_name;
    _tuple_desc_id = table_sink.tuple_id;
    _schema.reset(new OlapTableSchemaParam());
    RETURN_IF_ERROR(_schema->init(table_sink.schema));
    _partition = _pool->add(new OlapTablePartitionParam(_schema, table_sink.partition));
    RETURN_IF_ERROR(_partition->init());
    _location = _pool->add(new OlapTableLocationParam(table_sink.location));
    _nodes_info = _pool->add(new DorisNodesInfo(table_sink.nodes_info));

    if (table_sink.__isset.load_channel_timeout_s) {
        _load_channel_timeout_s = table_sink.load_channel_timeout_s;
    } else {
        _load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    }

    return Status::OK();
}

/*执行OlapTableSink的准备工作*/
Status OlapTableSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state)); // 执行父类DataSink的prepare()函数

    _sender_id = state->per_fragment_instance_idx();
    _num_senders = state->num_per_fragment_instances();

    // profile must add to state's object pool
    _profile = state->obj_pool()->add(new RuntimeProfile("OlapTableSink"));
    _mem_tracker = _pool->add(new MemTracker(-1, "OlapTableSink", state->instance_mem_tracker()));

    SCOPED_TIMER(_profile->total_time_counter());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(
            Expr::prepare(_output_expr_ctxs, state, _input_row_desc, _expr_mem_tracker.get()));

    // get table's tuple descriptor
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_desc_id);
    if (_output_tuple_desc == nullptr) {
        LOG(WARNING) << "unknown destination tuple descriptor, id=" << _tuple_desc_id;
        return Status::InternalError("unknown destination tuple descriptor");
    }
    if (!_output_expr_ctxs.empty()) {
        if (_output_expr_ctxs.size() != _output_tuple_desc->slots().size()) {
            LOG(WARNING) << "number of exprs is not same with slots, num_exprs="
                         << _output_expr_ctxs.size()
                         << ", num_slots=" << _output_tuple_desc->slots().size();
            return Status::InternalError("number of exprs is not same with slots");
        }
        for (int i = 0; i < _output_expr_ctxs.size(); ++i) {
            if (!is_type_compatible(_output_expr_ctxs[i]->root()->type().type,
                                    _output_tuple_desc->slots()[i]->type().type)) {
                LOG(WARNING) << "type of exprs is not match slot's, expr_type="
                             << _output_expr_ctxs[i]->root()->type().type
                             << ", slot_type=" << _output_tuple_desc->slots()[i]->type().type
                             << ", slot_name=" << _output_tuple_desc->slots()[i]->col_name();
                return Status::InternalError("expr's type is not same with slot's");
            }
        }
    }

    _output_row_desc = _pool->add(new RowDescriptor(_output_tuple_desc, false));
    _output_batch.reset(new RowBatch(*_output_row_desc, state->batch_size(), _mem_tracker));

    _max_decimal_val.resize(_output_tuple_desc->slots().size());
    _min_decimal_val.resize(_output_tuple_desc->slots().size());

    _max_decimalv2_val.resize(_output_tuple_desc->slots().size());
    _min_decimalv2_val.resize(_output_tuple_desc->slots().size());
    // check if need validate batch
    for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        auto slot = _output_tuple_desc->slots()[i];
        switch (slot->type().type) {
        case TYPE_DECIMAL:
            _max_decimal_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimal_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            _need_validate_data = true;
            break;
        case TYPE_DECIMALV2:
            _max_decimalv2_val[i].to_max_decimal(slot->type().precision, slot->type().scale);
            _min_decimalv2_val[i].to_min_decimal(slot->type().precision, slot->type().scale);
            _need_validate_data = true;
            break;
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_HLL:
        case TYPE_OBJECT:
            _need_validate_data = true;
            break;
        default:
            break;
        }
    }

    // add all counter
    _input_rows_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _output_rows_counter = ADD_COUNTER(_profile, "RowsReturned", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "RowsFiltered", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _convert_batch_timer = ADD_TIMER(_profile, "ConvertBatchTime");
    _validate_data_timer = ADD_TIMER(_profile, "ValidateDataTime");
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseWaitTime");
    _non_blocking_send_timer = ADD_TIMER(_profile, "NonBlockingSendTime");
    _serialize_batch_timer = ADD_TIMER(_profile, "SerializeBatchTime");
    _load_mem_limit = state->get_load_mem_limit();

    // open all channels
    auto& partitions = _partition->get_partitions();
    for (int i = 0; i < _schema->indexes().size(); ++i) { // 遍历table下的每一个rollup，每一个rollup对应一个index
        // collect all tablets belong to this rollup
        std::vector<TTabletWithPartition> tablets; // 保存当前rollup下的所有tablet
        auto index = _schema->indexes()[i];
        for (auto part : partitions) { // 依次遍历当前rollup的每一个partition
            for (auto tablet : part->indexes[i].tablets) { // 依次遍历当前rollup的某个partition下的所有tablet
                TTabletWithPartition tablet_with_partition;
                tablet_with_partition.partition_id = part->id;
                tablet_with_partition.tablet_id = tablet;
                tablets.emplace_back(std::move(tablet_with_partition));
            }
        }
        auto channel = _pool->add(new IndexChannel(this, index->index_id, index->schema_hash)); // 创建IndexChannel对象
        RETURN_IF_ERROR(channel->init(state, tablets)); // 初始化当前的IndexChannel
        _channels.emplace_back(channel); // 将当前的IndexChannel对象添加到成员变量
    }

    return Status::OK();
}

/*打开OlapTableSink*/
Status OlapTableSink::open(RuntimeState* state) {
    SCOPED_TIMER(_profile->total_time_counter());
    SCOPED_TIMER(_open_timer);
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));

    // 遍历每一个rollup对应的index_channel，打开每一个index_channel对应的所有NodeChannel
    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel([](NodeChannel* ch) { ch->open(); });
    }

    // 遍历每一个rollup对应的index_channel，打开每一个index_channel对应的所有NodeChannel
    for (auto index_channel : _channels) {
        index_channel->for_each_node_channel([&index_channel](NodeChannel* ch) {
            auto st = ch->open_wait();
            if (!st.ok()) {
                LOG(WARNING) << ch->name() << ": tablet open failed, " << ch->print_load_info()
                             << ", node=" << ch->node_info()->host << ":"
                             << ch->node_info()->brpc_port << ", errmsg=" << st.get_error_msg();
                index_channel->mark_as_failed(ch);
            }
        });

        if (index_channel->has_intolerable_failure()) {
            LOG(WARNING) << "open failed, load_id=" << _load_id;
            return Status::InternalError("intolerable failure in opening node channels");
        }
    }

    _sender_thread = std::thread(&OlapTableSink::_send_batch_process, this); // 创建数据发送线程

    return Status::OK();
}

/*发送一个RowBatch的数据，会在PlanFragmentExecutor中被调用*/
Status OlapTableSink::send(RuntimeState* state, RowBatch* input_batch) {
    SCOPED_TIMER(_profile->total_time_counter());
    _number_input_rows += input_batch->num_rows(); // 更新成员变量_number_input_rows，记录发送的数据行数
    // update incrementally so that FE can get the progress.
    // the real 'num_rows_load_total' will be set when sink being closed.
    state->update_num_rows_load_total(input_batch->num_rows());
    state->update_num_bytes_load_total(input_batch->total_byte_size());
    DorisMetrics::instance()->load_rows_total.increment(input_batch->num_rows());
    DorisMetrics::instance()->load_bytes_total.increment(input_batch->total_byte_size());
    RowBatch* batch = input_batch; // 获取需要发送的数据batch
    if (!_output_expr_ctxs.empty()) {
        SCOPED_RAW_TIMER(&_convert_batch_ns);
        _output_batch->reset();
        _convert_batch(state, input_batch, _output_batch.get());
        batch = _output_batch.get();
    }

    int num_invalid_rows = 0;
    if (_need_validate_data) {
        SCOPED_RAW_TIMER(&_validate_data_ns);
        _filter_bitmap.Reset(batch->num_rows());
        num_invalid_rows = _validate_data(state, batch, &_filter_bitmap); // 进行数据校验，如果某一行被过滤，则更新成员变量_filter_bitmap中的对应位
        _number_filtered_rows += num_invalid_rows; // 更新成员变量_number_filtered_rows，记录被过滤的数据行数
    }
    SCOPED_RAW_TIMER(&_send_data_ns);
    for (int i = 0; i < batch->num_rows(); ++i) { // 遍历batch中的每一行数据
        Tuple* tuple = batch->get_row(i)->get_tuple(0); // 获取一行数据，保存在Tuple中
        if (num_invalid_rows > 0 && _filter_bitmap.Get(i)) { // 判断当前行的数据是否已经被过滤。如果该行数据已经被过滤，则跳过该行数据
            continue;
        }
        const OlapTablePartition* partition = nullptr;
        uint32_t dist_hash = 0;
        if (!_partition->find_tablet(tuple, &partition, &dist_hash)) { // 计算当前行数据所在的partition和分桶hash
            std::stringstream ss;
            ss << "no partition for this tuple. tuple="
               << Tuple::to_string(tuple, *_output_tuple_desc);
#if BE_TEST
            LOG(INFO) << ss.str();
#else
            state->append_error_msg_to_file("", ss.str()); // 未找到当前行对应的partition，增加错误信息
#endif
            _number_filtered_rows++;
            continue;
        }
        _partition_ids.emplace(partition->id); // 将当前行数据所在的partition添加到成员变量_partition_ids中
        uint32_t tablet_index = dist_hash % partition->num_buckets; // 获取tablet在partition中的序号
        for (int j = 0; j < partition->indexes.size(); ++j) {       // 依次遍历partition对应的每一个rollup
            int64_t tablet_id = partition->indexes[j].tablets[tablet_index]; // 根据tablet在partition中的序号获取该行数据在当前rollup中tablet id
            RETURN_IF_ERROR(_channels[j]->add_row(tuple, tablet_id)); // 将该行数据添加到当前rollup对应的IndexChannel，进而，根据该行数据所在的tablet在BE上的分布，将该行数据分别添加到对应的NodeChannel中
            _number_output_rows++;
        }
    }
    return Status::OK();
}

/*关闭OlapTableSink*/
Status OlapTableSink::close(RuntimeState* state, Status close_status) {
    Status status = close_status;
    if (status.ok()) {
        // only if status is ok can we call this _profile->total_time_counter().
        // if status is not ok, this sink may not be prepared, so that _profile is null
        SCOPED_TIMER(_profile->total_time_counter());
        // BE id -> add_batch method counter
        std::unordered_map<int64_t, AddBatchCounter> node_add_batch_counter_map;
        int64_t serialize_batch_ns = 0, mem_exceeded_block_ns = 0, queue_push_lock_ns = 0,
                actual_consume_ns = 0;
        {
            SCOPED_TIMER(_close_timer);
            // 遍历每一个rollup对应的index_channel，将每一个index_channel下的所有NodeChannel标记为关闭
            for (auto index_channel : _channels) {
                index_channel->for_each_node_channel([](NodeChannel* ch) { ch->mark_close(); });
            }

            // 遍历每一个rollup对应的index_channel，关闭每一个index_channel下的所有NodeChannel
            for (auto index_channel : _channels) {
                index_channel->for_each_node_channel([&status, &state, &node_add_batch_counter_map,
                                                      &serialize_batch_ns, &mem_exceeded_block_ns,
                                                      &queue_push_lock_ns,
                                                      &actual_consume_ns](NodeChannel* ch) {
                    status = ch->close_wait(state);
                    if (!status.ok()) {
                        LOG(WARNING)
                                << ch->name() << ": close channel failed, " << ch->print_load_info()
                                << ". error_msg=" << status.get_error_msg();
                    }
                    ch->time_report(&node_add_batch_counter_map, &serialize_batch_ns,
                                    &mem_exceeded_block_ns, &queue_push_lock_ns,
                                    &actual_consume_ns);
                });
            }
        }
        // TODO need to be improved
        LOG(INFO) << "total mem_exceeded_block_ns=" << mem_exceeded_block_ns
                  << ", total queue_push_lock_ns=" << queue_push_lock_ns
                  << ", total actual_consume_ns=" << actual_consume_ns;

        COUNTER_SET(_input_rows_counter, _number_input_rows);
        COUNTER_SET(_output_rows_counter, _number_output_rows);
        COUNTER_SET(_filtered_rows_counter, _number_filtered_rows);
        COUNTER_SET(_send_data_timer, _send_data_ns);
        COUNTER_SET(_convert_batch_timer, _convert_batch_ns);
        COUNTER_SET(_validate_data_timer, _validate_data_ns);
        COUNTER_SET(_non_blocking_send_timer, _non_blocking_send_ns);
        COUNTER_SET(_serialize_batch_timer, serialize_batch_ns);
        // _number_input_rows don't contain num_rows_load_filtered and num_rows_load_unselected in scan node
        int64_t num_rows_load_total = _number_input_rows + state->num_rows_load_filtered() +
                                      state->num_rows_load_unselected();
        state->set_num_rows_load_total(num_rows_load_total);
        state->update_num_rows_load_filtered(_number_filtered_rows);

        // print log of add batch time of all node, for tracing load performance easily
        std::stringstream ss;
        ss << "finished to close olap table sink. load_id=" << print_id(_load_id)
           << ", txn_id=" << _txn_id << ", node add batch time(ms)/wait lock time(ms)/num: ";
        for (auto const& pair : node_add_batch_counter_map) {
            ss << "{" << pair.first << ":(" << (pair.second.add_batch_execution_time_us / 1000)
               << ")(" << (pair.second.add_batch_wait_lock_time_us / 1000) << ")("
               << pair.second.add_batch_num << ")} ";
        }
        LOG(INFO) << ss.str();
    } else {
        // 遍历每一个rollup对应的index_channel，取消每一个index_channel对应的所有NodeChannel
        for (auto channel : _channels) {
            channel->for_each_node_channel([](NodeChannel* ch) { ch->cancel(); });
        }
    }

    // Sender join() must put after node channels mark_close/cancel.
    // But there is no specific sequence required between sender join() & close_wait().
    if (_sender_thread.joinable()) {
        _sender_thread.join(); // join数据发送线程
    }

    Expr::close(_output_expr_ctxs, state);
    _output_batch.reset();
    return status;
}

void OlapTableSink::_convert_batch(RuntimeState* state, RowBatch* input_batch,
                                   RowBatch* output_batch) {
    DCHECK_GE(output_batch->capacity(), input_batch->num_rows());
    int commit_rows = 0;
    for (int i = 0; i < input_batch->num_rows(); ++i) {
        auto src_row = input_batch->get_row(i);
        Tuple* dst_tuple =
                (Tuple*)output_batch->tuple_data_pool()->allocate(_output_tuple_desc->byte_size());
        bool ignore_this_row = false;
        for (int j = 0; j < _output_expr_ctxs.size(); ++j) {
            auto src_val = _output_expr_ctxs[j]->get_value(src_row);
            auto slot_desc = _output_tuple_desc->slots()[j];
            // The following logic is similar to BaseScanner::fill_dest_tuple
            // Todo(kks): we should unify it
            if (src_val == nullptr) {
                // Only when the expr return value is null, we will check the error message.
                std::string expr_error = _output_expr_ctxs[j]->get_error_msg();
                if (!expr_error.empty()) {
                    state->append_error_msg_to_file(slot_desc->col_name(), expr_error);
                    _number_filtered_rows++;
                    ignore_this_row = true;
                    // The ctx is reused, so must clear the error state and message.
                    _output_expr_ctxs[j]->clear_error_msg();
                    break;
                }
                if (!slot_desc->is_nullable()) {
                    std::stringstream ss;
                    ss << "null value for not null column, column=" << slot_desc->col_name();
#if BE_TEST
                    LOG(INFO) << ss.str();
#else
                    state->append_error_msg_to_file("", ss.str());
#endif
                    _number_filtered_rows++;
                    ignore_this_row = true;
                    break;
                }
                dst_tuple->set_null(slot_desc->null_indicator_offset());
                continue;
            }
            if (slot_desc->is_nullable()) {
                dst_tuple->set_not_null(slot_desc->null_indicator_offset());
            }
            void* slot = dst_tuple->get_slot(slot_desc->tuple_offset());
            RawValue::write(src_val, slot, slot_desc->type(), _output_batch->tuple_data_pool());
        }

        if (!ignore_this_row) {
            output_batch->get_row(commit_rows)->set_tuple(0, dst_tuple);
            commit_rows++;
        }
    }
    output_batch->commit_rows(commit_rows);
}

int OlapTableSink::_validate_data(RuntimeState* state, RowBatch* batch, Bitmap* filter_bitmap) {
    int filtered_rows = 0;
    for (int row_no = 0; row_no < batch->num_rows(); ++row_no) {
        Tuple* tuple = batch->get_row(row_no)->get_tuple(0);
        bool row_valid = true;
        std::stringstream ss; // error message
        for (int i = 0; row_valid && i < _output_tuple_desc->slots().size(); ++i) {
            SlotDescriptor* desc = _output_tuple_desc->slots()[i];
            if (desc->is_nullable() && tuple->is_null(desc->null_indicator_offset())) {
                if (desc->type().type == TYPE_OBJECT) {
                    ss << "null is not allowed for bitmap column, column_name: "
                       << desc->col_name();
                    row_valid = false;
                }
                continue;
            }
            void* slot = tuple->get_slot(desc->tuple_offset());
            switch (desc->type().type) {
            case TYPE_CHAR:
            case TYPE_VARCHAR: {
                // Fixed length string
                StringValue* str_val = (StringValue*)slot;
                if (str_val->len > desc->type().len) {
                    ss << "the length of input is too long than schema. "
                       << "column_name: " << desc->col_name() << "; "
                       << "input_str: [" << std::string(str_val->ptr, str_val->len) << "] "
                       << "schema length: " << desc->type().len << "; "
                       << "actual length: " << str_val->len << "; ";
                    row_valid = false;
                    continue;
                }
                // padding 0 to CHAR field
                if (desc->type().type == TYPE_CHAR && str_val->len < desc->type().len) {
                    auto new_ptr = (char*)batch->tuple_data_pool()->allocate(desc->type().len);
                    memcpy(new_ptr, str_val->ptr, str_val->len);
                    memset(new_ptr + str_val->len, 0, desc->type().len - str_val->len);

                    str_val->ptr = new_ptr;
                    str_val->len = desc->type().len;
                }
                break;
            }
            case TYPE_DECIMAL: {
                DecimalValue* dec_val = (DecimalValue*)slot;
                if (dec_val->scale() > desc->type().scale) {
                    int code = dec_val->round(dec_val, desc->type().scale, HALF_UP);
                    if (code != E_DEC_OK) {
                        ss << "round one decimal failed.value=" << dec_val->to_string();
                        row_valid = false;
                        continue;
                    }
                }
                if (*dec_val > _max_decimal_val[i] || *dec_val < _min_decimal_val[i]) {
                    ss << "decimal value is not valid for defination, column=" << desc->col_name()
                       << ", value=" << dec_val->to_string()
                       << ", precision=" << desc->type().precision
                       << ", scale=" << desc->type().scale;
                    row_valid = false;
                    continue;
                }
                break;
            }
            case TYPE_DECIMALV2: {
                DecimalV2Value dec_val(reinterpret_cast<const PackedInt128*>(slot)->value);
                if (dec_val.greater_than_scale(desc->type().scale)) {
                    int code = dec_val.round(&dec_val, desc->type().scale, HALF_UP);
                    reinterpret_cast<PackedInt128*>(slot)->value = dec_val.value();
                    if (code != E_DEC_OK) {
                        ss << "round one decimal failed.value=" << dec_val.to_string();
                        row_valid = false;
                        continue;
                    }
                }
                if (dec_val > _max_decimalv2_val[i] || dec_val < _min_decimalv2_val[i]) {
                    ss << "decimal value is not valid for defination, column=" << desc->col_name()
                       << ", value=" << dec_val.to_string()
                       << ", precision=" << desc->type().precision
                       << ", scale=" << desc->type().scale;
                    row_valid = false;
                    continue;
                }
                break;
            }
            case TYPE_HLL: {
                Slice* hll_val = (Slice*)slot;
                if (!HyperLogLog::is_valid(*hll_val)) {
                    ss << "Content of HLL type column is invalid"
                       << "column_name: " << desc->col_name() << "; ";
                    row_valid = false;
                    continue;
                }
                break;
            }
            default:
                break;
            }
        }

        if (!row_valid) {
            filtered_rows++;
            filter_bitmap->Set(row_no, true);
#if BE_TEST
            LOG(INFO) << ss.str();
#else
            state->append_error_msg_to_file("", ss.str());
#endif
        }
    }
    return filtered_rows;
}

/*数据发送线程的执行函数*/
void OlapTableSink::_send_batch_process() {
    SCOPED_RAW_TIMER(&_non_blocking_send_ns);
    while (true) {
        int running_channels_num = 0;
        for (auto index_channel : _channels) { // 遍历每一个index_channel
            index_channel->for_each_node_channel([&running_channels_num](NodeChannel* ch) {
                running_channels_num += ch->try_send_and_fetch_status(); // 发送一个batch的数据
            });
        }

        if (running_channels_num == 0) {
            LOG(INFO) << "all node channels are stopped(maybe finished/offending/cancelled), "
                         "consumer thread exit.";
            return;
        }
        SleepFor(MonoDelta::FromMilliseconds(config::olap_table_sink_send_interval_ms));
    }
}

} // namespace stream_load
} // namespace doris
