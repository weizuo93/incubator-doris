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

#include "exec/exec_node.h"

#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>
#include <unistd.h>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/aggregation_node.h"
#include "exec/analytic_eval_node.h"
#include "exec/assert_num_rows_node.h"
#include "exec/broker_scan_node.h"
#include "exec/cross_join_node.h"
#include "exec/csv_scan_node.h"
#include "exec/empty_set_node.h"
#include "exec/es_http_scan_node.h"
#include "exec/es_scan_node.h"
#include "exec/except_node.h"
#include "exec/exchange_node.h"
#include "exec/hash_join_node.h"
#include "exec/intersect_node.h"
#include "exec/merge_join_node.h"
#include "exec/merge_node.h"
#include "exec/mysql_scan_node.h"
#include "exec/olap_rewrite_node.h"
#include "exec/olap_scan_node.h"
#include "exec/partitioned_aggregation_node.h"
#include "exec/repeat_node.h"
#include "exec/schema_scan_node.h"
#include "exec/select_node.h"
#include "exec/spill_sort_node.h"
#include "exec/topn_node.h"
#include "exec/union_node.h"
#include "exprs/expr_context.h"
#include "runtime/exec_env.h"
#include "runtime/descriptors.h"
#include "runtime/initial_reservations.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/runtime_profile.h"

namespace doris {

const std::string ExecNode::ROW_THROUGHPUT_COUNTER = "RowsReturnedRate";

int ExecNode::get_node_id_from_profile(RuntimeProfile* p) {
    return p->metadata();
}

ExecNode::RowBatchQueue::RowBatchQueue(int max_batches) :
    BlockingQueue<RowBatch*>(max_batches) {
}

ExecNode::RowBatchQueue::~RowBatchQueue() {
    DCHECK(cleanup_queue_.empty());
}

void ExecNode::RowBatchQueue::AddBatch(RowBatch* batch) {
  if (!blocking_put(batch)) {
    std::lock_guard<std::mutex> lock(lock_);
    cleanup_queue_.push_back(batch);
  }
}

bool ExecNode::RowBatchQueue::AddBatchWithTimeout(RowBatch* batch,
    int64_t timeout_micros) {
    // return blocking_put_with_timeout(batch, timeout_micros);
    return blocking_put(batch);
}

RowBatch* ExecNode::RowBatchQueue::GetBatch() {
  RowBatch* result = NULL;
  if (blocking_get(&result)) return result;
  return NULL;
}

int ExecNode::RowBatchQueue::Cleanup() {
  int num_io_buffers = 0;

  // RowBatch* batch = NULL;
  // while ((batch = GetBatch()) != NULL) {
  //   num_io_buffers += batch->num_io_buffers();
  //   delete batch;
  // }

  lock_guard<std::mutex> l(lock_);
  for (std::list<RowBatch*>::iterator it = cleanup_queue_.begin();
      it != cleanup_queue_.end(); ++it) {
    // num_io_buffers += (*it)->num_io_buffers();
    delete *it;
  }
  cleanup_queue_.clear();
  return num_io_buffers;
}

ExecNode::ExecNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) :
        _id(tnode.node_id),
        _type(tnode.node_type),
        _pool(pool),
        _tuple_ids(tnode.row_tuples),
        _row_descriptor(descs, tnode.row_tuples, tnode.nullable_tuples),
        _resource_profile(tnode.resource_profile),
        _debug_phase(TExecNodePhase::INVALID),
        _debug_action(TDebugAction::WAIT),
        _limit(tnode.limit),
        _num_rows_returned(0),
        _rows_returned_counter(NULL),
        _rows_returned_rate(NULL),
        _memory_used_counter(NULL),
        _is_closed(false){
    init_runtime_profile(print_plan_node_type(tnode.node_type));
}

ExecNode::~ExecNode() {
}

void ExecNode::push_down_predicate(
        RuntimeState* state, std::list<ExprContext*>* expr_ctxs) {
    if (_type != TPlanNodeType::AGGREGATION_NODE) {
        for (int i = 0; i < _children.size(); ++i) {
            _children[i]->push_down_predicate(state, expr_ctxs);
            if (expr_ctxs->size() == 0) {
                return;
            }
        }
    }

    std::list<ExprContext*>::iterator iter = expr_ctxs->begin();
    while (iter != expr_ctxs->end()) {
        if ((*iter)->root()->is_bound(&_tuple_ids)) {
            // LOG(INFO) << "push down success expr is " << (*iter)->debug_string()
            //          << " and node is " << debug_string();
            (*iter)->prepare(state, row_desc(), _expr_mem_tracker.get());
            (*iter)->open(state);
            _conjunct_ctxs.push_back(*iter);
            iter = expr_ctxs->erase(iter);
        } else {
            ++iter;
        }
    }
}

/*node初始化*/
Status ExecNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(
        Expr::create_expr_trees(_pool, tnode.conjuncts, &_conjunct_ctxs));
    return Status::OK();
}

/*执行prepare*/
Status ExecNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::PREPARE));
    DCHECK(_runtime_profile.get() != NULL);
    _rows_returned_counter =
        ADD_COUNTER(_runtime_profile, "RowsReturned", TUnit::UNIT);
    _rows_returned_rate = runtime_profile()->add_derived_counter(
                              ROW_THROUGHPUT_COUNTER, TUnit::UNIT_PER_SECOND,
                              boost::bind<int64_t>(&RuntimeProfile::units_per_second,
                                                   _rows_returned_counter,
                                                   runtime_profile()->total_time_counter()),
                              "");
    _mem_tracker.reset(new MemTracker(_runtime_profile.get(), -1, _runtime_profile->name(), state->instance_mem_tracker()));
    _expr_mem_tracker.reset(new MemTracker(-1, "Exprs", _mem_tracker.get()));
    _expr_mem_pool.reset(new MemPool(_expr_mem_tracker.get()));
    // TODO chenhao
    RETURN_IF_ERROR(Expr::prepare(_conjunct_ctxs, state, row_desc(), expr_mem_tracker())); // 执行prepare
    // TODO(zc):
    // AddExprCtxsToFree(_conjunct_ctxs);

    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->prepare(state)); // 依次对每一个子节点递归执行prepare
    }

    return Status::OK();
}

Status ExecNode::open(RuntimeState* state) {
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    return Expr::open(_conjunct_ctxs, state);
}


Status ExecNode::reset(RuntimeState* state) {
    _num_rows_returned = 0;
    for (int i = 0; i < _children.size(); ++i) {
        RETURN_IF_ERROR(_children[i]->reset(state));
    }   
    return Status::OK();
}

Status ExecNode::collect_query_statistics(QueryStatistics* statistics) {
    DCHECK(statistics != nullptr);
    for (auto child_node : _children) {
        child_node->collect_query_statistics(statistics);
    } 
    return Status::OK();
}

Status ExecNode::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    _is_closed = true;
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));

    if (_rows_returned_counter != NULL) {
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    Status result;
    for (int i = 0; i < _children.size(); ++i) {
        auto st = _children[i]->close(state);
        if (result.ok() && !st.ok()) {
            result = st;
        }
    }

    Expr::close(_conjunct_ctxs, state);

    if (expr_mem_pool() != nullptr) {
        _expr_mem_pool->free_all();
    }

    if (_buffer_pool_client.is_registered()) {
        VLOG_FILE << _id << " returning reservation " << _resource_profile.min_reservation;
        state->initial_reservations()->Return(
            &_buffer_pool_client, _resource_profile.min_reservation);
        state->exec_env()->buffer_pool()->DeregisterClient(&_buffer_pool_client);
    }

    if (_expr_mem_tracker != nullptr) { 
        _expr_mem_tracker->close();
    }
  
    if (_mem_tracker != nullptr) {
        _mem_tracker->close();
    }

    return result;
}

void ExecNode::add_runtime_exec_option(const std::string& str) {
    lock_guard<mutex> l(_exec_options_lock);

    if (_runtime_exec_options.empty()) {
        _runtime_exec_options = str;
    } else {
        _runtime_exec_options.append(", ");
        _runtime_exec_options.append(str);
    }

    runtime_profile()->add_info_string("ExecOption", _runtime_exec_options);
}

/*根据fragment的plan创建fragment的tree结构，其中会创建不同的节点（比如：OLAP_SCAN_NODE）*/
Status ExecNode::create_tree(RuntimeState* state, ObjectPool* pool, const TPlan& plan,
                            const DescriptorTbl& descs, ExecNode** root) {
    if (plan.nodes.size() == 0) { // 类TPlan会根据Thrift在gensrc/build/gen_cpp/目录下生成的PlanNodes_types.h文件中定义，其中成员变量nodes的类型为std::vector<TPlanNode>
        *root = NULL;
        return Status::OK();
    }

    int node_idx = 0;
    RETURN_IF_ERROR(create_tree_helper(state, pool, plan.nodes, descs, NULL, &node_idx, root)); // 根据plan.nodes创建fragment的tree结构（parent为NULL，因此root返回根节点）

    if (node_idx + 1 != plan.nodes.size()) {
        // TODO: print thrift msg for diagnostic purposes.
        return Status::InternalError(
                   "Plan tree only partially reconstructed. Not all thrift nodes were used.");
    }

    return Status::OK();
}

/*根据fragment的plan中的nodes创建fragment的tree结构，tree是由不同的node构成*/
Status ExecNode::create_tree_helper(
    RuntimeState* state,
    ObjectPool* pool,
    const vector<TPlanNode>& tnodes,
    const DescriptorTbl& descs,
    ExecNode* parent,
    int* node_idx,
    ExecNode** root) {
    // propagate error case
    if (*node_idx >= tnodes.size()) {
        // TODO: print thrift msg
        return Status::InternalError("Failed to reconstruct plan tree from thrift.");
    }
    const TPlanNode& tnode = tnodes[*node_idx]; // 获取一个TPlanNode对象，获取第一个TPlanNode对象时，*node_idx值为0，获取到的是root node，即PlanFragmentExecutor中的_plan

    int num_children = tnodes[*node_idx].num_children; // 获取tnodes[*node_idx]的子节点个数
    ExecNode* node = NULL;
    RETURN_IF_ERROR(create_node(state, pool, tnodes[*node_idx], descs, &node)); // 根据Node类型创建一个Node（此处创建当前层次的第一个Node，即root Node），通过参数node传回

    // assert(parent != NULL || (node_idx == 0 && root_expr != NULL));
    if (parent != NULL) { // 一个Node下的所有子Node都保存在Node对象的vector<ExecNode*>类型的成员变量_children中
        parent->_children.push_back(node);
    } else {
        *root = node; // 获取当前fragment中所有的执行节点（这些执行节点都是ExecNode类的子类对象，包括OLAP_SCAN_NODE、HASH_JOIN_NODE、AGGREGATION_NODE、EXCHANGE_NODE等）的根Node
    }

    for (int i = 0; i < num_children; i++) {
        ++*node_idx;
        RETURN_IF_ERROR(create_tree_helper(state, pool, tnodes, descs, node, node_idx, NULL)); // 依次递归地创建其他的Node，前面创建的node为新节点的parent

        // we are expecting a child, but have used all nodes
        // this means we have been given a bad tree and must fail
        if (*node_idx >= tnodes.size()) {
            // TODO: print thrift msg
            return Status::InternalError("Failed to reconstruct plan tree from thrift.");
        }
    }

    RETURN_IF_ERROR(node->init(tnode, state)); // 初始化Node

    // build up tree of profiles; add children >0 first, so that when we print
    // the profile, child 0 is printed last (makes the output more readable)
    for (int i = 1; i < node->_children.size(); ++i) {
        node->runtime_profile()->add_child(node->_children[i]->runtime_profile(), true, NULL); // 依次将当前节点的每一个子节点（第0个除外）的profile添加到当前节点的profile
    }

    if (!node->_children.empty()) {
        node->runtime_profile()->add_child(node->_children[0]->runtime_profile(), true, NULL); // 将当前节点的每0个子节点的profile添加到当前节点的profile
    }

    return Status::OK();
}

/*根据Node的类型创建一个特定Node对象，比如：创建一个OlapScanNode对象、HASH_JOIN_NODE对象或AGGREGATION_NODE对象*/
Status ExecNode::create_node(RuntimeState* state, ObjectPool* pool, const TPlanNode& tnode,
                            const DescriptorTbl& descs, ExecNode** node) {
    std::stringstream error_msg;

    VLOG(2) << "tnode:\n" << apache::thrift::ThriftDebugString(tnode);
    switch (tnode.node_type) {
    case TPlanNodeType::CSV_SCAN_NODE:
        *node = pool->add(new CsvScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::MYSQL_SCAN_NODE:
#ifdef DORIS_WITH_MYSQL
        *node = pool->add(new MysqlScanNode(pool, tnode, descs));
        return Status::OK();
#else
        return Status::InternalError("Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif

    case TPlanNodeType::ES_SCAN_NODE:
        *node = pool->add(new EsScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ES_HTTP_SCAN_NODE:
        *node = pool->add(new EsHttpScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SCHEMA_SCAN_NODE:
        *node = pool->add(new SchemaScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::OLAP_SCAN_NODE:
        *node = pool->add(new OlapScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::AGGREGATION_NODE:
        if (config::enable_partitioned_aggregation) {
            *node = pool->add(new PartitionedAggregationNode(pool, tnode, descs));
        } else {
            *node = pool->add(new AggregationNode(pool, tnode, descs));
        }
        return Status::OK();
    case TPlanNodeType::HASH_JOIN_NODE:
        *node = pool->add(new HashJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::CROSS_JOIN_NODE:
        *node = pool->add(new CrossJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::MERGE_JOIN_NODE:
        *node = pool->add(new MergeJoinNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EMPTY_SET_NODE:
        *node = pool->add(new EmptySetNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EXCHANGE_NODE:
        *node = pool->add(new ExchangeNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SELECT_NODE:
        *node = pool->add(new SelectNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::OLAP_REWRITE_NODE:
        *node = pool->add(new OlapRewriteNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::SORT_NODE:
        if (tnode.sort_node.use_top_n) {
            *node = pool->add(new TopNNode(pool, tnode, descs));
        } else {
            *node = pool->add(new SpillSortNode(pool, tnode, descs));
        }

        return Status::OK();
    case TPlanNodeType::ANALYTIC_EVAL_NODE:
        *node = pool->add(new AnalyticEvalNode(pool, tnode, descs));
        break;

    case TPlanNodeType::MERGE_NODE:
        *node = pool->add(new MergeNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::UNION_NODE:
        *node = pool->add(new UnionNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::INTERSECT_NODE:
        *node = pool->add(new IntersectNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::EXCEPT_NODE:
        *node = pool->add(new ExceptNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::BROKER_SCAN_NODE:
        *node = pool->add(new BrokerScanNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::REPEAT_NODE:
        *node = pool->add(new RepeatNode(pool, tnode, descs));
        return Status::OK();

    case TPlanNodeType::ASSERT_NUM_ROWS_NODE:
        *node = pool->add(new AssertNumRowsNode(pool, tnode, descs));
        return Status::OK();

    default:
        map<int, const char*>::const_iterator i =
            _TPlanNodeType_VALUES_TO_NAMES.find(tnode.node_type);
        const char* str = "unknown node type";

        if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
            str = i->second;
        }

        error_msg << str << " not implemented";
        return Status::InternalError(error_msg.str());
    }

    return Status::OK();
}

void ExecNode::set_debug_options(
    int node_id, TExecNodePhase::type phase, TDebugAction::type action,
    ExecNode* root) {
    if (root->_id == node_id) {
        root->_debug_phase = phase;
        root->_debug_action = action;
        return;
    }

    for (int i = 0; i < root->_children.size(); ++i) {
        set_debug_options(node_id, phase, action, root->_children[i]);
    }
}

std::string ExecNode::debug_string() const {
    std::stringstream out;
    this->debug_string(0, &out);
    return out.str();
}

void ExecNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << " conjuncts=" << Expr::debug_string(_conjuncts);
    *out << " id=" << _id;
    *out << " type=" << print_plan_node_type(_type);
    *out << " tuple_ids=[";
    for (auto id : _tuple_ids) {
        *out << id << ", ";
    }
    *out << "]";

    for (int i = 0; i < _children.size(); ++i) {
        *out << "\n";
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

bool ExecNode::eval_conjuncts(ExprContext* const* ctxs, int num_ctxs, TupleRow* row) {
    for (int i = 0; i < num_ctxs; ++i) {
        BooleanVal v = ctxs[i]->get_boolean_val(row);
        if (v.is_null || !v.val) {
            return false;
        }
    }
    return true;
}

/*获取以当前Node对象为root的Node tree中类型为node_type的所有Node(包含当前节点)，通过vector类型的参数nodes传回*/
void ExecNode::collect_nodes(TPlanNodeType::type node_type, vector<ExecNode*>* nodes) {
    if (_type == node_type) { // 如果当前Node对象的类型与需要收集的Node类型一致，则把当前Node对象添加到nodes中
        nodes->push_back(this);
    }

    for (int i = 0; i < _children.size(); ++i) {      // 遍历当前Node对象的所有child Node对象
        _children[i]->collect_nodes(node_type, nodes);
    }
}

/*获取以当前Node对象为root的Node tree中所有的scan node(包含当前节点)，通过vector类型的参数nodes传回*/
void ExecNode::collect_scan_nodes(vector<ExecNode*>* nodes) {
    collect_nodes(TPlanNodeType::OLAP_SCAN_NODE, nodes);   // OlapScanNode是ScanNode的子类
    collect_nodes(TPlanNodeType::BROKER_SCAN_NODE, nodes); // BrokerScanNode是ScanNode的子类
    collect_nodes(TPlanNodeType::ES_SCAN_NODE, nodes);     // EsScanNode是ScanNode的子类
    collect_nodes(TPlanNodeType::ES_HTTP_SCAN_NODE, nodes);// EsHttpScanNode是ScanNode的子类
}

/*尝试进行聚合操作的优化*/
void ExecNode::try_do_aggregate_serde_improve() {
    std::vector<ExecNode*> agg_node;
    collect_nodes(TPlanNodeType::AGGREGATION_NODE, &agg_node); // 获取以当前Node对象为root的Node tree中所有的aggregation node(包含当前节点)
    if (agg_node.size() != 1) { // 如果aggregation node的个数不为1，则返回
        return;
    }

    if (agg_node[0]->_children.size() != 1) { // 如果aggregation node的子节点个数不为1，则返回
        return;
    }

    if (agg_node[0]->_children[0]->type() != TPlanNodeType::OLAP_SCAN_NODE) { // 如果aggregation node的子节点类型不是OlapScanNode，则返回
        return;
    }

    OlapScanNode* scan_node = static_cast<OlapScanNode*>(agg_node[0]->_children[0]); // 获取类型为OlapScanNode的aggregation node的子节点
    scan_node->set_no_agg_finalize(); // 设置aggregation node的OlapScanNode类型的子节点不做聚合操作，因为aggregation node中会做聚合操作
}

void ExecNode::init_runtime_profile(const std::string& name) {
    std::stringstream ss;
    ss << name << " (id=" << _id << ")";
    _runtime_profile.reset(new RuntimeProfile(ss.str()));
    _runtime_profile->set_metadata(_id);
}

Status ExecNode::exec_debug_action(TExecNodePhase::type phase) {
    DCHECK(phase != TExecNodePhase::INVALID);

    if (_debug_phase != phase) {
        return Status::OK();
    }

    if (_debug_action == TDebugAction::FAIL) {
        return Status::InternalError("Debug Action: FAIL");
    }

    if (_debug_action == TDebugAction::WAIT) {
        while (true) {
            sleep(1);
        }
    }

    return Status::OK();
}

Status ExecNode::claim_buffer_reservation(RuntimeState* state) {
    DCHECK(!_buffer_pool_client.is_registered());
    BufferPool* buffer_pool = ExecEnv::GetInstance()->buffer_pool();
    // Check the minimum buffer size in case the minimum buffer size used by the planner
    // doesn't match this backend's.
        std::stringstream ss; 
    if (_resource_profile.__isset.spillable_buffer_size &&
        _resource_profile.spillable_buffer_size < buffer_pool->min_buffer_len()) {
        ss << "Spillable buffer size for node " << _id << " of " << _resource_profile.spillable_buffer_size
           << "bytes is less than the minimum buffer pool buffer size of "
           <<  buffer_pool->min_buffer_len() << "bytes";
        return Status::InternalError(ss.str());
    }   
 
    ss << print_plan_node_type(_type) << " id=" << _id << " ptr=" << this;
    RETURN_IF_ERROR(buffer_pool->RegisterClient(ss.str(),
                                                state->instance_buffer_reservation(),
                                                mem_tracker(), buffer_pool->GetSystemBytesLimit(), 
                                                runtime_profile(),
                                                &_buffer_pool_client));
    
    state->initial_reservations()->Claim(&_buffer_pool_client, _resource_profile.min_reservation);
/*
    if (debug_action_ == TDebugAction::SET_DENY_RESERVATION_PROBABILITY &&
        (debug_phase_ == TExecNodePhase::PREPARE || debug_phase_ == TExecNodePhase::OPEN)) {
       // We may not have been able to enable the debug action at the start of Prepare() or
       // Open() because the client is not registered then. Do it now to be sure that it is
       // effective.
               RETURN_IF_ERROR(EnableDenyReservationDebugAction());
    } 
*/  
    return Status::OK();
}

Status ExecNode::release_unused_reservation() {
  return _buffer_pool_client.DecreaseReservationTo(_resource_profile.min_reservation);
}
/*
Status ExecNode::enable_deny_reservation_debug_action() {
  DCHECK_EQ(debug_action_, TDebugAction::SET_DENY_RESERVATION_PROBABILITY);
  DCHECK(_buffer_pool_client.is_registered());
  // Parse [0.0, 1.0] probability.
  StringParser::ParseResult parse_result;
  double probability = StringParser::StringToFloat<double>(
      debug_action_param_.c_str(), debug_action_param_.size(), &parse_result);
  if (parse_result != StringParser::PARSE_SUCCESS || probability < 0.0
      || probability > 1.0) {
    return Status::InternalError(Substitute(
        "Invalid SET_DENY_RESERVATION_PROBABILITY param: '$0'", debug_action_param_));
  }
  _buffer_pool_client.SetDebugDenyIncreaseReservation(probability);
  return Status::OK()();
}
*/

Status ExecNode::QueryMaintenance(RuntimeState* state, const std::string& msg) {
  // TODO chenhao , when introduce latest AnalyticEvalNode open it
  // ScalarExprEvaluator::FreeLocalAllocations(evals_to_free_);
  return state->check_query_state(msg);
}

}
