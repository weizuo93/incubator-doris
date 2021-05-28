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

#include "txn_manager.h"

#include <signal.h>

#include <algorithm>
#include <cstdio>
#include <new>
#include <queue>
#include <set>
#include <random>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/storage_engine.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "util/time.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"

using apache::thrift::ThriftDebugString;
using boost::filesystem::canonical;
using boost::filesystem::directory_iterator;
using boost::filesystem::path;
using boost::filesystem::recursive_directory_iterator;
using std::back_inserter;
using std::copy;
using std::inserter;
using std::list;
using std::map;
using std::nothrow;
using std::pair;
using std::priority_queue;
using std::set;
using std::set_difference;
using std::string;
using std::stringstream;
using std::vector;

namespace doris {

TxnManager::TxnManager(int32_t txn_map_shard_size, int32_t txn_shard_size)
        : _txn_map_shard_size(txn_map_shard_size),
          _txn_shard_size(txn_shard_size) {
    DCHECK_GT(_txn_map_shard_size, 0);
    DCHECK_GT(_txn_shard_size, 0);
    DCHECK_EQ(_txn_map_shard_size & (_txn_map_shard_size - 1), 0);
    DCHECK_EQ(_txn_shard_size & (_txn_shard_size - 1), 0);
    _txn_map_locks = new RWMutex[_txn_map_shard_size];
    _txn_tablet_maps = new txn_tablet_map_t[_txn_map_shard_size];
    _txn_partition_maps = new txn_partition_map_t[_txn_map_shard_size];
    _txn_mutex = new Mutex[_txn_shard_size];
}

OLAPStatus TxnManager::prepare_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id, 
                                   const PUniqueId& load_id) {
    return prepare_txn(partition_id, transaction_id, tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid(), load_id);
}

OLAPStatus TxnManager::commit_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                                  const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr, bool is_recovery) {
    return commit_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id, tablet->tablet_id(), 
        tablet->schema_hash(), tablet->tablet_uid(), load_id, rowset_ptr, is_recovery);
}

OLAPStatus TxnManager::publish_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id,
                                   const Version& version, VersionHash version_hash) {
    return publish_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id, tablet->tablet_id(), 
        tablet->schema_hash(), tablet->tablet_uid(), version, version_hash);
}

// delete the txn from manager if it is not committed(not have a valid rowset)
OLAPStatus TxnManager::rollback_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id) {
    return rollback_txn(partition_id, transaction_id, tablet->tablet_id(), tablet->schema_hash(), tablet->tablet_uid());
}

OLAPStatus TxnManager::delete_txn(TPartitionId partition_id, const TabletSharedPtr& tablet, TTransactionId transaction_id) {
    return delete_txn(tablet->data_dir()->get_meta(), partition_id, transaction_id, tablet->tablet_id(), 
        tablet->schema_hash(), tablet->tablet_uid());
}

// prepare txn should always be allowed because ingest task will be retried 
// could not distinguish rollup, schema change or base table, prepare txn successfully will allow
// ingest retried
/* 向一个tablet中开始导入一个新的数据版本，将transaction添加到manager中进行管理*/
OLAPStatus TxnManager::prepare_txn(
    TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, 
    const PUniqueId& load_id) {

    TxnKey key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    WriteLock txn_wrlock(&_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id); // 获取当前transaction_id所在的txn_tablet_map_t
    auto it = txn_tablet_map.find(key); // 在txn_tablet_map_t中查找参数传入的partition_id和transaction_id
    if (it != txn_tablet_map.end()) { // 参数传入的partition_id和transaction_id已经存在在txn manager中
        auto load_itr = it->second.find(tablet_info); // 判断参数传入的tablet是否已经在txn manager中被管理
        if (load_itr != it->second.end()) { // 参数传入的tablet已经在txn manager中被管理
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            TabletTxnInfo& load_info = load_itr->second; // 获取当前tablet已经在txn manager中管理的TabletTxnInfo对象
            // check if load id is equal
            if (load_info.load_id.hi() == load_id.hi()
                && load_info.load_id.lo() == load_id.lo()
                && load_info.rowset != nullptr) {        // 判断当前tablet已经在txn manager中管理的TabletTxnInfo对象的load id与参数传入的load id是否相同，如果相同，则表示txn被重复prepare了
                LOG(WARNING) << "find transaction exists when add to engine."
                    << "partition_id: " << key.first
                    << ", transaction_id: " << key.second
                    << ", tablet: " << tablet_info.to_string();
                return OLAP_SUCCESS;
            }
        }
    }
    // 参数传入的partition_id和transaction_id在txn manager中不存在

    // check if there are too many transactions on running.
    // if yes, reject the request.
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id); // 获取当前transaction_id所在的txn_partition_map_t
    if (txn_partition_map.size() > config::max_runnings_transactions_per_txn_map) { // 判断当前txn_partition_map_t中正在运行的txn是否已经超过阈值
        LOG(WARNING) << "too many transactions: " << txn_tablet_map.size() << ", limit: " << config::max_runnings_transactions_per_txn_map;
        return OLAP_ERR_TOO_MANY_TRANSACTIONS;
    }

    // not found load id
    // case 1: user start a new txn, rowset_ptr = null
    // case 2: loading txn from meta env
    TabletTxnInfo load_info(load_id, nullptr); // 使用参数传入的load_id创建TabletTxnInfo对象，rowset赋值为null
    txn_tablet_map[key][tablet_info] = load_info; // 将新创建的TabletTxnInfo对象添加到txn_tablet_map中
    _insert_txn_partition_map_unlocked(transaction_id, partition_id); // 将当前partition_id添加到transaction_id对应的txn_partition_map_t中

    VLOG(3) << "add transaction to engine successfully."
            << "partition_id: " << key.first
            << ", transaction_id: " << key.second
            << ", tablet: " << tablet_info.to_string();
    return OLAP_SUCCESS;
}

/* 向一个tablet中导入数据并生成新的rowset完成，将rowset的meta进行持久化，并且将数据导入生成的新的rowset添加到txn manager中管理*/
OLAPStatus TxnManager::commit_txn(
    OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
    TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
    const PUniqueId& load_id, const RowsetSharedPtr& rowset_ptr, bool is_recovery) {
    if (partition_id < 1 || transaction_id < 1 || tablet_id < 1) { // 对参数传入的partition_id、transaction_id和tablet_id的合法性进行判断
        LOG(FATAL) << "invalid commit req "
                   << " partition_id=" << partition_id
                   << " transaction_id=" << transaction_id
                   << " tablet_id=" << tablet_id;
    }
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    if (rowset_ptr == nullptr) { // 判断参数传入rowset_ptr是否为null
        LOG(WARNING) << "could not commit txn because rowset ptr is null. "
                     << "partition_id: " << key.first
                     << ", transaction_id: " << key.second
                     << ", tablet: " << tablet_info.to_string();
        return OLAP_ERR_ROWSET_INVALID;
    }

    MutexLock txn_lock(&_get_txn_lock(transaction_id));
    {
        // get tx
        ReadLock rdlock(&_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id); // 获取当前transaction_id所在的txn_tablet_map_t
        auto it = txn_tablet_map.find(key); // 在txn_tablet_map_t中查找参数传入的partition_id和transaction_id
        if (it != txn_tablet_map.end()) {   // 参数传入的partition_id和transaction_id已经存在在txn manager中
            auto load_itr = it->second.find(tablet_info); // 判断参数传入的tablet是否已经在txn manager中被管理
            if (load_itr != it->second.end()) { // 参数传入的tablet已经在txn manager中被管理
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                TabletTxnInfo& load_info = load_itr->second;
                // check if load id is equal
                if (load_info.load_id.hi() == load_id.hi()
                    && load_info.load_id.lo() == load_id.lo()
                    && load_info.rowset != nullptr
                    && load_info.rowset->rowset_id() == rowset_ptr->rowset_id()) { // 本次导入的rowset已经在txn manager中存在，并且rowset id相同，这种情况应该是重复调用
                    // find a rowset with same rowset id, then it means a duplicate call
                    LOG(INFO) << "find transaction exists when add to engine."
                              << "partition_id: " << key.first
                              << ", transaction_id: " << key.second
                              << ", tablet: " << tablet_info.to_string()
                              << ", rowset_id: " << load_info.rowset->rowset_id();
                    return OLAP_SUCCESS;
                } else if (load_info.load_id.hi() == load_id.hi()
                    && load_info.load_id.lo() == load_id.lo()
                    && load_info.rowset != nullptr
                    && load_info.rowset->rowset_id() != rowset_ptr->rowset_id()) { // 本次导入的rowset已经在txn manager中存在，但是rowset id不同，这种情况不应该发生
                    // find a rowset with different rowset id, then it should not happen, just return errors
                    LOG(WARNING) << "find transaction exists when add to engine. but rowset ids are not same."
                                 << "partition_id: " << key.first
                                 << ", transaction_id: " << key.second
                                 << ", tablet: " << tablet_info.to_string()
                                 << ", exist rowset_id: " << load_info.rowset->rowset_id()
                                 << ", new rowset_id: " << rowset_ptr->rowset_id();
                    return OLAP_ERR_PUSH_TRANSACTION_ALREADY_EXIST;
                }
            }
        }
    }

    // if not in recovery mode, then should persist the meta to meta env
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (!is_recovery) {
        RowsetMetaPB rowset_meta_pb;
        rowset_ptr->rowset_meta()->to_rowset_pb(&rowset_meta_pb); // 将参数传入的rowset的meta信息序列化为PB
        OLAPStatus save_status = RowsetMetaManager::save(meta, tablet_uid, rowset_ptr->rowset_id(), rowset_meta_pb); // 持久化导入数据生成的新的rowset
        if (save_status != OLAP_SUCCESS) {
            LOG(WARNING) << "save committed rowset failed. when commit txn rowset_id:"
                        << rowset_ptr->rowset_id()
                        << "tablet id: " << tablet_id
                        << "txn id:" << transaction_id;
            return OLAP_ERR_ROWSET_SAVE_FAILED;
        }
    }

    {
        WriteLock wrlock(&_get_txn_map_lock(transaction_id));
        TabletTxnInfo load_info(load_id, rowset_ptr); // 根据参数传入的load_id和rowset_ptr创建TabletTxnInfo对象
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id); // 获取当前transaction_id所在的txn_tablet_map_t
        txn_tablet_map[key][tablet_info] = load_info; // 更新txn manager中管理的当前tablet的TabletTxnInfo（主要是添加了导入数据生成的新的rowset）
        _insert_txn_partition_map_unlocked(transaction_id, partition_id); // 将当前partition_id添加到transaction_id对应的txn_partition_map_t中 （是否重复调用了，因为在prepare_txn()时已经调用了）
        LOG(INFO) << "commit transaction to engine successfully."
                << " partition_id: " << key.first
                << ", transaction_id: " << key.second
                << ", tablet: " << tablet_info.to_string()
                << ", rowsetid: " << rowset_ptr->rowset_id()
                << ", version: " << rowset_ptr->version().first;
    }
    return OLAP_SUCCESS;
}

// remove a txn from txn manager
/*将transaction_id下参数传入的tablet中数据导入生成的rowset变成可见版本、持久化rowset meta信息，并从txn manager中删除该tabelt的本次数据导入transaction*/
OLAPStatus TxnManager::publish_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                   TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid,
                                   const Version& version, VersionHash version_hash) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    RowsetSharedPtr rowset_ptr = nullptr;
    MutexLock txn_lock(&_get_txn_lock(transaction_id));
    {
        ReadLock rlock(&_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id); // 获取当前transaction_id所在的txn_tablet_map_t
        auto it = txn_tablet_map.find(key); // 在txn_tablet_map_t中查找参数传入的partition_id和transaction_id
        if (it != txn_tablet_map.end()) { // 参数传入的partition_id和transaction_id已经存在在txn manager中
            auto load_itr = it->second.find(tablet_info); // 从txn manager中查找tablet_info
            if (load_itr != it->second.end()) { // 从txn manager中查找到了tablet_info
                // found load for txn,tablet
                // case 1: user commit rowset, then the load id must be equal
                TabletTxnInfo& load_info = load_itr->second; // 从txn manager中获取参数传入的tablet、partition_id和transaction_id对应的TabletTxnInfo
                rowset_ptr = load_info.rowset; // 获取当前transaction_id对应的数据导入在参数传入的tabelt下生成的rowset
            }
        }
    }
    // save meta need access disk, it maybe very slow, so that it is not in global txn lock
    // it is under a single txn lock
    if (rowset_ptr != nullptr) {
        // TODO(ygl): rowset is already set version here, memory is changed, if save failed
        // it maybe a fatal error
        rowset_ptr->make_visible(version, version_hash); // 使当前transaction_id对应的数据导入在参数传入的tabelt下生成的rowset变成可见版本
        RowsetMetaPB rowset_meta_pb;
        rowset_ptr->rowset_meta()->to_rowset_pb(&rowset_meta_pb); // 将rowset meta序列化成PB
        OLAPStatus save_status = RowsetMetaManager::save(meta, tablet_uid, rowset_ptr->rowset_id(), rowset_meta_pb); // 持久化导入数据生成的新的rowset
        if (save_status != OLAP_SUCCESS) {
            LOG(WARNING) << "save committed rowset failed. when publish txn rowset_id:"
                         << rowset_ptr->rowset_id()
                         << ", tablet id: " << tablet_id
                         << ", txn id:" << transaction_id;
            return OLAP_ERR_ROWSET_SAVE_FAILED;
        }
    } else {
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
    {
        WriteLock wrlock(&_get_txn_map_lock(transaction_id));
        txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id); // 获取当前transaction_id所在的txn_tablet_map_t
        auto it = txn_tablet_map.find(key); // 在txn_tablet_map_t中查找参数传入的partition_id和transaction_id
        if (it != txn_tablet_map.end()) { // 参数传入的partition_id和transaction_id已经存在在txn manager中
            it->second.erase(tablet_info); // 从txn manager中该transaction_id下删除参数传入的tablet
            LOG(INFO) << "publish txn successfully."
                      << " partition_id: " << key.first
                      << ", txn_id: " << key.second
                      << ", tablet: " << tablet_info.to_string()
                      << ", rowsetid: " << rowset_ptr->rowset_id();
            if (it->second.empty()) {  // 判断当前partition_id和transaction_id的pair下的tablet是否已经均被删除了
                txn_tablet_map.erase(it); // 从txn manager中删除该partition id和transaction_id的pair
                _clear_txn_partition_map_unlocked(transaction_id, partition_id); // 从transaction_id对应的txn_partition_map_t中删除partition_id
            }
        }
        return OLAP_SUCCESS;
    }
}

// txn could be rollbacked if it does not have related rowset
// if the txn has related rowset then could not rollback it, because it
// may be committed in another thread and our current thread meets errors when writing to data file
// BE has to wait for fe call clear txn api
OLAPStatus TxnManager::rollback_txn(TPartitionId partition_id, TTransactionId transaction_id,
                                    TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    WriteLock wrlock(&_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it != txn_tablet_map.end()) {
        auto load_itr = it->second.find(tablet_info);
        if (load_itr != it->second.end()) {
            // found load for txn,tablet
            // case 1: user commit rowset, then the load id must be equal
            TabletTxnInfo& load_info = load_itr->second;
            if (load_info.rowset != nullptr) {
                // if rowset is not null, it means other thread may commit the rowset
                // should not delete txn any more
                return OLAP_ERR_TRANSACTION_ALREADY_COMMITTED;
            }
        }
        it->second.erase(tablet_info);
        LOG(INFO) << "rollback transaction from engine successfully."
                  << " partition_id: " << key.first
                  << ", transaction_id: " << key.second
                  << ", tablet: " << tablet_info.to_string();
        if (it->second.empty()) {
            txn_tablet_map.erase(it);
            _clear_txn_partition_map_unlocked(transaction_id, partition_id);
        }
    }
    return OLAP_SUCCESS;
}

// fe call this api to clear unused rowsets in be
// could not delete the rowset if it already has a valid version
OLAPStatus TxnManager::delete_txn(OlapMeta* meta, TPartitionId partition_id, TTransactionId transaction_id,
                                  TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    WriteLock txn_wrlock(&_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        return OLAP_ERR_TRANSACTION_NOT_EXIST;
    }
    auto load_itr = it->second.find(tablet_info);
    if (load_itr != it->second.end()) {
        // found load for txn,tablet
        // case 1: user commit rowset, then the load id must be equal
        TabletTxnInfo& load_info = load_itr->second;
        if (load_info.rowset != nullptr && meta != nullptr) {
            if (load_info.rowset->version().first > 0) { 
                LOG(WARNING) << "could not delete transaction from engine, "
                                << "just remove it from memory not delete from disk" 
                                << " because related rowset already published."
                                << ",partition_id: " << key.first
                                << ", transaction_id: " << key.second
                                << ", tablet: " << tablet_info.to_string()
                                << ", rowset id: " << load_info.rowset->rowset_id()
                                << ", version: " << load_info.rowset->version().first;
                return OLAP_ERR_TRANSACTION_ALREADY_VISIBLE;
            } else {
                RowsetMetaManager::remove(meta, tablet_uid, load_info.rowset->rowset_id());
                #ifndef BE_TEST
                StorageEngine::instance()->add_unused_rowset(load_info.rowset);
                #endif
                VLOG(3) << "delete transaction from engine successfully."
                        << " partition_id: " << key.first
                        << ", transaction_id: " << key.second
                        << ", tablet: " << tablet_info.to_string()
                        << ", rowset: " << (load_info.rowset != nullptr ?  load_info.rowset->rowset_id().to_string(): "0");
            }
        }
    }
    it->second.erase(tablet_info);
    if (it->second.empty()) {
        txn_tablet_map.erase(it);
        _clear_txn_partition_map_unlocked(transaction_id, partition_id);
    }
    return OLAP_SUCCESS;
}

void TxnManager::get_tablet_related_txns(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, 
    int64_t* partition_id, std::set<int64_t>* transaction_ids) {
    if (partition_id == nullptr || transaction_ids == nullptr) {
        LOG(WARNING) << "parameter is null when get transactions by tablet";
        return;
    }

    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        ReadLock txn_rdlock(&_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto& it : txn_tablet_map) {
            if (it.second.find(tablet_info) != it.second.end()) {
                *partition_id = it.first.first;
                transaction_ids->insert(it.first.second);
                VLOG(3) << "find transaction on tablet."
                        << "partition_id: " << it.first.first
                        << ", transaction_id: " << it.first.second
                        << ", tablet: " << tablet_info.to_string();
            }
        }
    }
}

// force drop all txns related with the tablet
// maybe lock error, because not get txn lock before remove from meta
void TxnManager::force_rollback_tablet_related_txns(OlapMeta* meta, TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid) {
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        WriteLock txn_wrlock(&_txn_map_locks[i]);
        txn_tablet_map_t& txn_tablet_map = _txn_tablet_maps[i];
        for (auto it = txn_tablet_map.begin(); it != txn_tablet_map.end();) {
            auto load_itr = it->second.find(tablet_info);
            if (load_itr != it->second.end()) {
                TabletTxnInfo& load_info = load_itr->second;
                if (load_info.rowset != nullptr && meta != nullptr) {
                    LOG(INFO) << " delete transaction from engine "
                              << ", tablet: " << tablet_info.to_string()
                              << ", rowset id: " << load_info.rowset->rowset_id();
                    RowsetMetaManager::remove(meta, tablet_uid,
                                              load_info.rowset->rowset_id());
                }
                LOG(INFO) << "remove tablet related txn."
                          << " partition_id: " << it->first.first
                          << ", transaction_id: " << it->first.second
                          << ", tablet: " << tablet_info.to_string() << ", rowset: "
                          << (load_info.rowset != nullptr
                              ? load_info.rowset->rowset_id().to_string()
                              : "0");
                it->second.erase(tablet_info);
            }
            if (it->second.empty()) {
                _clear_txn_partition_map_unlocked(it->first.second,
                                                  it->first.first);
                it = txn_tablet_map.erase(it);
            } else {
                ++it;
            }
        }
    }
}

void TxnManager::get_txn_related_tablets(const TTransactionId transaction_id,
                                         TPartitionId partition_id,
                                         std::map<TabletInfo, RowsetSharedPtr>* tablet_infos) {
    // get tablets in this transaction
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    ReadLock txn_rdlock(&_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    if (it == txn_tablet_map.end()) {
        VLOG(3) << "could not find tablet for"
                << " partition_id=" << partition_id 
                << ", transaction_id=" << transaction_id;
        return;
    }
    std::map<TabletInfo, TabletTxnInfo>& load_info_map = it->second;

    // each tablet
    for (auto& load_info : load_info_map) {
        const TabletInfo& tablet_info = load_info.first;
        // must not check rowset == null here, because if rowset == null
        // publish version should failed
	    tablet_infos->emplace(tablet_info, load_info.second.rowset);
    }
}

void TxnManager::get_all_related_tablets(std::set<TabletInfo>* tablet_infos) {
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        ReadLock txn_rdlock(&_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            for (auto& tablet_load_it : it.second) {
                tablet_infos->emplace(tablet_load_it.first);
            }
        }
    }
}                                

bool TxnManager::has_txn(TPartitionId partition_id, TTransactionId transaction_id,
                         TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid) {
    pair<int64_t, int64_t> key(partition_id, transaction_id);
    TabletInfo tablet_info(tablet_id, schema_hash, tablet_uid);
    ReadLock txn_rdlock(&_get_txn_map_lock(transaction_id));
    txn_tablet_map_t& txn_tablet_map = _get_txn_tablet_map(transaction_id);
    auto it = txn_tablet_map.find(key);
    bool found = it != txn_tablet_map.end()
                 && it->second.find(tablet_info) != it->second.end();

    return found;
}

void TxnManager::build_expire_txn_map(std::map<TabletInfo, std::vector<int64_t>>* expire_txn_map) {
    int64_t now = UnixSeconds();
    // traverse the txn map, and get all expired txns
    for (int32_t i = 0; i < _txn_map_shard_size; i++) {
        ReadLock txn_rdlock(&_txn_map_locks[i]);
        for (auto& it : _txn_tablet_maps[i]) {
            auto txn_id = it.first.second;
            for (auto& t_map : it.second) {
                double diff = difftime(now, t_map.second.creation_time);
                if (diff >= config::pending_data_expire_time_sec) {
                    (*expire_txn_map)[t_map.first].push_back(txn_id);
                    if (VLOG_IS_ON(3)) {
                        VLOG(3) << "find expired txn."
                                << " tablet=" << t_map.first.to_string()
                                << " transaction_id=" << txn_id
                                << " exist_sec=" << diff;
                    }
                }
            }
        }
    }
}

void TxnManager::get_partition_ids(const TTransactionId transaction_id, std::vector<TPartitionId>* partition_ids) {
    ReadLock txn_rdlock(&_get_txn_map_lock(transaction_id));
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id);
    auto it = txn_partition_map.find(transaction_id);
    if (it != txn_partition_map.end()) {
        for (int64_t partition_id : it->second) {
            partition_ids->push_back(partition_id);
        }
    }
}

/*将partition_id添加到transaction_id对应的txn_partition_map_t中*/
void TxnManager::_insert_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id); // 计算transaction_id应该对应的txn_partition_map_t
    auto find = txn_partition_map.find(transaction_id); //判断参数传入的transaction_id是否在txn_partition_map_t中存在
    if (find == txn_partition_map.end()) {
        txn_partition_map[transaction_id] = std::unordered_set<int64_t>(); // 如果参数传入的transaction_id在txn_partition_map_t中不存在，则在其中添加当前的transaction_id
    }
    txn_partition_map[transaction_id].insert(partition_id); // 将partition_id添加到transaction_id对应的txn_partition_map_t中
}

/*从transaction_id对应的txn_partition_map_t中删除partition_id*/
void TxnManager::_clear_txn_partition_map_unlocked(int64_t transaction_id, int64_t partition_id) {
    txn_partition_map_t& txn_partition_map = _get_txn_partition_map(transaction_id); // 计算transaction_id应该对应的txn_partition_map_t
    auto it = txn_partition_map.find(transaction_id);//判断参数传入的transaction_id是否在txn_partition_map_t中存在
    if (it != txn_partition_map.end()) {
        it->second.erase(partition_id); // 如果参数传入的transaction_id在txn_partition_map_t中不存在，则从其中删除当前的partition_id
        if (it->second.empty()) {
            txn_partition_map.erase(it); // 如果txn_partition_map_t中transaction_id对应的partition_id均已被删除，则从txn_partition_map_t中删除该transaction_id
        }
    }
}

} // namespace doris
