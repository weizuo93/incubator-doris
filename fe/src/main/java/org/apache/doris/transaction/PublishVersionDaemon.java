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

package org.apache.doris.transaction;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.TabletInvertedIndex;
import org.apache.doris.catalog.TabletMeta;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PublishVersionTask;
import org.apache.doris.thrift.TPartitionVersionInfo;
import org.apache.doris.thrift.TTaskType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PublishVersionDaemon extends MasterDaemon {
    
    private static final Logger LOG = LogManager.getLogger(PublishVersionDaemon.class);
    
    public PublishVersionDaemon() {
        super("PUBLISH_VERSION", Config.publish_version_interval_ms);
    }
    
    @Override
    protected void runAfterCatalogReady() {
        try {
            publishVersion();
        } catch (Throwable t) {
            LOG.error("errors while publish version to all backends", t);
        }
    }

    /*publish version*/
    private void publishVersion() throws UserException {
        GlobalTransactionMgr globalTransactionMgr = Catalog.getCurrentGlobalTransactionMgr();
        List<TransactionState> readyTransactionStates = globalTransactionMgr.getReadyToPublishTransactions(); // 获取需要publish的transaction
        if (readyTransactionStates == null || readyTransactionStates.isEmpty()) {
            return;
        }

        // TODO yiguolei: could publish transaction state according to multi-tenant cluster info
        // but should do more work. for example, if a table is migrate from one cluster to another cluster
        // should publish to two clusters.
        // attention here, we publish transaction state to all backends including dead backend, if not publish to dead backend
        // then transaction manager will treat it as success
        List<Long> allBackends = Catalog.getCurrentSystemInfo().getBackendIds(false); // 获取所有的BE
        if (allBackends.isEmpty()) {
            LOG.warn("some transaction state need to publish, but no backend exists");
            return;
        }
        // every backend-transaction identified a single task
        AgentBatchTask batchTask = new AgentBatchTask();
        // traverse all ready transactions and dispatch the publish version task to all backends
        for (TransactionState transactionState : readyTransactionStates) { // 依次遍历每一个需要publish的transaction
            if (transactionState.hasSendTask()) { // 判断当前transaction的publish任务是否已经提交，如果已经提交过了继续判断下一个transaction
                continue;
            }
            List<PartitionCommitInfo> partitionCommitInfos = new ArrayList<>();
            for (TableCommitInfo tableCommitInfo : transactionState.getIdToTableCommitInfos().values()) { // 依次遍历每一个table
                partitionCommitInfos.addAll(tableCommitInfo.getIdToPartitionCommitInfo().values()); // 获取PartitionCommitInfo
            }
            List<TPartitionVersionInfo> partitionVersionInfos = new ArrayList<>(partitionCommitInfos.size());
            for (PartitionCommitInfo commitInfo : partitionCommitInfos) { // 依次遍历每一个partition
                TPartitionVersionInfo versionInfo = new TPartitionVersionInfo(commitInfo.getPartitionId(), 
                        commitInfo.getVersion(), 
                        commitInfo.getVersionHash());
                partitionVersionInfos.add(versionInfo); // 获取TPartitionVersionInfo
                if (LOG.isDebugEnabled()) {
                    LOG.debug("try to publish version info partitionid [{}], version [{}], version hash [{}]", 
                            commitInfo.getPartitionId(), 
                            commitInfo.getVersion(), 
                            commitInfo.getVersionHash());
                }
            }
            Set<Long> publishBackends = transactionState.getPublishVersionTasks().keySet();
            // public version tasks are not persisted in catalog, so publishBackends may be empty.
            // so we have to try publish to all backends;
            if (publishBackends.isEmpty()) {
                // could not just add to it, should new a new object, or the back map will destroyed
                publishBackends = Sets.newHashSet();
                publishBackends.addAll(allBackends);
            }

            for (long backendId : publishBackends) { // 依次遍历每一个需要publish的BE
                PublishVersionTask task = new PublishVersionTask(backendId, // 创建PublishVersionTask对象
                        transactionState.getTransactionId(),
                        transactionState.getDbId(),
                        partitionVersionInfos);
                // add to AgentTaskQueue for handling finish report.
                // not check return value, because the add will success
                AgentTaskQueue.addTask(task); // 将PublishVersionTask对象提交给AgentTaskQueue
                batchTask.addTask(task);
                transactionState.addPublishVersionTask(backendId, task); // 将提交的task记录到transactionState
            }
            transactionState.setHasSendTask(true); // 设置已经提交publish任务的标志为true
            LOG.info("send publish tasks for transaction: {}", transactionState.getTransactionId());
        }
        if (!batchTask.getAllTasks().isEmpty()) {
            AgentTaskExecutor.submit(batchTask); // 提交任务
        }
        
        TabletInvertedIndex tabletInvertedIndex = Catalog.getCurrentInvertedIndex();
        // try to finish the transaction, if failed just retry in next loop
        for (TransactionState transactionState : readyTransactionStates) { // 依次遍历每一个transaction，其中包含之前提交过publish任务的transaction
            Map<Long, PublishVersionTask> transTasks = transactionState.getPublishVersionTasks(); // 获取当前transaction下的所有PublishVersionTask
            Set<Long> publishErrorReplicaIds = Sets.newHashSet(); // 保存当前transaction publish失败的replica
            List<PublishVersionTask> unfinishedTasks = Lists.newArrayList(); // 保存当前transaction下还未完成的publish 任务
            for (PublishVersionTask publishVersionTask : transTasks.values()) { // 依次遍历当前transaction下的每一个PublishVersionTask
                if (publishVersionTask.isFinished()) { // 判断当前PublishVersionTask是否已经完成
                    // sometimes backend finish publish version task, but it maybe failed to change transactionid to version for some tablets
                    // and it will upload the failed tabletinfo to fe and fe will deal with them
                    List<Long> errorTablets = publishVersionTask.getErrorTablets(); // 获取publish失败的tablet
                    if (errorTablets == null || errorTablets.isEmpty()) {
                        continue;
                    } else {
                        for (long tabletId : errorTablets) { // 依次遍历每一个publish失败的tablet
                            // tablet inverted index also contains rollingup index
                            // if tablet meta not contains the tablet, skip this tablet because this tablet is dropped
                            // from fe
                            if (tabletInvertedIndex.getTabletMeta(tabletId) == null) {
                                continue;
                            }
                            Replica replica = tabletInvertedIndex.getReplica(tabletId, publishVersionTask.getBackendId()); // 获取当前publish失败的tablet在指定BE上的replica
                            if (replica != null) {
                                publishErrorReplicaIds.add(replica.getId()); // 将publish失败的replica添加到publishErrorReplicaIds中
                            } else {
                                LOG.info("could not find related replica with tabletid={}, backendid={}", 
                                        tabletId, publishVersionTask.getBackendId());
                            }
                        }
                    }
                } else {
                    unfinishedTasks.add(publishVersionTask); // 将当前未完成的publish task添加到unfinishedTasks中
                }
            }

            boolean shouldFinishTxn = false;
            if (!unfinishedTasks.isEmpty()) { // 判断当前transaction中是否存在未完成的publish任务
                if (transactionState.isPublishTimeout()) { // 判断当前transaction是否publish超时
                    // transaction's publish is timeout, but there still has unfinished tasks.
                    // we need to collect all error replicas, and try to finish this txn.
                    for (PublishVersionTask unfinishedTask : unfinishedTasks) { // 依次遍历当前transaction下的每一个未完成的publish task
                        // set all replicas in the backend to error state
                        List<TPartitionVersionInfo> versionInfos = unfinishedTask.getPartitionVersionInfos(); // 获取当前publish task对应的所有partition
                        Set<Long> errorPartitionIds = Sets.newHashSet();
                        for (TPartitionVersionInfo versionInfo : versionInfos) {
                            errorPartitionIds.add(versionInfo.getPartition_id()); // 将当前publish task对应的所有partition记录到errorPartitionIds中
                        }
                        if (errorPartitionIds.isEmpty()) {
                            continue;
                        }

                        // get all tablets of these error partitions, and mark their replicas as error.
                        // current we don't have partition to tablet map in FE, so here we use an inefficient way.
                        // TODO(cmy): this is inefficient, but just keep it simple. will change it later.
                        List<Long> tabletIds = tabletInvertedIndex.getTabletIdsByBackendId(unfinishedTask.getBackendId()); // 获取BE上所有的tablet
                        List<TabletMeta> tabletMetaList = tabletInvertedIndex.getTabletMetaList(tabletIds);
                        for (int i = 0; i < tabletIds.size(); i++) { // 依次遍历每一个tablet，寻找存在errorPartitionIds中的tablet
                            long tabletId = tabletIds.get(i);
                            TabletMeta tabletMeta = tabletMetaList.get(i); // 获取tablet meta
                            if (tabletMeta == TabletInvertedIndex.NOT_EXIST_TABLET_META) {
                                continue;
                            }
                            long partitionId = tabletMeta.getPartitionId();
                            if (errorPartitionIds.contains(partitionId)) { // 判断当前tablet是否在errorPartitionIds中
                                Replica replica = tabletInvertedIndex.getReplica(tabletId,
                                                                                 unfinishedTask.getBackendId()); // 在指定BE上获取指定tablet的publish失败的replica
                                if (replica != null) {
                                    publishErrorReplicaIds.add(replica.getId()); // 将publish失败的replica添加到publishErrorReplicaIds中
                                } else {
                                    LOG.info("could not find related replica with tabletid={}, backendid={}", 
                                            tabletId, unfinishedTask.getBackendId());
                                }
                            }
                        }
                    }

                    shouldFinishTxn = true;
                }
                // 当前transaction还存在未完成的publish任务，并且publish还未超时，等待下一轮继续判断
                // transaction's publish is not timeout, waiting next round.
            } else {
                // all publish tasks are finished, try to finish this txn.
                shouldFinishTxn = true; // 当前transaction下的所有的publish任务都已经完成，需要执行finish txn
            }
            
            if (shouldFinishTxn) { // 判断是否需要执行finish txn
                globalTransactionMgr.finishTransaction(transactionState.getDbId(), transactionState.getTransactionId(), publishErrorReplicaIds); // finish txn
                if (transactionState.getTransactionStatus() != TransactionStatus.VISIBLE) { // finish txn失败，transaction的状态为VISIBLE
                    // if finish transaction state failed, then update publish version time, should check 
                    // to finish after some interval
                    transactionState.updateSendTaskTime();
                    LOG.debug("publish version for transation {} failed, has {} error replicas during publish", 
                            transactionState, publishErrorReplicaIds.size());
                }
            }

            if (transactionState.getTransactionStatus() == TransactionStatus.VISIBLE) { // finish txn完成，transaction的状态为VISIBLE
                for (PublishVersionTask task : transactionState.getPublishVersionTasks().values()) { // 依次遍历transaction的每一个publish任务
                    AgentTaskQueue.removeTask(task.getBackendId(), TTaskType.PUBLISH_VERSION, task.getSignature());  // 从AgentTaskQueue中移除当前publish任务
                }
            }
        } // end for readyTransactionStates
    }
}
