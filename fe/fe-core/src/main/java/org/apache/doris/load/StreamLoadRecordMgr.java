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

package org.apache.doris.load;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.doris.analysis.ShowStreamLoadStmt.StreamLoadState;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStreamLoadAudit;
import org.apache.doris.thrift.TStreamLoadAuditResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.stream.Collectors;

public class StreamLoadRecordMgr {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);

    private class StreamLoadItem {
        private String label;
        private long dbId;
        private String finishTime;

        public StreamLoadItem(String label, long dbId, String finishTime) {
            this.label = label;
            this.dbId = dbId;
            this.finishTime = finishTime;
        }

        public String getLabel() {
            return label;
        }

        public long getDbId() {
            return dbId;
        }

        public String getFinishTime() {
            return finishTime;
        }
    }

    class StreamLoadComparator implements Comparator<StreamLoadItem> {
        public int compare(StreamLoadItem s1, StreamLoadItem s2) {
            return s1.getFinishTime().compareTo(s2.getFinishTime());
        }
    }

    Queue<StreamLoadItem> streamLoadRecordHeap = new PriorityQueue<>(new StreamLoadComparator());
    private Map<Long, Map<String, StreamLoadRecord>> dbIdToLabelToStreamLoadRecord = Maps.newConcurrentMap();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public StreamLoadRecordMgr() {
        Thread pullStreamLoadRecordThread = new Thread(new PullStreamLoadRecordThread());
        pullStreamLoadRecordThread.start();
    }

    public void addStreamLoadRecord(long dbId, String label, StreamLoadRecord streamLoadRecord) {
        writeLock();
        while (isQueueFull()) {
            StreamLoadItem record = streamLoadRecordHeap.poll();
            if (record != null) {
                String de_label = record.getLabel();
                long de_dbId = record.getDbId();

                Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(de_dbId);
                Iterator<Map.Entry<String, StreamLoadRecord>> iter_record = labelToStreamLoadRecord.entrySet().iterator();
                while (iter_record.hasNext()) {
                    String labelInMap = iter_record.next().getKey();
                    if (labelInMap.equals(de_label)) {
                        iter_record.remove();
                        break;
                    }
                }
            }
        }

        StreamLoadItem record = new StreamLoadItem(label, dbId, streamLoadRecord.getFinishTime());
        streamLoadRecordHeap.offer(record);

        if (!dbIdToLabelToStreamLoadRecord.containsKey(dbId)) {
            dbIdToLabelToStreamLoadRecord.put(dbId, new ConcurrentHashMap<>());
        }
        Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(dbId);
        if (!labelToStreamLoadRecord.containsKey(label)) {
            labelToStreamLoadRecord.put(label, streamLoadRecord);
        }
        writeUnlock();
    }

    public List<List<Comparable>> getStreamLoadRecordByDb(long dbId, String label, boolean accurateMatch, StreamLoadState state) {
        LinkedList<List<Comparable>> streamLoadRecords = new LinkedList<List<Comparable>>();

        readLock();
        try {
            if (!dbIdToLabelToStreamLoadRecord.containsKey(dbId)) {
                return streamLoadRecords;
            }

            List<StreamLoadRecord> streamLoadRecordList = Lists.newArrayList();
            Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(dbId);
            if (Strings.isNullOrEmpty(label)) {
                streamLoadRecordList.addAll(labelToStreamLoadRecord.values().stream().collect(Collectors.toList()));
            } else {
                // check label value
                if (accurateMatch) {
                    if (!labelToStreamLoadRecord.containsKey(label)) {
                        return streamLoadRecords;
                    }
                    streamLoadRecordList.add(labelToStreamLoadRecord.get(label));
                } else {
                    // non-accurate match
                    for (Map.Entry<String, StreamLoadRecord> entry : labelToStreamLoadRecord.entrySet()) {
                        if (entry.getKey().contains(label)) {
                            streamLoadRecordList.add(entry.getValue());
                        }
                    }
                }
            }

            for (StreamLoadRecord streamLoadRecord : streamLoadRecordList) {
                try {
                    if (state != null && !String.valueOf(state).equalsIgnoreCase(streamLoadRecord.getStatus())) {
                        continue;
                    }
                    streamLoadRecords.add(streamLoadRecord.getStreamLoadInfo());
                } catch (Exception e) {
                    continue;
                }

            }
            return streamLoadRecords;
        } finally {
            readUnlock();
        }
    }

    public boolean isQueueFull() {
        return streamLoadRecordHeap.size() >= Config.max_stream_load_record_size;
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

    private class PullStreamLoadRecordThread implements Runnable {
        @Override
        public void run() {
            ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();

            while (true) {
                long start = System.currentTimeMillis();
                for (Backend backend : backends.values()) {
                    BackendService.Client client = null;
                    TNetworkAddress address = null;
                    boolean ok = false;
                    try {
                        address = new TNetworkAddress(backend.getHost(), backend.getBePort());
                        client = ClientPool.backendPool.borrowObject(address);
                        TStreamLoadAuditResult result = client.getStreamLoadAudit(backend.getLastStreamLoadTime());
                        Map<String, TStreamLoadAudit> streamLoadAudit = result.getStreamLoadAudit();
                        LOG.info("receive stream load audit info from backend: {}. batch size: {}", backend.getHost(), streamLoadAudit.size());
                        for (Map.Entry<String, TStreamLoadAudit> entry : streamLoadAudit.entrySet()) {
                            TStreamLoadAudit streamLoadItem= entry.getValue();
                            LOG.info("receive stream load audit info from backend: {}. label: {}, db: {}, tbl: {}, user: {}, user_ip: {}," +
                                            " status: {}, message: {}, error_url: {}, total_rows: {}, loaded_rows: {}, filtered_rows: {}," +
                                            " unselected_rows: {}, load_bytes: {}, start_time: {}, finish_time: {}.",
                                    backend.getHost(), streamLoadItem.getLabel(), streamLoadItem.getDb(), streamLoadItem.getTbl(), streamLoadItem.getUser(), streamLoadItem.getUserIp(),
                                    streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(), streamLoadItem.getTotalRows(), streamLoadItem.getLoadedRows(),
                                    streamLoadItem.getFilteredRows(), streamLoadItem.getUnselectedRows(), streamLoadItem.getLoadBytes(), streamLoadItem.getStartTime(),
                                    streamLoadItem.getFinishTime());

                            StreamLoadRecord streamLoadRecord = new StreamLoadRecord(streamLoadItem.getLabel(), streamLoadItem.getDb(), streamLoadItem.getTbl(),
                                    streamLoadItem.getUser(), streamLoadItem.getUserIp(), streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(),
                                    String.valueOf(streamLoadItem.getTotalRows()), String.valueOf(streamLoadItem.getLoadedRows()),
                                    String.valueOf(streamLoadItem.getFilteredRows()), String.valueOf(streamLoadItem.getUnselectedRows()),
                                    String.valueOf(streamLoadItem.getLoadBytes()), streamLoadItem.getStartTime(), streamLoadItem.getFinishTime());

                            String cluster = streamLoadItem.getCluster();
                            if (Strings.isNullOrEmpty(cluster)) {
                                cluster = SystemInfoService.DEFAULT_CLUSTER;
                            }
                            Catalog catalog = Catalog.getCurrentCatalog();
                            String fullDbName = ClusterNamespace.getFullName(cluster, streamLoadItem.getDb());
                            Database db = catalog.getDb(fullDbName);
                            if (db == null) {
                                String dbName = fullDbName;
                                if (Strings.isNullOrEmpty(streamLoadItem.getCluster())) {
                                    dbName = streamLoadItem.getDb();
                                }
                                throw new UserException("unknown database, database=" + dbName);
                            }
                            long dbId = db.getId();
                            catalog.getStreamLoadRecordMgr().addStreamLoadRecord(dbId, streamLoadItem.getLabel(), streamLoadRecord);
                            if (entry.getKey().compareTo(backend.getLastStreamLoadTime()) > 0) {
                                backend.setLastStreamLoadTime(entry.getKey());
                            }
                        }
                        ok = true;
                    } catch (Exception e) {
                        LOG.warn("task exec error. backend[{}]", backend.getId(), e);
                    } finally {
                        if (ok) {
                            ClientPool.backendPool.returnObject(address, client);
                        } else {
                            ClientPool.backendPool.invalidateObject(address, client);
                        }
                    }
                }
                LOG.info("finished to pull stream load records of all backends. cost: {} ms",
                        (System.currentTimeMillis() - start));

                try {
                    TimeUnit.SECONDS.sleep(5);
                } catch (InterruptedException e1) {
                    // do nothing
                }
            }

        }
    }

}
