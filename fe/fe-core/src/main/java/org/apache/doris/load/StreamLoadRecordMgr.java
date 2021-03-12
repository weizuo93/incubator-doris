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

//import com.google.common.base.Strings;
//import com.google.common.collect.Lists;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
//import org.apache.doris.analysis.ShowStreamLoadStmt.StreamLoadState;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStreamLoadAudit;
import org.apache.doris.thrift.TStreamLoadAuditResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
//import java.util.LinkedList;
//import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
//import java.util.stream.Collectors;

public class StreamLoadRecordMgr {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);

    private class LabelAndDb {
        private String label;
        private long dbId;

        public LabelAndDb(String label, long dbId) {
            this.label = label;
            this.dbId = dbId;
        }

        public String getLabel() {
            return label;
        }

        public long getDbId() {
            return dbId;
        }
    }

    private LinkedBlockingQueue<LabelAndDb> streamLoadRecordQueue = Queues.newLinkedBlockingQueue();
    private Map<Long, Map<String, StreamLoadRecord>> dbIdToLabelToStreamLoadRecord = Maps.newConcurrentMap();
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public StreamLoadRecordMgr() {
        Thread pullStreamLoadRecordThread = new Thread(new PullStreamLoadRecordThread());
        pullStreamLoadRecordThread.start();
    }

    public void addStreamLoadRecord(long dbId, String label, StreamLoadRecord streamLoadRecord) {
        writeLock();
        while (isQueueFull()) {
            LabelAndDb record = streamLoadRecordQueue.poll();
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

        LabelAndDb record = new LabelAndDb(label, dbId);
        streamLoadRecordQueue.offer(record);

        if (!dbIdToLabelToStreamLoadRecord.containsKey(dbId)) {
            dbIdToLabelToStreamLoadRecord.put(dbId, new ConcurrentHashMap<>());
        }
        Map<String, StreamLoadRecord> labelToStreamLoadRecord = dbIdToLabelToStreamLoadRecord.get(dbId);
        if (!labelToStreamLoadRecord.containsKey(label)) {
            labelToStreamLoadRecord.put(label, streamLoadRecord);
        }
        writeUnlock();
    }

    /*
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
    */

    public boolean isQueueFull() {
        return streamLoadRecordQueue.size() >= Config.max_stream_load_record_size;
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
                        TStreamLoadAuditResult result = client.getStreamLoadAudit("");
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
