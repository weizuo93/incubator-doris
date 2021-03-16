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

import com.google.common.collect.ImmutableMap;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.plugin.AuditEvent;
import org.apache.doris.plugin.AuditEvent.EventType;
import org.apache.doris.plugin.StreamLoadAuditEvent;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TStreamLoadRecord;
import org.apache.doris.thrift.TStreamLoadRecordResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class StreamLoadRecordMgr {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);
    private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public StreamLoadRecordMgr() {
        Thread pullStreamLoadRecordThread = new Thread(new PullStreamLoadRecordThread());
        pullStreamLoadRecordThread.start();
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
                        TStreamLoadRecordResult result = client.getStreamLoadRecord(backend.getLastStreamLoadTime());
                        Map<String, TStreamLoadRecord> streamLoadRecordBatch = result.getStreamLoadRecord();
                        LOG.info("receive stream load audit info from backend: {}. batch size: {}", backend.getHost(), streamLoadRecordBatch.size());
                        for (Map.Entry<String, TStreamLoadRecord> entry : streamLoadRecordBatch.entrySet()) {
                            TStreamLoadRecord streamLoadItem= entry.getValue();
                            LOG.info("receive stream load record info from backend: {}. label: {}, db: {}, tbl: {}, user: {}, user_ip: {}," +
                                            " status: {}, message: {}, error_url: {}, total_rows: {}, loaded_rows: {}, filtered_rows: {}," +
                                            " unselected_rows: {}, load_bytes: {}, start_time: {}, finish_time: {}.",
                                    backend.getHost(), streamLoadItem.getLabel(), streamLoadItem.getDb(), streamLoadItem.getTbl(), streamLoadItem.getUser(), streamLoadItem.getUserIp(),
                                    streamLoadItem.getStatus(), streamLoadItem.getMessage(), streamLoadItem.getUrl(), streamLoadItem.getTotalRows(), streamLoadItem.getLoadedRows(),
                                    streamLoadItem.getFilteredRows(), streamLoadItem.getUnselectedRows(), streamLoadItem.getLoadBytes(), streamLoadItem.getStartTime(),
                                    streamLoadItem.getFinishTime());

                            AuditEvent auditEvent = new StreamLoadAuditEvent.AuditEventBuilder().setEventType(EventType.STREAM_LOAD_FINISH)
                                    .setLabel(streamLoadItem.getLabel()).setDb(streamLoadItem.getDb()).setTable(streamLoadItem.getTbl())
                                    .setUser(streamLoadItem.getUser()).setClientIp(streamLoadItem.getUserIp()).setStatus(streamLoadItem.getStatus())
                                    .setMessage(streamLoadItem.getMessage()).setUrl(streamLoadItem.getUrl()).setTotalRows(streamLoadItem.getTotalRows())
                                    .setLoadedRows( streamLoadItem.getLoadedRows()).setFilteredRows(streamLoadItem.getFilteredRows())
                                    .setUnselectedRows(streamLoadItem.getUnselectedRows()).setLoadBytes(streamLoadItem.getLoadBytes())
                                    .setStartTime(streamLoadItem.getStartTime()).setFinishTime(streamLoadItem.getFinishTime())
                                    .build();
                            Catalog.getCurrentCatalog().getAuditEventProcessor().handleAuditEvent(auditEvent);
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
                    TimeUnit.SECONDS.sleep(Config.fetch_stream_load_record_interval_second);
                } catch (InterruptedException e1) {
                    // do nothing
                }
            }

        }
    }

}
