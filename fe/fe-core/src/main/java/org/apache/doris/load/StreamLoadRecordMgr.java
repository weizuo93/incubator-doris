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

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.MasterDaemon;
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

import com.google.common.collect.Maps;
import com.google.common.collect.ImmutableMap;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.Map;

public class StreamLoadRecordMgr extends MasterDaemon {
    private static final Logger LOG = LogManager.getLogger(StreamLoadRecordMgr.class);

    @Override
    protected void runAfterCatalogReady() {
        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();

        while (true) {
            long start = System.currentTimeMillis();
            int pullRecordSize = 0;
            Map<Long, String> beIdToLastStreamLoad = Maps.newHashMap();
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
                    pullRecordSize += streamLoadRecordBatch.size();
                    String lastStreamLoadTime = "";
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
                        if (entry.getKey().compareTo(lastStreamLoadTime) > 0) {
                            lastStreamLoadTime = entry.getKey();
                        }
                    }
                    backend.setLastStreamLoadTime(lastStreamLoadTime);
                    beIdToLastStreamLoad.put(backend.getId(), lastStreamLoadTime);
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
            LOG.info("finished to pull stream load records of all backends. record size: {}, cost: {} ms", pullRecordSize, (System.currentTimeMillis() - start));
            FetchStreamLoadRecord fetchStreamLoadRecord = new FetchStreamLoadRecord(beIdToLastStreamLoad);
            Catalog.getCurrentCatalog().getEditLog().logFetchStreamLoadRecord(fetchStreamLoadRecord);

            try {
                TimeUnit.SECONDS.sleep(Config.fetch_stream_load_record_interval_second);
            } catch (InterruptedException e1) {
                // do nothing
            }
        }
    }

    public void replayFetchStreamLoadRecord(FetchStreamLoadRecord fetchStreamLoadRecord) {
        ImmutableMap<Long, Backend> backends = Catalog.getCurrentSystemInfo().getIdToBackend();
        Map<Long, String> beIdToLastStreamLoad = fetchStreamLoadRecord.getBeIdToLastStreamLoad();
        for (Backend backend : backends.values()) {
            String lastStreamLoadTime = beIdToLastStreamLoad.get(backend.getId());
            if (lastStreamLoadTime != null) {
                LOG.info("Replay stream load bdbje. backend: {}, last stream load version: {}", backend.getHost(), lastStreamLoadTime);
                backend.setLastStreamLoadTime(lastStreamLoadTime);
            }
        }
    }

    public static class FetchStreamLoadRecord implements Writable {
        private Map<Long, String> beIdToLastStreamLoad;

        public FetchStreamLoadRecord(Map<Long, String> beIdToLastStreamLoad) {
            this.beIdToLastStreamLoad = beIdToLastStreamLoad;
        }

        public void setBeIdToLastStreamLoad(Map<Long, String> beIdToLastStreamLoad) {
            this.beIdToLastStreamLoad = beIdToLastStreamLoad;
        }

        public Map<Long, String> getBeIdToLastStreamLoad() {
            return beIdToLastStreamLoad;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            for (Map.Entry<Long, String> entry : beIdToLastStreamLoad.entrySet()) {
                out.writeBoolean(true);
                out.writeLong(entry.getKey());
                out.writeBoolean(true);
                Text.writeString(out, entry.getValue());
                LOG.debug("Write stream load bdbje. key: {}, value: {} ", entry.getKey(), entry.getValue());
            }
        }

        public static FetchStreamLoadRecord read(DataInput in) throws IOException {
            Map<Long, String> idToLastStreamLoad = Maps.newHashMap();
            int beNum = Catalog.getCurrentSystemInfo().getIdToBackend().size();
            for (int i = 0; i < beNum; i++) {
                long beId = -1;
                String lastStreamLoad = null;
                if (in.readBoolean()) {
                    beId = in.readLong();
                }
                if (in.readBoolean()) {
                    lastStreamLoad = Text.readString(in);
                }
                if (beId != -1 && lastStreamLoad != null) {
                    idToLastStreamLoad.put(beId, lastStreamLoad);
                }
                LOG.debug("Read stream load bdbje. key: {}, value: {} ", beId, lastStreamLoad);
            }
            FetchStreamLoadRecord fetchStreamLoadRecord = new FetchStreamLoadRecord(idToLastStreamLoad);
            return fetchStreamLoadRecord;
        }
    }
}
