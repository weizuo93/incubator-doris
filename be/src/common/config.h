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

#ifndef DORIS_BE_SRC_COMMON_CONFIG_H
#define DORIS_BE_SRC_COMMON_CONFIG_H

#include "configbase.h"

namespace doris {
namespace config {
// Dir of custom config file
CONF_String(custom_config_dir, "${DORIS_HOME}/conf");

// cluster id
CONF_Int32(cluster_id, "-1");
// port on which BackendService is exported
CONF_Int32(be_port, "9060");

// port for brpc
CONF_Int32(brpc_port, "8060");

// the number of bthreads for brpc, the default value is set to -1, which means the number of bthreads is #cpu-cores
CONF_Int32(brpc_num_threads, "-1")

// Declare a selection strategy for those servers have many ips.
// Note that there should at most one ip match this list.
// this is a list in semicolon-delimited format, in CIDR notation, e.g. 10.10.10.0/24
// If no ip match this rule, will choose one randomly.
CONF_String(priority_networks, "");

////
//// tcmalloc gc parameter
////
// min memory for TCmalloc, when used memory is smaller than this, do not returned to OS
CONF_mInt64(tc_use_memory_min, "10737418240");
// free memory rate.[0-100]
CONF_mInt64(tc_free_memory_rate, "20");

// Bound on the total amount of bytes allocated to thread caches.
// This bound is not strict, so it is possible for the cache to go over this bound
// in certain circumstances. This value defaults to 1GB
// If you suspect your application is not scaling to many threads due to lock contention in TCMalloc,
// you can try increasing this value. This may improve performance, at a cost of extra memory
// use by TCMalloc.
// reference: https://gperftools.github.io/gperftools/tcmalloc.html: TCMALLOC_MAX_TOTAL_THREAD_CACHE_BYTES
//            https://github.com/gperftools/gperftools/issues/1111
CONF_Int64(tc_max_total_thread_cache_bytes, "1073741824");

// process memory limit specified as number of bytes
// ('<int>[bB]?'), megabytes ('<float>[mM]'), gigabytes ('<float>[gG]'),
// or percentage of the physical memory ('<int>%').
// defaults to bytes if no unit is given"
// must larger than 0. and if larger than physical memory size,
// it will be set to physical memory size.
CONF_String(mem_limit, "80%");

// the port heartbeat service used
CONF_Int32(heartbeat_service_port, "9050");
// the count of heart beat service
CONF_Int32(heartbeat_service_thread_count, "1");
// the count of thread to create table
CONF_Int32(create_tablet_worker_count, "3");
// the count of thread to drop table
CONF_Int32(drop_tablet_worker_count, "3");
// the count of thread to batch load
CONF_Int32(push_worker_count_normal_priority, "3");
// the count of thread to high priority batch load
CONF_Int32(push_worker_count_high_priority, "3");
// the count of thread to publish version
CONF_Int32(publish_version_worker_count, "8");
// the count of thread to clear transaction task
CONF_Int32(clear_transaction_task_worker_count, "1");
// the count of thread to delete
CONF_Int32(delete_worker_count, "3");
// the count of thread to alter table
CONF_Int32(alter_tablet_worker_count, "3");
// the count of thread to clone
CONF_Int32(clone_worker_count, "3");
// the count of thread to clone
CONF_Int32(storage_medium_migrate_count, "1");
// the count of thread to check consistency
CONF_Int32(check_consistency_worker_count, "1");
// the count of thread to upload
CONF_Int32(upload_worker_count, "1");
// the count of thread to download
CONF_Int32(download_worker_count, "1");
// the count of thread to make snapshot
CONF_Int32(make_snapshot_worker_count, "5");
// the count of thread to release snapshot
CONF_Int32(release_snapshot_worker_count, "5");
// the interval time(seconds) for agent report tasks signatrue to FE
CONF_mInt32(report_task_interval_seconds, "10");
// the interval time(seconds) for agent report disk state to FE
CONF_mInt32(report_disk_state_interval_seconds, "60");
// the interval time(seconds) for agent report olap table to FE
CONF_mInt32(report_tablet_interval_seconds, "60");
// the interval time(seconds) for agent report plugin status to FE
// CONF_Int32(report_plugin_interval_seconds, "120");
// the timeout(seconds) for alter table
// CONF_Int32(alter_tablet_timeout_seconds, "86400");
// the timeout(seconds) for make snapshot
// CONF_Int32(make_snapshot_timeout_seconds, "600");
// the timeout(seconds) for release snapshot
// CONF_Int32(release_snapshot_timeout_seconds, "600");
// the max download speed(KB/s)
CONF_mInt32(max_download_speed_kbps, "50000");
// download low speed limit(KB/s)
CONF_mInt32(download_low_speed_limit_kbps, "50");
// download low speed time(seconds)
CONF_mInt32(download_low_speed_time, "300");
// curl verbose mode
// CONF_Int64(curl_verbose_mode, "1");
// seconds to sleep for each time check table status
// CONF_Int32(check_status_sleep_time_seconds, "10");
// sleep time for one second
CONF_Int32(sleep_one_second, "1");
// sleep time for five seconds
CONF_Int32(sleep_five_seconds, "5");

// log dir
CONF_String(sys_log_dir, "${DORIS_HOME}/log");
CONF_String(user_function_dir, "${DORIS_HOME}/lib/udf");
// INFO, WARNING, ERROR, FATAL
CONF_String(sys_log_level, "INFO");
// TIME-DAY, TIME-HOUR, SIZE-MB-nnn
CONF_String(sys_log_roll_mode, "SIZE-MB-1024");
// log roll num
CONF_Int32(sys_log_roll_num, "10");
// verbose log
CONF_Strings(sys_log_verbose_modules, "");
// verbose log level
CONF_Int32(sys_log_verbose_level, "10");
// log buffer level
CONF_String(log_buffer_level, "");

// Pull load task dir
CONF_String(pull_load_task_dir, "${DORIS_HOME}/var/pull_load");

// the maximum number of bytes to display on the debug webserver's log page
// CONF_Int64(web_log_bytes, "1048576");
// number of threads available to serve backend execution requests
CONF_Int32(be_service_threads, "64");
// key=value pair of default query options for Doris, separated by ','
CONF_String(default_query_options, "");

// If non-zero, Doris will output memory usage every log_mem_usage_interval'th fragment completion.
// CONF_Int32(log_mem_usage_interval, "0");

// cgroups allocated for doris
CONF_String(doris_cgroups, "");

// Controls the number of threads to run work per core.  It's common to pick 2x
// or 3x the number of cores.  This keeps the cores busy without causing excessive
// thrashing.
CONF_Int32(num_threads_per_core, "3");
// if true, compresses tuple data in Serialize
CONF_Bool(compress_rowbatches, "true");
// serialize and deserialize each returned row batch
CONF_Bool(serialize_batch, "false");
// interval between profile reports; in seconds
CONF_mInt32(status_report_interval, "5");
// Local directory to copy UDF libraries from HDFS into
CONF_String(local_library_dir, "${UDF_RUNTIME_DIR}");
// number of olap scanner thread pool size
CONF_Int32(doris_scanner_thread_pool_thread_num, "48");
// number of olap scanner thread pool queue size
CONF_Int32(doris_scanner_thread_pool_queue_size, "102400");
// number of etl thread pool size
CONF_Int32(etl_thread_pool_size, "8");
// number of etl thread pool size
CONF_Int32(etl_thread_pool_queue_size, "256");
// port on which to run Doris test backend
CONF_Int32(port, "20001");
// default thrift client connect timeout(in seconds)
CONF_mInt32(thrift_connect_timeout_seconds, "3");
// default thrift client retry interval (in milliseconds)
CONF_mInt64(thrift_client_retry_interval_ms, "1000");
// max row count number for single scan range
CONF_mInt32(doris_scan_range_row_count, "524288");
// size of scanner queue between scanner thread and compute thread
CONF_mInt32(doris_scanner_queue_size, "1024");
// single read execute fragment row size
CONF_mInt32(doris_scanner_row_num, "16384");
// number of max scan keys
CONF_mInt32(doris_max_scan_key_num, "1024");
// the max number of push down values of a single column.
// if exceed, no conditions will be pushed down for that column.
CONF_mInt32(max_pushdown_conditions_per_column, "1024");
// return_row / total_row
CONF_mInt32(doris_max_pushdown_conjuncts_return_rate, "90");
// (Advanced) Maximum size of per-query receive-side buffer
CONF_mInt32(exchg_node_buffer_size_bytes, "10485760");
// insert sort threshold for sorter
// CONF_Int32(insertion_threshold, "16");
// the block_size every block allocate for sorter
// CONF_mInt32(sorter_block_size, "8388608");
// push_write_mbytes_per_sec
CONF_mInt32(push_write_mbytes_per_sec, "10");

CONF_mInt64(column_dictionary_key_ratio_threshold, "0");
CONF_mInt64(column_dictionary_key_size_threshold, "0");
// if true, output IR after optimization passes
// CONF_mBool(dump_ir, "false");
// if set, saves the generated IR to the output file.
//CONF_String(module_output, "");
// memory_limitation_per_thread_for_schema_change unit GB
CONF_mInt32(memory_limitation_per_thread_for_schema_change, "2");

// CONF_mInt64(max_unpacked_row_block_size, "104857600");

CONF_mInt32(file_descriptor_cache_clean_interval, "3600");
CONF_mInt32(disk_stat_monitor_interval, "5");
CONF_mInt32(unused_rowset_monitor_interval, "30");
CONF_String(storage_root_path, "${DORIS_HOME}/storage");

// Config is used to check incompatible old format hdr_ format
// whether doris uses strict way. When config is true, process will log fatal
// and exit. When config is false, process will only log warning.
CONF_Bool(storage_strict_check_incompatible_old_format, "true");

// BE process will exit if the percentage of error disk reach this value.
CONF_mInt32(max_percentage_of_error_disk, "0");
// CONF_mInt32(default_num_rows_per_data_block, "1024");
CONF_mInt32(default_num_rows_per_column_file_block, "1024");
// pending data policy
CONF_mInt32(pending_data_expire_time_sec, "1800");
// inc_rowset expired interval
CONF_mInt32(inc_rowset_expired_sec, "1800");
// inc_rowset snapshot rs sweep time interval
CONF_mInt32(tablet_rowset_stale_sweep_time_sec, "1800");
// garbage sweep policy
CONF_Int32(max_garbage_sweep_interval, "3600");
CONF_Int32(min_garbage_sweep_interval, "180");
CONF_mInt32(snapshot_expire_time_sec, "172800");
// 仅仅是建议值，当磁盘空间不足时，trash下的文件保存期可不遵守这个参数
CONF_mInt32(trash_file_expire_time_sec, "259200");
// check row nums for BE/CE and schema change. true is open, false is closed.
CONF_mBool(row_nums_check, "true");
//file descriptors cache, by default, cache 32768 descriptors
CONF_mInt32(file_descriptor_cache_capacity, "32768");
// minimum file descriptor number
// modify them upon necessity
CONF_Int32(min_file_descriptor_number, "60000");
CONF_Int64(index_stream_cache_capacity, "10737418240");
// CONF_mInt64(max_packed_row_block_size, "20971520");

// Cache for storage page size
CONF_String(storage_page_cache_limit, "20%");
// whether to disable page cache feature in storage
CONF_mBool(disable_storage_page_cache, "false");

// be policy
// whether disable automatic compaction task
CONF_mBool(disable_auto_compaction, "false");
// check the configuration of auto compaction in seconds when auto compaction disabled
CONF_mInt32(check_auto_compaction_interval_seconds, "5");

// CONF_Int64(base_compaction_start_hour, "20");
// CONF_Int64(base_compaction_end_hour, "7");
CONF_mInt64(base_compaction_num_cumulative_deltas, "5");
CONF_mDouble(base_cumulative_delta_ratio, "0.3");
CONF_mInt64(base_compaction_interval_seconds_since_last_operation, "86400");
CONF_mInt32(base_compaction_write_mbytes_per_sec, "5");

// config the cumulative compaction policy
// Valid configs: num_base, size_based
// num_based policy, the original version of cumulative compaction, cumulative version compaction once.
// size_based policy, a optimization version of cumulative compaction, targeting the use cases requiring
// lower write amplification, trading off read amplification and space amplification.
CONF_String(cumulative_compaction_policy, "size_based");

// In size_based policy, output rowset of cumulative compaction total disk size exceed this config size,
// this rowset will be given to base compaction, unit is m byte.
CONF_mInt64(cumulative_size_based_promotion_size_mbytes, "1024");
// In size_based policy, output rowset of cumulative compaction total disk size exceed this config ratio of
// base rowset's total disk size, this rowset will be given to base compaction. The value must be between
// 0 and 1.
CONF_mDouble(cumulative_size_based_promotion_ratio, "0.05");
// In size_based policy, the smallest size of rowset promotion. When the rowset is less than this config, this
// rowset will be not given to base compaction. The unit is m byte.
CONF_mInt64(cumulative_size_based_promotion_min_size_mbytes, "64");
// The lower bound size to do cumulative compaction. When total disk size of candidate rowsets is less than
// this size, size_based policy may not do to cumulative compaction. The unit is m byte.
CONF_mInt64(cumulative_size_based_compaction_lower_size_mbytes, "64");

// cumulative compaction policy: min and max delta file's number
CONF_mInt64(min_cumulative_compaction_num_singleton_deltas, "5");
CONF_mInt64(max_cumulative_compaction_num_singleton_deltas, "1000");
CONF_mInt64(cumulative_compaction_budgeted_bytes, "104857600");
// CONF_Int32(cumulative_compaction_write_mbytes_per_sec, "100");
// cumulative compaction skips recently published deltas in order to prevent
// compacting a version that might be queried (in case the query planning phase took some time).
// the following config set the window size
CONF_mInt32(cumulative_compaction_skip_window_seconds, "30");

// if compaction of a tablet failed, this tablet should not be chosen to
// compaction until this interval passes.
CONF_mInt64(min_compaction_failure_interval_sec, "600"); // 10 min

// This config can be set to limit thread number in compaction thread pool.
CONF_mInt32(min_compaction_threads, "10");
CONF_mInt32(max_compaction_threads, "10");

// The upper limit of "permits" held by all compaction tasks. This config can be set to limit memory consumption for compaction.
CONF_mInt64(total_permits_for_compaction_score, "10000");

// Compaction task number per disk.
CONF_mInt32(compaction_task_num_per_disk, "2");

// How many rounds of cumulative compaction for each round of base compaction when compaction tasks generation.
CONF_mInt32(cumulative_compaction_rounds_for_each_base_compaction_round, "9");

// Merge log will be printed for each "row_step_for_compaction_merge_log" rows merged during compaction
CONF_mInt64(row_step_for_compaction_merge_log, "0");

// Threshold to logging compaction trace, in seconds.
CONF_mInt32(base_compaction_trace_threshold, "10");
CONF_mInt32(cumulative_compaction_trace_threshold, "2");

// Threshold to logging agent task trace, in seconds.
CONF_mInt32(agent_task_trace_threshold_sec, "2");

// time interval to record tablet scan count in second for the purpose of calculating tablet scan frequency
CONF_mInt64(tablet_scan_frequency_time_node_interval_second, "300");
// coefficient for tablet scan frequency and compaction score when finding a tablet for compaction
CONF_mInt32(compaction_tablet_scan_frequency_factor, "0");
CONF_mInt32(compaction_tablet_compaction_score_factor, "1");

// Port to start debug webserver on
CONF_Int32(webserver_port, "8040");
// Number of webserver workers
CONF_mnt32(webserver_num_workers, "48");
// Period to update rate counters and sampling counters in ms.
CONF_mInt32(periodic_counter_update_period_ms, "500");

// Used for mini Load. mini load data file will be removed after this time.
CONF_Int64(load_data_reserve_hours, "4");
// log error log will be removed after this time
CONF_mInt64(load_error_log_reserve_hours, "48");
CONF_Int32(number_tablet_writer_threads, "16");

// The maximum amount of data that can be processed by a stream load
CONF_mInt64(streaming_load_max_mb, "10240");
// Some data formats, such as JSON, cannot be streamed.
// Therefore, it is necessary to limit the maximum number of
// such data when using stream load to prevent excessive memory consumption.
CONF_mInt64(streaming_load_json_max_mb, "100");
// the alive time of a TabletsChannel.
// If the channel does not receive any data till this time,
// the channel will be removed.
CONF_mInt32(streaming_load_rpc_max_alive_time_sec, "1200");
// the timeout of a rpc to open the tablet writer in remote BE.
// short operation time, can set a short timeout
CONF_mInt32(tablet_writer_open_rpc_timeout_sec, "60");
// Deprecated, use query_timeout instead
// the timeout of a rpc to process one batch in tablet writer.
// you may need to increase this timeout if using larger 'streaming_load_max_mb',
// or encounter 'tablet writer write failed' error when loading.
// CONF_Int32(tablet_writer_rpc_timeout_sec, "600");
// OlapTableSink sender's send interval, should be less than the real response time of a tablet writer rpc.
CONF_mInt32(olap_table_sink_send_interval_ms, "1");

// Fragment thread pool
CONF_Int32(fragment_pool_thread_num_min, "64");
CONF_Int32(fragment_pool_thread_num_max, "512");
CONF_Int32(fragment_pool_queue_size, "2048");

//for cast
// CONF_Bool(cast, "true");

// Spill to disk when query
// Writable scratch directories, split by ";"
CONF_String(query_scratch_dirs, "${DORIS_HOME}");

// Control the number of disks on the machine.  If 0, this comes from the system settings.
CONF_Int32(num_disks, "0");
// The maximum number of the threads per disk is also the max queue depth per disk.
CONF_Int32(num_threads_per_disk, "0");
// The read size is the size of the reads sent to os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
CONF_Int32(read_size, "8388608");    // 8 * 1024 * 1024, Read Size (in bytes)
CONF_Int32(min_buffer_size, "1024"); // 1024, The minimum read buffer size (in bytes)

// For each io buffer size, the maximum number of buffers the IoMgr will hold onto
// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
CONF_Int32(max_free_io_buffers, "128");

CONF_Bool(disable_mem_pools, "false");

// Whether to allocate chunk using mmap. If you enable this, you'd better to
// increase vm.max_map_count's value whose default value is 65530.
// you can do it as root via "sysctl -w vm.max_map_count=262144" or
// "echo 262144 > /proc/sys/vm/max_map_count"
// NOTE: When this is set to true, you must set chunk_reserved_bytes_limit
// to a relative large number or the performance is very very bad.
CONF_mBool(use_mmap_allocate_chunk, "false");

// Chunk Allocator's reserved bytes limit,
// Default value is 2GB, increase this variable can improve performance, but will
// acquire more free memory which can not be used by other modules
CONF_mInt64(chunk_reserved_bytes_limit, "2147483648");

// The probing algorithm of partitioned hash table.
// Enable quadratic probing hash table
CONF_mBool(enable_quadratic_probing, "false");

// for pprof
CONF_String(pprof_profile_dir, "${DORIS_HOME}/log");

// for partition
// CONF_mBool(enable_partitioned_hash_join, "false")
CONF_mBool(enable_partitioned_aggregation, "true");

// to forward compatibility, will be removed later
CONF_mBool(enable_token_check, "true");

// to open/close system metrics
CONF_mBool(enable_system_metrics, "true");

CONF_mBool(enable_prefetch, "true");

// Number of cores Doris will used, this will effect only when it's greater than 0.
// Otherwise, Doris will use all cores returned from "/proc/cpuinfo".
CONF_mInt32(num_cores, "0");

// CONF_mBool(thread_creation_fault_injection, "false");

// Set this to encrypt and perform an integrity
// check on all data spilled to disk during a query
// CONF_mBool(disk_spill_encryption, "false");

// When BE start, If there is a broken disk, BE process will exit by default.
// Otherwise, we will ignore the broken disk,
CONF_mBool(ignore_broken_disk, "false");

// Writable scratch directories
CONF_String(scratch_dirs, "/tmp");

// If false and --scratch_dirs contains multiple directories on the same device,
// then only the first writable directory is used
// CONF_mBool(allow_multiple_scratch_dirs_per_device, "false");

// linux transparent huge page
CONF_mBool(madvise_huge_pages, "false");

// whether use mmap to allocate memory
CONF_mBool(mmap_buffers, "false");

// max memory can be allocated by buffer pool
CONF_String(buffer_pool_limit, "80G");

// clean page can be hold by buffer pool
CONF_String(buffer_pool_clean_pages_limit, "20G");

// Sleep time in seconds between memory maintenance iterations
CONF_mInt64(memory_maintenance_sleep_time_s, "10");

// Alignment
CONF_mInt32(memory_max_alignment, "16");

// write buffer size before flush
CONF_mInt64(write_buffer_size, "104857600");

// following 2 configs limit the memory consumption of load process on a Backend.
// eg: memory limit to 80% of mem limit config but up to 100GB(default)
// NOTICE(cmy): set these default values very large because we don't want to
// impact the load performance when user upgrading Doris.
// user should set these configs properly if necessary.
CONF_mInt64(load_process_max_memory_limit_bytes, "107374182400"); // 100GB
CONF_mInt32(load_process_max_memory_limit_percent, "80");         // 80%

// update interval of tablet stat cache
CONF_mInt32(tablet_stat_cache_update_interval_second, "300");

// result buffer cancelled time (unit: second)
CONF_mInt32(result_buffer_cancelled_interval_time, "300");

// the increased frequency of priority for remaining tasks in BlockingPriorityQueue
CONF_mInt32(priority_queue_remaining_tasks_increased_frequency, "512");

// sync tablet_meta when modifying meta
CONF_mBool(sync_tablet_meta, "false");

// default thrift rpc timeout ms
CONF_mInt32(thrift_rpc_timeout_ms, "5000");

// txn commit rpc timeout
CONF_mInt32(txn_commit_rpc_timeout_ms, "10000");

// If set to true, metric calculator will run
CONF_mBool(enable_metric_calculator, "true");

// max consumer num in one data consumer group, for routine load
CONF_mInt32(max_consumer_num_per_group, "3");

// the size of thread pool for routine load task.
// this should be larger than FE config 'max_concurrent_task_num_per_be' (default 5)
CONF_mInt32(routine_load_thread_pool_size, "10");

// Is set to true, index loading failure will not causing BE exit,
// and the tablet will be marked as bad, so that FE will try to repair it.
// CONF_mBool(auto_recover_index_loading_failure, "false");

// max external scan cache batch count, means cache max_memory_cache_batch_count * batch_size row
// default is 20, batch_size's default value is 1024 means 20 * 1024 rows will be cached
CONF_mInt32(max_memory_sink_batch_count, "20");

// This configuration is used for the context gc thread schedule period
// note: unit is minute, default is 5min
CONF_mInt32(scan_context_gc_interval_min, "5");

// es scroll keep-alive
CONF_String(es_scroll_keepalive, "5m");

// HTTP connection timeout for es
CONF_mInt32(es_http_timeout_ms, "5000");

// the max client cache number per each host
// There are variety of client cache in BE, but currently we use the
// same cache size configuration.
// TODO(cmy): use different config to set different client cache if necessary.
CONF_mInt32(max_client_cache_size_per_host, "10");

// Dir to save files downloaded by SmallFileMgr
CONF_String(small_file_dir, "${DORIS_HOME}/lib/small_file/");
// path gc
CONF_mBool(path_gc_check, "true");
CONF_mInt32(path_gc_check_interval_second, "86400");
CONF_mInt32(path_gc_check_step, "1000");
CONF_mInt32(path_gc_check_step_interval_ms, "10");
CONF_mInt32(path_scan_interval_second, "86400");

// The following 2 configs limit the max usage of disk capacity of a data dir.
// If both of these 2 threshold reached, no more data can be writen into that data dir.
// The percent of max used capacity of a data dir
CONF_mInt32(storage_flood_stage_usage_percent, "95"); // 95%
// The min bytes that should be left of a data dir
CONF_mInt64(storage_flood_stage_left_capacity_bytes, "1073741824"); // 1GB
// number of thread for flushing memtable per store
CONF_mInt32(flush_thread_num_per_store, "2");

// config for tablet meta checkpoint
CONF_mInt32(tablet_meta_checkpoint_min_new_rowsets_num, "10");
CONF_mInt32(tablet_meta_checkpoint_min_interval_secs, "600");

// config for default rowset type
// Valid configs: ALPHA, BETA
CONF_String(default_rowset_type, "BETA");

// Maximum size of a single message body in all protocols
CONF_mInt64(brpc_max_body_size, "209715200");
// Max unwritten bytes in each socket, if the limit is reached, Socket.Write fails with EOVERCROWDED
CONF_mInt64(brpc_socket_max_unwritten_bytes, "67108864");

// max number of txns for every txn_partition_map in txn manager
// this is a self protection to avoid too many txns saving in manager
CONF_mInt64(max_runnings_transactions_per_txn_map, "100");

// tablet_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage tablet
CONF_mInt32(tablet_map_shard_size, "1");

CONF_String(plugin_path, "${DORIS_HOME}/plugin");

// txn_map_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to manage txn
CONF_mInt32(txn_map_shard_size, "128");

// txn_lock shard size, the value is 2^n, n=0,1,2,3,4
// this is a an enhancement for better performance to commit and publish txn
CONF_mInt32(txn_shard_size, "1024");

// Whether to continue to start be when load tablet from header failed.
CONF_mBool(ignore_load_tablet_failure, "false");

// Whether to continue to start be when load tablet from header failed.
CONF_mBool(ignore_rowset_stale_unconsistent_delete, "false");

// Soft memory limit as a fraction of hard memory limit.
CONF_mDouble(soft_mem_limit_frac, "0.9");

// Set max cache's size of query results, the unit is M byte
CONF_mInt32(query_cache_max_size_mb, "256");

// Cache memory is pruned when reach query_cache_max_size_mb + query_cache_elasticity_size_mb
CONF_mInt32(query_cache_elasticity_size_mb, "128");

// Maximum number of cache partitions corresponding to a SQL
CONF_mInt32(query_cache_max_partition_count, "1024");

// Maximum number of version of a tablet. If the version num of a tablet exceed limit,
// the load process will reject new incoming load job of this tablet.
// This is to avoid too many version num.
CONF_mInt32(max_tablet_version_num, "500");

// Frontend mainly use two thrift sever type: THREAD_POOL, THREADED. if fe use THREADED model for thrift server,
// the thrift_server_type_of_fe should be set THREADED to make be thrift client to fe constructed with TFramedTransport
CONF_String(thrift_server_type_of_fe, "THREAD_POOL");

} // namespace config

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_CONFIG_H
