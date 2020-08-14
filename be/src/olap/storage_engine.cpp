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

#include "olap/storage_engine.h"

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

#include "env/env.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/lru_cache.h"
#include "olap/memtable_flush_executor.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/rowset/alpha_rowset.h"
#include "olap/schema_change.h"
#include "olap/data_dir.h"
#include "olap/utils.h"
#include "olap/rowset/alpha_rowset_meta.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/olap_snapshot_converter.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/fs/file_block_manager.h"
#include "util/time.h"
#include "util/trace.h"
#include "util/doris_metrics.h"
#include "util/pretty_printer.h"
#include "util/file_utils.h"
#include "util/scoped_cleanup.h"
#include "agent/cgroups_mgr.h"

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
using strings::Substitute;

namespace doris {

  /*
    整个BE是建立在以下2个⼦存储系统上来完成存储的管理工作的:
    文件系统:这个主要是底层的rowset读写⽂件需要这个，⽐如创建文件，读取⽂件，删除文件，拷贝⽂件等。这块考虑到未来我们可能用BOS作为我们的存储系统，所以这块我们做了一个抽象叫做 FsEnv。
    元数据存储系统:是底层存储tablet和rowset的元数据的⼀个系统，⽬前我们使用rocksdb实现，未来⽐如用BOS作为存储的时候这个可能也会变化，所以我们把这块也做了抽象叫做 MetaEnv。

    FsEnv和MetaEnv其实都跟底层的存储介质相关，并且它们经常需要一起使⽤，而且需要在多个对象之间传递，所以在FsEnv和MetaEnv之上我们构建了一个DataDir层，封装了FsEnv和MetaEnv，
    每个 DataDir都绑定了一个存储介质StorageMedium。data dir 并不仅仅封装了FsEnv和MetaEnv， 还封装了部分磁盘⽬录管理的逻辑，⽐如当前我们⼀个磁盘目录下最多允许1000个子目录,
    当我们要create⼀个新的tablet的时候，data dir就需要做⼀下这个⽬录的选择了,未来汇报存储状态的时候我们也直接让DataDir来汇报就可以了.
  */
  /*
  Rowset代表了一个或多个批次的数据（写入tablet中的不同批次的数据，每个批次是一个rowset），抽象它的原因是想以后能支持更多的文件存储格式。
  tablet是不不感知磁盘的，真正感知存储路路径的是rowset，每个rowset都应该有一个单独的磁盘目录，比如我们在做磁盘迁移的时候，一个tablet的数据可能跨越了多个磁盘，
  这样实际上就要求每个rowset有⾃己的存储⽬目录。rowset⼀旦生成了就应该是⼀个不可变的结构，理论上任何状态都不能改变，这样有利于控制我们系统的读写并发。rowset的元
  数据尽量量少⼀些，因为⼀个节点可能有几十万个tablet，每个tablet⼜可能有⼏百到几千个rowset，而rowset的元数据位于内存中，如果数据量太⼤内存可能爆掉。rowset的设
  计要满⾜我们磁盘拷⻉的需求，⽐如我们把⼀个磁盘的数据和元数据都拷⻉一下到另外⼀个磁盘上或者把磁盘目录变更一下，系统应该是能够加载的。所以不能直接在rowset meta中
  存储data dir。rowset的id在⼀个be内要唯一， 但是在不同的be上会有重复。rowset id只是⼀个唯⼀的标识，并不代表任何含义，两个BE上如果有两个rowset的id相同，他们的
  数据内容不一定一样。
*/

/***********************对成员变量_store_map的管理是StorageEngine类最重要的功能之一********************************/

StorageEngine* StorageEngine::_s_instance = nullptr;//静态成员变量只能在类外进行初始化

/*验证tablet将要存放的路径是否为空
        options的类型EngineOptions是一个结构体，存储的是tablet将要存放的路径以及BE的UUID
        （UUID是系统为BE节点自动生成的唯一标识，BE节点每次重启时，UUID会被重置）

        struct EngineOptions {
            // list paths that tablet will be put into.
            std::vector<StorePath> store_paths;
            // BE's UUID. It will be reset every time BE restarts.
            UniqueId backend_uid{0, 0};
        };
*/
static Status _validate_options(const EngineOptions& options) {
    if (options.store_paths.empty()) {
        return Status::InternalError("store paths is empty");;
    }
    return Status::OK();
}

/*打开存储引擎：根据teblet的options信息打开存储引擎，通过参数engine_ptr传回*/
Status StorageEngine::open(const EngineOptions& options, StorageEngine** engine_ptr) {
    RETURN_IF_ERROR(_validate_options(options));//验证tablet将要存放的路径是否为空
    LOG(INFO) << "starting backend using uid:" << options.backend_uid.to_string();
    std::unique_ptr<StorageEngine> engine(new StorageEngine(options));//创建一个StorageEngine对象
    RETURN_NOT_OK_STATUS_WITH_WARN(engine->_open(), "open engine failed");//调用了_open()函数来打开存储引擎
    *engine_ptr = engine.release();//unique_ptr的release()函数：放弃对它所指对象的控制权，并返回保存的指针
    LOG(INFO) << "success to init storage engine.";
    return Status::OK();
}

/*StorageEngine的构造函数*/
StorageEngine::StorageEngine(const EngineOptions& options)
        : _options(options),
        _available_storage_medium_type_count(0),
        _effective_cluster_id(-1),
        _is_all_cluster_id_exist(true),
        _index_stream_lru_cache(NULL),
        _file_cache(nullptr),
        _tablet_manager(new TabletManager(config::tablet_map_shard_size)),//  common/config.h中定义了CONF_Int32(tablet_map_shard_size, "1");
        _txn_manager(new TxnManager(config::txn_map_shard_size, config::txn_shard_size)),//  common/config.h中定义了CONF_Int32(txn_shard_size, "1024");
        _rowset_id_generator(new UniqueRowsetIdGenerator(options.backend_uid)),//RowsetIdGenerator对象负责生成rowset id
        _memtable_flush_executor(nullptr),//MemTableFlushExecutor用来负责将memtable刷写到磁盘上
        _block_manager(nullptr),//BlockManager对象用来进行块的生命周期管理
        _default_rowset_type(ALPHA_ROWSET),//初始化默认的rowset类型
        _heartbeat_flags(nullptr) //HeartbeatFlags对象负责从FE和BE之间的heartbeat消息中解析控制标识（flag）
        {
    if (_s_instance == nullptr) {
        _s_instance = this;
    }

    REGISTER_GAUGE_DORIS_METRIC(unused_rowsets_count, [this]() {//？？？？？什么意思？
        MutexLock lock(&_gc_mutex);
        return _unused_rowsets.size();
    });
}

/*StorageEngine的析构函数*/
StorageEngine::~StorageEngine() {
    _clear();
}

/*加载已经存在的data dir路径，并从中加载tablet*/
void StorageEngine::load_data_dirs(const std::vector<DataDir*>& data_dirs) {
    std::vector<std::thread> threads;
    for (auto data_dir : data_dirs) {

        threads.emplace_back([data_dir] {//Ｃ++11中的vector使用emplace_back代替push_back
            auto res = data_dir->load();//从meta和data文件中加载数据，包括从meta中加载rowset，从meta中加载tablet【TabletMetaManager::traverse_headers(_meta, load_tablet_func)】
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "io error when init load tables. res=" << res
                    << ", data dir=" << data_dir->path();
                    // TODO(lingbin): why not exit progress, to force OP to change the conf
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
}

/*打开存储引擎*/
Status StorageEngine::_open() {
    // init store_map
    RETURN_NOT_OK_STATUS_WITH_WARN(_init_store_map(), "_init_store_map failed");//初始化store map,将tablet存储的路径保存在成员变量_store_map中，其中store_map的类型为std::map<std::string, DataDir*>

    _effective_cluster_id = config::cluster_id;// common/config.h文件中定义了CONF_Int32(cluster_id, "-1");
    RETURN_NOT_OK_STATUS_WITH_WARN(_check_all_root_path_cluster_id(), "fail to check cluster id");//检查所有root path的cluster id来获取有效的cluster id，并保存在成员变量_effective_cluster_id中

    _update_storage_medium_type_count();//更新可用的存储介质类型的数量

    RETURN_NOT_OK_STATUS_WITH_WARN(_check_file_descriptor_number(), "check fd number failed");//检查文件描述器的数量

    _index_stream_lru_cache = new_lru_cache(config::index_stream_cache_capacity);//创建特定容量大小的cache供index_stream使用，该cache中使用LRU（最近最少使用）置换算法

    _file_cache.reset(new_lru_cache(config::file_descriptor_cache_capacity));//创建特定容量大小的cache供文件描述器使用，该cache中使用LRU（最近最少使用）置换算法

    auto dirs = get_stores<false>();//根据store_map的内容获取所有的存储路径data dir，不包含未使用的路径
    load_data_dirs(dirs);//*****************************从存储路径中加载tablet*******************************

    _memtable_flush_executor.reset(new MemTableFlushExecutor());//复位memtable的刷写执行器。MemTableFlushExecutor负责将memtable刷写到磁盘上，其中包含了一个线程池来处理所有任务。

    /*MemTableFlushExecutor需要在存储引擎打开之后被初始化，因为需要每一个data dir的路径hash。
      对于每一个tablet，都会有多个memtable，这些memtable需要按照产生的顺序一个接一个地刷写到磁
      盘上，如果有一个memtable刷写失败，那么会拒绝后续的所有memtable提交刷写，对于已经提交的
      memtable也不需要执行刷写，因为此次作业注定会失败。*/
    _memtable_flush_executor->init(dirs);//初始化memtable的刷写执行器时，会对刷写线程的最大和最小数目进行设定

    /*block是文件系统支持的最小数据单位。block接口反映了Doris磁盘存储设计原理：
      block仅能追加；block一旦写入便是不可变的；对block的读操作是线程安全的，并且可以被多个线程同时读；对block的写操作不是线程安全的。*/
    fs::BlockManagerOptions bm_opts;
    bm_opts.read_only = false;
    _block_manager.reset(new fs::FileBlockManager(Env::Default(), std::move(bm_opts)));

    _parse_default_rowset_type();//解析默认的rowset类型，有两种：ALPHA_ROWSET和BETA_ROWSET,其中ALPHA_ROWSET=0表示doris原有的列存，BETA_ROWSET=1表示新列存

    return Status::OK();
}

/*根据data dir初始化store map,将tablet的存储路径保存在成员变量_store_map中,其中store_map的类型为std::map<std::string, DataDir*>。
  data dir保存在成员变量_options中，结构体成员变量_options通过StorageEngine的构造函数传入*/
Status StorageEngine::_init_store_map() {
    std::vector<DataDir*> tmp_stores;
    std::vector<std::thread> threads;
    SpinLock error_msg_lock;
    std::string error_msg;
    for (auto& path : _options.store_paths) {//遍历options中保存的所有data dir的路径
        DataDir* store = new DataDir(path.path, path.capacity_bytes, path.storage_medium, _tablet_manager.get(), _txn_manager.get());//根据path创建DataDir对象
        tmp_stores.emplace_back(store);//将DataDir对象添加到vector类型的tmp_stores中
        threads.emplace_back([store, &error_msg_lock, &error_msg]() {
            auto st = store->init();
            if (!st.ok()) {
                {
                    std::lock_guard<SpinLock> l(error_msg_lock);
                    error_msg.append(st.to_string() + ";");
                }
                LOG(WARNING) << "Store load failed, status=" << st.to_string() << ", path=" << store->path();
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }

    if (!error_msg.empty()) {
        for (auto store : tmp_stores) {
            delete store;
        }
        return Status::InternalError(Substitute("init path failed, error=$0", error_msg));
    }

    for (auto store : tmp_stores) {
        _store_map.emplace(store->path(), store);//将tablet的存储路径保存在成员变量_store_map
    }
    return Status::OK();
}

/*更新可用的存储介质类型的数量*/
void StorageEngine::_update_storage_medium_type_count() {
    set<TStorageMedium::type> available_storage_medium_types;

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {//遍历成员变量_store_map中的所有data dir
        if (it.second->is_used()) {//判断磁盘是否可用
            available_storage_medium_types.insert(it.second->storage_medium());
        }
    }

    _available_storage_medium_type_count = available_storage_medium_types.size();
}

/*判断和更新有效的cluster id*/
Status StorageEngine::_judge_and_update_effective_cluster_id(int32_t cluster_id) {
    if (cluster_id == -1 && _effective_cluster_id == -1) {
        // maybe this is a new cluster, cluster id will get from heartbeat message
        return Status::OK();
    } else if (cluster_id != -1 && _effective_cluster_id == -1) {
        _effective_cluster_id = cluster_id;
        return Status::OK();
    } else if (cluster_id == -1 && _effective_cluster_id != -1) {
        // _effective_cluster_id is the right effective cluster id
        return Status::OK();
    } else {
        if (cluster_id != _effective_cluster_id) {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::Corruption(Substitute("multiple cluster ids is not equal. one=$0, other=", _effective_cluster_id, cluster_id)), "cluster id not equal");
        }
    }

    return Status::OK();
}

/*对指定的data dir是否可用的标志进行设置*/
void StorageEngine::set_store_used_flag(const string& path, bool is_used) {
    std::lock_guard<std::mutex> l(_store_lock);
    auto it = _store_map.find(path);
    if (it == _store_map.end()) {
        LOG(WARNING) << "store not exist, path=" << path;
    }

    it->second->set_is_used(is_used);//对指定的data dir是否可用的标志进行设置
    _update_storage_medium_type_count();//更新可用的存储介质类型的数量
}

/*根据store_map的内容获取所有的存储路径data dir*/
template<bool include_unused>
std::vector<DataDir*> StorageEngine::get_stores() {
    std::vector<DataDir*> stores;
    stores.reserve(_store_map.size());//根据store_map的大小为vector类型的变量stores分配存储空间

    std::lock_guard<std::mutex> l(_store_lock);
    if (include_unused) {//是否包含不可用的data dir
        for (auto& it : _store_map) {
            stores.push_back(it.second);
        }
    } else {
        for (auto& it : _store_map) {
            if (it.second->is_used()) {
                stores.push_back(it.second);
            }
        }
    }
    return stores;
}

template std::vector<DataDir*> StorageEngine::get_stores<false>();//根据store_map的内容获取所有的存储路径data dir，不包含不可用的data dir
template std::vector<DataDir*> StorageEngine::get_stores<true>(); //根据store_map的内容获取所有的存储路径data dir，包含不可用的data dir

/*获取所有data dir的信息，通过参数data_dir_infos返回*/
OLAPStatus StorageEngine::get_all_data_dir_info(vector<DataDirInfo>* data_dir_infos, bool need_update) {
    /*  结构体DataDirInfo在olap_common.h中进行了定义：
        struct DataDirInfo {
            DataDirInfo(): path_hash(0), disk_capacity(1), available(0), ata_used_capacity(0), is_used(false) { }

            std::string path;                  //路径
            size_t path_hash;                  //路径hash
            int64_t disk_capacity;             // actual disk capacity，磁盘容量
            int64_t available;                 // 可用空间，单位字节
            int64_t data_used_capacity;        //已经占用的空间容量
            bool is_used;                       // 是否可用标识
            TStorageMedium::type storage_medium;  // 存储介质类型：SSD|HDD
        };
    */
    OLAPStatus res = OLAP_SUCCESS;
    data_dir_infos->clear();//清空存储DataDirInfo的vector

    /*系统中的时间分为Monotonic clock和Wall clock两种。
    Monotonic clock是单调时间，指的是从某个点开始后（比如系统启动以后）流逝的时间，由变量jiffies来记录，一定是单调递增的。系统每次启动时，
    jiffies初始化为0，每来一个timer interrupt，jiffies加1，也就是说它代表系统启动后流逝的tick数。
    wall clock是挂钟时间，指的是现实的时间，由变量xtime来记录。系统每次启动时将CMOS上的RTC时间读入xtime，这个值是"自1970-01-01起经历的秒数、本秒中经历的纳秒数"，
    每来一个timer interrupt，也需要去更新xtime。
    特别要强调的是计算两个时间点的差值一定要用Monotonic clock，因为Wall clock是可以被修改的，比如计算机时间被回拨（比如校准或者人工回拨等情况），或者闰秒（ leap second）
    会导致两个wall clock可能出现负数。*/
    MonotonicStopWatch timer;
    timer.start();//起始时间

    // 1. update available capacity of each data dir get all root path info and construct a path map.     path -> DataDirInfo
    std::map<std::string, DataDirInfo> path_map;//保存每一个data dir的信息
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {//遍历_store_map中所有的data dir
            if (need_update) {//根据传入的参数need_update判断是否需要更新对应data dir的可用容量
                it.second->update_capacity();//调用DataDir类中的update_capacity()函数来更新对应data dir的可用容量
            }
            path_map.emplace(it.first, it.second->get_dir_info());//<路径，data dir信息>
        }
    }

    // 2. get total tablets' size of each data dir
    size_t tablet_count = 0;
    _tablet_manager->update_root_path_info(&path_map, &tablet_count);//更新每一个data dir上被所有tablet占用的容量

    // add path info to data_dir_infos
    for (auto& entry : path_map) {
        data_dir_infos->emplace_back(entry.second);//将每一个data dir的信息添加到data_dir_infos变量中
    }

    timer.stop();//结束时间
    LOG(INFO) << "get root path info cost: " << timer.elapsed_time() / 1000000
            << " ms. tablet counter: " << tablet_count;

    return res;
}

/*开始磁盘状态监控*/
void StorageEngine::_start_disk_stat_monitor() {
    for (auto& it : _store_map) {//遍历所有的data dir
        it.second->health_check();//磁盘健康检查，判断磁盘是否可用以及读写测试文件是否成功。如果读写测试文件失败，会将磁盘标记为不可用。
    }

    _update_storage_medium_type_count();//更新可用的存储介质类型的数量
    bool some_tablets_were_dropped = _delete_tablets_on_unused_root_path();//删除不可用磁盘上的tablet
    // If some tablets were dropped, we should notify disk_state_worker_thread and
    // tablet_worker_thread (see TaskWorkerPool) to make them report to FE ASAP.
    if (some_tablets_were_dropped) {
        trigger_report();//触发报告，通知所有线程
    }
}

/*检查文件描述器的数量*/
// TODO(lingbin): Should be in EnvPosix?
Status StorageEngine::_check_file_descriptor_number() {
    struct rlimit l;
    int ret = getrlimit(RLIMIT_NOFILE , &l);//getrlimit()为进程资源限制函数，每个进程都有一组资源限制，限制进程对于系统资源的申请量，成功执行时，返回0；失败返回-1。RLIMIT_NOFILE指每个进程能打开的最大文件数目。
    if (ret != 0) {
        LOG(WARNING) << "call getrlimit() failed. errno=" << strerror(errno)
                     << ", use default configuration instead.";
        return Status::OK();
    }
    if (l.rlim_cur < config::min_file_descriptor_number) {
        LOG(ERROR) << "File descriptor number is less than " << config::min_file_descriptor_number
                   << ". Please use (ulimit -n) to set a value equal or greater than "
                   << config::min_file_descriptor_number;
        return Status::InternalError("file descriptors limit is too small");
    }
    return Status::OK();
}

/*检查所有root path的cluster id，来获取有效的cluster id，并保存在成员变量_effective_cluster_id中*/
Status StorageEngine::_check_all_root_path_cluster_id() {
    int32_t cluster_id = -1;
    for (auto& it : _store_map) {//遍历所有的data dir
        int32_t tmp_cluster_id = it.second->cluster_id();//获取当前data dir的cluster id
        if (tmp_cluster_id == -1) {
            _is_all_cluster_id_exist = false;
        } else if (tmp_cluster_id == cluster_id) {
            // both hava right cluster id, do nothing
        } else if (cluster_id == -1) {
            cluster_id = tmp_cluster_id;
        } else {
            RETURN_NOT_OK_STATUS_WITH_WARN(
                Status::Corruption(Substitute("multiple cluster ids is not equal. one=$0, other=", cluster_id, tmp_cluster_id)), "cluster id not equal");
        }
    }

    // judge and get effective cluster id
    RETURN_IF_ERROR(_judge_and_update_effective_cluster_id(cluster_id));//对成员变量_effective_cluster_id进行更新

    // write cluster id into cluster_id_path if get effective cluster id success
    if (_effective_cluster_id != -1 && !_is_all_cluster_id_exist) {
        set_cluster_id(_effective_cluster_id);
    }

    return Status::OK();
}

/*设置成员变量_effective_cluster_id的值*/
Status StorageEngine::set_cluster_id(int32_t cluster_id) {
    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& it : _store_map) {
        RETURN_IF_ERROR(it.second->set_cluster_id(cluster_id));
    }
    _effective_cluster_id = cluster_id;
    _is_all_cluster_id_exist = true;
    return Status::OK();
}

/*获取可以创建teblet的data dir*/
std::vector<DataDir*> StorageEngine::get_stores_for_create_tablet(TStorageMedium::type storage_medium) {
    std::vector<DataDir*> stores;
    {
        std::lock_guard<std::mutex> l(_store_lock);
        for (auto& it : _store_map) {//遍历所有的data dir
            if (it.second->is_used()) {//判断当前磁盘是否可用
                //如果当前节点只有一种类型的存储介质（磁盘），则忽略参数传入的存储介质类型，直接在当前类型存在的存储介质上创建tablet；如果当前节点有多种类型的存储介质（磁盘），则创建tablet的磁盘类型需要和参数传入的存储介质类型一致。
                if (_available_storage_medium_type_count == 1 || it.second->storage_medium() == storage_medium) {
                    stores.push_back(it.second);
                }
            }
        }
    }
    //  TODO(lingbin): should it be a global util func?
    std::random_device rd;//标准库提供了一个非确定性随机数生成设备.在Linux的实现中,是读取/dev/urandom设备
    srand(rd());
    std::random_shuffle(stores.begin(), stores.end());//随机打乱vector中data dir的顺序（创建tablet时，会从向量stores中的第一个磁盘开始尝试，直到创建成功。此处打乱stores中磁盘的顺序，目的是创建tablet时让磁盘的选择是随机的）
    // Two random choices
    for (int i = 0; i < stores.size(); i++) {
        int j = i + 1;
        if (j < stores.size()) {
            if (stores[i]->tablet_set().size() > stores[j]->tablet_set().size()) {
                std::swap(stores[i], stores[j]);
            }
            std::random_shuffle(stores.begin() + j, stores.end());
        } else {
            break;
        }
    }
    return stores;
}

/*在_store_map中根据path获取data dir*/
DataDir* StorageEngine::get_store(const std::string& path) {
    // _store_map is unchanged, no need to lock
    auto it = _store_map.find(path);
    if (it == std::end(_store_map)) {
        return nullptr;
    }
    return it->second;
}

/*判断是否有太多的磁盘不可用。当磁盘总数为0，或不可用磁盘数量占磁盘总数的比例超过特定值时，返回true。*/
static bool too_many_disks_are_failed(uint32_t unused_num, uint32_t total_num) {
    return ((total_num == 0)
            || (unused_num * 100 / total_num > config::max_percentage_of_error_disk));//在commom/config.h文件中有配置：CONF_mInt32(max_percentage_of_error_disk, "0");
}

/*删除不可用磁盘上的tablet*/
bool StorageEngine::_delete_tablets_on_unused_root_path() {
    /*TabletInfo是olap_common.h中定义的一个结构体类型，其中包括：
        TTabletId tablet_id;
        TSchemaHash schema_hash;
        UniqueId tablet_uid;
      等信息。*/
    vector<TabletInfo> tablet_info_vec;
    uint32_t unused_root_path_num = 0;
    uint32_t total_root_path_num = 0;

    std::lock_guard<std::mutex> l(_store_lock);
    if (_store_map.size() == 0) {
        return false;
    }

    for (auto& it : _store_map) {//遍历_store_map上所有的data dir
        ++total_root_path_num;
        if (it.second->is_used()) {//依次判断每一个磁盘是否可用，如果可用则继续下一个
            continue;
        }
        //当前磁盘不可用
        it.second->clear_tablets(&tablet_info_vec);//获取不可用磁盘上的tablet信息
        ++unused_root_path_num;
    }

    if (too_many_disks_are_failed(unused_root_path_num, total_root_path_num)) {//如果不可用磁盘太多（磁盘总数为0或不可用磁盘数量占磁盘总数的比例超过特定值），BE进程会直接退出
        LOG(FATAL) << "meet too many error disks, process exit. "
                   << "max_ratio_allowed=" << config::max_percentage_of_error_disk << "%"
                   << ", error_disk_count=" << unused_root_path_num
                   << ", total_disk_count=" << total_root_path_num;
        exit(0);
    }

    _tablet_manager->drop_tablets_on_error_root_path(tablet_info_vec);//通过tablet manager清除不可用磁盘上的tablet
    // If tablet_info_vec is not empty, means we have dropped some tablets.
    return !tablet_info_vec.empty();
}

/*析构函数中会被调用，主要用来安全删除_index_stream_lru_cache，复位文件cache，停止磁盘相关的背景工作，清空store map等等*/
void StorageEngine::_clear() {
    SAFE_DELETE(_index_stream_lru_cache);
    _file_cache.reset();

    std::lock_guard<std::mutex> l(_store_lock);
    for (auto& store_pair : _store_map) {//遍历所有的data dir
        store_pair.second->stop_bg_worker();//停止当前磁盘相关的背景工作
        delete store_pair.second;//删除当前磁盘
        store_pair.second = nullptr;
    }
    _store_map.clear();//清空成员变量_store_map

    _stop_bg_worker = true;
}

/*清除事务任务*/
void StorageEngine::clear_transaction_task(const TTransactionId transaction_id) {
    // clear transaction task may not contains partitions ids, we should get partition id from txn manager.
    std::vector<int64_t> partition_ids;
    StorageEngine::instance()->txn_manager()->get_partition_ids(transaction_id, &partition_ids);
    clear_transaction_task(transaction_id, partition_ids);
}

void StorageEngine::clear_transaction_task(const TTransactionId transaction_id, const vector<TPartitionId>& partition_ids) {
    LOG(INFO) << "begin to clear transaction task. transaction_id=" <<  transaction_id;

    for (const TPartitionId& partition_id : partition_ids) {
        std::map<TabletInfo, RowsetSharedPtr> tablet_infos;
        StorageEngine::instance()->txn_manager()->get_txn_related_tablets(transaction_id, partition_id, &tablet_infos);

        // each tablet
        for (auto& tablet_info : tablet_infos) {
            // should use tablet uid to ensure clean txn correctly
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.first.tablet_id, tablet_info.first.schema_hash, tablet_info.first.tablet_uid);
            // The tablet may be dropped or altered, leave a INFO log and go on process other tablet
            if (tablet == nullptr) {
                LOG(INFO) << "tablet is no longer exist. tablet_id=" << tablet_info.first.tablet_id
                    << ", schema_hash=" << tablet_info.first.schema_hash
                    << ", tablet_uid=" << tablet_info.first.tablet_uid;
                continue;
            }
            StorageEngine::instance()->txn_manager()->delete_txn(partition_id, tablet, transaction_id);
        }
    }
    LOG(INFO) << "finish to clear transaction task. transaction_id=" << transaction_id;
}

/*清除文件描述器cache*/
void StorageEngine::_start_clean_fd_cache() {
    VLOG(10) << "start clean file descritpor cache";
    _file_cache->prune();
    VLOG(10) << "end clean file descritpor cache";
}

/*执行cumulative compaction*/
void StorageEngine::_perform_cumulative_compaction(DataDir* data_dir) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1e9 > config::cumulative_compaction_trace_threshold) {
            LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
    TRACE("start to perform cumulative compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(
            CompactionType::CUMULATIVE_COMPACTION, data_dir);
    if (best_tablet == nullptr) {
        return;
    }
    TRACE("found best tablet $0", best_tablet->get_tablet_info().tablet_id);

    DorisMetrics::instance()->cumulative_compaction_request_total.increment(1);
    CumulativeCompaction cumulative_compaction(best_tablet);

    OLAPStatus res = cumulative_compaction.compact();
    if (res != OLAP_SUCCESS) {
        best_tablet->set_last_cumu_compaction_failure_time(UnixMillis());
        if (res != OLAP_ERR_CUMULATIVE_NO_SUITABLE_VERSIONS) {
            DorisMetrics::instance()->cumulative_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to do cumulative compaction. res=" << res
                        << ", table=" << best_tablet->full_name();
        }
        return;
    }
    best_tablet->set_last_cumu_compaction_failure_time(0);
}

/*执行base compaction*/
void StorageEngine::_perform_base_compaction(DataDir* data_dir) {
    scoped_refptr<Trace> trace(new Trace);
    MonotonicStopWatch watch;
    watch.start();
    SCOPED_CLEANUP({
        if (watch.elapsed_time() / 1e9 > config::base_compaction_trace_threshold) {
            LOG(WARNING) << "Trace:" << std::endl << trace->DumpToString(Trace::INCLUDE_ALL);
        }
    });
    ADOPT_TRACE(trace.get());
    TRACE("start to perform base compaction");
    TabletSharedPtr best_tablet = _tablet_manager->find_best_tablet_to_compaction(
            CompactionType::BASE_COMPACTION, data_dir);
    if (best_tablet == nullptr) {
        return;
    }
    TRACE("found best tablet $0", best_tablet->get_tablet_info().tablet_id);

    DorisMetrics::instance()->base_compaction_request_total.increment(1);
    BaseCompaction base_compaction(best_tablet);
    OLAPStatus res = base_compaction.compact();
    if (res != OLAP_SUCCESS) {
        best_tablet->set_last_base_compaction_failure_time(UnixMillis());
        if (res != OLAP_ERR_BE_NO_SUITABLE_VERSION) {
            DorisMetrics::instance()->base_compaction_request_failed.increment(1);
            LOG(WARNING) << "failed to init base compaction. res=" << res
                        << ", table=" << best_tablet->full_name();
        }
        return;
    }
    best_tablet->set_last_base_compaction_failure_time(0);
}

/*获取cache的状态信息*/
void StorageEngine::get_cache_status(rapidjson::Document* document) const {
    return _index_stream_lru_cache->get_cache_status(document);
}

/*开始清理trash和snapshot文件，返回清理后的磁盘使用量*/
OLAPStatus StorageEngine::_start_trash_sweep(double* usage) {
    OLAPStatus res = OLAP_SUCCESS;
    LOG(INFO) << "start trash and snapshot sweep.";

    const int32_t snapshot_expire = config::snapshot_expire_time_sec;
    const int32_t trash_expire = config::trash_file_expire_time_sec;
    const double guard_space = config::storage_flood_stage_usage_percent / 100.0;
    std::vector<DataDirInfo> data_dir_infos;
    RETURN_NOT_OK_LOG(get_all_data_dir_info(&data_dir_infos, false),
                      "failed to get root path stat info when sweep trash.")

    time_t now = time(nullptr); //获取UTC时间
    tm local_tm_now;
    if (localtime_r(&now, &local_tm_now) == nullptr) {
        LOG(WARNING) << "fail to localtime_r time. time=" << now;
        return OLAP_ERR_OS_ERROR;
    }
    const time_t local_now = mktime(&local_tm_now); //得到当地日历时间

    for (DataDirInfo& info : data_dir_infos) {
        if (!info.is_used) {
            continue;
        }

        double curr_usage = (double) (info.disk_capacity - info.available) / info.disk_capacity;
        *usage = *usage > curr_usage ? *usage : curr_usage;

        OLAPStatus curr_res = OLAP_SUCCESS;
        string snapshot_path = info.path + SNAPSHOT_PREFIX;
        curr_res = _do_sweep(snapshot_path, local_now, snapshot_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep snapshot. path=" << snapshot_path
                    << ", err_code=" << curr_res;
            res = curr_res;
        }

        string trash_path = info.path + TRASH_PREFIX;
        curr_res = _do_sweep(trash_path, local_now,
                curr_usage > guard_space ? 0 : trash_expire);
        if (curr_res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to sweep trash. [path=%s" << trash_path
                    << ", err_code=" << curr_res;
            res = curr_res;
        }
    }

    // clear expire incremental rowset, move deleted tablet to trash
    _tablet_manager->start_trash_sweep();//清除到期的rowset，将已经删除的tablet移到垃圾中去

    // clean rubbish transactions
    _clean_unused_txns();//清除不可用的垃圾事务

    // clean unused rowset metas in OlapMeta
    _clean_unused_rowset_metas();//清除不可用的rowset meta

    return res;
}

/*清理不可用的rowset meta信息*/
void StorageEngine::_clean_unused_rowset_metas() {
    std::vector<RowsetMetaSharedPtr> invalid_rowset_metas;
    auto clean_rowset_func = [this, &invalid_rowset_metas](TabletUid tablet_uid, RowsetId rowset_id,
        const std::string& meta_str) -> bool {

        RowsetMetaSharedPtr rowset_meta(new AlphaRowsetMeta());
        bool parsed = rowset_meta->init(meta_str);
        if (!parsed) {
            LOG(WARNING) << "parse rowset meta string failed for rowset_id:" << rowset_id;
            // return false will break meta iterator, return true to skip this error
            return true;
        }
        if (rowset_meta->tablet_uid() != tablet_uid) {
            LOG(WARNING) << "tablet uid is not equal, skip the rowset"
                         << ", rowset_id=" << rowset_meta->rowset_id()
                         << ", in_put_tablet_uid=" << tablet_uid
                         << ", tablet_uid in rowset meta=" << rowset_meta->tablet_uid();
            return true;
        }

        TabletSharedPtr tablet = _tablet_manager->get_tablet(rowset_meta->tablet_id(), rowset_meta->tablet_schema_hash(), tablet_uid);
        if (tablet == nullptr) {
            return true;
        }
        if (rowset_meta->rowset_state() == RowsetStatePB::VISIBLE && (!tablet->rowset_meta_is_useful(rowset_meta))) {
            LOG(INFO) << "rowset meta is useless any more, remote it. rowset_id=" << rowset_meta->rowset_id();
            invalid_rowset_metas.push_back(rowset_meta);
        }
        return true;
    };
    auto data_dirs = get_stores();
    for (auto data_dir : data_dirs) {
        RowsetMetaManager::traverse_rowset_metas(data_dir->get_meta(), clean_rowset_func);
        for (auto& rowset_meta : invalid_rowset_metas) {
            RowsetMetaManager::remove(data_dir->get_meta(), rowset_meta->tablet_uid(), rowset_meta->rowset_id());
        }
        invalid_rowset_metas.clear();
    }
}

/*清除不可用的txn（txn是transaction的简称）*/
void StorageEngine::_clean_unused_txns() {
    std::set<TabletInfo> tablet_infos;
    _txn_manager->get_all_related_tablets(&tablet_infos);
    for (auto& tablet_info : tablet_infos) {
        TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash, tablet_info.tablet_uid, true);
        if (tablet == nullptr) {
            // TODO(ygl) :  should check if tablet still in meta, it's a improvement
            // case 1: tablet still in meta, just remove from memory
            // case 2: tablet not in meta store, remove rowset from meta
            // currently just remove them from memory
            // nullptr to indicate not remove them from meta store
            _txn_manager->force_rollback_tablet_related_txns(nullptr, tablet_info.tablet_id, tablet_info.schema_hash,
                tablet_info.tablet_uid);
        }
    }
}

/*垃圾清除*/
OLAPStatus StorageEngine::_do_sweep(
        const string& scan_root, const time_t& local_now, const int32_t expire) {
    OLAPStatus res = OLAP_SUCCESS;
    if (!FileUtils::check_exist(scan_root)) {
        // dir not existed. no need to sweep trash.
        return res;
    }

    try {
        path boost_scan_root(scan_root);
        directory_iterator item(boost_scan_root);
        directory_iterator item_end;
        for (; item != item_end; ++item) {
            string path_name = item->path().string();
            string dir_name = item->path().filename().string();
            string str_time = dir_name.substr(0, dir_name.find('.'));
            tm local_tm_create;
            if (strptime(str_time.c_str(), "%Y%m%d%H%M%S", &local_tm_create) == nullptr) {
                LOG(WARNING) << "fail to strptime time. [time=" << str_time << "]";
                res = OLAP_ERR_OS_ERROR;
                continue;
            }

            int32_t actual_expire = expire;
            // try get timeout in dir name, the old snapshot dir does not contain timeout
            // eg: 20190818221123.3.86400, the 86400 is timeout, in second
            size_t pos = dir_name.find('.', str_time.size() + 1);
            if (pos != string::npos) {
                actual_expire = std::stoi(dir_name.substr(pos + 1));
            }
            VLOG(10) << "get actual expire time " << actual_expire << " of dir: " << dir_name;

            if (difftime(local_now, mktime(&local_tm_create)) >= actual_expire) {
                Status ret = FileUtils::remove_all(path_name);
                if (!ret.ok()) {
                    LOG(WARNING) << "fail to remove file or directory. path=" << path_name
                                 << ", error=" << ret.to_string();
                    res = OLAP_ERR_OS_ERROR;
                    continue;
                }
            }
        }
    } catch (...) {
        LOG(WARNING) << "Exception occur when scan directory. path=" << scan_root;
        res = OLAP_ERR_IO_ERROR;
    }

    return res;
}

/*解析默认的rowset类型*/
// invalid rowset type config will return ALPHA_ROWSET for system to run smoothly
void StorageEngine::_parse_default_rowset_type() {
    std::string default_rowset_type_config = config::default_rowset_type;
    boost::to_upper(default_rowset_type_config);
    if (default_rowset_type_config == "BETA") {
        _default_rowset_type = BETA_ROWSET;
    } else {
        _default_rowset_type = ALPHA_ROWSET;
    }
}

/*删除不可用的rowset*/
void StorageEngine::start_delete_unused_rowset() {                  //？？？什么是不可用的rowset？？？
    MutexLock lock(&_gc_mutex);
    for (auto it = _unused_rowsets.begin(); it != _unused_rowsets.end();) {
        if (it->second.use_count() != 1) {
            ++it;
        } else if (it->second->need_delete_file()) {
            VLOG(3) << "start to remove rowset:" << it->second->rowset_id()
                    << ", version:" << it->second->version().first << "-"
                    << it->second->version().second;
            OLAPStatus status = it->second->remove();//删除rowset
            VLOG(3) << "remove rowset:" << it->second->rowset_id()
                    << " finished. status:" << status;
            it = _unused_rowsets.erase(it);
        }
    }
}

/*将不可用的rowset添加到成员变量_unused_rowsets中*/
void StorageEngine::add_unused_rowset(RowsetSharedPtr rowset) {
    if (rowset == nullptr) {
        return;
    }

    VLOG(3) << "add unused rowset, rowset id:" << rowset->rowset_id()
            << ", version:" << rowset->version().first << "-" << rowset->version().second
            << ", unique id:" << rowset->unique_id();

    auto rowset_id = rowset->rowset_id().to_string();//获取rowset id

    MutexLock lock(&_gc_mutex);
    auto it = _unused_rowsets.find(rowset_id);//在map类型的成员变量_unused_rowsets中查找rowset id是否存在
    if (it == _unused_rowsets.end()) {//如果不存在，需要将其添加到_unused_rowsets
        rowset->set_need_delete_file();//设置需要删除文件，将Rowset类中的成员函数_need_delete_file设为true
        rowset->close();//清除rowset拥有的资源，包括打开的文件，索引等等
        _unused_rowsets[rowset_id] = rowset;//将rowset id添加到成员变量_unused_rowsets
        release_rowset_id(rowset->rowset_id());//释放rowset id，从可用的rowset列表中删除当前rowset id
    }
}

/*创建tablet*/
// TODO(zc): refactor this funciton
OLAPStatus StorageEngine::create_tablet(const TCreateTabletReq& request) { //以字符“T”开头的请求类表示该类的对象是来自Thrift的请求
    // Get all available stores, use ref_root_path if the caller specified
    std::vector<DataDir*> stores;
    stores = get_stores_for_create_tablet(request.storage_medium);//获取当前节点上所有可用于创建tablet的磁盘
    if (stores.empty()) {
        LOG(WARNING) << "there is no available disk that can be used to create tablet.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }
    return _tablet_manager->create_tablet(request, stores);//调用tablet manager来创建tablet
}

/*将tablet恢复到特定的版本*/
OLAPStatus StorageEngine::recover_tablet_until_specfic_version(const TRecoverTabletReq& recover_tablet_req) {
    TabletSharedPtr tablet = _tablet_manager->get_tablet(recover_tablet_req.tablet_id,
                                   recover_tablet_req.schema_hash);
    if (tablet == nullptr) { return OLAP_ERR_TABLE_NOT_FOUND; }
    RETURN_NOT_OK(tablet->recover_tablet_until_specfic_version(recover_tablet_req.version,
                                                        recover_tablet_req.version_hash));
    return OLAP_SUCCESS;
}

/*获取shard路径*/
OLAPStatus StorageEngine::obtain_shard_path(TStorageMedium::type storage_medium, std::string* shard_path, DataDir** store) {
    LOG(INFO) << "begin to process obtain root path. storage_medium=" << storage_medium;

    if (shard_path == NULL) {
        LOG(WARNING) << "invalid output parameter which is null pointer.";
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    auto stores = get_stores_for_create_tablet(storage_medium);
    if (stores.empty()) {
        LOG(WARNING) << "no available disk can be used to create tablet.";
        return OLAP_ERR_NO_AVAILABLE_ROOT_PATH;
    }

    OLAPStatus res = OLAP_SUCCESS;
    uint64_t shard = 0;
    res = stores[0]->get_shard(&shard);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to get root path shard. res=" << res;
        return res;
    }

    stringstream root_path_stream;
    root_path_stream << stores[0]->path() << DATA_PREFIX << "/" << shard;
    *shard_path = root_path_stream.str();
    *store = stores[0];

    LOG(INFO) << "success to process obtain root path. path=" << shard_path;
    return res;
}

/*加载header*/
OLAPStatus StorageEngine::load_header(const string& shard_path, const TCloneReq& request, bool restore) {
    LOG(INFO) << "begin to process load headers."
              << "tablet_id=" << request.tablet_id
              << ", schema_hash=" << request.schema_hash;
    OLAPStatus res = OLAP_SUCCESS;

    DataDir* store = nullptr;
    {
        // TODO(zc)
        try {
            auto store_path =
                boost::filesystem::path(shard_path).parent_path().parent_path().string();
            store = get_store(store_path);
            if (store == nullptr) {
                LOG(WARNING) << "invalid shard path, path=" << shard_path;
                return OLAP_ERR_INVALID_ROOT_PATH;
            }
        } catch (...) {
            LOG(WARNING) << "invalid shard path, path=" << shard_path;
            return OLAP_ERR_INVALID_ROOT_PATH;
        }
    }

    stringstream schema_hash_path_stream; // schema hash路径：{shard_path}/{tablet_id}/{schema_hash}
    schema_hash_path_stream << shard_path
                            << "/" << request.tablet_id
                            << "/" << request.schema_hash;
    // not surely, reload and restore tablet action call this api
    // reset tablet uid here

    string header_path = TabletMeta::construct_header_file_path(schema_hash_path_stream.str(), request.tablet_id); //构建header文件(tablet meta)的存储路径
    res = TabletMeta::reset_tablet_uid(header_path);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail reset tablet uid file path = " << header_path
                     << " res=" << res;
        return res;
    }
    res = _tablet_manager->load_tablet_from_dir(//从store中加载tablet
            store,
            request.tablet_id, request.schema_hash,
            schema_hash_path_stream.str(), false, restore);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to process load headers. res=" << res;
        return res;
    }

    LOG(INFO) << "success to process load headers.";
    return res;
}

/*task任务执行*/
OLAPStatus StorageEngine::execute_task(EngineTask* task) {
    // 1. add wlock to related tablets
    // 2. do prepare work
    // 3. release wlock
    {
        vector<TabletInfo> tablet_infos;
        task->get_related_tablets(&tablet_infos);//获取任务相关的tablet的信息，通过类型为vector的参数tablet_infos返回
        sort(tablet_infos.begin(), tablet_infos.end());//对类型为vector的tablet_infos进行排序
        vector<TabletSharedPtr> related_tablets;
        for (TabletInfo& tablet_info : tablet_infos) {//依次获取task相关的tablet对象，并存入vector类型的related_tablets中
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);//tablet manager根据tablet的信息获取对应的tablet对象
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);//将获取的tablet对象添加到vector类型的related_tablets
                tablet->obtain_header_wrlock();//对tablet添加写锁
            } else {
                LOG(WARNING) << "could not get tablet before prepare tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus prepare_status = task->prepare();//执行task的准备工作
        for (TabletSharedPtr& tablet : related_tablets) {
            tablet->release_header_lock();//依次释放相关tablet的锁
        }
        if (prepare_status != OLAP_SUCCESS) {
            return prepare_status;
        }
    }

    // do execute work without lock
    OLAPStatus exec_status = task->execute();//task任务执行，任务执行过程中，tablet不需要加锁
    if (exec_status != OLAP_SUCCESS) {
        return exec_status;
    }

    // 1. add wlock to related tablets
    // 2. do finish work
    // 3. release wlock
    {
        vector<TabletInfo> tablet_infos;
        // related tablets may be changed after execute task, so that get them here again
        //task执行结束之后，task相关的tablet可能会发生改变，因此需要重新获取task相关的tablet
        task->get_related_tablets(&tablet_infos);//获取任务相关的tablet的信息，通过类型为vector的参数tablet_infos返回
        sort(tablet_infos.begin(), tablet_infos.end());//对类型为vector的tablet_infos进行排序
        vector<TabletSharedPtr> related_tablets;
        for (TabletInfo& tablet_info : tablet_infos) {//依次获取task相关的tablet对象，并存入vector类型的related_tablets中
            TabletSharedPtr tablet = _tablet_manager->get_tablet(tablet_info.tablet_id, tablet_info.schema_hash);//tablet manager根据tablet的信息获取对应的tablet对象
            if (tablet != nullptr) {
                related_tablets.push_back(tablet);//将获取的tablet对象添加到vector类型的related_tablets
                tablet->obtain_header_wrlock();//对tablet添加写锁
            } else {
                LOG(WARNING) << "could not get tablet before finish tabletid: "
                             << tablet_info.tablet_id;
            }
        }
        // add write lock to all related tablets
        OLAPStatus fin_status = task->finish();//执行task的完成工作
        for (TabletSharedPtr& tablet : related_tablets) {
            tablet->release_header_lock();//依次释放相关tablet的锁
        }
        return fin_status;
    }
}

/*通过rowset id检查某一个rowset是否在不可用rowset的集合中*/
// check whether any unused rowsets's id equal to rowset_id
bool StorageEngine::check_rowset_id_in_unused_rowsets(const RowsetId& rowset_id) {
    MutexLock lock(&_gc_mutex);
    auto search = _unused_rowsets.find(rowset_id.to_string());
    return search != _unused_rowsets.end();
}

}  // namespace doris
