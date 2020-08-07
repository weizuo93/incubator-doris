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

#include "olap/tablet_manager.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/filesystem.hpp>
#include <rapidjson/document.h>
#include <re2/re2.h>
#include <thrift/protocol/TDebugProtocol.h>

#include "gutil/strings/strcat.h"

#include "env/env.h"
#include "olap/base_compaction.h"
#include "olap/cumulative_compaction.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/push_handler.h"
#include "olap/reader.h"
#include "olap/rowset/column_data_writer.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/schema_change.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/utils.h"
#include "util/doris_metrics.h"
#include "util/file_utils.h"
#include "util/pretty_printer.h"
#include "util/path_util.h"
#include "util/scoped_cleanup.h"
#include "util/time.h"

using std::list;
using std::map;
using std::set;
using std::string;
using std::vector;
using strings::Substitute;

namespace doris {

/*通过创建时间比较两个tablet*/
static bool _cmp_tablet_by_create_time(const TabletSharedPtr& a, const TabletSharedPtr& b) {
    return a->creation_time() < b->creation_time();
}

/*TabletManager的构造函数*/
TabletManager::TabletManager(int32_t tablet_map_lock_shard_size)
    : _tablet_map_lock_shard_size(tablet_map_lock_shard_size),
      _last_update_stat_ms(0) {
    DCHECK_GT(_tablet_map_lock_shard_size, 0);
    DCHECK_EQ(_tablet_map_lock_shard_size & (tablet_map_lock_shard_size - 1), 0);
    _tablet_map_lock_array = new RWMutex[_tablet_map_lock_shard_size];
    _tablet_map_array = new tablet_map_t[_tablet_map_lock_shard_size];//初始化tablet_map_t
}

/*TabletManager的析构函数*/
TabletManager::~TabletManager() {
    delete [] _tablet_map_lock_array;//成员变量_tablet_map_array的读写锁
    delete [] _tablet_map_array;//保存了tablet与表实例（TableInstances）的对应关系
}

/****************************************************************************************************/
/*给TabletManager增加tablet，将传入的tablet添加到map中，可以通过tablet id从map中获取表实例（TableInstances），进而获取表实例中所有的tablet*/
OLAPStatus TabletManager::_add_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, const TabletSharedPtr& tablet, bool update_meta, bool force) {
    OLAPStatus res = OLAP_SUCCESS;
    VLOG(3) << "begin to add tablet to TabletManager. " << "tablet_id=" << tablet_id
            << ", schema_hash=" << schema_hash << ", force=" << force;

    //（1）通过map找到传入的tablet id对应的表实例，遍历表实例（TableInstances）中的所有tablet，查看其中是否存在与传入的tablet具有相同tablet id和schema hash的tablet
    TabletSharedPtr existed_tablet = nullptr;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id); //根据tablet id计算该tablet存放在哪个lock_shard上，即存放在那个tablet map中
    for (TabletSharedPtr item : tablet_map[tablet_id].table_arr) {//通过对应的tablet map找到tablet id对应的TableInstances，遍历表实例（TableInstances）中的所有tablet
        if (item->equal(tablet_id, schema_hash)) {//是否表实例（TableInstances）中某一个tablet的tablet id和schema hash同时与参数传入的相同。tablet可能会有多种schema hash。
            existed_tablet = item;
            break;
        }
    }

    //（2）如果不存在，则将传入的tablet添加到map中，函数返回。
    if (existed_tablet == nullptr) {//map中不存在参数传入的tablet，则将该tablet添加到map中，可以通过tablet id来获取表实例（TableInstances）
        return _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta, false /*keep_files*/, false /*drop_old*/);
    }

    //（3）如果存在，则分别获取map中已经存在的tablet以及传入的tablet中的最新版本rowset。若传入的tablet中的最新版本rowset比已经存在的tablet中的最新版本rowset的版本更新，
    //    或者创建时间更近，则将传入的tablet添加到map中。
    if (!force) {//是否强制添加
        if (existed_tablet->tablet_path() == tablet->tablet_path()) {//tablet_path()是BaseTablet类中的成员函数，返回tablet的路径（String类型）
            LOG(WARNING) << "add the same tablet twice! tablet_id=" << tablet_id
                         << ", schema_hash=" << schema_hash
                         << ", tablet_path=" << tablet->tablet_path();
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
        if (existed_tablet->data_dir() == tablet->data_dir()) {//data_dir()是BaseTablet类中的成员函数，返回tablet的存储磁盘（DataDir*类型）
            LOG(WARNING) << "add tablet with same data dir twice! tablet_id=" << tablet_id
                         << ", schema_hash=" << schema_hash;
            return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
        }
    }

    existed_tablet->obtain_header_rdlock();//给tablet添加锁
    const RowsetSharedPtr old_rowset = existed_tablet->rowset_with_max_version();//获取map中已经存在的tablet中的最新版本rowset
    const RowsetSharedPtr new_rowset = tablet->rowset_with_max_version();        //获取传入的tablet中的最新版本rowset

    // If new tablet is empty, it is a newly created schema change tablet.
    // the old tablet is dropped before add tablet. it should not exist old tablet
    if (new_rowset == nullptr) {//判断传入的tablet是否为空
        existed_tablet->release_header_lock();//释放锁
        // it seems useless to call unlock and return here.
        // it could prevent error when log level is changed in the future.
        LOG(FATAL) << "new tablet is empty and old tablet exists. it should not happen."
                   << " tablet_id=" << tablet_id << " schema_hash=" << schema_hash;
        return OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    //传入的tablet不为空
    int64_t old_time = old_rowset == nullptr ? -1 : old_rowset->creation_time();//(old_rowset == nullptr)
    int64_t new_time = new_rowset->creation_time();
    int32_t old_version = old_rowset == nullptr ? -1 : old_rowset->end_version();//(old_rowset == nullptr)
    int32_t new_version = new_rowset->end_version();
    existed_tablet->release_header_lock();//释放锁

    // In restore process, we replace all origin files in tablet dir with
    // the downloaded snapshot files. Then we try to reload tablet header.
    // force == true means we forcibly replace the Tablet in tablet_map
    // with the new one. But if we do so, the files in the tablet dir will be
    // dropped when the origin Tablet deconstruct.
    // So we set keep_files == true to not delete files when the
    // origin Tablet deconstruct.
    bool keep_files = force ? true : false;
    if (force || (new_version > old_version || (new_version == old_version && new_time > old_time))) {
        // check if new tablet's meta is in store and add new tablet's meta to meta store
        res = _add_tablet_to_map_unlocked(tablet_id, schema_hash, tablet, update_meta, keep_files, true /*drop_old*/);//将传入的tablet添加到map中，并删除旧版本的tablet
    } else {
        res = OLAP_ERR_ENGINE_INSERT_EXISTS_TABLE;
    }
    LOG(WARNING) << "add duplicated tablet. force=" << force << ", res=" << res
            << ", tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
            << ", old_version=" << old_version << ", new_version=" << new_version
            << ", old_time=" << old_time << ", new_time=" << new_time
            << ", old_tablet_path=" << existed_tablet->tablet_path()
            << ", new_tablet_path=" << tablet->tablet_path();

    return res;
}

/*将tablet添加到map中，可以通过tablet id来获取表实例（TableInstances）*/
OLAPStatus TabletManager::_add_tablet_to_map_unlocked(TTabletId tablet_id, SchemaHash schema_hash, const TabletSharedPtr& tablet, bool update_meta, bool keep_files, bool drop_old) {
     // check if new tablet's meta is in store and add new tablet's meta to meta store
    OLAPStatus res = OLAP_SUCCESS;
    if (update_meta) {
        // call tablet save meta in order to valid the meta
        tablet->save_meta();
    }
    if (drop_old) {
        // If the new tablet is fresher than the existing one, then replace the existing tablet with the new one.
        RETURN_NOT_OK_LOG(_drop_tablet_unlocked(tablet_id, schema_hash, keep_files),//删除与传入的tablet具有相同tablet id以及schema hash的旧版本的tablet，可以设置是否保留原始文件
                Substitute("failed to drop old tablet when add new tablet. tablet_id=$0, schema_hash=$1", tablet_id, schema_hash));
    }
    // Register tablet into DataDir, so that we can manage tablet from the perspective of root path.
    // Example: unregister all tables when a bad disk found.
    tablet->register_tablet_into_dir();//将tablet在data dir中进行注册
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//根据tablet id获取tablet map
    tablet_map[tablet_id].table_arr.push_back(tablet);    //将传入的tablet添加到tablet_map中（tablet id对应的表实例中）
    tablet_map[tablet_id].table_arr.sort(_cmp_tablet_by_create_time);//tablet添加完成后，根据创建时间对tablet map中表实例的tablet进行升序排列
    _add_tablet_to_partition(*tablet);//将新添加的tablet添加到partition中

    VLOG(3) << "add tablet to map successfully."
            << " tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    return res;
}

/*检查传入的tablet id是否存在（加锁之后调用了未加锁的函数）*/
bool TabletManager::check_tablet_id_exist(TTabletId tablet_id) {
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    ReadLock rlock(&tablet_map_lock);
    return _check_tablet_id_exist_unlocked(tablet_id);
}

/*检查传入的tablet id是否存在（未加锁）*/
bool TabletManager::_check_tablet_id_exist_unlocked(TTabletId tablet_id) {
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//根据tablet id获取tablet map
    tablet_map_t::iterator it = tablet_map.find(tablet_id);//在tablet map中查找是否有对应于tablet id的表实例
    //如果tablet map中存在对应于tablet id的表实例，并且表实例中包含所谓tablet不为空，则表示传入的tablet id存在；否则，传入的tablet id不存在
    return it != tablet_map.end() && !it->second.table_arr.empty();
}

/*从节点中可用于创建tablet的磁盘中选择一个磁盘来创建tablet*/
OLAPStatus TabletManager::create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores) {//以字符“T”开头的请求类表示该类的对象是来自Thrift的请求
    DorisMetrics::instance()->create_tablet_requests_total.increment(1);

    int64_t tablet_id = request.tablet_id;//从request中获取tablet id
    int32_t schema_hash = request.tablet_schema.schema_hash;//从request中获取schema hash
    LOG(INFO) << "begin to create tablet. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;

    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    WriteLock wrlock(&tablet_map_lock);
    // Make create_tablet operation to be idempotent:
    // 1. Return true if tablet with same tablet_id and schema_hash exist;
    //           false if tablet with same tablet_id but different schema_hash exist.
    // 2. When this is an alter task, if the tablet(both tablet_id and schema_hash are
    // same) already exist, then just return true(an duplicate request). But if
    // tablet_id exist but with different schema_hash, return an error(report task will
    // eventually trigger its deletion).
    if (_check_tablet_id_exist_unlocked(tablet_id)) {//判断需要创建的tablet id是否已经存在
                                /*需要创建的tablet id存在*/
        TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash);//根据tablet id和schema hash获取对应的tablet
        if (tablet != nullptr) {
            //与创建要求具有相同tablet id以及schema hash的tablet已经存在，函数返回（可能是重复请求）
            LOG(INFO) << "success to create tablet. tablet already exist. tablet_id=" << tablet_id;
            return OLAP_SUCCESS;
        } else {
            //与创建要求具有相同tablet id的tablet已经存在，但是拥有不同的schema hash，创建失败，函数返回
            //如果某个tablet id已经存在，那么就不能再创建相同id的tablet。具有相同id的tablet只能是通过base tablet进行alter schema得到。
            LOG(WARNING) << "fail to create tablet. tablet exist but with different schema_hash. "
                    << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
            DorisMetrics::instance()->create_tablet_requests_failed.increment(1);
            return OLAP_ERR_CE_TABLET_ID_EXIST;
        }
        /*
          这意味着FE通过Thrift请求创建tablet时，要创建的tablet_id（request中的参数tablet_id）不能与已经存在的tablet的tablet id相同，不论schema hash是否相同，函数在此处总会返回。
          即使该请求是 alter schema。那么问题来了，TableInstances中的第二个元素std::list<TabletSharedPtr> table_arr中保存的具有相同tablet id的那些tablet是如何产生的？
        */
    }
                            /*需要创建的tablet id不存在*/
    TabletSharedPtr base_tablet = nullptr;
    bool is_schema_change = false;
    // If the CreateTabletReq has base_tablet_id then it is a alter-tablet request
    //如果这个请求中包含了base_tablet_id，那么该请求就是一个alter schema的请求
    if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
        is_schema_change = true;//表示该请求就是一个alter schema的请求
        base_tablet = _get_tablet_unlocked(request.base_tablet_id, request.base_schema_hash);//根据base tablet id和schema hash获取对应的base tablet
        if (base_tablet == nullptr) {
            LOG(WARNING) << "fail to create tablet(change schema), base tablet does not exist. "
                         << "new_tablet_id=" << tablet_id << ", new_schema_hash=" << schema_hash
                         << ", base_tablet_id=" << request.base_tablet_id
                         << ", base_schema_hash=" << request.base_schema_hash;
            DorisMetrics::instance()->create_tablet_requests_failed.increment(1);
            return OLAP_ERR_TABLE_CREATE_META_ERROR;
        }
        /*如果该请求是一个alter schema的请求，那么alter schema之后生成的tablet必须与base tablet在相同的磁盘上,因为linked-schema-change类型
          需要Linux硬链接，而Linux不支持跨磁盘的硬链接。其他schema-change类型alter schema之后生成的tablet不需要与base tablet在相同的磁盘上。
          Doris中目前进行 Schema Change 的方式有三种：Sorted Schema Change，Direct Schema Change, Linked Schema Change。
          （1）Sorted Schema Change：改变了列的排序方式，需对数据进行重新排序。例如删除排序列中的一列, 字段重排序。
          （2）Direct Schema Change: 无需重新排序，但是需要对数据做一次转换。例如修改列的类型，在稀疏索引中加一列等。
          （3）Linked Schema Change: 无需转换数据，直接完成。例如加列操作。（alter schema之后生成的table会和base tablet共享数据，因此需要硬链接）
         */
        // If we are doing schema-change, we should use the same data dir
        // TODO(lingbin): A litter trick here, the directory should be determined before
        // entering this method
        /*vector类型的参数store中存储了所有可供创建tablet的候选磁盘，因为该创建tablet的命令有可能会执行Linked Schema Change，
          alter schema之后生成的tablet必须与 base tablet在相同的磁盘上，就不能随便传入一个磁盘来创建tablet*/
        stores.clear();//清空stores
        stores.push_back(base_tablet->data_dir());//将base_tablet所在的磁盘放入stores。将alter schema之后生成的tablet与 base tablet存储在相同的磁盘上。
    }

    // set alter type to schema-change. it is useless
    TabletSharedPtr tablet = _internal_create_tablet_unlocked(AlterTabletType::SCHEMA_CHANGE, request, is_schema_change, base_tablet.get(), stores);
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to create tablet. tablet_id=" << request.tablet_id;
        DorisMetrics::instance()->create_tablet_requests_failed.increment(1);
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    }

    LOG(INFO) << "success to create tablet. tablet_id=" << tablet_id
              << ", schema_hash=" << schema_hash;
    return OLAP_SUCCESS;
}

TabletSharedPtr TabletManager::_internal_create_tablet_unlocked(const AlterTabletType alter_type, const TCreateTabletReq& request,
        const bool is_schema_change, const Tablet* base_tablet, const std::vector<DataDir*>& data_dirs) {
    // If in schema-change state, base_tablet must also be provided.
    // i.e., is_schema_change and base_tablet are either assigned or not assigned
    /*断言 DCHECK/CHECK：如果断言失败，运行着的程序会立即终止。其中，DCHECK 只对 调试版 (debug) 有效，而 CHECK 也可用于布版 (release)，从而避免在发布版进行无用的检查。*/
    DCHECK((is_schema_change && base_tablet) || (!is_schema_change && !base_tablet));// is_schema_change为true的同时base_tablet存在，或者is_schema_change为false的同时base_tablet不存在

    // NOTE: The existence of tablet_id and schema_hash has already been checked, no need check again here.

    auto tablet = _create_tablet_meta_and_dir_unlocked(request, is_schema_change, base_tablet, data_dirs);//选择一个磁盘创建tablet meta以及tablet
    if (tablet == nullptr) {
        return nullptr;
    }

    int64_t new_tablet_id = request.tablet_id;
    int32_t new_schema_hash = request.tablet_schema.schema_hash;

    // should remove the tablet's pending_id no matter create-tablet success or not
    DataDir* data_dir = tablet->data_dir(); //获取tablet所在的磁盘信息
    SCOPED_CLEANUP({
        data_dir->remove_pending_ids(StrCat(TABLET_ID_PREFIX, new_tablet_id)); //olap_define.h文件中定义：const std::string TABLET_ID_PREFIX = "t_";
    });

    // TODO(yiguolei)
    // the following code is very difficult to understand because it mixed alter tablet v2
    // and alter tablet v1 should remove alter tablet v1 code after v0.12
    OLAPStatus res = OLAP_SUCCESS;
    bool is_tablet_added = false;
    do {
        res = tablet->init();//初始化Tablet类中管理rowset的成员变量_rs_version_map和_inc_rs_version_map
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "tablet init failed. tablet:" << tablet->full_name();
            break;
        }
        // TODO(lingbin): is it needed? because all type of create_tablet will be true.
        // 1. !is_schema_change: not in schema-change state;
        // 2. request.base_tablet_id > 0: in schema-change state;
        if (!is_schema_change || (request.__isset.base_tablet_id && request.base_tablet_id > 0)) {
            // Create init version if this is not a restore mode replica and request.version is set
            // bool in_restore_mode = request.__isset.in_restore_mode && request.in_restore_mode;
            // if (!in_restore_mode && request.__isset.version) {
            // create inital rowset before add it to storage engine could omit many locks
            res = _create_inital_rowset_unlocked(request, tablet.get());//变量tablet的类型为TabletSharedPtr,此处的get()是智能指针shared_ptr的成员函数，用于返回shared_ptr保存的指针
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "fail to create initial version for tablet. res=" << res;
                break;
            }
        }
        if (is_schema_change) {
            if (request.__isset.base_tablet_id && request.base_tablet_id > 0) {
                LOG(INFO) << "request for alter-tablet v2, do not add alter task to tablet";
                // if this is a new alter tablet, has to set its state to not ready
                // because schema change hanlder depends on it to check whether history data
                // convert finished
                tablet->set_tablet_state(TabletState::TABLET_NOTREADY);
            } else {
                // add alter task to new tablet if it is a new tablet during schema change
                tablet->add_alter_task(base_tablet->tablet_id(), base_tablet->schema_hash(), vector<Version>(), alter_type);//？？？？？？？？？？？？？
            }
            // 有可能出现以下2种特殊情况：
            // 1. 因为操作系统时间跳变，导致新生成的表的creation_time小于旧表的creation_time时间
            // 2. 因为olap engine代码中统一以秒为单位，所以如果2个操作(比如create一个表,
            //    然后立即alter该表)之间的时间间隔小于1s，则alter得到的新表和旧表的creation_time会相同
            //
            // 当出现以上2种情况时，为了能够区分alter得到的新表和旧表，这里把新表的creation_time设置为
            // 旧表的creation_time加1
            if (tablet->creation_time() <= base_tablet->creation_time()) {
                LOG(WARNING) << "new tablet's create time is less than or equal to old tablet"
                             << "new_tablet_create_time=" << tablet->creation_time()
                             << ", base_tablet_create_time=" << base_tablet->creation_time();
                int64_t new_creation_time = base_tablet->creation_time() + 1;
                tablet->set_creation_time(new_creation_time);
            }
        }
        // Add tablet to StorageEngine will make it visiable to user
        res = _add_tablet_unlocked(new_tablet_id, new_schema_hash, tablet, true, false); //将tablet添加到存储引擎中（实际上是添加到map中，可以通过tablet id从map中获取TableInstances）
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to add tablet to StorageEngine. res=" << res;
            break;
        }
        is_tablet_added = true;

        // TODO(lingbin): The following logic seems useless, can be removed?
        // Because if _add_tablet_unlocked() return OK, we must can get it from map.
        TabletSharedPtr tablet_ptr = _get_tablet_unlocked(new_tablet_id, new_schema_hash);//从map中查找对应的tablet，验证tablet是否真正成功地添加到了存储引擎中
        if (tablet_ptr == nullptr) {
            res = OLAP_ERR_TABLE_NOT_FOUND;
            LOG(WARNING) << "fail to get tablet. res=" << res;
            break;
        }
    } while (0);

    if (res == OLAP_SUCCESS) { //如果成功添加tablet到了存储引擎中并且能查到，则直接返回该tablet
        return tablet;
    }
    // something is wrong, we need clear environment
    if (is_tablet_added) { //如果向存储引擎中成功地添加了tablet，但是查不出来，就需要将该tablet从存储引擎中重新删掉
        OLAPStatus status = _drop_tablet_unlocked(new_tablet_id, new_schema_hash, false); //从map中删除添加的tablet
        if (status != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to drop tablet when create tablet failed. res=" << res;
        }
    } else { //如果向存储引擎中添加tablet失败（即_add_tablet_unlocked()函数执行失败），则需要从磁盘上删除该tablet的meta信息
        tablet->delete_all_files(); //刪除tablet中所有的rowset，并清空Tablet的成员变量_inc_rs_version_map和_rs_version_map
        TabletMetaManager::remove(data_dir, new_tablet_id, new_schema_hash); //从磁盘上将tablet mate信息删除
    }
    return nullptr;
}

/*生成tablet的存储路径，格式为：data dir/DATA_PREFIX/shard_id/tablet_id */
static string _gen_tablet_dir(const string& dir, int16_t shard_id, int64_t tablet_id) {
    string path = dir;
    path = path_util::join_path_segments(path, DATA_PREFIX);//olap_define.h文件中定义了static const std::string DATA_PREFIX = "/data";
    path = path_util::join_path_segments(path, std::to_string(shard_id));
    path = path_util::join_path_segments(path, std::to_string(tablet_id));
    return path;
}

/*从第一个磁盘开始尝试创建tablet meta以及tablet，直到创建成功为止*/
TabletSharedPtr TabletManager::_create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request, const bool is_schema_change, const Tablet* base_tablet, const std::vector<DataDir*>& data_dirs) {
    string pending_id = StrCat(TABLET_ID_PREFIX, request.tablet_id);//olap_define.h文件中定义：const std::string TABLET_ID_PREFIX = "t_";
    // Many attempts are made here in the hope that even if a disk fails, it can still continue.
    DataDir* last_dir = nullptr;
    for (auto& data_dir : data_dirs) {
        if (last_dir != nullptr) {
            // If last_dir != null, it means the last attempt to create a tablet failed
            last_dir->remove_pending_ids(pending_id);
        }
        last_dir = data_dir;

        TabletMetaSharedPtr tablet_meta;
        // if create meta faild, do not need to clean dir, because it is only in memory
        OLAPStatus res = _create_tablet_meta_unlocked(request, data_dir, is_schema_change, base_tablet, &tablet_meta);//在data dir上创建tablet meta
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create tablet meta. res=" << res
                         << ", root=" << data_dir->path();
            continue;
        }

        string tablet_dir = _gen_tablet_dir(data_dir->path(), tablet_meta->shard_id(), request.tablet_id); //生成tablet的存储路径，格式为：{data dir}/DATA_PREFIX/{shard_id}/{tablet_id}, 文件olap_define.h中定义了static const std::string DATA_PREFIX = "/data";
        string schema_hash_dir = path_util::join_path_segments(tablet_dir, std::to_string(request.tablet_schema.schema_hash)); //生成当前tablet的schema hash路径,格式为：{data dir}/DATA_PREFIX/{shard_id}/{tablet_id}/{schema_hash}

        // Because the tablet is removed asynchronously, so that the dir may still exist when BE
        // receive create-tablet request again, For example retried schema-change request
        if (FileUtils::check_exist(schema_hash_dir)) {
            LOG(WARNING) << "skip this dir because tablet path exist, path="<< schema_hash_dir;
            continue;
        } else {
            data_dir->add_pending_ids(pending_id);
            Status st = FileUtils::create_dir(schema_hash_dir); //创建当前tablet的schema hash路径
            if(!st.ok()) {
                LOG(WARNING) << "create dir fail. path=" << schema_hash_dir
                             << " error=" << st.to_string();
                continue;
            }
        }

        TabletSharedPtr new_tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir); //根据tablet的meta信息在磁盘上创建tablet
        DCHECK(new_tablet != nullptr);
        return new_tablet;
    }
    return nullptr;
}

/*根据tablet_id和schema hash从tablet map中查找对应的tablet并删除（调用未加锁函数）*/
OLAPStatus TabletManager::drop_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    WriteLock wlock(&tablet_map_lock);
    return _drop_tablet_unlocked(tablet_id, schema_hash, keep_files);
}

/*根据tablet_id和schema hash从tablet map中查找对应的tablet并删除（未加锁）*/
// Drop specified tablet, the main logical is as follows:
// 1. tablet not in schema change:
//      drop specified tablet directly;
// 2. tablet in schema change:
//      a. schema change not finished && the dropping tablet is a base-tablet:
//          base-tablet cannot be dropped;
//      b. other cases:
//          drop specified tablet directly and clear schema change info.
OLAPStatus TabletManager::_drop_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    LOG(INFO) << "begin drop tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    DorisMetrics::instance()->drop_tablet_requests_total.increment(1);

    // （1）获取需要删除的tablet          Fetch tablet which need to be droped
    TabletSharedPtr to_drop_tablet = _get_tablet_unlocked(tablet_id, schema_hash);//根据tablet id和schema hash从tablet map中查找需要删除的tablet
    if (to_drop_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
        return OLAP_SUCCESS;
    }

    //（2）获取待删除tablet的schema change信息，查看tablet是否处于schema change状态
    // Try to get schema change info, we can drop tablet directly if it is not in schema-change state.
    AlterTabletTaskSharedPtr alter_task = to_drop_tablet->alter_task();

    //tablet没有处于schema change状态，则直接删除tablet，函数返回
    if (alter_task == nullptr) {
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);//tablet没有处于schema change状态，则直接删除tablet，函数返回
    }

    AlterTabletState alter_state = alter_task->alter_state();
    TTabletId related_tablet_id = alter_task->related_tablet_id();
    TSchemaHash related_schema_hash = alter_task->related_schema_hash();;

    TabletSharedPtr related_tablet = _get_tablet_unlocked(related_tablet_id, related_schema_hash);//根据tablet id和schema hash从tablet map中查找相应的tablet
    if (related_tablet == nullptr) {
        // TODO(lingbin): in what case, can this happen?
        LOG(WARNING) << "drop tablet directly when related tablet not found. "
                     << " tablet_id=" << related_tablet_id
                     << " schema_hash=" << related_schema_hash;
        return _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);//直接删除tablet，并返回
    }

    // Check whether the tablet we want to delete is in schema-change state
    bool is_schema_change_finished = (alter_state == ALTER_FINISHED || alter_state == ALTER_FAILED);//检查待删除的tablet是否处于schema change状态

    //（3）检查待删除的tablet是否为base tablet
    // Check whether the tablet we want to delete is base-tablet
    bool is_dropping_base_tablet = false;
    if (to_drop_tablet->creation_time() < related_tablet->creation_time()) {//检查待删除的tablet是否为base tablet
        is_dropping_base_tablet = true;
    }

    if (is_dropping_base_tablet && !is_schema_change_finished) {//待删除的tablet为base tablet并且是处于schema change状态，则不能删除
        LOG(WARNING) << "fail to drop tablet. it is in schema-change state. tablet="
                     << to_drop_tablet->full_name();
        return OLAP_ERR_PREVIOUS_SCHEMA_CHANGE_NOT_FINISHED;
    }

    // When the code gets here, there are two possibilities:
    // 1. The tablet currently being deleted is a base-tablet, and the corresponding
    //    schema-change process has finished;
    // 2. The tablet we are currently trying to drop is not base-tablet(i.e. a tablet
    //    generated from its base-tablet due to schema-change). For example, the current
    //    request is triggered by cancel alter). In this scenario, the corresponding
    //    schema-change task may still in process.

    // Drop specified tablet and clear schema-change info
    // NOTE: must first break the hard-link and then drop the tablet.
    // Otherwise, if first drop tablet, then break link. If BE restarts during execution,
    // after BE restarts, the tablet is no longer in metadata, but because the hard-link
    // is still there, the corresponding file may never be deleted from disk.
    related_tablet->obtain_header_wrlock();
    // should check the related tablet_id in alter task is current tablet to be dropped
    // For example: A related to B, BUT B related to C.
    // If drop A, should not clear B's alter task
    OLAPStatus res = OLAP_SUCCESS;
    AlterTabletTaskSharedPtr related_alter_task = related_tablet->alter_task();
    if (related_alter_task != nullptr && related_alter_task->related_tablet_id() == tablet_id && related_alter_task->related_schema_hash() == schema_hash) {
        related_tablet->delete_alter_task();
        related_tablet->save_meta();
    }
    related_tablet->release_header_lock();
    res = _drop_tablet_directly_unlocked(tablet_id, schema_hash, keep_files);
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to drop tablet which in schema change. tablet="
                     << to_drop_tablet->full_name();
        return res;
    }

    LOG(INFO) << "finish to drop tablet. res=" << res;
    return res;
}

/***************************************************************************/
/*通过tablet manager清除不可用磁盘上的tablet*/
OLAPStatus TabletManager::drop_tablets_on_error_root_path(const vector<TabletInfo>& tablet_info_vec) {
    OLAPStatus res = OLAP_SUCCESS;
    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {//遍历所有的shard
        RWMutex& tablet_map_lock = _tablet_map_lock_array[i];
        WriteLock wlock(&tablet_map_lock);
        for (const TabletInfo& tablet_info : tablet_info_vec) {
            TTabletId tablet_id = tablet_info.tablet_id;
            if ((tablet_id & (_tablet_map_lock_shard_size - 1)) != i) {
                continue;
            }
            TSchemaHash schema_hash = tablet_info.schema_hash;
            VLOG(3) << "drop_tablet begin. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
            TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);//根据tablet id和schema hash从tablet map中查找相应的tablet
            if (dropped_tablet == nullptr) {//待删除的tablet不存在
                LOG(WARNING) << "dropping tablet not exist. "
                             << " tablet=" << tablet_id
                             << " schema_hash=" << schema_hash;
                continue;
            } else {//待删除的tablet存在
                tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//获取tablet id对应的tablet_map
                for (list<TabletSharedPtr>::iterator it = tablet_map[tablet_id].table_arr.begin(); it != tablet_map[tablet_id].table_arr.end();) {
                    if ((*it)->equal(tablet_id, schema_hash)) {
                        // We should first remove tablet from partition_map to avoid iterator
                        // becoming invalid.
                        _remove_tablet_from_partition(*(*it));//从partition中移除查找到的tablet
                        it = tablet_map[tablet_id].table_arr.erase(it);//从表实例中删除查找到的tablet
                    } else {
                        ++it;
                    }
                }
            }
        }
    }
    return res;
}

/*根据tablet_id和schema hash从tablet map中查找对应的tablet，可以通过设置参数include_deleted为true来从成员变量_shutdown_tablets中获取已经被删除的tablet（已加锁）*/
TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool include_deleted, string* err) {
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    ReadLock rlock(&tablet_map_lock);
    return _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
}
/**************************************************************************************************************************/
/*根据tablet_id和schema hash从tablet map中查找对应的tablet，可以通过设置参数include_deleted为true来从成员变量_shutdown_tablets中获取已经被删除的tablet（未加锁）*/
TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool include_deleted, string* err) {
    TabletSharedPtr tablet;
    tablet = _get_tablet_unlocked(tablet_id, schema_hash);//根据tablet id和schema hash从tablet map中查找相应的tablet
    if (tablet == nullptr && include_deleted) {，
        //如果没有查找到，需要在删除列表中查找
        ReadLock rlock(&_shutdown_tablets_lock);
        for (auto& deleted_tablet : _shutdown_tablets) {//通过成员变量_shutdown_tablets访问已经被删掉的tablet
            CHECK(deleted_tablet != nullptr) << "deleted tablet is nullptr";
            if (deleted_tablet->tablet_id() == tablet_id && deleted_tablet->schema_hash() == schema_hash) {//如果某个已经被删掉的tablet的tablet id以及schema hash都与输入一致，则查询成功
                tablet = deleted_tablet;
                break;
            }
        }
    }

    if (tablet == nullptr) {
        if (err != nullptr) {
            *err = "tablet does not exist";
        }
        return nullptr;
    }
    //查找到了tablet
    if (!tablet->is_used()) {//判断tablet是否可用
        LOG(WARNING) << "tablet cannot be used. tablet=" << tablet_id;
        if (err != nullptr) {
            *err = "tablet cannot be used";
        }
        return nullptr;
    }

    return tablet;
}

/*根据tablet_id，schema hash以及tablet uid从tablet map中查找对应的tablet，可以通过设置参数include_deleted为true来从成员变量_shutdown_tablets中获取已经被删除的tablet（已加锁）*/
TabletSharedPtr TabletManager::get_tablet(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, bool include_deleted, string* err) {
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    ReadLock rlock(&tablet_map_lock);
    TabletSharedPtr tablet = _get_tablet_unlocked(tablet_id, schema_hash, include_deleted, err);
    if (tablet != nullptr && tablet->tablet_uid() == tablet_uid) {
        return tablet;
    }
    return nullptr;
}

/*通过路径获取tablet id和schema hash*/
bool TabletManager::get_tablet_id_and_schema_hash_from_path(const string& path, TTabletId* tablet_id, TSchemaHash* schema_hash) {
    static re2::RE2 normal_re("/data/\\d+/(\\d+)/(\\d+)($|/)");
    if (RE2::PartialMatch(path, normal_re, tablet_id, schema_hash)) {
        return true;
    }

    // If we can't match normal path pattern, this may be a path which is a empty tablet
    // directory. Use this pattern to match empty tablet directory. In this case schema_hash
    // will be set to zero.
    static re2::RE2 empty_tablet_re("/data/\\d+/(\\d+)($|/$)");
    if (!RE2::PartialMatch(path, empty_tablet_re, tablet_id)) {
        return false;
    }
    *schema_hash = 0;
    return true;
}

/*通过路径获取rowset id*/
bool TabletManager::get_rowset_id_from_path(const string& path, RowsetId* rowset_id) {
    static re2::RE2 re("/data/\\d+/\\d+/\\d+/([A-Fa-f0-9]+)_.*");
    string id_str;
    bool ret = RE2::PartialMatch(path, re, &id_str);
    if (ret) {
        rowset_id->init(id_str);
        return true;
    }
    return false;
}

/*获取tablet状态*/
void TabletManager::get_tablet_stat(TTabletStatResult* result) {
    int64_t curr_ms = UnixMillis();
    // Update cache if it is too old
    {
        int interval_sec = config::tablet_stat_cache_update_interval_second;//CONF_mInt32(tablet_stat_cache_update_interval_second, "300");
        std::lock_guard<std::mutex> l(_tablet_stat_mutex);
        if (curr_ms - _last_update_stat_ms > interval_sec * 1000) {
            VLOG(3) << "update tablet stat.";
            _build_tablet_stat();//获取tablet状态
            _last_update_stat_ms = UnixMillis();
        }
    }

    result->__set_tablets_stats(_tablet_stat_cache);
}

/*根据compaction的类型寻找最适合进行compaction的tablet*/
TabletSharedPtr TabletManager::find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir) {
    int64_t now_ms = UnixMillis();
    const string& compaction_type_str = compaction_type == CompactionType::BASE_COMPACTION ? "base" : "cumulative";
    uint32_t highest_score = 0;
    TabletSharedPtr best_tablet;
    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {
        ReadLock tablet_map_rdlock(&_tablet_map_lock_array[i]);
        tablet_map_t& tablet_map = _tablet_map_array[i];
        for (tablet_map_t::value_type& table_ins : tablet_map){
            for (TabletSharedPtr& tablet_ptr : table_ins.second.table_arr) {
                AlterTabletTaskSharedPtr cur_alter_task = tablet_ptr->alter_task();
                if (cur_alter_task != nullptr
                    && cur_alter_task->alter_state() != ALTER_FINISHED
                    && cur_alter_task->alter_state() != ALTER_FAILED) {
                    TabletSharedPtr related_tablet = _get_tablet_unlocked(
                            cur_alter_task->related_tablet_id(), cur_alter_task->related_schema_hash());
                    if (related_tablet != nullptr
                        && tablet_ptr->creation_time() > related_tablet->creation_time()) {
                        // Current tablet is newly created during schema-change or rollup, skip it
                        continue;
                    }
                }
                // A not-ready tablet maybe a newly created tablet under schema-change, skip it
                if (tablet_ptr->tablet_state() == TABLET_NOTREADY) {
                    continue;
                }

                if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash()
                    || !tablet_ptr->is_used()
                    || !tablet_ptr->init_succeeded()
                    || !tablet_ptr->can_do_compaction()) {
                    continue;
                }

                int64_t last_failure_ms = tablet_ptr->last_cumu_compaction_failure_time();
                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    last_failure_ms = tablet_ptr->last_base_compaction_failure_time();
                }
                if (now_ms - last_failure_ms <= config::min_compaction_failure_interval_sec * 1000) {
                    VLOG(1) << "Too often to check compaction, skip it."
                            << "compaction_type=" << compaction_type_str
                            << ", last_failure_time_ms=" << last_failure_ms
                            << ", tablet_id=" << tablet_ptr->tablet_id();
                    continue;
                }

                if (compaction_type == CompactionType::BASE_COMPACTION) {
                    MutexLock lock(tablet_ptr->get_base_lock(), TRY_LOCK);
                    if (!lock.own_lock()) {
                        continue;
                    }
                } else {
                    MutexLock lock(tablet_ptr->get_cumulative_lock(), TRY_LOCK);
                    if (!lock.own_lock()) {
                        continue;
                    }
                }


                uint32_t table_score = 0;
                {
                    ReadLock rdlock(tablet_ptr->get_header_lock_ptr());
                    if (compaction_type == CompactionType::BASE_COMPACTION) {
                        table_score = tablet_ptr->calc_base_compaction_score();
                    } else if (compaction_type == CompactionType::CUMULATIVE_COMPACTION) {
                        table_score = tablet_ptr->calc_cumulative_compaction_score();
                    }
                }
                if (table_score > highest_score) {
                    highest_score = table_score;
                    best_tablet = tablet_ptr;
                }
            }
        }
    }

    if (best_tablet != nullptr) {
        LOG(INFO) << "Found the best tablet for compaction. "
                  << "compaction_type=" << compaction_type_str
                  << ", tablet_id=" << best_tablet->tablet_id()
                  << ", highest_score=" << highest_score;
        // TODO(lingbin): Remove 'max' from metric name, it would be misunderstood as the
        // biggest in history(like peak), but it is really just the value at current moment.
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            DorisMetrics::instance()->tablet_base_max_compaction_score.set_value(highest_score);
        } else {
            DorisMetrics::instance()->tablet_cumulative_max_compaction_score.set_value(highest_score);
        }
    }
    return best_tablet;
}


/*通过tablet meta加载tablet*/
OLAPStatus TabletManager::load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id, TSchemaHash schema_hash, const string& meta_binary, bool update_meta, bool force, bool restore) {
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    WriteLock wlock(&tablet_map_lock);
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus status = tablet_meta->deserialize(meta_binary);//从字符串value中解析出tablet meta信息，并保存在tablet_meta对象中
    if (status != OLAP_SUCCESS) {
        LOG(WARNING) << "fail to load tablet because can not parse meta_binary string. "
                     << "tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash
                     << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    // check if tablet meta is valid
    if (tablet_meta->tablet_id() != tablet_id || tablet_meta->schema_hash() != schema_hash) {//判断解析出的tablet id以及schema hash信息与传入的ablet id以及schema hash是否一致
        LOG(WARNING) << "fail to load tablet because meet invalid tablet meta. "
                     << "trying to load tablet(tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash << ")"
                     << ", but meet tablet=" << tablet_meta->full_name()
                     << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }
    if (tablet_meta->tablet_uid().hi == 0 && tablet_meta->tablet_uid().lo == 0) {//判断解析出的uid是否为0
        LOG(WARNING) << "fail to load tablet because its uid == 0. "
                     << "tablet=" << tablet_meta->full_name()
                     << ", path=" << data_dir->path();
        return OLAP_ERR_HEADER_PB_PARSE_FAILED;
    }

    if (restore) {
        // we're restoring tablet from trash, tablet state should be changed from shutdown back to running
        tablet_meta->set_tablet_state(TABLET_RUNNING);//将tablet的状态设为running，从trash中恢复已经删除的tablet
    }

    TabletSharedPtr tablet = Tablet::create_tablet_from_meta(tablet_meta, data_dir);//*****通过tablet meta创建tablet*****
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to load tablet. tablet_id=" << tablet_id
                     << ", schema_hash:" << schema_hash;
        return OLAP_ERR_TABLE_CREATE_FROM_HEADER_ERROR;
    }

    if (tablet_meta->tablet_state() == TABLET_SHUTDOWN) {//判断tablet是否已经被删除
        LOG(INFO) << "fail to load tablet because it is to be deleted. tablet_id=" << tablet_id
                  << " schema_hash=" << schema_hash << ", path=" << data_dir->path();
        {
            WriteLock shutdown_tablets_wlock(&_shutdown_tablets_lock);
            _shutdown_tablets.push_back(tablet);
        }
        return OLAP_ERR_TABLE_ALREADY_DELETED_ERROR;
    }
    // NOTE: We do not check tablet's initial version here, because if BE restarts when
    // one tablet is doing schema-change, we may meet empty tablet.
    if (tablet->max_version().first == -1 && tablet->tablet_state() == TABLET_RUNNING) {
        LOG(WARNING) << "fail to load tablet. it is in running state but without delta. "
                     << "tablet=" << tablet->full_name() << ", path=" << data_dir->path();
        // tablet state is invalid, drop tablet
        return OLAP_ERR_TABLE_INDEX_VALIDATE_ERROR;
    }

    RETURN_NOT_OK_LOG(tablet->init(), Substitute("tablet init failed. tablet=$0", tablet->full_name()));//tablet初始化
    RETURN_NOT_OK_LOG(_add_tablet_unlocked(tablet_id, schema_hash, tablet, update_meta, force),         //向Tabletanager中添加创建的tablet
                      Substitute("fail to add tablet. tablet=$0", tablet->full_name()));

    return OLAP_SUCCESS;
}
/************************************************************************************************************************/
//通过路径加载tablet。首先，根据tablet_id以及schema_hash_path生成tablet meta的存储路径；然后，加载tablet meta；最后，根据tablet meta加载tablet。
OLAPStatus TabletManager::load_tablet_from_dir(DataDir* store, TTabletId tablet_id, SchemaHash schema_hash, const string& schema_hash_path, bool force, bool restore) {
    LOG(INFO) << "begin to load tablet from dir. "
              << " tablet_id=" << tablet_id
              << " schema_hash=" << schema_hash
              << " path = " << schema_hash_path
              << " force = " << force
              << " restore = " << restore;
    //（1）首先，根据tablet_id以及schema_hash_path生成tablet meta的存储路径
    // not add lock here, because load_tablet_from_meta already add lock
    string header_path = TabletMeta::construct_header_file_path(schema_hash_path, tablet_id);//构建header文件的路径
    // should change shard id before load tablet
    string shard_path = path_util::dir_name(path_util::dir_name(path_util::dir_name(header_path)));//构建shard路径
    string shard_str = shard_path.substr(shard_path.find_last_of('/') + 1);
    int32_t shard = stol(shard_str);
    // load dir is called by clone, restore, storage migration
    // should change tablet uid when tablet object changed
    RETURN_NOT_OK_LOG(TabletMeta::reset_tablet_uid(header_path), Substitute(
            "failed to set tablet uid when copied meta file. header_path=%0", header_path));;

    if (!Env::Default()->path_exists(header_path).ok()) {//判断header文件是否存在
        LOG(WARNING) << "fail to find header file. [header_path=" << header_path << "]";
        return OLAP_ERR_FILE_NOT_EXIST;
    }

    //（2）然后，根据header路径加载tablet meta
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    if (tablet_meta->create_from_file(header_path) != OLAP_SUCCESS) {//加载header文件，即tablet mata文件
        LOG(WARNING) << "fail to load tablet_meta. file_path=" << header_path;
        return OLAP_ERR_ENGINE_LOAD_INDEX_TABLE_ERROR;
    }
    // has to change shard id here, because meta file maybe copyed from other source its shard is different from local shard
    tablet_meta->set_shard_id(shard);//设置tablet meta中的shard信息
    string meta_binary;
    tablet_meta->serialize(&meta_binary);//将tablet meta信息序列化到meta_binary中

    //（3）最后，根据tablet meta加载tablet
    RETURN_NOT_OK_LOG(load_tablet_from_meta(store, tablet_id, schema_hash, meta_binary, true, force, restore),//根据tablet meta加载tablet
            Substitute("fail to load tablet. header_path=$0", header_path));

    return OLAP_SUCCESS;
}

/*释放schema change锁*/
void TabletManager::release_schema_change_lock(TTabletId tablet_id) {
    VLOG(3) << "release_schema_change_lock begin. tablet_id=" << tablet_id;
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    ReadLock rlock(&tablet_map_lock);

    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//根据tablet id获取tablet_map
    tablet_map_t::iterator it = tablet_map.find(tablet_id);//从tablet map中查找Key为tablet id的元素
    if (it == tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet=" << tablet_id;
    } else {
        it->second.schema_change_lock.unlock();//释放schema change锁
    }
    VLOG(3) << "release_schema_change_lock end. tablet_id=" << tablet_id;
}

/*获取单个tablet的各项报告信息，通过参数tablet_info传回*/
OLAPStatus TabletManager::report_tablet_info(TTabletInfo* tablet_info) {
    DorisMetrics::instance()->report_tablet_requests_total.increment(1);
    LOG(INFO) << "begin to process report tablet info."
              << "tablet_id=" << tablet_info->tablet_id
              << ", schema_hash=" << tablet_info->schema_hash;

    OLAPStatus res = OLAP_SUCCESS;

    TabletSharedPtr tablet = get_tablet(tablet_info->tablet_id, tablet_info->schema_hash);//根据tablet id和schema hash获取tablet
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. " << " tablet=" << tablet_info->tablet_id
                     << " schema_hash=" << tablet_info->schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    tablet->build_tablet_report_info(tablet_info);//获取tablet的各项信息，通过参数tablet_info传回
    VLOG(10) << "success to process report tablet info.";
    return res;
}

/*获取所有tablet的各项信息，通过std::map<TTabletId, TTablet>*类型的参数tablets_info传回*/
OLAPStatus TabletManager::report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info) {
    DCHECK(tablets_info != nullptr);
    LOG(INFO) << "begin to report all tablets info";

    // build the expired txn map first, outside the tablet map lock
    std::map<TabletInfo, std::vector<int64_t>> expire_txn_map;
    StorageEngine::instance()->txn_manager()->build_expire_txn_map(&expire_txn_map);
    LOG(INFO) << "find expired transactions for " << expire_txn_map.size() << " tablets";

    DorisMetrics::instance()->report_all_tablets_requests_total.increment(1);

    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {
        ReadLock rlock(&_tablet_map_lock_array[i]);
        tablet_map_t& tablet_map = _tablet_map_array[i];
        for (const auto& item : tablet_map) {
            if (item.second.table_arr.size() == 0) {
                continue;
            }

            uint64_t tablet_id = item.first;
            TTablet t_tablet;
            for (TabletSharedPtr tablet_ptr : item.second.table_arr) {
                TTabletInfo tablet_info;
                tablet_ptr->build_tablet_report_info(&tablet_info);

                // find expired transaction corresponding to this tablet
                TabletInfo tinfo(tablet_id, tablet_ptr->schema_hash(), tablet_ptr->tablet_uid());
                auto find = expire_txn_map.find(tinfo);
                if (find != expire_txn_map.end()) {
                    tablet_info.__set_transaction_ids(find->second);
                    expire_txn_map.erase(find);
                }
                t_tablet.tablet_infos.push_back(tablet_info);
            }

            if (!t_tablet.tablet_infos.empty()) {
                tablets_info->emplace(tablet_id, t_tablet);
            }
        }
    }
    LOG(INFO) << "success to report all tablets info. tablet_count=" << tablets_info->size();
    return OLAP_SUCCESS;
}

/*垃圾清理。
（1）删除每一个tablet中到期的rowset
（2）清除表实例中tablet数目为0的tablet
（3）删除shutdown状态的tablet，也就是已经被删除的tablet
*/
OLAPStatus TabletManager::start_trash_sweep() {
    {
        std::vector<int64_t> tablets_to_clean;//保存需要清除的tablet
        std::vector<TabletSharedPtr> all_tablets; // 保存所有的tablet   we use this vector to save all tablet ptr for saving lock time.

        //将所有的tablet添加到向量all_tablets中
        for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {
            tablet_map_t& tablet_map = _tablet_map_array[i];
            {
                ReadLock r_lock(&_tablet_map_lock_array[i]);
                for (auto& item : tablet_map) {
                    // try to clean empty item
                    if (item.second.table_arr.empty()) {//判断该item的表实例中tablet数目为0
                        tablets_to_clean.push_back(item.first);
                    }
                    for (TabletSharedPtr tablet : item.second.table_arr) {
                        all_tablets.push_back(tablet);
                    }
                }
            }

            for (const auto& tablet : all_tablets) {
                tablet->delete_expired_inc_rowsets();//遍历所有的tablet，并删除每一个tablet中到期的rowset
            }
            all_tablets.clear();//清空向量all_tablets

            if (!tablets_to_clean.empty()) {
                WriteLock w_lock(&_tablet_map_lock_array[i]);
                // clean empty tablet id item
                for (const auto& tablet_id_to_clean : tablets_to_clean) {
                    auto& item = tablet_map[tablet_id_to_clean];
                    if (item.table_arr.empty()) {
                        // try to get schema change lock if could get schema change lock, then nobody
                        // own the lock could remove the item
                        // it will core if schema change thread may hold the lock and this thread will deconstruct lock
                        if (item.schema_change_lock.trylock() == OLAP_SUCCESS) {
                            item.schema_change_lock.unlock();
                            tablet_map.erase(tablet_id_to_clean);//从tablet map中删除需要清除的tablet
                        }
                    }
                }
                tablets_to_clean.clear(); //清空向量tablets_to_clean We should clear the vector before next loop
            }
        }
    }

    int32_t clean_num = 0;
    do {
        sleep(1);
        clean_num = 0;
        // should get write lock here, because it will remove tablet from shut_down_tablets
        // and get tablet will access shut_down_tablets
        WriteLock wlock(&_shutdown_tablets_lock);
        auto it = _shutdown_tablets.begin();
        while (it != _shutdown_tablets.end()) {
            // check if the meta has the tablet info and its state is shutdown
            if (it->use_count() > 1) {//判断当前的tablet是否正在被其他线程引用
                // it means current tablet is referenced by other thread
                ++it;
                continue;
            }
            //当前的tablet没有被其他线程引用
            TabletMetaSharedPtr tablet_meta(new TabletMeta());
            OLAPStatus check_st = TabletMetaManager::get_meta((*it)->data_dir(), (*it)->tablet_id(), (*it)->schema_hash(), tablet_meta);//获取tablet meta，结果通过参数tablet_meta传回
            if (check_st == OLAP_SUCCESS) {
                if (tablet_meta->tablet_state() != TABLET_SHUTDOWN || tablet_meta->tablet_uid() != (*it)->tablet_uid()) {//tablet状态变为正常
                    LOG(WARNING) << "tablet's state changed to normal, skip remove dirs"
                                << " tablet id = " << tablet_meta->tablet_id()
                                << " schema hash = " << tablet_meta->schema_hash()
                                << " old tablet_uid=" << (*it)->tablet_uid()
                                << " cur tablet_uid=" << tablet_meta->tablet_uid();
                    // remove it from list
                    it = _shutdown_tablets.erase(it);//从成员变量_shutdown_tablets移除状态变为正常的tablet
                    continue;
                }
                // move data to trash
                string tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    // take snapshot of tablet meta
                    string meta_file_path = path_util::join_path_segments((*it)->tablet_path(), std::to_string((*it)->tablet_id()) + ".hdr");
                    (*it)->tablet_meta()->save(meta_file_path);//保存tablet meta，对tablet meta保存快照
                    LOG(INFO) << "start to move tablet to trash. tablet_path = " << tablet_path;
                    OLAPStatus rm_st = move_to_trash(tablet_path, tablet_path);//将tablet移除trash，第一个参数用于计算trash的位置，第二个参数指要删除的文件
                    if (rm_st != OLAP_SUCCESS) {
                        LOG(WARNING) << "fail to move dir to trash. dir=" << tablet_path;
                        ++it;
                        continue;
                    }
                }
                // remove tablet meta
                TabletMetaManager::remove((*it)->data_dir(), (*it)->tablet_id(), (*it)->schema_hash());//删除tablet meta
                LOG(INFO) << "successfully move tablet to trash. "
                          << "tablet_id=" << (*it)->tablet_id()
                          << ", schema_hash=" << (*it)->schema_hash()
                          << ", tablet_path=" << tablet_path;
                it = _shutdown_tablets.erase(it);//从成员变量_shutdown_tablets移除被清理后的tablet
                ++ clean_num;
            } else {
                // if could not find tablet info in meta store, then check if dir existed
                string tablet_path = (*it)->tablet_path();
                if (Env::Default()->path_exists(tablet_path).ok()) {
                    LOG(WARNING) << "errors while load meta from store, skip this tablet. "
                                 << "tablet_id=" << (*it)->tablet_id()
                                 << ", schema_hash=" << (*it)->schema_hash();
                    ++it;
                } else {
                    LOG(INFO) << "could not find tablet dir, skip it and remove it from gc-queue. "
                            << "tablet_id=" << (*it)->tablet_id()
                            << ", schema_hash=" << (*it)->schema_hash()
                            << ", tablet_path=" << tablet_path;
                    it = _shutdown_tablets.erase(it);
                }
            }

            // yield to avoid hoding _tablet_map_lock for too long
            if (clean_num >= 200) {
                break;
            }
        }
    } while (clean_num >= 200);
    return OLAP_SUCCESS;
} // start_trash_sweep


/*锁定tablet的schema change*/
bool TabletManager::try_schema_change_lock(TTabletId tablet_id) {
    bool res = false;
    VLOG(3) << "try_schema_change_lock begin. tablet_id=" << tablet_id;
    RWMutex& tablet_map_lock = _get_tablet_map_lock(tablet_id);
    ReadLock rlock(&tablet_map_lock);
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);
    tablet_map_t::iterator it = tablet_map.find(tablet_id);
    if (it == tablet_map.end()) {
        LOG(WARNING) << "tablet does not exists. tablet_id=" << tablet_id;
    } else {
        res = (it->second.schema_change_lock.trylock() == OLAP_SUCCESS);//锁定tablet的schema change
    }
    VLOG(3) << "try_schema_change_lock end. tablet_id=" <<  tablet_id;
    return res;
}

/*更新所有磁盘被占用的容量信息*/
void TabletManager::update_root_path_info(std::map<string, DataDirInfo>* path_map, size_t* tablet_count) {
    DCHECK(tablet_count != 0);//DCHECK是 Chromium/base 库提供的 断言，如果断言失败，运行着的程序会立即终止。（？？？为什么不是*tablet_count != 0？？？）
    *tablet_count = 0;
    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {//遍历所有的shard
        ReadLock rlock(&_tablet_map_lock_array[i]);
        for (auto& entry : _tablet_map_array[i]) {//遍历shard中第i个tablet map，entry是1次tablet id到TableInstances的映射
            const TableInstances& instance = entry.second;
            for (auto& tablet : instance.table_arr) {//遍历每一个TableInstances中保存的所有tablet
                ++(*tablet_count);
                int64_t data_size = tablet->tablet_footprint();//获取tablet的大小
                auto iter = path_map->find(tablet->data_dir()->path());//通过tablet的path在path_map中查找data dir信息
                if (iter == path_map->end()) {
                    continue;
                }
                if (iter->second.is_used) {//判断当前data dir是否可用
                    iter->second.data_used_capacity += data_size;//向磁盘的占用容量中添加当前tablet的大小
                }
            }
        }
    }
}

/*获取某一个partition相关的tablet，结果通过参数tablet_infos返回*/
void TabletManager::get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos) {
    ReadLock rlock(&_partition_tablet_map_lock);
    if (_partition_tablet_map.find(partition_id) != _partition_tablet_map.end()) {
        *tablet_infos = _partition_tablet_map[partition_id];
    }
}

/*对所有处于running状态的tablet的meta进行checkpoint*/
void TabletManager::do_tablet_meta_checkpoint(DataDir* data_dir) {
    vector<TabletSharedPtr> related_tablets;
    {
        for (int32 i = 0 ; i < _tablet_map_lock_shard_size; i++) {
            ReadLock tablet_map_rdlock(&_tablet_map_lock_array[i]);
            for (tablet_map_t::value_type& table_ins : _tablet_map_array[i]){
                for (TabletSharedPtr& tablet_ptr : table_ins.second.table_arr) {
                    //遍历每一个tablet
                    if (tablet_ptr->tablet_state() != TABLET_RUNNING) {
                        continue;
                    }

                    if (tablet_ptr->data_dir()->path_hash() != data_dir->path_hash() || !tablet_ptr->is_used() || !tablet_ptr->init_succeeded()) {
                        continue;
                    }
                    related_tablets.push_back(tablet_ptr);
                }
            }
        }
    }
    for (TabletSharedPtr tablet : related_tablets) {
        tablet->do_tablet_meta_checkpoint();//依次对各个tablet进行checkpoint
    }
    return;
}

/*获取所有tablet的状态信息，保存在成员变量_tablet_stat_cache中*/
void TabletManager::_build_tablet_stat() {
    _tablet_stat_cache.clear();//清除所有tablet的状态信息
    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {//遍历所有的shard
        ReadLock rdlock(&_tablet_map_lock_array[i]);
        for (const auto& item : _tablet_map_array[i]) {//遍历每一个shard中的所有tablet map（每一个tablet id对应一个tablet map）
            if (item.second.table_arr.size() == 0) {
                continue;
            }

            TTabletStat stat;
            stat.tablet_id = item.first;
            for (TabletSharedPtr tablet : item.second.table_arr) {//遍历每一个tablet map中表实例下的所有tablet
                // TODO(lingbin): if it is nullptr, why is it not deleted?
                if (tablet == nullptr) {
                    continue;
                }
                stat.__set_data_size(tablet->tablet_footprint());//tablet的数据大小
                stat.__set_row_num(tablet->num_rows());//tablet的行数
                VLOG(3) << "building tablet stat. tablet_id=" << item.first
                        << ", data_size=" << tablet->tablet_footprint()
                        << ", row_num=" << tablet->num_rows();
                break;
            }

            _tablet_stat_cache.emplace(item.first, stat);//保存tablet的状态信息，
        }
    }
}

/*创建初始的rowset*/
OLAPStatus TabletManager::_create_inital_rowset_unlocked(const TCreateTabletReq& request, Tablet* tablet) {
    OLAPStatus res = OLAP_SUCCESS;
    if (request.version < 1) {
        LOG(WARNING) << "init version of tablet should at least 1. req.ver=" << request.version;
        return OLAP_ERR_CE_CMD_PARAMS_ERROR;
    } else {
        Version version(0, request.version);
        VLOG(3) << "begin to create init version. version=" << version;
        RowsetSharedPtr new_rowset;
        do {
            RowsetWriterContext context;
            context.rowset_id = StorageEngine::instance()->next_rowset_id();
            context.tablet_uid = tablet->tablet_uid();
            context.tablet_id = tablet->tablet_id();
            context.partition_id = tablet->partition_id();
            context.tablet_schema_hash = tablet->schema_hash();
            if (!request.__isset.storage_format || request.storage_format == TStorageFormat::DEFAULT) {
                context.rowset_type = StorageEngine::instance()->default_rowset_type();
            } else if (request.storage_format == TStorageFormat::V1){
                context.rowset_type = RowsetTypePB::ALPHA_ROWSET;
            } else if (request.storage_format == TStorageFormat::V2) {
                context.rowset_type = RowsetTypePB::BETA_ROWSET;
            } else {
                LOG(ERROR) << "invalid TStorageFormat: " << request.storage_format;
                DCHECK(false);
                context.rowset_type = StorageEngine::instance()->default_rowset_type();
            }
            context.rowset_path_prefix = tablet->tablet_path();
            context.tablet_schema = &(tablet->tablet_schema());
            context.rowset_state = VISIBLE;
            context.version = version;
            context.version_hash = request.version_hash;
            // there is no data in init rowset, so overlapping info is unknown.
            context.segments_overlap = OVERLAP_UNKNOWN;

            std::unique_ptr<RowsetWriter> builder;
            res = RowsetFactory::create_rowset_writer(context, &builder);//创建rowset writer，其中会初始化RowsetWriterContext对象
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to init rowset writer for tablet " << tablet->full_name();
                break;
            }
            res = builder->flush();//将buffer中行flush到segment文件中
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to flush rowset writer for tablet " << tablet->full_name();
                break;
            }

            new_rowset = builder->build();//build之后，会返回一个指向完成build的rowset的指针
            res = tablet->add_rowset(new_rowset, false);//向tablet中添加rowset
            if (res != OLAP_SUCCESS) {
                LOG(WARNING) << "failed to add rowset for tablet " << tablet->full_name();
                break;
            }
        } while (0);

        // Unregister index and delete files(index and data) if failed
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to create initial rowset. res=" << res << " version=" << version;
            StorageEngine::instance()->add_unused_rowset(new_rowset);//如果向tablet中添加rowset失败，需要将rowset设置为不可用rowset
            return res;
        }
    }
    tablet->set_cumulative_layer_point(request.version + 1);//设置tablet中cumulative compaction的合并点
    // NOTE: should not save tablet meta here, because it will be saved if add to map successfully

    return res;
}

/*创建tablet meta*/
OLAPStatus TabletManager::_create_tablet_meta_unlocked(const TCreateTabletReq& request, DataDir* store, const bool is_schema_change, const Tablet* base_tablet, TabletMetaSharedPtr* tablet_meta) {
    uint32_t next_unique_id = 0; //保存下一个要添加进来的列的id
    std::unordered_map<uint32_t, uint32_t> col_idx_to_unique_id; //保存每一个列的索引值（即该列是第几列）与该列的id值之间的对应关系
    //对于具有相同tablet id 不同 schema hash 的两个tablet，具有相同列名称的两个column， 这两个列的id值是相同的，但这两个列的索引值可能不同，即这两个列在各自的tablet的schema中的顺序是不同的
    if (!is_schema_change) {

                /*此次请求不是schema change，只是单纯创建新的tablet的请求*/
        for (uint32_t col_idx = 0; col_idx < request.tablet_schema.columns.size(); ++col_idx) {//依次为该tablet schema的每一列设置列id（col_idx表示列索引 column index，即该列是第几列），列id从0开始
            col_idx_to_unique_id[col_idx] = col_idx; //新创建tablet时，每一列的id值就设置为该列的索引值
        }
        next_unique_id = request.tablet_schema.columns.size(); //如果以后 alter schema 需要添加新的列，则新列的id为当前schema最后一列的id值加 1
    } else {

                            /*此次请求为schema change*/
        next_unique_id = base_tablet->next_unique_id(); //为新添加的第 1 列获取列id
        size_t old_num_columns = base_tablet->num_columns(); //获取 base tablet 的列数目
        auto& new_columns = request.tablet_schema.columns;   //获取 alter schema 之后需要产生的tablet所有的列（这些列信息通过request请求的tablet_schema.columns参数传入）
        for (uint32_t new_col_idx = 0; new_col_idx < new_columns.size(); ++new_col_idx) { //遍历新tablet的每一个列
            const TColumn& column = new_columns[new_col_idx];
            // For schema change, compare old_tablet and new_tablet:
            // 1. if column exist in both new_tablet and old_tablet, choose the column's
            //    unique_id in old_tablet to be the column's ordinal number in new_tablet
            // 2. if column exists only in new_tablet, assign next_unique_id of old_tablet
            //    to the new column
            size_t old_col_idx = 0;
            for (old_col_idx = 0 ; old_col_idx < old_num_columns; ++old_col_idx) { //遍历 base tablet 中的每一个列
                const string& old_name = base_tablet->tablet_schema().column(old_col_idx).name(); //根据列索引获取列名称
                if (old_name == column.column_name) {
                    //新tablet中当前的列已经在 base tablet 中存在
                    uint32_t old_unique_id = base_tablet->tablet_schema().column(old_col_idx).unique_id();
                    col_idx_to_unique_id[new_col_idx] = old_unique_id; //将新tablet中该列的id设置为与 base tablet 中相同
                    break;
                }
            }
            // Not exist in old tablet, it is a new added column
            if (old_col_idx == old_num_columns) {
                //新tablet中当前的列在 base tablet 中不存在
                col_idx_to_unique_id[new_col_idx] = next_unique_id++; //将新tablet中当前的列id设置为next_unique_id， 并将 next_unique_id 值增加1
            }
        }
    }
    LOG(INFO) << "creating tablet meta. next_unique_id=" << next_unique_id;

    // We generate a new tablet_uid for this new tablet.
    uint64_t shard_id = 0;
    RETURN_NOT_OK_LOG(store->get_shard(&shard_id), "fail to get root path shard");//在磁盘上获取一个shard供创建tablet时使用，获取结果通过参数传回

    /*
      TabletMeta::create()中使用request请求传入的request.table_id和request.tablet_schema.schema_hash创建tablet meta， 即使当前的request请求为schema change，
      也不会使用 base_tablet_id来创建新的tablet，因此，alter schema生成的tablet的tablet id与base tablet的tablet id是不一样的。
    */
    OLAPStatus res = TabletMeta::create(request, TabletUid::gen_uid(), shard_id, next_unique_id, col_idx_to_unique_id, tablet_meta); //创建table meta

    // TODO(lingbin): when beta-rowset is default, should remove it
    if (request.__isset.storage_format && request.storage_format == TStorageFormat::V2) {
        (*tablet_meta)->set_preferred_rowset_type(BETA_ROWSET); //当request中请求的存储格式是v2时，设置rowset的类型为BETA
    } else {
        (*tablet_meta)->set_preferred_rowset_type(ALPHA_ROWSET);
    }
    return res;
}

/*直接删除tablet。从partition中移除查找到的tablet，从表实例的.table_arr中删除查找到的tablet，将删除的tablet从data dir中注销*/
OLAPStatus TabletManager::_drop_tablet_directly_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files) {
    TabletSharedPtr dropped_tablet = _get_tablet_unlocked(tablet_id, schema_hash);//根据tablet id和schema hash获取待删除的tablet
    if (dropped_tablet == nullptr) {
        LOG(WARNING) << "fail to drop tablet because it does not exist. "
                     << " tablet_id=" << tablet_id
                     << ", schema_hash=" << schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//根据tablet id获取tablet map
    list<TabletSharedPtr>& candidate_tablets = tablet_map[tablet_id].table_arr;//通过tablet map获取tablet id对应的表实例中所有的tablet
    list<TabletSharedPtr>::iterator it = candidate_tablets.begin();
    while (it != candidate_tablets.end()) {//遍历表实例中所有的tablet
        if (!(*it)->equal(tablet_id, schema_hash)) {//查看是否某一个tablet的tablet id以及schema hash与参数传入的tablet id以及schema hash相同
            ++it;
            continue;
        }

        //找到了某一个tablet的tablet id以及schema hash与参数传入的tablet id以及schema hash相同
        TabletSharedPtr tablet = *it;
        _remove_tablet_from_partition(*(*it));//从partition中移除查找到的tablet
        it = candidate_tablets.erase(it);//从表实例的.table_arr中删除查找到的tablet
        if (!keep_files) {//如果不保存源文件
            // drop tablet will update tablet meta, should lock
            WriteLock wrlock(tablet->get_header_lock_ptr());
            LOG(INFO) << "set tablet to shutdown state and remove it from memory. "
                      << "tablet_id=" << tablet_id
                      << ", schema_hash=" << schema_hash
                      << ", tablet_path=" << dropped_tablet->tablet_path();
            // NOTE: has to update tablet here, but must not update tablet meta directly.
            // because other thread may hold the tablet object, they may save meta too.
            // If update meta directly here, other thread may override the meta
            // and the tablet will be loaded at restart time.
            // To avoid this exception, we first set the state of the tablet to `SHUTDOWN`.
            tablet->set_tablet_state(TABLET_SHUTDOWN);//将tablet状态设为shutdown
            tablet->save_meta();//保存tablet meta
            {
                WriteLock wlock(&_shutdown_tablets_lock);
                _shutdown_tablets.push_back(tablet);//将删除的tablet加入成员变量_shutdown_tablets中
            }
        }
    }

    dropped_tablet->deregister_tablet_from_dir();//将删除的tablet从data dir中注销
    return OLAP_SUCCESS;
}

/*根据tablet_id和schema hash从tablet map中查找对应的tablet*/
TabletSharedPtr TabletManager::_get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash) {
    VLOG(3) << "begin to get tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    tablet_map_t& tablet_map = _get_tablet_map(tablet_id);//根据tablet id获取tablet map
    tablet_map_t::iterator it = tablet_map.find(tablet_id);//从tablet map中查找传入的tablet id
    if (it != tablet_map.end()) {
        for (TabletSharedPtr tablet : it->second.table_arr) {//遍历tablet id对应的表实例中的所有tablet
            CHECK(tablet != nullptr) << "tablet is nullptr. tablet_id=" << tablet_id;
            if (tablet->equal(tablet_id, schema_hash)) {//依次判断遍历过程中每一个tablet的tablet id以及schema hash与参数传入的tablet id以及schema hash是否相同
                //如果找到，则将找到的tablet返回
                VLOG(3) << "get tablet success. tablet_id=" << tablet_id
                        << ", schema_hash=" << schema_hash;
                return tablet;
            }
        }
    }

    //遍历结束，还未找到任何一个tablet的tablet id以及schema hash与参数传入的tablet id以及schema hash相同，则返回空指针
    VLOG(3) << "fail to get tablet. tablet_id=" << tablet_id << ", schema_hash=" << schema_hash;
    // Return nullptr tablet if fail
    TabletSharedPtr tablet;
    return tablet;
}

/*将tablet添加到partition中*/
void TabletManager::_add_tablet_to_partition(const Tablet& tablet) {
    WriteLock wlock(&_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].insert(tablet.get_tablet_info());//将tablet的信息添加到对应的partition中（成员变量_partition_tablet_map[partition_id]）
}

/*从partition中移除tablet*/
void TabletManager::_remove_tablet_from_partition(const Tablet& tablet) {
    WriteLock wlock(&_partition_tablet_map_lock);
    _partition_tablet_map[tablet.partition_id()].erase(tablet.get_tablet_info());//从对应的partition中（成员变量_partition_tablet_map[partition_id]）删除tablet的信息
    //从partition中移除tablet之后，要判断当前partition中的是否为空，如果为空，则删除当前的partition（从std::map<int64_t, std::set<TabletInfo>>类型的成员变量_partition_tablet_map中删除当前的partition id）
    if (_partition_tablet_map[tablet.partition_id()].empty()) {
        _partition_tablet_map.erase(tablet.partition_id());
    }
}


/*根据节点id获取对应节点上的所有tablet*/
void TabletManager::obtain_all_tablets(vector<TabletInfo> &tablets_info) {
    for (int32 i = 0; i < _tablet_map_lock_shard_size; i++) {//遍历所有的lock_shard
        ReadLock rdlock(&_tablet_map_lock_array[i]);
        for (const auto& item : _tablet_map_array[i]) {//遍历每一个lock_shard对应的tablet map, _tablet_map_array[i]表示一个tablet map，每个item表示map中的一个元素
            if (item.second.table_arr.size() == 0) {
                continue;
            }

            for (TabletSharedPtr tablet : item.second.table_arr) {//遍历tablet id为item.first的所有tablet
                // TODO(lingbin): if it is nullptr, why is it not deleted?
                if (tablet == nullptr) {
                    continue;
                }

                TabletInfo* tablet_info = new TabletInfo(tablet->get_tablet_info().tablet_id, tablet->get_tablet_info().schema_hash, tablet->get_tablet_info().tablet_uid);//创建TabletInfo对象
                tablets_info.emplace_back(*tablet_info);
            }

        }
    }
    LOG(INFO) << "success to get all tablets info in the BE node. tablet_count=" << tablets_info.size();
}

} // end namespace doris
