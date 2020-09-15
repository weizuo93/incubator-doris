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

#ifndef DORIS_BE_SRC_OLAP_TABLET_MANAGER_H
#define DORIS_BE_SRC_OLAP_TABLET_MANAGER_H

#include <list>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <vector>
#include <unordered_map>

#include "agent/status.h"
#include "common/status.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/BackendService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/olap_define.h"
#include "olap/tablet.h"
#include "olap/olap_meta.h"
#include "olap/options.h"

namespace doris {

class Tablet;
class DataDir;

/*TabletManager为存储引擎提供了获取，添加，删除tablet等方法*/
// TabletManager provides get, add, delete tablet method for storage engine
// NOTE: If you want to add a method that needs to hold meta-lock before you can call it,
// please uniformly name the method in "xxx_unlocked()" mode
class TabletManager {
public:
    TabletManager(int32_t tablet_map_lock_shard_size);//构造函数？？？tablet_map_lock_shard_size是什么东西？？？
    ~TabletManager();//析构函数

    bool check_tablet_id_exist(TTabletId tablet_id);//检查传入的tablet id是否存在

    // The param stores holds all candidate data_dirs for this tablet.
    // NOTE: If the request is from a schema-changing tablet, The directory selected by the
    // new tablet should be the same as the directory of origin tablet. Because the
    // linked-schema-change type requires Linux hard-link, which does not support cross disk.
    // TODO(lingbin): Other schema-change type do not need to be on the same disk. Because
    // there may be insufficient space on the current disk, which will lead the schema-change
    // task to be fail, even if there is enough space on other disks
    OLAPStatus create_tablet(const TCreateTabletReq& request, std::vector<DataDir*> stores);//创建tablet，参数stores中保存了所有可以进行tablet存储的data dir

    // Drop a tablet by description
    // If set keep_files == true, files will NOT be deleted when deconstruction.
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_DELETE_NOEXIST_ERROR, if tablet not exist
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus drop_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files = false);//根据tablet id，schema hash等描述信息删除tablet，可以选择是否删除文件

    OLAPStatus drop_tablets_on_error_root_path(const std::vector<TabletInfo>& tablet_info_vec);//根据tablet info清除不可用磁盘上的所有tablet

    TabletSharedPtr find_best_tablet_to_compaction(CompactionType compaction_type, DataDir* data_dir);//根据compaction的类型从data dir上寻找最佳的compaction tablet

    TabletSharedPtr get_tablet(TTabletId tablet_id, SchemaHash schema_hash, bool include_deleted = false, std::string* err = nullptr);//根据tablet id，schema hash等信息获取tablet

    TabletSharedPtr get_tablet(TTabletId tablet_id, SchemaHash schema_hash, TabletUid tablet_uid, bool include_deleted = false, std::string* err = nullptr);//根据tablet id，schema hash等信息获取tablet

    // Extract tablet_id and schema_hash from given path.
    //
    // The normal path pattern is like "/data/{shard_id}/{tablet_id}/{schema_hash}/xxx.data".
    // Besides that, this also support empty tablet path, which path looks like
    // "/data/{shard_id}/{tablet_id}"
    //
    // Return true when the path matches the path pattern, and tablet_id and schema_hash is
    // saved in input params. When input path is an empty tablet directory, schema_hash will
    // be set to 0. Return false if the path don't match valid pattern.
    static bool get_tablet_id_and_schema_hash_from_path(const std::string& path, TTabletId* tablet_id, TSchemaHash* schema_hash);//从给定的路径中提取tablet的tablet id和schema hash

    static bool get_rowset_id_from_path(const std::string& path, RowsetId* rowset_id);//从给定的路径中提取rowset的rowset id

    void get_tablet_stat(TTabletStatResult* result);//获取表状态，结果通过类型为TTabletStatResult的参数result返回

    // parse tablet header msg to generate tablet object
    // - restore: whether the request is from restore tablet action,
    //   where we should change tablet status from shutdown back to running
    OLAPStatus load_tablet_from_meta(DataDir* data_dir, TTabletId tablet_id,//通过meta加载tablet
                                     TSchemaHash schema_hash, const std::string& header,
                                     bool update_meta,
                                     bool force = false,
                                     bool restore = false);

    OLAPStatus load_tablet_from_dir(DataDir* data_dir,//通过data dir加载tablet
                                    TTabletId tablet_id,
                                    SchemaHash schema_hash,
                                    const std::string& schema_hash_path,
                                    bool force = false,
                                    bool restore = false);

    void release_schema_change_lock(TTabletId tablet_id);//针对tablet id释放schema change的锁

    // 获取所有tables的名字
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_INPUT_PARAMETER_ERROR, if tables is null
    OLAPStatus report_tablet_info(TTabletInfo* tablet_info);

    OLAPStatus report_all_tablets_info(std::map<TTabletId, TTablet>* tablets_info);

    OLAPStatus start_trash_sweep();//垃圾清除
    // Prevent schema change executed concurrently.
    bool try_schema_change_lock(TTabletId tablet_id);//针对tablet id对schema change加锁

    void update_root_path_info(std::map<std::string, DataDirInfo>* path_map, size_t* tablet_counter);//更新data dir的信息

    void get_partition_related_tablets(int64_t partition_id, std::set<TabletInfo>* tablet_infos);//根据partition id获取相关的tablet信息，通过参数talet_infos传回

    void do_tablet_meta_checkpoint(DataDir* data_dir);//根据data dir针对tablet meta进行checkpoint

    void  obtain_all_tablets(vector<TabletInfo> &tablets_info);//获取当前节点上所有tablet的信息

private:
    // Add a tablet pointer to StorageEngine
    // If force, drop the existing tablet add this new one
    //
    // Return OLAP_SUCCESS, if run ok
    //        OLAP_ERR_TABLE_INSERT_DUPLICATION_ERROR, if find duplication
    //        OLAP_ERR_NOT_INITED, if not inited
    OLAPStatus _add_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                    const TabletSharedPtr& tablet, bool update_meta, bool force);

    OLAPStatus _add_tablet_to_map_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                           const TabletSharedPtr& tablet, bool update_meta,
                                           bool keep_files, bool drop_old);

    bool _check_tablet_id_exist_unlocked(TTabletId tablet_id);
    OLAPStatus _create_inital_rowset_unlocked(const TCreateTabletReq& request,
                                              Tablet* tablet);

    OLAPStatus _drop_tablet_directly_unlocked(TTabletId tablet_id,
                                              TSchemaHash schema_hash,
                                              bool keep_files = false);

    OLAPStatus _drop_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash, bool keep_files);

    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash);
    TabletSharedPtr _get_tablet_unlocked(TTabletId tablet_id, SchemaHash schema_hash,
                                         bool include_deleted, std::string* err);

    TabletSharedPtr _internal_create_tablet_unlocked(const AlterTabletType alter_type,
                                                     const TCreateTabletReq& request,
                                                     const bool is_schema_change,
                                                     const Tablet* base_tablet,
                                                     const std::vector<DataDir*>& data_dirs);
    TabletSharedPtr _create_tablet_meta_and_dir_unlocked(const TCreateTabletReq& request,
                                                         const bool is_schema_change,
                                                         const Tablet* base_tablet,
                                                         const std::vector<DataDir*>& data_dirs);
    OLAPStatus _create_tablet_meta_unlocked(const TCreateTabletReq& request,
                                            DataDir* store,
                                            const bool is_schema_change_tablet,
                                            const Tablet* base_tablet,
                                            TabletMetaSharedPtr* tablet_meta);

    void _build_tablet_stat();

    void _add_tablet_to_partition(const Tablet& tablet);

    void _remove_tablet_from_partition(const Tablet& tablet);

    inline RWMutex& _get_tablet_map_lock(TTabletId tabletId);

private:
    DISALLOW_COPY_AND_ASSIGN(TabletManager);

    // TODO(lingbin): should be TabletInstances?
    // should be removed after schema_hash be removed
    struct TableInstances {//表实例
        Mutex schema_change_lock;
        /* 错误解释：
        // The first element(i.e. tablet_arr[0]) is the base tablet. When we add new tablet
        // to tablet_arr, we will sort all the elements in create-time ascending order,
        // which will ensure the first one is base-tablet
          table_arr中保存了多个tablet，其中第一个元素table_arr[0]为base tablet，也就是最初的tablet。tablet可能会进行alter schema，
          schema 变化之后的tablet也会添加进来，当添加新的schema change之后的tablet时，需要对所有的tablet进行排序，使所有的tablet按
          照创建时间升序排列。同一个TableInstance对象中的所有tablet具有相同的tablet id，但是具有不同的schema hash
        */
        /*正确解释：
          table_arr中保存了多个tablet，这些tablet都具有相同的tablet id和不同的schema hash，但是这些tablet并不是同一个base tablet通过schema change得
          到的，这些tablet都具有相同的tablet id单纯地只是因为FE通过Thrift请求Storagengine在BE上创建tablet时传过来的参数tablet id值是相同的。另外，通过schema change
          之后新创建的tablet的tablet id与base tablet的tablet id是不同的。每次创建tablet的tablet id是什么，只取决于FE通过Thrift发来请求中参数tablet_id的值。
        */
        std::list<TabletSharedPtr> table_arr;
    };
    // tablet_id -> TableInstances
    typedef std::unordered_map<int64_t, TableInstances> tablet_map_t;//typedef类型定义：该类型定义的变量保存了tablet id与表实例（TableInstances）的对应关系

    const int32_t _tablet_map_lock_shard_size;//保存了lock_shard的数量（此处的lock_shard是逻辑概念，与磁盘shard不是一回事，此处是将所有的tablet分配到多个lock_shard中，可以分别加锁，以解决对所有tablets同时加锁的话，锁的力度太大了，锁定的tablet太多）
    // _tablet_map_lock_array[i] protect _tablet_map_array[i], i=0,1,2...,and i < _tablet_map_lock_shard_size
    RWMutex *_tablet_map_lock_array;//数组元素与_tablet_map_array[]一一对应，用来保护_tablet_map_array[]
    tablet_map_t *_tablet_map_array;//每一个元素都保存了一组tablet与表实例（TableInstances）的对应关系

    // Protect _partition_tablet_map, should not be obtained before _tablet_map_lock to avoid dead lock
    RWMutex _partition_tablet_map_lock;
    // Protect _shutdown_tablets, should not be obtained before _tablet_map_lock to avoid dead lock
    RWMutex _shutdown_tablets_lock;
    // partition_id => tablet_info
    std::map<int64_t, std::set<TabletInfo>> _partition_tablet_map;//保存partition id与tablet的对应关系
    std::vector<TabletSharedPtr> _shutdown_tablets;

    std::mutex _tablet_stat_mutex;
    // cache to save tablets' statistics, such as data-size and row-count
    // TODO(cmy): for now, this is a naive implementation
    std::map<int64_t, TTabletStat> _tablet_stat_cache;//保存tablet id与tablet状态之间的对应关系
    // last update time of tablet stat cache
    int64_t _last_update_stat_ms;//tablet状态cache的上一次更新时间

    inline tablet_map_t& _get_tablet_map(TTabletId tablet_id);

    void  obtain_all_tablets(vector<TabletInfo> &tablets_info);
    void get_tablets_distribution_on_different_disks(
            std::map<int64_t, std::map<DataDir*, int>> &tablets_num_on_disk,
            std::map<int64_t, std::map<DataDir*, int>> &tablets_size_on_disk,
            std::map<int64_t, std::map<DataDir*, std::vector<TabletSize>>> &tablets_info_on_disk);
};

inline RWMutex& TabletManager::_get_tablet_map_lock(TTabletId tabletId) {
    return _tablet_map_lock_array[tabletId & (_tablet_map_lock_shard_size - 1)];//？？？？？？不知道_tablet_map_lock_shard_size为什么要减1？是不是与base table有关？？？？？？？？？？？
}

/*
  节点上lock_shard的数量为_tablet_map_lock_shard_size，某一个tablet具体存放在哪一个lock_shard上是通过规则 “tabletId & (_tablet_map_lock_shard_size - 1)” 计算出来的。
  不同的tablet可以通过该规则被映射到相同的lock_shard上。每一个lock_shard对应一个tablet_map。（注意： a & b 的结果一定小于或等于a b两者中较小的数。）
*/
inline TabletManager::tablet_map_t& TabletManager::_get_tablet_map(TTabletId tabletId) {
    return _tablet_map_array[tabletId & (_tablet_map_lock_shard_size - 1)];
}

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_TABLET_MANAGER_H
