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

#include "olap/task/engine_storage_migration_task.h"

#include "olap/snapshot_manager.h"
#include "olap/tablet_meta_manager.h"

namespace doris {

using std::stringstream;

EngineStorageMigrationTask::EngineStorageMigrationTask(TStorageMediumMigrateReq& storage_medium_migrate_req) :
        _storage_medium_migrate_req(storage_medium_migrate_req) {

}

OLAPStatus EngineStorageMigrationTask::execute() {
    return _storage_medium_migrate(
        _storage_medium_migrate_req.tablet_id,
        _storage_medium_migrate_req.schema_hash,
        _storage_medium_migrate_req.storage_medium);
}

/*将tablet迁移到特定介质类型的磁盘上*/
OLAPStatus EngineStorageMigrationTask::_storage_medium_migrate(
        TTabletId tablet_id, TSchemaHash schema_hash,
        TStorageMedium::type storage_medium) {
    LOG(INFO) << "begin to process storage media migrate. "
              << "tablet_id=" << tablet_id << ", schema_hash=" << schema_hash
              << ", dest_storage_medium=" << storage_medium;
    DorisMetrics::instance()->storage_migrate_requests_total.increment(1);

    //根据tablet id和schema hash获取tablet
    OLAPStatus res = OLAP_SUCCESS;
    TabletSharedPtr tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "can't find tablet. tablet_id= " << tablet_id
                     << " schema_hash=" << schema_hash;
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    // judge case when no need to migrate
    uint32_t count = StorageEngine::instance()->available_storage_medium_type_count();//获取当前BE节点上磁盘的数量
    if (count <= 1) {
        LOG(INFO) << "available storage medium type count is less than 1, "
                  << "no need to migrate. count=" << count;
        return OLAP_SUCCESS;
    }

    TStorageMedium::type src_storage_medium = tablet->data_dir()->storage_medium(); //获取tablet当前所在的磁盘的介质类型
    if (src_storage_medium == storage_medium) { //判断tablet当前所在的磁盘类型与要迁移的目的磁盘类型是否相同
        LOG(INFO) << "tablet is already on specified storage medium. "
                  << "storage_medium=" << storage_medium;
        return OLAP_SUCCESS;
    }

    WriteLock migration_wlock(tablet->get_migration_lock_ptr(), TRY_LOCK);
    if (!migration_wlock.own_lock()) {
        return OLAP_ERR_RWLOCK_ERROR;
    }

    //获取tablet相关的transaction
    int64_t partition_id;
    std::set<int64_t> transaction_ids;
    StorageEngine::instance()->txn_manager()->get_tablet_related_txns(tablet->tablet_id(), 
        tablet->schema_hash(), tablet->tablet_uid(), &partition_id, &transaction_ids);
    if (transaction_ids.size() > 0) { //如果存在与当前tablet相关的transaction，则tablet不能进行迁移
        LOG(WARNING) << "could not migration because has unfinished txns, "
                     << " tablet=" << tablet->full_name();
        return OLAP_ERR_HEADER_HAS_PENDING_DATA;
    }

    tablet->obtain_push_lock();

    // TODO(ygl): the tablet should not under schema change or rollup or load
    do {
        // get all versions to be migrate
        tablet->obtain_header_rdlock();
        const RowsetSharedPtr lastest_version = tablet->rowset_with_max_version();//获取tablet最新版本的rowset
        if (lastest_version == nullptr) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            LOG(WARNING) << "tablet has not any version.";
            break;
        }

        int32_t end_version = lastest_version->end_version();
        vector<RowsetSharedPtr> consistent_rowsets;
        res = tablet->capture_consistent_rowsets(Version(0, end_version), &consistent_rowsets);
        if (consistent_rowsets.empty()) {
            tablet->release_header_lock();
            res = OLAP_ERR_VERSION_NOT_EXIST;
            LOG(WARNING) << "fail to capture consistent rowsets. tablet=" << tablet->full_name()
                         << ", version=" << end_version;
            break;
        }
        tablet->release_header_lock();

        // 随机获取特定介质类型的磁盘 get a random store of specified storage medium
        auto stores = StorageEngine::instance()->get_stores_for_create_tablet(storage_medium); //随机获取特定介质类型的磁盘
        if (stores.empty()) {
            res = OLAP_ERR_INVALID_ROOT_PATH;
            LOG(WARNING) << "fail to get root path for create tablet.";
            break;
        }

        // tablet迁移之后是否超过目标磁盘容量上限  check disk capacity
        int64_t tablet_size = tablet->tablet_footprint(); //获取tablet的大小
        if (stores[0]->reach_capacity_limit(tablet_size)) {
            res = OLAP_ERR_DISK_REACH_CAPACITY_LIMIT;
            break;
        }

        // 获取shard   get shard
        uint64_t shard = 0;
        res = stores[0]->get_shard(&shard); //在磁盘上获取一个shard供迁移tablet时使用
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to get root path shard. res=" << res;
            break;
        }

        stringstream root_path_stream;
        root_path_stream << stores[0]->path() << DATA_PREFIX << "/" << shard; // static const std::string DATA_PREFIX = "/data";
        string schema_hash_path = SnapshotManager::instance()->get_schema_hash_full_path(tablet, root_path_stream.str());
        // if dir already exist then return err, it should not happen
        // should not remove the dir directly
        if (FileUtils::check_exist(schema_hash_path)) {
            LOG(INFO) << "schema hash path already exist, skip this path. "
                      << "schema_hash_path=" << schema_hash_path;
            res = OLAP_ERR_FILE_ALREADY_EXIST;
            break;
        }

        //判断目标磁盘上要迁移的tablet是否已经存在
        TabletMetaSharedPtr new_tablet_meta(new(std::nothrow) TabletMeta());
        res = TabletMetaManager::get_meta(stores[0], tablet->tablet_id(), tablet->schema_hash(), new_tablet_meta);
        if (res != OLAP_ERR_META_KEY_NOT_FOUND) {
            LOG(WARNING) << "tablet_meta already exists. "
                         << "data_dir:" << stores[0]->path()
                         << "tablet:" << tablet->full_name();
            res = OLAP_ERR_META_ALREADY_EXIST;
            break;
        }

        //创建tablet的迁移路径
        Status st = FileUtils::create_dir(schema_hash_path);
        if (!st.ok()) {
            res = OLAP_ERR_CANNOT_CREATE_DIR;
            LOG(WARNING) << "fail to create path. path=" << schema_hash_path << ", error:" << st.to_string();
            break;
        }

        // migrate all index and data files but header file
        res = _copy_index_and_data_files(schema_hash_path, tablet, consistent_rowsets);//将consistent_rowsets中的所有rowset的index和数据文件拷贝到schema_hash_path目录下
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "fail to copy index and data files when migrate. res=" << res;
            break;
        }
        tablet->obtain_header_rdlock();
        _generate_new_header(stores[0], shard, tablet, consistent_rowsets, new_tablet_meta);
        tablet->release_header_lock();
        std::string new_meta_file = schema_hash_path + "/" + std::to_string(tablet_id) + ".hdr";
        res = new_tablet_meta->save(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to save meta to path" << new_meta_file;
            break;
        }

        res = TabletMeta::reset_tablet_uid(new_meta_file);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "errors while set tablet uid: '" << new_meta_file;
            break;
        } 

        // it will change rowset id and its create time
        // rowset create time is useful when load tablet from meta to check which tablet is the tablet to load
        res = SnapshotManager::instance()->convert_rowset_ids(schema_hash_path, tablet_id, schema_hash);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert rowset id when do storage migration"
                         << " path = " << schema_hash_path;
            break;
        }

        res = StorageEngine::instance()->tablet_manager()->load_tablet_from_dir(stores[0], 
            tablet_id, schema_hash, schema_hash_path, false);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to load tablet from new path. tablet_id=" << tablet_id
                         << " schema_hash=" << schema_hash
                         << " path = " << schema_hash_path;
            break;
        }

        // if old tablet finished schema change, then the schema change status of the new tablet is DONE
        // else the schema change status of the new tablet is FAILED
        TabletSharedPtr new_tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
        if (new_tablet == nullptr) {
            LOG(WARNING) << "get null tablet. tablet_id=" << tablet_id
                         << " schema_hash=" << schema_hash;
            res = OLAP_ERR_TABLE_NOT_FOUND;
            break;
        }
        AlterTabletTaskSharedPtr alter_task = tablet->alter_task();
        if (alter_task != nullptr) {
            if (alter_task->alter_state() == ALTER_FINISHED) {
                new_tablet->set_alter_state(ALTER_FINISHED);
            } else {
                new_tablet->delete_alter_task();
            }
        }
    } while (0);

    tablet->release_push_lock();

    return res;
}

// TODO(ygl): lost some infomation here, such as cumulative layer point
void EngineStorageMigrationTask::_generate_new_header(
        DataDir* store, const uint64_t new_shard,
        const TabletSharedPtr& tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets,
        TabletMetaSharedPtr new_tablet_meta) {
    DCHECK(store != nullptr);
    tablet->generate_tablet_meta_copy(new_tablet_meta);

    vector<RowsetMetaSharedPtr> rs_metas;
    for (auto& rs : consistent_rowsets) {
        rs_metas.push_back(rs->rowset_meta());
    }
    new_tablet_meta->revise_rs_metas(std::move(rs_metas));
    new_tablet_meta->set_shard_id(new_shard);
    // should not save new meta here, because new tablet may failed
    // should not remove the old meta here, because the new header maybe not valid
    // remove old meta after the new tablet is loaded successfully
}

/*将tablet中的所有rowset的index和数据文件拷贝到schema_hash_path目录下*/
OLAPStatus EngineStorageMigrationTask::_copy_index_and_data_files(
        const string& schema_hash_path,
        const TabletSharedPtr& ref_tablet,
        const std::vector<RowsetSharedPtr>& consistent_rowsets) const {
    OLAPStatus status = OLAP_SUCCESS;
    for (const auto& rs : consistent_rowsets) {
        status = rs->copy_files_to(schema_hash_path); //将每一个rowset对应的文件拷贝到schema_hash_path目录下
        if (status != OLAP_SUCCESS) {
            Status ret = FileUtils::remove_all(schema_hash_path);//如果有一个rowset拷贝失败，则tablet迁移失败，移除schema_hash_path目录
            if (!ret.ok()) {
                LOG(FATAL) << "remove storage migration path failed. "
                           << "schema_hash_path:" << schema_hash_path
                           << " error: " << ret.to_string();
            }
            break;
        }
    }
    return status;
}

} // doris
