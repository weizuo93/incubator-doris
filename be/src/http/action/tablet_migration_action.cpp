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

#include "http/action/tablet_migration_action.h"

#include <string>

#include "gutil/strings/substitute.h"
#include "http/http_channel.h"
#include "http/http_headers.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "olap/task/engine_storage_migration_task.h"
#include "util/json_util.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletMigrationAction::TabletMigrationAction() {
}

void TabletMigrationAction::handle(HttpRequest* req) {
    TabletSharedPtr tablet;
    DataDir* dest_store;
    Status status = _check_migrate_request(req, tablet, &dest_store);
    if (status.ok()) {
        status = _execute_tablet_migration(tablet, dest_store);
    }
    std::string status_result = to_json(status);
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, status_result);
}

Status TabletMigrationAction::_execute_tablet_migration(TabletSharedPtr& tablet, DataDir* dest_store) {
    EngineStorageMigrationTask engine_task(tablet, dest_store);
    OLAPStatus res = StorageEngine::instance()->execute_task(&engine_task);
    Status status = Status::OK();
    if (res != OLAP_SUCCESS) {
        LOG(WARNING) << "tablet migrate failed. status: " << res;
        status = Status::InternalError("tablet migrate failed");
    } else {
        LOG(INFO) << "tablet migrate success. status:" << res;
    }
    return status;
}

Status TabletMigrationAction::_check_migrate_request(HttpRequest* req, TabletSharedPtr& tablet, DataDir** dest_store) {
    const std::string& req_tablet_id = req->param("tablet_id");
    const std::string& req_schema_hash = req->param("schema_hash");
    const std::string& req_data_dir = req->param("disk");
    uint64_t tablet_id = 0;
    uint32_t schema_hash = 0;
    try {
        tablet_id = std::stoull(req_tablet_id);
        schema_hash = std::stoul(req_schema_hash);
    } catch (const std::exception& e) {
        LOG(WARNING) << "invalid argument.tablet_id:" << req_tablet_id
                     << ", schema_hash:" << req_schema_hash;
        return Status::InternalError(strings::Substitute("Convert failed, $0", e.what()));
    }

    tablet = StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, schema_hash);
    if (tablet == nullptr) {
        LOG(WARNING) << "no tablet for tablet_id:" << tablet_id << " schema hash:" << schema_hash;
        return Status::NotFound("Tablet not found");
    }

    // request specify the data dir
    *dest_store = StorageEngine::instance()->get_store(req_data_dir);
    if (*dest_store == nullptr) {
        LOG(WARNING) << "data dir not found: " << req_data_dir;
        return Status::NotFound("Disk not found");
    }

    if(tablet->data_dir() == *dest_store) {
        LOG(WARNING) << "tablet already exist in destine disk: " << req_data_dir;
        return Status::AlreadyExist("Tablet already exist in destination disk");
    }

    // check disk capacity
    int64_t tablet_size = tablet->tablet_footprint();
    if ((*dest_store)->reach_capacity_limit(tablet_size)) {
        LOG(WARNING) << "reach the capacity limit of path: " << (*dest_store)->path()
                     << ", tablet size: " << tablet_size;
        return Status::InternalError("Insufficient disk capacity");
    }

    return Status::OK();
}

} // namespace doris
