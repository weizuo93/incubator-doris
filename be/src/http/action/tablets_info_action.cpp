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

#include "http/action/tablets_info_action.h"

#include <string>

#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "service/backend_options.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletsInfoAction::TabletsInfoAction() {
    _host = BackendOptions::get_localhost();
}

void TabletsInfoAction::handle(HttpRequest *req) {
    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, get_tablets_info().ToString());
}

EasyJson TabletsInfoAction::get_tablets_info() {
    EasyJson tablets_info_ej;
    tablets_info_ej["msg"] = "OK";
    tablets_info_ej["code"] = 0;
    EasyJson data = tablets_info_ej.Set("data", EasyJson::kObject);
    data["host"] = _host;
    std::vector<TabletInfo> tablets_info;
    TabletManager* tablet_manager = StorageEngine::instance()->tablet_manager();
    tablet_manager->obtain_all_tablets(tablets_info);
    EasyJson tablets = data.Set("tablets", EasyJson::kArray);
    for(int i = 0; i < tablets_info.size(); i++) {
        EasyJson tablet = tablets.PushBack(EasyJson::kObject);
        tablet["tablet_id"] = tablets_info[i].tablet_id;
        tablet["schema_hash"] = tablets_info[i].schema_hash;
    }
    tablets_info_ej["count"] = tablets_info.size();
    return tablets_info_ej;
}
} // namespace doris

