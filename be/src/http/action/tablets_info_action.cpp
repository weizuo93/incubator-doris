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

#include <sstream>
#include <string>
#include <unistd.h>/* gethostname */
#include <netdb.h> /* struct hostent */
#include <arpa/inet.h> /* inet_ntop */


#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_response.h"
#include "http/http_headers.h"
#include "http/http_status.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "util/easy_json.h"

namespace doris {

const static std::string HEADER_JSON = "application/json";

TabletsInfoAction::TabletsInfoAction(ExecEnv* exec_env) :
        _exec_env(exec_env) {
}

void TabletsInfoAction::handle(HttpRequest *req) {
    
    EasyJson ej;//定义EasyJson对象

    std::string hostname;
    try{
        get_localhost_info(hostname);//获取本机的主机名

    }catch(const std::exception& e){

        LOG(WARNING) << "Error when obtain hostname!";
        return;
    }
    ej["hostname"] = hostname;

    std::vector<TabletInfo> tablets_info;
    try{
        TabletManager* tablet_manager =StorageEngine::instance()->tablet_manager();//根据存储引擎的实例获取TabletManager对象
        tablet_manager->TabletManager::obtain_all_tablets(tablets_info);//通过TabletManager对象调用TabletManager::obtain_all_tablets()来获取当前节点上的所有tablet的信息
    }catch(const std::exception& e){

        LOG(WARNING) << "Error when obtain tablets from node:" << hostname;
        return;
    }

    ej["tablet number"] = tablets_info.size();
    EasyJson tablets = ej.Set("tablets", EasyJson::kArray);

    for(int i=0; i<tablets_info.size(); i++){

        //tablet["NO"] = i + 1;
        EasyJson tablet = tablets.PushBack(EasyJson::kObject);
        tablet["tablet_id"] = tablets_info[i].tablet_id;
        tablet["schema_hash"] = tablets_info[i].schema_hash;

    }

    std::string result = ej.ToString();//将EasyJson对象转换为string类型
    /*
    {"hostname":"tj1-hadoop-doris-staging-be01.kscn","tablet number":30,"tablets":[{"tablet_id":10252,"schema_hash":1520686811},.......,{"tablet_id":10196,"schema_hash":1520686811}]}
    */

    req->add_output_header(HttpHeaders::CONTENT_TYPE, HEADER_JSON.c_str());
    HttpChannel::send_reply(req, HttpStatus::OK, result);//对http请求的响应
#if 0
    HttpResponse response(HttpStatus::OK, HEADER_JSON, &result);
    channel->send_response(response);
#endif
}

/*获取本机的主机名*/
void TabletsInfoAction::get_localhost_info(std::string& hostName){
        char name[256];
        gethostname(name, sizeof(name));
        hostName = name;
}






} // end namespace doris

