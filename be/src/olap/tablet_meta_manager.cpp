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

#include "olap/tablet_meta_manager.h"

#include <vector>
#include <sstream>
#include <string>
#include <fstream>
#include <boost/algorithm/string/trim.hpp>

#include "olap/olap_define.h"
#include "olap/storage_engine.h"
#include "olap/olap_meta.h"
#include "common/logging.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Slice;
using rocksdb::Iterator;
using rocksdb::Status;
using rocksdb::kDefaultColumnFamilyName;

namespace doris {

/*根据data dir， tablet id和schema hash获取对应tablet的meta信息，结果通过参数tablet_meta传回*/
// should use tablet's generate tablet meta copy method to get a copy of current tablet meta
// there are some rowset meta in local meta store and in in-memory tablet meta
// but not in tablet meta in local meta store
OLAPStatus TabletMetaManager::get_meta( DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, TabletMetaSharedPtr tablet_meta) {
    OlapMeta* meta = store->get_meta();//获取data dir的meta信息
    std::stringstream key_stream;
    key_stream << HEADER_PREFIX << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();
    std::string value;
    OLAPStatus s = meta->get(META_COLUMN_FAMILY_INDEX, key, &value);//通过Key从data dir上读取tablet meta信息，tablet meta信息为Value（tablet mate信息以K-V形式存储在data dir上）
    if (s == OLAP_ERR_META_KEY_NOT_FOUND) {
        LOG(WARNING) << "tablet_id:" << tablet_id << ", schema_hash:" << schema_hash << " not found.";
        return OLAP_ERR_META_KEY_NOT_FOUND;
    } else if (s != OLAP_SUCCESS) {
        LOG(WARNING) << "load tablet_id:" << tablet_id << ", schema_hash:" << schema_hash << " failed.";
        return s;
    }
    return tablet_meta->deserialize(value);//从字符串value中解析出tablet meta信息
}

/*根据data dir， tablet id和schema hash获取对应tablet的meta信息，结果通过参数json_meta（json格式的字符串）传回*/
OLAPStatus TabletMetaManager::get_json_meta(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, std::string* json_meta) {
    TabletMetaSharedPtr tablet_meta(new TabletMeta());
    OLAPStatus s = get_meta(store, tablet_id, schema_hash, tablet_meta);//根据data dir， tablet id和schema hash获取对应tablet的meta信息，结果保存在参数tablet_meta中
    if (s != OLAP_SUCCESS) {
        return s;
    }
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    tablet_meta->to_json(json_meta, json_options);//获取的tablet meta转化为json
    return OLAP_SUCCESS;
}

/*将tablet mate信息以K-V形式存储在data dir上*/
// TODO(ygl):
// 1. if term > 0 then save to remote meta store first using term
// 2. save to local meta store
OLAPStatus TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, TabletMetaSharedPtr tablet_meta, const string& header_prefix) {
    std::stringstream key_stream;
    key_stream << header_prefix << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();//存储时的Key
    std::string value;
    tablet_meta->serialize(&value);//将tablet meta的各项信息序列化为字符串保存在value中
    OlapMeta* meta = store->get_meta();//获取data dir的meta信息
    LOG(INFO) << "save tablet meta"
              << ", key:" << key
              << ", meta length:" << value.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, value);//将序列化后的tablet meta信息以K-V形式存储在data dir上
}

/*将tablet mate信息以K-V形式存储在data dir上*/
OLAPStatus TabletMetaManager::save(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, const std::string& meta_binary, const string& header_prefix) {
    std::stringstream key_stream;
    key_stream << header_prefix << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();//tablet meta信息存储时的Key
    VLOG(3) << "save tablet meta to meta store: key = " << key;
    OlapMeta* meta = store->get_meta();//获取data dir的meta信息

    // 为了确保传入的字符串格式的tablet meta（参数meta_binary）的格式是正确的，将字符串meta_binary进行反序列化，如果能够成功反序列化，说明传入的字符串格式的tablet meta没有问题。
    TabletMetaPB de_tablet_meta_pb;
    bool parsed = de_tablet_meta_pb.ParseFromString(meta_binary);//从字符串meta_binary中解析（反序列化）tablet meta信息到TabletMetaPB对象中
    if (!parsed) {
        LOG(FATAL) << "deserialize from previous serialize result failed";
    }

    LOG(INFO) << "save tablet meta " 
              << ", key:" << key
              << " meta_size=" << meta_binary.length();
    return meta->put(META_COLUMN_FAMILY_INDEX, key, meta_binary);//将参数传入的tablet meta信息以K-V形式存储在data dir上
}

/*根据Key，从data dir上将tablet mate信息删除*/
// TODO(ygl): 
// 1. remove load data first
// 2. remove from load meta store using term if term > 0
OLAPStatus TabletMetaManager::remove(DataDir* store, TTabletId tablet_id, TSchemaHash schema_hash, const string& header_prefix) {
    std::stringstream key_stream;
    key_stream << header_prefix << tablet_id << "_" << schema_hash;
    std::string key = key_stream.str();//tablet meta信息在data dir上存储时的Key
    OlapMeta* meta = store->get_meta();//获取data dir的meta信息
    LOG(INFO) << "start to remove tablet_meta, key:" << key;
    OLAPStatus res = meta->remove(META_COLUMN_FAMILY_INDEX, key);//根据Key，从data dir上将tablet mate信息删除
    LOG(INFO) << "remove tablet_meta, key:" << key << ", res:" << res;
    return res;
}

/*遍历headers*/
OLAPStatus TabletMetaManager::traverse_headers(OlapMeta* meta, std::function<bool(long, long, const std::string&)> const& func, const string& header_prefix) {
    auto traverse_header_func = [&func](const std::string& key, const std::string& value) -> bool {
        std::vector<std::string> parts;
        // key format: "hdr_" + tablet_id + "_" + schema_hash
        split_string<char>(key, '_', &parts);//使用下划线“—”分割字符串key，并将分割结果保存在变量parts（类型为vector<std::string>）中
        if (parts.size() != 3) {
            LOG(WARNING) << "invalid tablet_meta key:" << key << ", splitted size:" << parts.size();
            return true;
        }
        TTabletId tablet_id = std::stol(parts[1].c_str(), nullptr, 10);//获取key中的tablet id（part[1]）
        TSchemaHash schema_hash = std::stol(parts[2].c_str(), nullptr, 10);//获取key中的schema hash（part[2]）
        return func(tablet_id, schema_hash, value);
    };
    OLAPStatus status = meta->iterate(META_COLUMN_FAMILY_INDEX, header_prefix, traverse_header_func);//？？？？？？？？？？？？？？？？？？
    return status;
}

/*读取特定路径（参数meta_path）上的json文件，并将文件中保存的tablet meta信息以K-V形式存储在data dir上*/
OLAPStatus TabletMetaManager::load_json_meta(DataDir* store, const std::string& meta_path) {
    std::ifstream infile(meta_path);//打开文件
    char buffer[102400];
    std::string json_meta;
    while (!infile.eof()) {//读取文件中json格式的tablet meta数据，并将读出的数据保存在字符串变量json_meta中
        infile.getline(buffer, 102400);
        json_meta = json_meta + buffer;
    }
    boost::algorithm::trim(json_meta);//删除字符串首尾部空格
    TabletMetaPB tablet_meta_pb;
    bool ret = json2pb::JsonToProtoMessage(json_meta, &tablet_meta_pb);//将json字符串转化为protobuf
    if (!ret) {
        return OLAP_ERR_HEADER_LOAD_JSON_HEADER;
    }

    std::string meta_binary;
    tablet_meta_pb.SerializeToString(&meta_binary);//将TabletMetaPB对象序列化为一个字符串，并将序列化结果通过参数meta_binary传回
    TTabletId tablet_id = tablet_meta_pb.tablet_id();//获取TabletMetaPB对象中存储的tablet id
    TSchemaHash schema_hash = tablet_meta_pb.schema_hash();//获取TabletMetaPB对象中存储的schema hash
    return save(store, tablet_id, schema_hash, meta_binary);//调用save()函数将序列化后的tablet mate信息以K-V形式存储在data dir上
}

}
