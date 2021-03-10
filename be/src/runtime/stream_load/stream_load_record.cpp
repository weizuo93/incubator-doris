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

#include "runtime/stream_load/stream_load_record.h"

#include "common/status.h"
#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"

using rocksdb::DB;
using rocksdb::DBOptions;
using rocksdb::ColumnFamilyDescriptor;
using rocksdb::ColumnFamilyHandle;
using rocksdb::ColumnFamilyOptions;
using rocksdb::ReadOptions;
using rocksdb::WriteOptions;
using rocksdb::Slice;
using rocksdb::Iterator;
using rocksdb::kDefaultColumnFamilyName;
using rocksdb::NewFixedPrefixTransform;

namespace doris {
const std::string STREAM_LOAD_POSTFIX = "/stream_load";
const size_t PREFIX_LENGTH = 4;

StreamLoadRecord::StreamLoadRecord(const std::string& root_path)
        : _root_path(root_path),
          _db(nullptr) {
}

StreamLoadRecord::~StreamLoadRecord() {
    for (auto handle : _handles) {
        _db->DestroyColumnFamilyHandle(handle);
        handle = nullptr;
    }
    if (_db != nullptr) {
        delete _db;
        _db= nullptr;
    }
}

Status StreamLoadRecord::init() {
    // init db
    DBOptions options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::string db_path = _root_path + STREAM_LOAD_POSTFIX;
    std::vector<ColumnFamilyDescriptor> column_families;
    // default column family is required
    column_families.emplace_back(DEFAULT_COLUMN_FAMILY, ColumnFamilyOptions());
    // stream load column family add prefix extractor to improve performance and ensure correctness
    ColumnFamilyOptions stream_load_column_family;
    stream_load_column_family.prefix_extractor.reset(NewFixedPrefixTransform(PREFIX_LENGTH));
    column_families.emplace_back(STREAM_LOAD_COLUMN_FAMILY, stream_load_column_family);
    rocksdb::Status s = DB::Open(options, db_path, column_families, &_handles, &_db);
    if (!s.ok() || _db == nullptr) {
        LOG(WARNING) << "rocks db open failed, reason:" << s.ToString();
        return Status::InternalError("Stream load record rocksdb open failed");
    }
    return Status::OK();
}

Status StreamLoadRecord::put(const int column_family_index, const std::string& key, const std::string& value) {
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    WriteOptions write_options;
    write_options.sync = config::sync_tablet_meta;
    rocksdb::Status s = _db->Put(write_options, handle, Slice(key), Slice(value));
    if (!s.ok()) {
        LOG(WARNING) << "rocks db put key:" << key << " failed, reason:" << s.ToString();
        return Status::InternalError("Stream load record rocksdb put failed");
    }
    return Status::OK();
}

Status StreamLoadRecord::get_batch(const int column_family_index, const std::string& start, const int batch_size, std::map<string, string> &stream_load_records) {
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    std::unique_ptr<Iterator> it(_db->NewIterator(ReadOptions(), handle));
    if (prefix == "") {
        it->SeekToFirst();
    } else {
        it->Seek(prefix);
    }
    rocksdb::Status status = it->status();
    if (!status.ok()) {
        LOG(WARNING) << "rocksdb seek failed. reason:" << status.ToString();
        return Status::InternalError("Stream load record rocksdb seek failed");
    }
    int num = 0;
    for (; it->Valid(); it->Next()) {
        num++;
        if (num >= batch_size) {
            return Status::OK();
        }
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        stream_load_records[key] = value;
    }
    return Status::OK();
}

Status StreamLoadRecord::remove(const int column_family_index, const std::string& key) {
    rocksdb::ColumnFamilyHandle* handle = _handles[column_family_index];
    WriteOptions write_options;
    write_options.sync = false;
    rocksdb::Status s = _db->Delete(write_options, handle, Slice(key));
    if (!s.ok()) {
        LOG(WARNING) << "rocks db delete key:" << key << " failed, reason:" << s.ToString();
        return Status::InternalError("Stream load record rocksdb delete key failed");
    }
    return Status::OK();
}

} // namespace doris