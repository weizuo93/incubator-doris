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

#include "common/config.h"
#include "common/status.h"
#include "rocksdb/db.h"

#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/utilities/db_ttl.h"
#include "util/time.h"


namespace doris {
const std::string STREAM_LOAD_POSTFIX = "/stream_load";
const size_t PREFIX_LENGTH = 4;

StreamLoadRecord::StreamLoadRecord(const std::string& root_path)
        : _root_path(root_path),
          _db(nullptr),
          _last_compaction_time(UnixMillis()) {
}

StreamLoadRecord::~StreamLoadRecord() {
    if (_db != nullptr) {
        for (auto handle : _handles) {
            _db->DestroyColumnFamilyHandle(handle);
            handle = nullptr;
        }
        delete _db;
        _db= nullptr;
    }
}

Status StreamLoadRecord::init() {
    // init db
    rocksdb::DBOptions options;
    options.IncreaseParallelism();
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    std::string db_path = _root_path + STREAM_LOAD_POSTFIX;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    // default column family is required
    column_families.emplace_back(DEFAULT_COLUMN_FAMILY, rocksdb::ColumnFamilyOptions());
    // stream load column family add prefix extractor to improve performance and ensure correctness
//    rocksdb::ColumnFamilyOptions stream_load_column_family;
//    stream_load_column_family.prefix_extractor.reset(rocksdb::NewFixedPrefixTransform(PREFIX_LENGTH));
//    stream_load_column_family.max_bytes_for_level_base = 2 * 1024 * 1024;
//    stream_load_column_family.write_buffer_size = 1024 * 1024;
//    stream_load_column_family.level0_file_num_compaction_trigger = 2;
//    column_families.emplace_back(STREAM_LOAD_COLUMN_FAMILY, stream_load_column_family);
    column_families.emplace_back(STREAM_LOAD_COLUMN_FAMILY, rocksdb::ColumnFamilyOptions());
    std::vector<int32_t> ttls = {config::stream_load_record_expire_time_secs, config::stream_load_record_expire_time_secs};
    rocksdb::Status s = rocksdb::DBWithTTL::Open(options, db_path, column_families, &_handles, &_db, ttls);

    if (!s.ok() || _db == nullptr) {
        LOG(WARNING) << "rocks db open failed, reason:" << s.ToString();
        return Status::InternalError("Stream load record rocksdb open failed");
    }
    return Status::OK();
}

Status StreamLoadRecord::put(const std::string& key, const std::string& value) {
    rocksdb::ColumnFamilyHandle* handle = _handles[1];
    rocksdb::WriteOptions write_options;
    write_options.sync = false;
    rocksdb::Status s = _db->Put(write_options, handle, rocksdb::Slice(key), rocksdb::Slice(value));
    if (!s.ok()) {
        LOG(WARNING) << "rocks db put key:" << key << " failed, reason:" << s.ToString();
        return Status::InternalError("Stream load record rocksdb put failed");
    }

    if ((UnixMillis() - _last_compaction_time) / 1000 > config::clean_stream_load_record_interval_secs) {
        rocksdb::CompactRangeOptions options;
        s = _db->CompactRange(options, _handles[1], nullptr, nullptr);
        if (s.ok()) {
            _last_compaction_time = UnixMillis();
        }
    }
    return Status::OK();
}

Status StreamLoadRecord::get_batch(const std::string& start, const int batch_size, std::map<std::string, std::string>* stream_load_records) {
    rocksdb::ColumnFamilyHandle* handle = _handles[1];
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions(), handle));
    if (start == "") {
        it->SeekToFirst();
    } else {
        it->Seek(start);
        rocksdb::Status status = it->status();
        if (!status.ok()) {
            it->SeekToFirst();
        }
    }
    rocksdb::Status status = it->status();
    if (!status.ok()) {
        LOG(WARNING) << "rocksdb seek failed. reason:" << status.ToString();
        return Status::InternalError("Stream load record rocksdb seek failed");
    }
    int num = 0;
    for (it->Next(); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        std::string value = it->value().ToString();
        (*stream_load_records)[key] = value;
        num++;
        if (num >= batch_size) {
            return Status::OK();
        }
    }
    return Status::OK();
}

} // namespace doris
