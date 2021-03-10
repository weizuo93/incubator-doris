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

#pragma once

namespace doris {

class Status;

class StreamLoadRecord {
public:
    StreamLoadRecord(const std::string& root_path);

    virtual ~StreamLoadRecord();

    Status init();

    Status put(const int column_family_index, const std::string& key, const std::string& value);

    Status get_batch(const int column_family_index, const std::string& start, const int batch_size);

    Status remove(const int column_family_index, const std::string& key);



private:
    std::string _root_path;
    rocksdb::DB* _db;
    std::vector<rocksdb::ColumnFamilyHandle*> _handles;
};

} // namespace doris
