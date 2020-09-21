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

#include "olap/compaction_permit_limiter.h"

namespace doris {

uint32_t CompactionPermitLimiter::_total_permits;
uint32_t CompactionPermitLimiter::_used_permits;
std::mutex CompactionPermitLimiter::_mutex;

OLAPStatus CompactionPermitLimiter::init(uint32_t total_permits) {
    _total_permits = total_permits;
    _used_permits = 0;
    return OLAP_SUCCESS;
}

bool CompactionPermitLimiter::request(uint32_t permits) {
    while ((_total_permits - _used_permits) < permits) {
        sleep(5);
    }
    std::unique_lock<std::mutex> lock(_mutex);
    _used_permits += permits;
    return true;
}

OLAPStatus CompactionPermitLimiter::release(uint32_t permits) {
    std::unique_lock<std::mutex> lock(_mutex);
    _used_permits = _used_permits - permits;
    return OLAP_SUCCESS;
}
}  // namespace doris