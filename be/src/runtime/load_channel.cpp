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

#include "runtime/load_channel.h"

#include "runtime/mem_tracker.h"
#include "runtime/tablets_channel.h"
#include "olap/lru_cache.h"

namespace doris {

LoadChannel::LoadChannel(const UniqueId& load_id, int64_t mem_limit,
                         int64_t timeout_s, MemTracker* mem_tracker) :
        _load_id(load_id), _timeout_s(timeout_s) {
    _mem_tracker.reset(new MemTracker(mem_limit, _load_id.to_string(), mem_tracker));//创建MemTracker对象，并初始化成员变量_mem_tracker
    // _last_updated_time should be set before being inserted to
    // _load_channels in load_channel_mgr, or it may be erased
    // immediately by gc thread.
    _last_updated_time.store(time(nullptr));
}

LoadChannel::~LoadChannel() {
    LOG(INFO) << "load channel mem peak usage=" << _mem_tracker->peak_consumption()
        << ", info=" << _mem_tracker->debug_string()
        << ", load_id=" << _load_id;
}

/*打开LoadChannel，实质上是创建并初始化TabletsChannel对象，更新成员变量_tablets_channels*/
Status LoadChannel::open(const PTabletWriterOpenRequest& params) {
    int64_t index_id = params.index_id(); //获取本次batch加载的id,,PTabletWriterOpenRequest中的元素index_id表示当前load job中一个数据batch的id
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id);
        if (it != _tablets_channels.end()) {
            channel = it->second;
        } else {
            // create a new tablets channel
            TabletsChannelKey key(params.id(), index_id);
            channel.reset(new TabletsChannel(key, _mem_tracker.get())); //创建TabletsChannel对象，并使用其初始化channel
            _tablets_channels.insert({index_id, channel}); //向成员变量_tablets_channels中添加channel, index_id表示一次数据加载过程中的一个batch加载
        }
    }

    RETURN_IF_ERROR(channel->open(params)); //打开TabletsChannel

    _opened = true; //初始化成员变量_opened
    _last_updated_time.store(time(nullptr)); //更新原子变量_last_updated_time
    return Status::OK();
}

/*添加一个batch的数据，数据保存在参数request中*/
Status LoadChannel::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {

    int64_t index_id = request.index_id();
    // 1. get tablets channel
    std::shared_ptr<TabletsChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _tablets_channels.find(index_id); //根据index id在成员变量_tablets_channels中查找tablets channel
        if (it == _tablets_channels.end()) {
            if (_finished_channel_ids.find(index_id) != _finished_channel_ids.end()) {
                // this channel is already finished, just return OK
                return Status::OK();
            }
            std::stringstream ss;
            ss << "load channel " << _load_id << " add batch with unknown index id: " << index_id;
            return Status::InternalError(ss.str());
        }
        channel = it->second;
    }

    // 2. check if mem consumption exceed limit
    handle_mem_exceed_limit(false); //处理内存越界

    // 3. add batch to tablets channel
    if (request.has_row_batch()) {
        RETURN_IF_ERROR(channel->add_batch(request)); //添加一个batch的数据到tablet channel
    }

    // 4. handle eos
    Status st;
    if (request.has_eos() && request.eos()) { //判断当前batch是否为当前sender的最后一个batch
        bool finished = false;
        RETURN_IF_ERROR(channel->close(request.sender_id(), &finished,
                                       request.partition_ids(), tablet_vec)); //关闭tablet channel
        if (finished) {
            std::lock_guard<std::mutex> l(_lock);
            _tablets_channels.erase(index_id); //从成员变量_tablets_channels中清除index_id
            _finished_channel_ids.emplace(index_id); //将index_id添加到成员变量_tablets_channels中
        }
    }
    _last_updated_time.store(time(nullptr)); //更新原子变量_last_updated_time
    return st;
}

/*处理内存越界*/
void LoadChannel::handle_mem_exceed_limit(bool force) {
    // lock so that only one thread can check mem limit
    std::lock_guard<std::mutex> l(_lock);
    if (!(force || _mem_tracker->limit_exceeded())) {
        return;
    }

    if (!force) {
        LOG(INFO) << "reducing memory of " << *this
                  << " because its mem consumption " << _mem_tracker->consumption()
                  << " has exceeded limit " << _mem_tracker->limit();
    }

    std::shared_ptr<TabletsChannel> channel;
    if (_find_largest_consumption_channel(&channel)) { //查找内存占用最大的TabletsChannel
        channel->reduce_mem_usage(); //刷写特定的memtable降低内存占用
    } else {
        // should not happen, add log to observe
        LOG(WARNING) << "fail to find suitable tablets-channel when memory execeed. "
                     << "load_id=" << _load_id;
    }
}

/*获取内存占用量最大的tablets channels*/
// lock should be held when calling this method
bool LoadChannel::_find_largest_consumption_channel(std::shared_ptr<TabletsChannel>* channel) {
    int64_t max_consume = 0;
    for (auto& it : _tablets_channels) {
        if (it.second->mem_consumption() > max_consume) {//依次遍历本次数据加载过程中的每一个batch对应的TabletsChannel对象，并获取占用内存空间最大的TabletsChannel
            max_consume = it.second->mem_consumption();
            *channel = it.second;
        }
    }
    return max_consume > 0;
}

/*获取本次数据加载是否完成，当本次数据加载过程中的所有batch都完成后（每一个batch对应一个index id），才表示本次数据加载完成*/
bool LoadChannel::is_finished() {
    if (!_opened) {
        return false;
    }
    std::lock_guard<std::mutex> l(_lock);
    return _tablets_channels.empty(); //返回成员变量_tablets_channels是否为空
}

/*取消本次数据加载*/
Status LoadChannel::cancel() {
    std::lock_guard<std::mutex> l(_lock);
    for (auto& it : _tablets_channels) {
        it.second->cancel(); //依次遍历本次数据加载过程中的每一个batch对应的TabletsChannel对象，并取消对应的batch加载
    }
    return Status::OK();
}

}
