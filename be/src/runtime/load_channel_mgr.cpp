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

#include "runtime/load_channel_mgr.h"

#include "gutil/strings/substitute.h"
#include "olap/lru_cache.h"
#include "runtime/load_channel.h"
#include "runtime/mem_tracker.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/stopwatch.hpp"

namespace doris {

// Calculate the total memory limit of all load tasks on this BE
/*计算当前BE节点上设置的所有load tasks的总的内存上限*/
static int64_t calc_process_max_load_memory(int64_t process_mem_limit) {
    if (process_mem_limit == -1) {
        // no limit
        return -1;
    }
    int32_t max_load_memory_percent = config::load_process_max_memory_limit_percent;
    int64_t max_load_memory_bytes = process_mem_limit * max_load_memory_percent / 100;
    return std::min<int64_t>(max_load_memory_bytes, config::load_process_max_memory_limit_bytes);
}

// Calculate the memory limit for a single load job.
/*计算当前BE节点上设置的单个load task的内存上限*/
static int64_t calc_job_max_load_memory(int64_t mem_limit_in_req, int64_t total_mem_limit) {
    // default mem limit is used to be compatible with old request.
    // new request should be set load_mem_limit.
    const int64_t default_load_mem_limit = 2 * 1024 * 1024 * 1024L; // 2GB
    int64_t load_mem_limit = default_load_mem_limit;
    if (mem_limit_in_req != -1) {
        // mem-limit of a certain load should between config::write_buffer_size
        // and total-memory-limit
        load_mem_limit = std::max<int64_t>(mem_limit_in_req, config::write_buffer_size);
        load_mem_limit = std::min<int64_t>(load_mem_limit, total_mem_limit);
    }
    return load_mem_limit;
}

/*计算load task的超时时间*/
static int64_t calc_job_timeout_s(int64_t timeout_in_req_s) {
    int64_t load_channel_timeout_s = config::streaming_load_rpc_max_alive_time_sec;
    if (timeout_in_req_s > 0) {
        load_channel_timeout_s = std::max<int64_t>(load_channel_timeout_s, timeout_in_req_s);
    }
    return load_channel_timeout_s;
}

/*构造函数*/
LoadChannelMgr::LoadChannelMgr() : _is_stopped(false) {
    REGISTER_GAUGE_DORIS_METRIC(load_channel_count, [this]() {
        std::lock_guard<std::mutex> l(_lock);
        return _load_channels.size();
    });// 注册metric： load_channel_count
    _lastest_success_channel = new_lru_cache(1024);
}

/*析构函数*/
LoadChannelMgr::~LoadChannelMgr() {
    _is_stopped.store(true); //更新原子变量_is_stopped
    if (_load_channels_clean_thread.joinable()) { //通过joinable()函数检查线程是否处于活动状态
        _load_channels_clean_thread.join(); //阻塞当前线程，直到线程_load_channels_clean_thread执行结束
    }
    delete _lastest_success_channel;
}

/*初始化LoadChannelMgr对象*/
Status LoadChannelMgr::init(int64_t process_mem_limit) {
    int64_t load_mem_limit = calc_process_max_load_memory(process_mem_limit);
    _mem_tracker.reset(new MemTracker(load_mem_limit, "load channel mgr"));
    RETURN_IF_ERROR(_start_bg_worker()); //启动线程，每间隔一段时间进行一次超时load任务的清理
    return Status::OK();
}

/*根据params打开对应load任务对应的LoadChannel*/
Status LoadChannelMgr::open(const PTabletWriterOpenRequest& params) {
    UniqueId load_id(params.id()); //获取load任务的id,PTabletWriterOpenRequest中的元素id表示当前load job的id
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it != _load_channels.end()) {
            channel = it->second; //对应的LoadChannel对象已经存在
        } else {
            // create a new load channel
            int64_t mem_limit_in_req = params.has_load_mem_limit() ? params.load_mem_limit() : -1;
            int64_t job_max_memory = calc_job_max_load_memory(mem_limit_in_req,
                                                              _mem_tracker->limit()); //计算任务的内存上限

            int64_t timeout_in_req_s = params.has_load_channel_timeout_s()
                    ? params.load_channel_timeout_s() : -1;
            int64_t job_timeout_s = calc_job_timeout_s(timeout_in_req_s); //计算任务超时时间

            channel.reset(new LoadChannel(load_id, job_max_memory, job_timeout_s, _mem_tracker.get())); //创建LoadChannel对象
            _load_channels.insert({load_id, channel}); //将创建的LoadChannel对象添加到std::unordered_map<UniqueId, std::shared_ptr<LoadChannel>>类型的成员变量_load_channels中管理
        }
    }

    RETURN_IF_ERROR(channel->open(params)); //根据参数打开LoadChannel
    return Status::OK();
}

static void dummy_deleter(const CacheKey& key, void* value) {
}

/*添加一个batch的数据，数据保存在参数request中*/
Status LoadChannelMgr::add_batch(
        const PTabletWriterAddBatchRequest& request,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        int64_t* wait_lock_time_ns) {
    UniqueId load_id(request.id()); //获取load id
    // 1. get load channel
    std::shared_ptr<LoadChannel> channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        auto it = _load_channels.find(load_id);
        if (it == _load_channels.end()) {
            auto handle = _lastest_success_channel->lookup(load_id.to_string());
            // success only when eos be true
            if (handle != nullptr) {
                _lastest_success_channel->release(handle);
                if (request.has_eos() && request.eos()) { //判断当前batch是否为当前sender的最后一个batch
                    return Status::OK();
                }
            }
            return Status::InternalError(strings::Substitute(
                     "fail to add batch in load channel. unknown load_id=$0", load_id.to_string()));
        }
        channel = it->second; //获取LoadChannel对象
    }

    // 2. check if mem consumption exceed limit
    _handle_mem_exceed_limit(); //处理内存越界

    // 3. add batch to load channel
    // batch may not exist in request(eg: eos request without batch),
    // this case will be handled in load channel's add batch method.
    RETURN_IF_ERROR(channel->add_batch(request, tablet_vec)); //向load channel中添加一个batch的数据

    // 4. handle finish
    if (channel->is_finished()) { //判断本次load job是否完成，即判断本次数据加载过程中的所有batch都完成加载
        LOG(INFO) << "removing load channel " << load_id << " because it's finished";
        {
            std::lock_guard<std::mutex> l(_lock);
            _load_channels.erase(load_id); //从成员变量_load_channels中清除当前的load job
            auto handle = _lastest_success_channel->insert(
                    load_id.to_string(), nullptr, 1, dummy_deleter);
            _lastest_success_channel->release(handle);
        }
        VLOG(1) << "removed load channel " << load_id;
    }
    return Status::OK();
}

/*处理内存越界*/
void LoadChannelMgr::_handle_mem_exceed_limit() {
    // lock so that only one thread can check mem limit
    std::lock_guard<std::mutex> l(_lock);
    if (!_mem_tracker->limit_exceeded()) {
        return;
    }

    int64_t max_consume = 0;
    std::shared_ptr<LoadChannel> channel;
    for (auto& kv : _load_channels) { //依次遍历每一个load任务对应的LoadChannel对象，并获取占用内存空间最大的LoadChannel
        if (kv.second->mem_consumption() > max_consume) {
            max_consume = kv.second->mem_consumption();
            channel = kv.second;
        }
    }
    if (max_consume == 0) {
        // should not happen, add log to observe
        LOG(WARNING) << "failed to find suitable load channel when total load mem limit execeed";
        return;
    }
    DCHECK(channel.get() != nullptr);

    // force reduce mem limit of the selected channel
    LOG(INFO) << "reducing memory of " << *channel
              << " because total load mem consumption " << _mem_tracker->consumption()
              << " has exceeded limit " << _mem_tracker->limit();
    channel->handle_mem_exceed_limit(true); //处理对应LoadChannel的内存越界
}

/*取消params对应的LoadChannel*/
Status LoadChannelMgr::cancel(const PTabletWriterCancelRequest& params) {
    UniqueId load_id(params.id()); //获取load id
    std::shared_ptr<LoadChannel> cancelled_channel;
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_load_channels.find(load_id) != _load_channels.end()) { //查找load id对应的LoadChannel
            cancelled_channel = _load_channels[load_id];
            _load_channels.erase(load_id); //从成员变量_load_channels中清除对应的LoadChannel
        }
    }

    if (cancelled_channel.get() != nullptr) {
        cancelled_channel->cancel(); //取消本次load任务对应的load channel
        LOG(INFO) << "load channel has been cancelled: " << load_id;
    }

    return Status::OK();
}

/*启动线程，每间隔一段时间进行一次超时load任务的清理*/
Status LoadChannelMgr::_start_bg_worker() {
    _load_channels_clean_thread = std::thread(
        [this] {
#ifdef GOOGLE_PROFILER
            ProfilerRegisterThread();
#endif

#ifndef BE_TEST
            uint32_t interval = 60;
#else
            uint32_t interval = 1;
#endif
            while (!_is_stopped.load()) {
                _start_load_channels_clean();
                sleep(interval);
            }
        });
    return Status::OK();
}

/*从成员变量_load_channels中将所有超时的load任务进行删除,并且取消对应的load channel*/
Status LoadChannelMgr::_start_load_channels_clean() {
    std::vector<std::shared_ptr<LoadChannel>> need_delete_channels;
    LOG(INFO) << "cleaning timed out load channels";
    time_t now = time(nullptr);
    {
        std::vector<UniqueId> need_delete_channel_ids;
        std::lock_guard<std::mutex> l(_lock);
        VLOG(1) << "there are " << _load_channels.size() << " running load channels";
        int i = 0;
        for (auto& kv : _load_channels) {
            VLOG(1) << "load channel[" << i++ << "]: " << *(kv.second);
            time_t last_updated_time = kv.second->last_updated_time();
            if (difftime(now, last_updated_time) >= kv.second->timeout()) { //判断load任务是否超时
                need_delete_channel_ids.emplace_back(kv.first); //将超时的load任务进行记录
                need_delete_channels.emplace_back(kv.second);   //将超时的load任务对应的load channel进行记录
            }
        }

        for(auto& key: need_delete_channel_ids) {
            _load_channels.erase(key); //从成员变量_load_channels中将所有超时的load任务进行删除
            LOG(INFO) << "erase timeout load channel: " << key;
        }
    }

    // we must cancel these load channels before destroying them.
    // otherwise some object may be invalid before trying to visit it.
    // eg: MemTracker in load channel
    for (auto& channel : need_delete_channels) {
        channel->cancel(); //将超时的load任务对应的load channel进行取消
        LOG(INFO) << "load channel has been safely deleted: " << channel->load_id()
                  << ", timeout(s): " << channel->timeout();
    }

    // this log print every 1 min, so that we could observe the mem consumption of load process
    // on this Backend
    LOG(INFO) << "load mem consumption(bytes). limit: " << _mem_tracker->limit()
            << ", current: " << _mem_tracker->consumption()
            << ", peak: " << _mem_tracker->peak_consumption();

    return Status::OK();
}

}
