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

#include "olap/memtable_flush_executor.h"

#include <functional>

#include "olap/memtable.h"
#include "util/scoped_cleanup.h"

namespace doris {

std::ostream& operator<<(std::ostream& os, const FlushStatistic& stat) {
    os << "(flush time(ms)=" << stat.flush_time_ns / 1000 / 1000
       << ", flush count=" << stat.flush_count << ")";
    return os;
}

// The type of parameter is safe to be a reference. Because the function object
// returned by std::bind() will increase the reference count of Memtable. i.e.,
// after the submit() method returns, even if the caller immediately releases the
// passed shared_ptr object, the Memtable object will not be destructed because
// its reference count is not 0.
/*提交flush memtable任务给线程池*/
OLAPStatus FlushToken::submit(const std::shared_ptr<MemTable>& memtable) {
    RETURN_NOT_OK(_flush_status.load()); //std::atomic::load()指加载并返回当前原子变量的值
    _flush_token->submit_func(std::bind(&FlushToken::_flush_memtable, this, memtable)); //向线程池提交flush memtable的任务
    return OLAP_SUCCESS;
}

/*关闭线程池(发生错误，关闭线程池，移除队列中的所有task)*/
void FlushToken::cancel() {
    _flush_token->shutdown();
}

/*等待通过该flush token提交的所有任务提交完成*/
OLAPStatus FlushToken::wait() {
    _flush_token->wait();
    return _flush_status.load();
}

/*刷写memtable
  对于一次数据导入，可能写入某个tablet的数据比较多，一个memtable不能容纳所有写入当前tablet的数据，就需要在一个memtable写满之后
  重新创建新的memtable来写入数据，这些memtable会被地顺序刷写成一个个segment文件，存放在一个rowset中。如果某一个memtable刷写
  失败，那么，就不允许后续的memtable提交flush task，已经提交flush task的memtable也没有必要执行刷写操作，因为本次数据导入已经
  注定失败了。
 */
void FlushToken::_flush_memtable(std::shared_ptr<MemTable> memtable) {
    SCOPED_CLEANUP({ memtable.reset(); });

    // If previous flush has failed, return directly
    if (_flush_status.load() != OLAP_SUCCESS) {//如果当前DeltaWriter对象维护的_flush_token之前提交的flush task执行失败，则直接退出
        return;
    }

    MonotonicStopWatch timer;
    timer.start();//本次刷写开始计时
    _flush_status.store(memtable->flush());//执行memtable flush，并用flush()返回结果更新原子变量_flush_status
    if (_flush_status.load() != OLAP_SUCCESS) {
        return;
    }

    // 更新当前DeltaWriter对象的刷写统计数据
    _stats.flush_time_ns += timer.elapsed_time(); //获取本次刷写结束计时，并更新当前DeltaWriter对象的刷写时间
    _stats.flush_count++;                         //更新当前DeltaWriter对象的刷写计数
    _stats.flush_size_bytes += memtable->memory_usage();//获取本次刷写的memtable大小，并更新当前DeltaWriter对象的刷写数据大小
}

/*初始化MemTableFlushExecutor对象，创建刷写memtable的线程池*/
void MemTableFlushExecutor::init(const std::vector<DataDir*>& data_dirs) {
    int32_t data_dir_num = data_dirs.size();
    size_t min_threads = std::max(1, config::flush_thread_num_per_store);//每个磁盘的刷写线程数
    size_t max_threads = data_dir_num * min_threads;
    ThreadPoolBuilder("MemTableFlushThreadPool")
            .set_min_threads(min_threads)
            .set_max_threads(max_threads)
            .build(&_flush_pool); //创建刷写memtable的线程池
}

// NOTE: we use SERIAL mode here to ensure all mem-tables from one tablet are flushed in order.
/*创建flush token*/
OLAPStatus MemTableFlushExecutor::create_flush_token(std::unique_ptr<FlushToken>* flush_token) {
    flush_token->reset(new FlushToken(_flush_pool->new_token(ThreadPool::ExecutionMode::SERIAL)));
    return OLAP_SUCCESS;
}

} // namespace doris
