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

#include "util/threadpool.h"

#include <cstdint>
#include <limits>
#include <ostream>

#include "common/logging.h"
#include "gutil/macros.h"
#include "gutil/map-util.h"
#include "gutil/strings/substitute.h"
#include "gutil/sysinfo.h"
#include "util/scoped_cleanup.h"
#include "util/thread.h"

namespace doris {

using std::string;
using strings::Substitute;

class FunctionRunnable : public Runnable {
public:
    explicit FunctionRunnable(std::function<void()> func) : _func(std::move(func)) {}

    void run() OVERRIDE {
        _func();
    }

private:
    std::function<void()> _func;
};

/*ThreadPoolBuilder的构造函数*/
ThreadPoolBuilder::ThreadPoolBuilder(string name) :
        _name(std::move(name)),
        _min_threads(0),
        _max_threads(base::NumCPUs()),
        _max_queue_size(std::numeric_limits<int>::max()),
        _idle_timeout(MonoDelta::FromMilliseconds(500)) {}

/*设置线程池中最小的线程数*/
ThreadPoolBuilder& ThreadPoolBuilder::set_min_threads(int min_threads) {
    CHECK_GE(min_threads, 0);
    _min_threads = min_threads;
    return *this;
}

/*设置线程池中最大的线程数*/
ThreadPoolBuilder& ThreadPoolBuilder::set_max_threads(int max_threads) {
    CHECK_GT(max_threads, 0);
    _max_threads = max_threads;
    return *this;
}

/*设置任务队列中最大的任务数*/
ThreadPoolBuilder& ThreadPoolBuilder::set_max_queue_size(int max_queue_size) {
    _max_queue_size = max_queue_size;
    return *this;
}

/*设置空闲超时时间*/
ThreadPoolBuilder& ThreadPoolBuilder::set_idle_timeout(const MonoDelta& idle_timeout) {
    _idle_timeout = idle_timeout;
    return *this;
}

/*创建并初始化参数传入的ThreadPool对象*/
Status ThreadPoolBuilder::build(std::unique_ptr<ThreadPool>* pool) const {
    pool->reset(new ThreadPool(*this)); //创建ThreadPool对象
    RETURN_IF_ERROR((*pool)->init());   //初始化ThreadPool对象
    return Status::OK();
}

/*ThreadPoolToken的构造函数*/
ThreadPoolToken::ThreadPoolToken(ThreadPool* pool,
        ThreadPool::ExecutionMode mode)
  : _mode(mode),
    _pool(pool),
    _state(State::IDLE),
    _not_running_cond(&pool->_lock),
    _active_threads(0) {}

/*ThreadPoolToken的析构函数*/
ThreadPoolToken::~ThreadPoolToken() {
    shutdown(); // shutdown当前ThreadPoolToken对象
    _pool->release_token(this); // 通过成员变量释放当前ThreadPoolToken对象
}

/*通过当前ThreadPoolToken对象向线程池提交一个task*/
Status ThreadPoolToken::submit(std::shared_ptr<Runnable> r) {
    return _pool->do_submit(std::move(r), this);
}

/*通过当前ThreadPoolToken对象向线程池提交一个任务函数*/
Status ThreadPoolToken::submit_func(std::function<void()> f) {
    return submit(std::make_shared<FunctionRunnable>(std::move(f))); //将任务函数封装成FunctionRunnable对象
}

/*关闭当前ThreadPoolToken*/
void ThreadPoolToken::shutdown() {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked(); // 检查当前线程是否不是线程池中的线程（如果是线程池中的线程，可能会造成死锁）

    // Clear the queue under the lock, but defer the releasing of the tasks
    // outside the lock, in case there are concurrent threads wanting to access
    // the ThreadPool. The task's destructors may acquire locks, etc, so this
    // also prevents lock inversions.
    std::deque<ThreadPool::Task> to_release = std::move(_entries); //获取当前token下在任务队列中的task
    _pool->_total_queued_tasks -= to_release.size(); //更新线程池排队任务的数量

    switch (state()) {
      case State::IDLE:
        // There were no tasks outstanding; we can quiesce the token immediately.
        transition(State::QUIESCED); //将当前ThreadPoolToken对象的状态转换为State::QUIESCED
        break;
      case State::RUNNING:
        // There were outstanding tasks. If any are still running, switch to
        // QUIESCING and wait for them to finish (the worker thread executing
        // the token's last task will switch the token to QUIESCED). Otherwise,
        // we can quiesce the token immediately.

        // Note: this is an O(n) operation, but it's expected to be infrequent.
        // Plus doing it this way (rather than switching to QUIESCING and waiting
        // for a worker thread to process the queue entry) helps retain state
        // transition symmetry with ThreadPool::shutdown.
        for (auto it = _pool->_queue.begin(); it != _pool->_queue.end();) {
            if (*it == this) {
                it = _pool->_queue.erase(it); //从当前ThreadPoolToken所在线程池对象_pool的成员变量_queue中移除当前ThreadPoolToken对象
            } else {
                it++;
            }
        }

        if (_active_threads == 0) {
            transition(State::QUIESCED); //将当前ThreadPoolToken对象的状态转换为State::QUIESCED
            break;
        }
        transition(State::QUIESCING);    //将当前ThreadPoolToken对象的状态转换为State::QUIESCING
        FALLTHROUGH_INTENDED;
      case State::QUIESCING:
        // The token is already quiescing. Just wait for a worker thread to
        // switch it to QUIESCED.
        while (state() != State::QUIESCED) { //等待当前ThreadPoolToken对象的状态变为State::QUIESCED
            _not_running_cond.wait(); //线程休眠，等待当前ThreadPoolToken对象的状态变为State::QUIESCED时被唤醒
        }
        break;
      default:
        break;
    }
}

/*线程休眠，直到通过当前ThreadPoolToken对象提交的task都被执行完毕*/
void ThreadPoolToken::wait() {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked();
    while (is_active()) {
        _not_running_cond.wait(); //线程休眠，等待被唤醒
    }
}

/*线程休眠，直到通过当前ThreadPoolToken对象提交的task都被执行完毕，或到达参数设定的时间点*/
bool ThreadPoolToken::wait_until(const MonoTime& until) {
    MutexLock unique_lock(&(_pool->_lock));
    _pool->check_not_pool_thread_unlocked();
    while (is_active()) {
        if (!_not_running_cond.wait_until(until)) {
            return false;
        }
    }
    return true;
}

/*线程休眠，直到通过当前ThreadPoolToken对象提交的task都被执行完毕，或休眠时长到达参数设定的值*/
bool ThreadPoolToken::wait_for(const MonoDelta& delta) {
    return wait_until(MonoTime::Now() + delta);
}

/*将ThreadPoolToken状态转换为参数传入的新状态*/
void ThreadPoolToken::transition(State new_state) {
#ifndef NDEBUG
    CHECK_NE(_state, new_state);

    switch (_state) { // 获取ThreadPoolToken对象的当前状态
      case State::IDLE:
        CHECK(new_state == State::RUNNING ||
                new_state == State::QUIESCED);
        if (new_state == State::RUNNING) {
            CHECK(!_entries.empty());
        } else {
            CHECK(_entries.empty());
            CHECK_EQ(_active_threads, 0);
        }
        break;
      case State::RUNNING:
        CHECK(new_state == State::IDLE ||
                new_state == State::QUIESCING ||
                new_state == State::QUIESCED);
        CHECK(_entries.empty());
        if (new_state == State::QUIESCING) {
            CHECK_GT(_active_threads, 0);
        }
        break;
      case State::QUIESCING:
        CHECK(new_state == State::QUIESCED);
        CHECK_EQ(_active_threads, 0);
        break;
      case State::QUIESCED:
        CHECK(false); // QUIESCED is a terminal state
        break;
      default:
        LOG(FATAL) << "Unknown token state: " << _state;
    }
#endif

    // Take actions based on the state we're entering.
    switch (new_state) {
        case State::IDLE:
        case State::QUIESCED:
            _not_running_cond.notify_all();
            break;
        default:
            break;
    }

    _state = new_state; // 使用新的状态更新_state
}

/*获取ThreadPoolToken对象状态对应的字符串*/
const char* ThreadPoolToken::state_to_string(State s) {
    switch (s) {
        case State::IDLE: return "IDLE"; break;
        case State::RUNNING: return "RUNNING"; break;
        case State::QUIESCING: return "QUIESCING"; break;
        case State::QUIESCED: return "QUIESCED"; break;
    }
    return "<cannot reach here>";
}

/*ThreadPool类的构造函数，使用ThreadPoolBuilder对象进行初始化*/
ThreadPool::ThreadPool(const ThreadPoolBuilder& builder)
  : _name(builder._name),
    _min_threads(builder._min_threads),
    _max_threads(builder._max_threads),
    _max_queue_size(builder._max_queue_size),
    _idle_timeout(builder._idle_timeout),
    _pool_status(Status::Uninitialized("The pool was not initialized.")),
    _idle_cond(&_lock),
    _no_threads_cond(&_lock),
    _num_threads(0),
    _num_threads_pending_start(0),
    _active_threads(0),
    _total_queued_tasks(0),
    _tokenless(new_token(ExecutionMode::CONCURRENT))
{}

ThreadPool::~ThreadPool() {
    // There should only be one live token: the one used in tokenless submission.
    CHECK_EQ(1, _tokens.size()) << Substitute(
            "Threadpool $0 destroyed with $1 allocated tokens",
            _name, _tokens.size());
    shutdown();
}

/*线程池初始化*/
Status ThreadPool::init() {
    if (!_pool_status.is_uninitialized()) {
        return Status::NotSupported("The thread pool is already initialized");
    }
    _pool_status = Status::OK();
    _num_threads_pending_start = _min_threads; // 初始化_num_threads_pending_start（_num_threads_pending_start中保存准备要创建一个新的线程，但是还没有开始创建的线程数。当一个线程创建成功，执行dispatch_thread()时，或线程创建失败时，该成员变量会减1）
    for (int i = 0; i < _min_threads; i++) { //初始状态，按照_min_threads创建多个线程
        Status status = create_thread(); //创建线程
        if (!status.ok()) {
            shutdown();
            return status;
        }
    }
    return Status::OK();
}

/*关闭线程池*/
void ThreadPool::shutdown() {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked(); // 检查执行shutdown的线程是否不是线程池中的线程（如果执行shutdown的线程是线程池中的线程，可能会造成死锁）

    // Note: this is the same error seen at submission if the pool is at
    // capacity, so clients can't tell them apart. This isn't really a practical
    // concern though because shutting down a pool typically requires clients to
    // be quiesced first, so there's no danger of a client getting confused.
    _pool_status = Status::ServiceUnavailable("The pool has been shut down."); // 更新线程池的状态为shutdown

    // Clear the various queues under the lock, but defer the releasing
    // of the tasks outside the lock, in case there are concurrent threads
    // wanting to access the ThreadPool. The task's destructors may acquire
    // locks, etc, so this also prevents lock inversions.
    _queue.clear(); //清空队列_queue
    std::deque<std::deque<Task>> to_release;
    for (auto* t : _tokens) { //依次遍历当前线程池下的每一个ThreadPoolToken对象
        if (!t->_entries.empty()) {
            to_release.emplace_back(std::move(t->_entries)); //将每一个ThreadPoolToken对象下的task添加到队列to_release中
        }
        // 更新token的状态
        switch (t->state()) {
          case ThreadPoolToken::State::IDLE:
            // The token is idle; we can quiesce it immediately.
            t->transition(ThreadPoolToken::State::QUIESCED); // 如果token的当前状态为IDLE，则可以直接更新状态为QUIESCED
            break;
          case ThreadPoolToken::State::RUNNING:
            // The token has tasks associated with it. If they're merely queued
            // (i.e. there are no active threads), the tasks will have been removed
            // above and we can quiesce immediately. Otherwise, we need to wait for
            // the threads to finish.
            t->transition(t->_active_threads > 0 ?
                    ThreadPoolToken::State::QUIESCING :  // 如果token的当前状态为RUNNING，并且当前token还有正在执行的task，则更新token状态为QUIESCING
                    ThreadPoolToken::State::QUIESCED);   // 如果token的当前状态为RUNNING，并且当前token没有正在执行的task，则更新token状态为QUIESCED
            break;
          default:
            break;
        }
    }

    // The queues are empty. Wake any sleeping worker threads and wait for all
    // of them to exit. Some worker threads will exit immediately upon waking,
    // while others will exit after they finish executing an outstanding task.
    _total_queued_tasks = 0; // 更新线程池的等待任务数为0
    while (!_idle_threads.empty()) {
        _idle_threads.front().not_empty.notify_one(); // 唤醒空闲线程
        _idle_threads.pop_front();                    // 从成员变量_idle_threads里移除空闲线程
    }
    while (_num_threads + _num_threads_pending_start > 0) {
        _no_threads_cond.wait(); //如果线程池中线程数不为0，则阻塞当前shudown过程，直到线程数为0时被唤醒
    }

    // All the threads have exited. Check the state of each token.
    for (auto* t : _tokens) {
        DCHECK(t->state() == ThreadPoolToken::State::IDLE ||
               t->state() == ThreadPoolToken::State::QUIESCED);
    }
}

/*创建一个新的token，并添加到成员变量_tokens中进行管理*/
std::unique_ptr<ThreadPoolToken> ThreadPool::new_token(ExecutionMode mode) {
    MutexLock unique_lock(&_lock);
    std::unique_ptr<ThreadPoolToken> t(new ThreadPoolToken(this,mode));
    InsertOrDie(&_tokens, t.get()); // 将新创建的token添加到成员变量_tokens中
    return t;
}

/*释放token，并从成员变量_tokens中移除*/
void ThreadPool::release_token(ThreadPoolToken* t) {
    MutexLock unique_lock(&_lock);
    CHECK(!t->is_active()) << Substitute("Token with state $0 may not be released",
            ThreadPoolToken::state_to_string(t->state()));
    CHECK_EQ(1, _tokens.erase(t)); // 从成员变量_tokens中删除token
}

/*向线程池提交任务，任务以Runnable对象的形式提交*/
Status ThreadPool::submit(std::shared_ptr<Runnable> r) {
    return do_submit(std::move(r), _tokenless.get());
}

/*向线程池提交一个任务函数，并将任务函数封装成Runnable对象进行提交*/
Status ThreadPool::submit_func(std::function<void()> f) {
    return submit(std::make_shared<FunctionRunnable>(std::move(f)));
}

/*通过ThreadPoolToken提交任务给线程池*/
Status ThreadPool::do_submit(std::shared_ptr<Runnable> r, ThreadPoolToken* token) {
    DCHECK(token);
    MonoTime submit_time = MonoTime::Now();

    MutexLock unique_lock(&_lock);
    if (PREDICT_FALSE(!_pool_status.ok())) { // 判断线程池的状态
        return _pool_status;
    }

    if (PREDICT_FALSE(!token->may_submit_new_tasks())) { // 判断当前token是否可以提交任务（token的状态不为QUIESCING和QUIESCED）
        return Status::ServiceUnavailable("Thread pool token was shut down");
    }

    // Size limit check.
    int64_t capacity_remaining = static_cast<int64_t>(_max_threads) // 计算线程池剩余容量
                               - _active_threads
                               + static_cast<int64_t>(_max_queue_size)
                               - _total_queued_tasks;
    if (capacity_remaining < 1) { // 判断线程池是否还有容量能够提交任务
        return Status::ServiceUnavailable(
                 Substitute("Thread pool is at capacity ($0/$1 tasks running, $2/$3 tasks queued)",
                   _num_threads + _num_threads_pending_start, _max_threads,
                   _total_queued_tasks, _max_queue_size));
    }

    // Should we create another thread?

    // We assume that each current inactive thread will grab one item from the
    // queue.  If it seems like we'll need another thread, we create one.
    //
    // Rather than creating the thread here, while holding the lock, we defer
    // it to down below. This is because thread creation can be rather slow
    // (hundreds of milliseconds in some cases) and we'd like to allow the
    // existing threads to continue to process tasks while we do so.
    //
    // In theory, a currently active thread could finish immediately after this
    // calculation but before our new worker starts running. This would mean we
    // created a thread we didn't really need. However, this race is unavoidable
    // and harmless.
    //
    // Of course, we never create more than _max_threads threads no matter what.
    int threads_from_this_submit =
        token->is_active() && token->mode() == ExecutionMode::SERIAL ? 0 : 1; // 如果当前token是活跃的（状态为RUNNING或QUIESCING，即有正在执行的任务），并且token的模式为SERIAL，则本次提交的任务不需要额外的线程（该token正在执行的任务结束之后，本次提交的任务才能执行，因为SERIAL模式的token下的任务同时只能有一个执行）
    int inactive_threads = _num_threads + _num_threads_pending_start - _active_threads; // 计算线程池中非活跃的线程数
    int additional_threads = static_cast<int>(_queue.size())
                           + threads_from_this_submit
                           - inactive_threads; // 计算要执行本次提交的任务以及任务队列中的所有任务还需要额外的线程数
    bool need_a_thread = false;
    if (additional_threads > 0 && _num_threads + _num_threads_pending_start < _max_threads) {
        need_a_thread = true;         // 需要创建一个新的线程
        _num_threads_pending_start++; // _num_threads_pending_start中保存准备要创建一个新的线程，但是还没有开始创建的线程数
    }

    Task task;
    task.runnable = std::move(r); //要提交的任务
    task.submit_time = submit_time;

    // Add the task to the token's queue.
    ThreadPoolToken::State state = token->state(); //获取ThreadPoolToken状态
    DCHECK(state == ThreadPoolToken::State::IDLE ||
            state == ThreadPoolToken::State::RUNNING);
    token->_entries.emplace_back(std::move(task)); //将任务添加到ThreadPoolToken对象的成员变量_entries中
    if (state == ThreadPoolToken::State::IDLE ||
            token->mode() == ExecutionMode::CONCURRENT) {
        // 对于SERIAL模式的token，同一个token对象同时只能在成员变量_queue中存在一份，该类型token的任务执行时，token从_queue中
        // 出队，任务执行结束之后，如果该token下还有其他任务，则该token会重新入队。对于CONCURRENT模式的token，同一个token对象同
        // 时可以在成员变量_queue中存在多份，通过该token每提交一次任务，token都会入队一次，每执行一次任务，token出队一次。
        _queue.emplace_back(token); // 如果token的状态为IDLE，或者token的模式为CONCURRENT，则将当前的参数传入的token添加到成员变量_queue中。如果当前token的模式为SERIAL，并且状态为RUNNING，则不需要将当前token添加到成员变量_queue中。token下正在执行的任务完成后，该token会被重新添加到成员变量_queue中，因为token下的所有任务中，一次只能有一个任务被执行）
        if (state == ThreadPoolToken::State::IDLE) {
            token->transition(ThreadPoolToken::State::RUNNING); // 如果token的当前状态为IDLE，则将token的状态切换为RUNNING
        }
    }
    _total_queued_tasks++; //更新成员变量_total_queued_tasks

    // Wake up an idle thread for this task. Choosing the thread at the front of
    // the list ensures LIFO semantics as idling threads are also added to the front.
    //
    // If there are no idle threads, the new task remains on the queue and is
    // processed by an active thread (or a thread we're about to create) at some
    // point in the future.
    if (!_idle_threads.empty()) {
        _idle_threads.front().not_empty.notify_one(); //唤醒一个空闲线程
        _idle_threads.pop_front();                    //将唤醒的空闲线程从_idle_threads中删除
    }
    unique_lock.unlock();

    if (need_a_thread) {
        Status status = create_thread(); //创建一个线程
        if (!status.ok()) {
            unique_lock.lock();
            _num_threads_pending_start--; // 创建线程失败，该成员变量减1
            if (_num_threads + _num_threads_pending_start == 0) {
                // If we have no threads, we can't do any work.
                return status;
            }
            // If we failed to create a thread, but there are still some other
            // worker threads, log a warning message and continue.
            LOG(ERROR) << "Thread pool failed to create thread: "
                       << status.to_string();
        }
    }

    return Status::OK();
}

/*线程休眠，直到被唤醒*/
void ThreadPool::wait() {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked();
    while (_total_queued_tasks > 0 || _active_threads > 0) {
        _idle_cond.wait();
    }
}

/*线程休眠，直到被唤醒或到达参数设定的时间点*/
bool ThreadPool::wait_until(const MonoTime& until) {
    MutexLock unique_lock(&_lock);
    check_not_pool_thread_unlocked();
    while (_total_queued_tasks > 0 || _active_threads > 0) {
        if (!_idle_cond.wait_until(until)) {
            return false;
        }
    }
    return true;
}

/*线程休眠，直到被唤醒或到达参数设定的时间间隔*/
bool ThreadPool::wait_for(const MonoDelta& delta) {
    return wait_until(MonoTime::Now() + delta);
}

/*线程创建时，会开始执行dispatch_thread()，循环从任务队列中出队并执行任务*/
void ThreadPool::dispatch_thread() {
    MutexLock unique_lock(&_lock);
    InsertOrDie(&_threads, Thread::current_thread()); // 将当前线程添加到成员变量_threads
    DCHECK_GT(_num_threads_pending_start, 0);
    _num_threads++;                 // 线程创建成功,已经启动的线程数增1（成员变量_num_threads保存已经启动的线程数，当线程被回收时，该成员变量会减1）
    _num_threads_pending_start--;   // 线程创建成功，执行dispatch_thread()，成员变量_num_threads_pending_start减1
    // If we are one of the first '_min_threads' to start, we must be
    // a "permanent" thread.
    bool permanent = _num_threads <= _min_threads;

    // Owned by this worker thread and added/removed from _idle_threads as needed.
    IdleThread me(&_lock);

    while (true) {
        // Note: Status::Aborted() is used to indicate normal shutdown.
        if (!_pool_status.ok()) { // 判断线程池的状态
            VLOG(2) << "DispatchThread exiting: " << _pool_status.to_string();
            break;
        }

        if (_queue.empty()) { // 判断任务队列是否为空
            // There's no work to do, let's go idle.
            //
            // Note: if FIFO behavior is desired, it's as simple as changing this to push_back().
            _idle_threads.push_front(me); // 如果线程池的任务队列为空，则将当前线程添加到成员变量_idle_threads中
            SCOPED_CLEANUP({
                // For some wake ups (i.e. shutdown or do_submit) this thread is
                // guaranteed to be unlinked after being awakened. In others (i.e.
                // spurious wake-up or Wait timeout), it'll still be linked.
                if (me.is_linked()) {
                    _idle_threads.erase(_idle_threads.iterator_to(me)); // 从成员变量_idle_threads中删除当前线程
                }
            });
            if (permanent) { // 判断当前线程是否需要在线程池中长久保存
                me.not_empty.wait(); // 线程休眠，直到有新任务提交给等待队列而被唤醒
            } else {
                if (!me.not_empty.wait_for(_idle_timeout)) { // 线程休眠，直到休眠超时（线程休眠超时，wait_for()返回false，有新任务提交给等待队列，该线程有可能被唤醒，wait_for()返回true）
                    // After much investigation, it appears that pthread condition variables have
                    // a weird behavior in which they can return ETIMEDOUT from timed_wait even if
                    // another thread did in fact signal. Apparently after a timeout there is some
                    // brief period during which another thread may actually grab the internal mutex
                    // protecting the state, signal, and release again before we get the mutex. So,
                    // we'll recheck the empty queue case regardless.
                    if (_queue.empty()) { // 线程休眠超时，判断任务队列是否还是为空
                        VLOG(3) << "Releasing worker thread from pool " << _name << " after "
                            << _idle_timeout.ToMilliseconds() << "ms of idle time.";
                        break; // 退出while()循环
                    }
                }
            }
            continue; // 继续while()下一轮循环
        }

        // 从任务队列中出队第一个task，交给当前线程来执行
        // Get the next token and task to execute.
        ThreadPoolToken* token = _queue.front();        //从成员变量_queue队列（_queue中保存的是当前线程池下的所有token）中获取一个新的ThreadPoolToken对象
        _queue.pop_front();                             //token从成员变量_queue中出队
        DCHECK_EQ(ThreadPoolToken::State::RUNNING, token->state());
        DCHECK(!token->_entries.empty()); // 判断是否还有通过该token提交的任务在排队（通过token提交的任务都会在ThreadPoolToken对象的成员变量_entries中保存）
        Task task = std::move(token->_entries.front()); //从当前token的_entries任务队列中获取一个task
        token->_entries.pop_front();                    //将task从当前token的_entries任务队列中出队
        token->_active_threads++;                       // 更新当前token的成员变量_active_threads
        --_total_queued_tasks;                          // 任务出队之后，当前线程池的排队任务数量减1
        ++_active_threads;                              // 任务出队之后，当前线程池的正在执行的线程数加1(成员变量_active_threads保存正在执行任务的线程数，任务执行结束，该成员变量值会减1)

        unique_lock.unlock();

        // Execute the task
        task.runnable->run();                           //执行任务

        // Destruct the task while we do not hold the lock.
        //
        // The task's destructor may be expensive if it has a lot of bound
        // objects, and we don't want to block submission of the threadpool.
        // In the worst case, the destructor might even try to do something
        // with this threadpool, and produce a deadlock.
        task.runnable.reset(); // 任务执行完成，reset task的runnable成员
        unique_lock.lock();

        // Possible states:
        // 1. The token was shut down while we ran its task. Transition to QUIESCED.
        // 2. The token has no more queued tasks. Transition back to IDLE.
        // 3. The token has more tasks. Requeue it and transition back to RUNNABLE.
        // 任务执行完成之后，根据token和任务队列情况更新token的状态
        ThreadPoolToken::State state = token->state(); // 获取token的当前状态
        DCHECK(state == ThreadPoolToken::State::RUNNING ||
                state == ThreadPoolToken::State::QUIESCING); // 检查token的当前状态是否为RUNNING或QUIESCING
        if (--token->_active_threads == 0) { // 任务执行完成之后，更新当前token的成员变量_active_threads，并判断当前token的成员变量_active_threads是否为0
            if (state == ThreadPoolToken::State::QUIESCING) {
                DCHECK(token->_entries.empty());
                token->transition(ThreadPoolToken::State::QUIESCED); // 如果当前token的成员变量_active_threads为0，并且token的当前状态为QUIESCING，则更新token的状态为QUIESCED
            } else if (token->_entries.empty()) {
                token->transition(ThreadPoolToken::State::IDLE); // token的当前状态为RUNNING，并且当前token没有排队的任务，则更新token的状态为IDLE
            } else if (token->mode() == ExecutionMode::SERIAL) {
                _queue.emplace_back(token); // 当前token还有排队的任务，并且token的模式为SERIAL，任务执行结束，将当前token重新添加到当前ThreadPool对象的成员变量_queue中
            }
        }
        if (--_active_threads == 0) { // 成员变量_active_threads保存正在执行任务的线程数，任务执行结束，该成员变量值减1
            _idle_cond.notify_all();
        }
    }
    // while()循环退出，当前线程需要被回收

    // It's important that we hold the lock between exiting the loop and dropping
    // _num_threads. Otherwise it's possible someone else could come along here
    // and add a new task just as the last running thread is about to exit.
    CHECK(unique_lock.own_lock());

    CHECK_EQ(_threads.erase(Thread::current_thread()), 1); // 从当前线程池的成员变量_threads中移除当前线程
    _num_threads--;   // 当线程被回收时，成员变量_num_threads减1
    if (_num_threads + _num_threads_pending_start == 0) {
        _no_threads_cond.notify_all(); // 唤醒shutdown（shutdown过程中，如果_num_threads + _num_threads_pending_start > 0， shutdown会休眠）

        // Sanity check: if we're the last thread exiting, the queue ought to be
        // empty. Otherwise it will never get processed.
        CHECK(_queue.empty());
        DCHECK_EQ(0, _total_queued_tasks);
    }
}

/*创建一个线程，并执行dispatch_thread()函数*/
Status ThreadPool::create_thread() {
    return Thread::create("thread pool", Substitute("$0 [worker]", _name),
            &ThreadPool::dispatch_thread, this, nullptr);
}

/*检查当前线程是否不是线程池中的线程（如果是线程池中的线程，可能会造成死锁）*/
void ThreadPool::check_not_pool_thread_unlocked() {
    Thread* current = Thread::current_thread(); //获取当前线程
    if (ContainsKey(_threads, current)) { //判断当前线程是否是线程池中的线程（是否在std::unordered_set<Thread*>类型的成员变量_threads中管理）
        LOG(FATAL) << Substitute("Thread belonging to thread pool '$0' with "
                "name '$1' called pool function that would result in deadlock",
                _name, current->name());
    }
}

std::ostream& operator<<(std::ostream& o, ThreadPoolToken::State s) {
    return o << ThreadPoolToken::state_to_string(s);
}

} // namespace doris
