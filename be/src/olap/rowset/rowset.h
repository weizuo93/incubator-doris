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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H

#include <memory>
#include <vector>
#include <mutex>

#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "olap/rowset/rowset_meta.h"

namespace doris {

class DataDir;
class OlapTuple;
class RowCursor;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetFactory;
class RowsetReader;
class TabletSchema;

// the rowset state transfer graph:
//    ROWSET_UNLOADED    <--|
//          ↓               |
//    ROWSET_LOADED         |
//          ↓               |
//    ROWSET_UNLOADING   -->| 
enum RowsetState {
    // state for new created rowset
    ROWSET_UNLOADED,
    // state after load() called
    ROWSET_LOADED,
    // state for closed() called but owned by some readers
    ROWSET_UNLOADING
};

class RowsetStateMachine {
public:
    RowsetStateMachine() : _rowset_state(ROWSET_UNLOADED) { }

    OLAPStatus on_load() {
        switch (_rowset_state) {
            case ROWSET_UNLOADED:
                _rowset_state = ROWSET_LOADED;
                break;

            default:
                return OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION;
        }
        return OLAP_SUCCESS;
    }

    OLAPStatus on_close(uint64_t refs_by_reader) {
        switch (_rowset_state) {
            case ROWSET_LOADED:
                if (refs_by_reader == 0) {
                    _rowset_state = ROWSET_UNLOADED;
                } else {
                    _rowset_state = ROWSET_UNLOADING;
                }
                break;

            default:
                return OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION;
        }
        return OLAP_SUCCESS;
    }

    OLAPStatus on_release() {
        switch (_rowset_state) {
            case ROWSET_UNLOADING:
                _rowset_state = ROWSET_UNLOADED;
                break;

            default:
                return OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION;
        }
        return OLAP_SUCCESS;
    }

    RowsetState rowset_state() {
        return _rowset_state;
    }

private:
    RowsetState _rowset_state;
};

/*
  Rowset代表了一个或多个批次的数据，抽象它的原因是想以后能支持更多的文件存储格式。

  tablet是不不感知磁盘的，真正感知存储路路径的是rowset，每个rowset都应该有一个单独的磁盘⽬目录，比如我们在做磁盘迁移的时候，一个tablet的数据可能跨越了多个磁盘，
  这样实际上就要求每个rowset有⾃己的存储⽬目录。rowset⼀旦生成了就应该是⼀个不可变的结构，理论上任何状态都不能改变，这样有利于控制我们系统的读写并发。rowset的元
  数据尽量量少⼀些，因为⼀个节点可能有几十万个tablet，每个tablet⼜可能有⼏百到几千个rowset，而rowset的元数据位于内存中，如果数据量太⼤内存可能爆掉。rowset的设
  计要满⾜我们磁盘拷⻉的需求，⽐如我们把⼀个磁盘的数据和元数据都拷⻉一下到另外⼀个磁盘上或者把磁盘目录变更一下，系统应该是能够加载的。所以不能直接在rowset meta中
  存储data dir。rowset的id在⼀个be内要唯一， 但是在不同的be上会有重复。rowset id只是⼀个唯⼀的标识，并不代表任何含义，两个BE上如果有两个rowset的id相同，他们的
  数据内容不一定一样。
*/

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    virtual ~Rowset() { }

    // Open all segment files in this rowset and load necessary metadata.
    // - `use_cache` : whether to use fd cache, only applicable to alpha rowset now
    //
    // May be called multiple times, subsequent calls will no-op.
    // Derived class implements the load logic by overriding the `do_load_once()` method.
    OLAPStatus load(bool use_cache = true);

    // returns OLAP_ERR_ROWSET_CREATE_READER when failed to create reader
    virtual OLAPStatus create_reader(std::shared_ptr<RowsetReader>* result) = 0;

    // Split range denoted by `start_key` and `end_key` into sub-ranges, each contains roughly
    // `request_block_row_count` rows. Sub-range is represented by pair of OlapTuples and added to `ranges`.
    //
    // e.g., if the function generates 2 sub-ranges, the result `ranges` should contain 4 tuple: t1, t2, t2, t3.
    // Note that the end tuple of sub-range i is the same as the start tuple of sub-range i+1.
    //
    // The first/last tuple must be start_key/end_key.to_tuple(). If we can't divide the input range,
    // the result `ranges` should be [start_key.to_tuple(), end_key.to_tuple()]
    virtual OLAPStatus split_range(const RowCursor& start_key,
                                   const RowCursor& end_key,
                                   uint64_t request_block_row_count,
                                   std::vector<OlapTuple>* ranges) = 0;

    const RowsetMetaSharedPtr& rowset_meta() const { return _rowset_meta; }

    bool is_pending() const { return _is_pending; }

    // publish rowset to make it visible to read
    void make_visible(Version version, VersionHash version_hash);

    // helper class to access RowsetMeta
    int64_t start_version() const { return rowset_meta()->version().first; }
    int64_t end_version() const { return rowset_meta()->version().second; }
    VersionHash version_hash() const { return rowset_meta()->version_hash(); }
    size_t index_disk_size() const { return rowset_meta()->index_disk_size(); }
    size_t data_disk_size() const { return rowset_meta()->total_disk_size(); }
    bool empty() const { return rowset_meta()->empty(); }
    bool zero_num_rows() const { return rowset_meta()->num_rows() == 0; }
    size_t num_rows() const { return rowset_meta()->num_rows(); }
    Version version() const { return rowset_meta()->version(); }
    RowsetId rowset_id() const { return rowset_meta()->rowset_id(); }
    int64_t creation_time() { return rowset_meta()->creation_time(); }
    PUniqueId load_id() const { return rowset_meta()->load_id(); }
    int64_t txn_id() const { return rowset_meta()->txn_id(); }
    int64_t partition_id() const { return rowset_meta()->partition_id(); }
    // flag for push delete rowset
    bool delete_flag() const { return rowset_meta()->delete_flag(); }
    int64_t num_segments() const { return rowset_meta()->num_segments(); }
    void to_rowset_pb(RowsetMetaPB* rs_meta) { return rowset_meta()->to_rowset_pb(rs_meta); }

    // remove all files in this rowset
    // TODO should we rename the method to remove_files() to be more specific?
    virtual OLAPStatus remove() = 0;

    // close to clear the resource owned by rowset
    // including: open files, indexes and so on
    // NOTICE: can not call this function in multithreads
    void close() {
        RowsetState old_state = _rowset_state_machine.rowset_state();
        if (old_state != ROWSET_LOADED) {
            return;
        }
        OLAPStatus st = OLAP_SUCCESS;
        {
            std::lock_guard<std::mutex> close_lock(_lock);
            uint64_t current_refs = _refs_by_reader;
            old_state = _rowset_state_machine.rowset_state();
            if (old_state != ROWSET_LOADED) {
                return;
            }
            if (current_refs == 0) {
                do_close();
            }
            st = _rowset_state_machine.on_close(current_refs);
        }
        if (st != OLAP_SUCCESS) {
            LOG(WARNING) << "state transition failed from:" << _rowset_state_machine.rowset_state();
            return;
        }
        VLOG(3) << "rowset is close. rowset state from:" << old_state
            << " to " << _rowset_state_machine.rowset_state()
            << ", version:" << start_version() << "-" << end_version()
            << ", tabletid:" << _rowset_meta->tablet_id();
    }

    // hard link all files in this rowset to `dir` to form a new rowset with id `new_rowset_id`.
    virtual OLAPStatus link_files_to(const std::string& dir, RowsetId new_rowset_id) = 0;

    // copy all files to `dir`
    virtual OLAPStatus copy_files_to(const std::string& dir) = 0;

    virtual OLAPStatus remove_old_files(std::vector<std::string>* files_to_remove) = 0;

    // return whether `path` is one of the files in this rowset
    virtual bool check_path(const std::string& path) = 0;

    // return an unique identifier string for this rowset
    std::string unique_id() const {
        return _rowset_path + "/" + rowset_id().to_string();
    }

    bool need_delete_file() const {
        return _need_delete_file;
    }

    void set_need_delete_file() {
        _need_delete_file = true;
    }

    bool contains_version(Version version) {
        return rowset_meta()->version().contains(version);
    }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

    // this function is called by reader to increase reference of rowset
    void aquire() {
        ++_refs_by_reader;
    }

    void release() {
        // if the refs by reader is 0 and the rowset is closed, should release the resouce
        uint64_t current_refs = --_refs_by_reader;
        if (current_refs == 0 && _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
            {
                std::lock_guard<std::mutex> release_lock(_lock);
                // rejudge _refs_by_reader because we do not add lock in create reader
                if (_refs_by_reader == 0 && _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
                    // first do close, then change state
                    do_close();
                    _rowset_state_machine.on_release();
                }
            }
            if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
                VLOG(3) << "close the rowset. rowset state from ROWSET_UNLOADING to ROWSET_UNLOADED"
                    << ", version:" << start_version() << "-" << end_version()
                    << ", tabletid:" << _rowset_meta->tablet_id();
            }
        }
    }

protected:
    friend class RowsetFactory;

    DISALLOW_COPY_AND_ASSIGN(Rowset);
    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    Rowset(const TabletSchema* schema,
           std::string rowset_path,
           RowsetMetaSharedPtr rowset_meta);

    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    virtual OLAPStatus init() = 0;

    // The actual implementation of load(). Guaranteed by to called exactly once.
    virtual OLAPStatus do_load(bool use_cache) = 0;

    // release resources in this api
    virtual void do_close() = 0;

    // allow subclass to add custom logic when rowset is being published
    virtual void make_visible_extra(Version version, VersionHash version_hash) {}

    const TabletSchema* _schema;
    std::string _rowset_path;
    RowsetMetaSharedPtr _rowset_meta;
    // init in constructor
    bool _is_pending;    // rowset is pending iff it's not in visible state
    bool _is_cumulative; // rowset is cumulative iff it's visible and start version < end version

    // mutex lock for load/close api because it is costly
    std::mutex _lock;
    bool _need_delete_file = false;
    // variable to indicate how many rowset readers owned this rowset
    std::atomic<uint64_t> _refs_by_reader;
    // rowset state machine
    RowsetStateMachine _rowset_state_machine;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
