#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement by scy
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> unique_lock(latch_);
  //在修改锁表和处理锁请求时，操作是线程安全的

  if (txn->GetIsolationLevel() == IsolationLevel::kReadUncommitted) {
    //如果事务的隔离级别是 IsolationLevel::kReadUncommitted，则事务会被设置为中止状态
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockSharedOnReadUncommitted);
  }

  LockPrepare(txn, rid);
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  //将新的锁请求添加到队列前端，并在map中存储其迭代器。
  lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kShared);
  //如果当前持有排他性写锁
  if (lock_request_queue.is_writing_) {
    lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || !lock_request_queue.is_writing_;
    });
  }
  //检查txn的state是否是abort
  CheckAbort(txn, lock_request_queue);
  //将 rid 添加到事务的共享锁集合中
  txn->GetSharedLockSet().emplace(rid);
  lock_request_queue.sharing_cnt_++;
  auto iter = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kShared;
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> unique_lock(latch_);

  LockPrepare(txn, rid);
  LockRequestQueue &lock_request_queue = lock_table_[rid];
  //将新的锁请求添加到队列前端，并在map中存储其迭代器。
  lock_request_queue.EmplaceLockRequest(txn->GetTxnId(), LockMode::kExclusive);
  //如果当前持有排他性写锁或共享锁
  if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 0) {
    lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || (!lock_request_queue.is_writing_ && 0 == lock_request_queue.sharing_cnt_);
    });
  }
  //检查txn的state是否是abort
  CheckAbort(txn, lock_request_queue);
  //将 rid 添加到事务的排他锁集合中
  txn->GetExclusiveLockSet().emplace(rid);
  lock_request_queue.is_writing_ = true;
  auto iter = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
  iter->granted_ = LockMode::kExclusive;
  return true;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> unique_lock(latch_);

  if (txn->GetState() == TxnState::kShrinking) {
    //如果事务是收缩阶段，事务不能申请新的锁，设置他为中止状态
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }

  LockRequestQueue &lock_request_queue = lock_table_[rid];
  if (lock_request_queue.is_upgrading_) {
    //如果事务正在upgrade
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kUpgradeConflict);
  }

  auto iter = lock_request_queue.GetLockRequestIter(txn->GetTxnId());

  //如果已经在请求了或者已经获得了
  if (iter->lock_mode_ == LockMode::kExclusive && iter->granted_ == LockMode::kExclusive) {
    return true;
  }

  iter->lock_mode_ = LockMode::kExclusive;
  iter->granted_ = LockMode::kShared;
  //如果当前持有排他性写锁或大于1的共享锁（唯一的共享锁是自己）
  if (lock_request_queue.is_writing_ || lock_request_queue.sharing_cnt_ > 1) {
    lock_request_queue.is_upgrading_ = true;
    lock_request_queue.cv_.wait(unique_lock, [&lock_request_queue, txn]() -> bool {
      return txn->GetState() == TxnState::kAborted || (!lock_request_queue.is_writing_ && 1 == lock_request_queue.sharing_cnt_);
    });
  }

  //如果事务中止了
  if (txn->GetState() == TxnState::kAborted) {
    lock_request_queue.is_upgrading_ = false;
  }
  CheckAbort(txn, lock_request_queue);

  //删除共享锁
  txn->GetSharedLockSet().erase(rid);
  //增加排他锁
  txn->GetExclusiveLockSet().emplace(rid);
  lock_request_queue.sharing_cnt_--;
  lock_request_queue.is_upgrading_ = false;
  lock_request_queue.is_writing_ = true;
  iter->granted_ = LockMode::kExclusive;
  return true;
}

bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  std::unique_lock<std::mutex> unique_lock(latch_);

  LockRequestQueue &lock_request_queue = lock_table_[rid];
  //删除所有锁
  txn->GetSharedLockSet().erase(rid);
  txn->GetExclusiveLockSet().erase(rid);

  auto iter = lock_request_queue.GetLockRequestIter(txn->GetTxnId());
  auto lock_mode = iter->lock_mode_;

  if (!lock_request_queue.EraseLockRequest(txn->GetTxnId())) {
    return false;
  }

  //表示事务正在释放其持有的锁，并且可能即将完成
  if (txn->GetState() == TxnState::kGrowing && !(txn->GetIsolationLevel() == IsolationLevel::kReadCommitted && lock_mode == LockMode::kShared)) {
    txn->SetState(TxnState::kShrinking);
  }
  if (lock_mode == LockMode::kShared) {
    lock_request_queue.sharing_cnt_--;
    lock_request_queue.cv_.notify_all();
  } else {
    lock_request_queue.is_writing_ = false;
    lock_request_queue.cv_.notify_all();
  }
  return true;
}

void LockManager::LockPrepare(Txn *txn, const RowId &rid) {
  if (txn->GetState() == TxnState::kShrinking) {
    //如果事务是收缩阶段，事务不能申请新的锁，设置他为中止状态
    txn->SetState(TxnState::kAborted);
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kLockOnShrinking);
  }
  if (lock_table_.find(rid) == lock_table_.end()) {
    //如果不存在rid相关的锁
    lock_table_.emplace(std::piecewise_construct, std::forward_as_tuple(rid), std::forward_as_tuple());
  }
}

void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
  if (txn->GetState() == TxnState::kAborted) {
    //如果事务是中止状态，从队列和map中移除锁请求
    req_queue.EraseLockRequest(txn->GetTxnId());
    throw TxnAbortException(txn->GetTxnId(), AbortReason::kDeadlock);
  }
}

/**
 * Implement by chenjy
 */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  // Add t1->t2, which means t1 waits t2
  waits_for_[t1].insert(t2);
}

/**
 * Implement by chenjy
 */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].erase(t2);
}

/**
 * Implement by chenjy
 */
bool LockManager::DFS(const txn_id_t txn_id) {
  if (visited_set_.count(txn_id)) {
    // If this node already exists, which means meeting cycles
    revisited_node_ = txn_id;
    return true;
  }

  // Add to path
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);
  // recursive search
  for (auto wait_id : waits_for_[txn_id]) {
    if (DFS(wait_id))
      return true;
  }
  // Delete from path
  visited_set_.erase(txn_id);
  visited_path_.pop();
  return false;
}

/**
 * Implement by chenjy
 */
bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  // Clear path
  revisited_node_ = INVALID_TXN_ID;
  visited_set_.clear();
  while(!visited_path_.empty()) visited_path_.pop();

  // Use a set to collect all txn, and sort them to ensure oldest txn deletes first
  std::set<txn_id_t> txn_set;
  for (auto [t1, neighbor]: waits_for_) {
    if(!neighbor.empty()) {
      txn_set.insert(t1);
      for (auto t2: neighbor) {
        txn_set.insert(t2);
      }
    }
  }

  // starts from each txn to search cycle
  for (auto start_id : txn_set) {
    if (DFS(start_id)) {
      // Find oldest node, starting from revisited_node
      newest_tid_in_cycle = revisited_node_;
      // Stop when finish the cycle
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }
  // No cycles
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id: txn->GetSharedLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id: txn->GetExclusiveLockSet()) {
    for (const auto &lock_req: lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

/**
 * Implement by chenjy
 */
void LockManager::RunCycleDetection() {
  // Always iterating and check cycles
  while (enable_cycle_detection_) {
    // Wait some time
    std::this_thread::sleep_for(cycle_detection_interval_);
    // Use block to destruct variable automatically
    {
      // Use unique lock, avoid conflicts
      std::unique_lock<std::mutex> l(latch_);
      std::unordered_map<txn_id_t, RowId> required_rec;
      // Clear wait relation
      waits_for_.clear();
      // build dependency graph
      for (const auto &[row_id, lock_req_queue] : lock_table_) {
        for (auto lock_req : lock_req_queue.req_list_) {
          // If lock has been grated, no need to check deadlock
          if (lock_req.granted_ != LockMode::kNone) continue;
          // Add txn-rowId correspondence to look up fast
          required_rec[lock_req.txn_id_] = row_id;
          for (auto granted_req : lock_req_queue.req_list_) {
            // Ensure granted_req has been granted lock
            if (granted_req.granted_ != LockMode::kNone)
              AddEdge(lock_req.txn_id_, granted_req.txn_id_);
          }
        }
      }
      // break each cycle
      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(txn_id)) {
        auto txn = txn_mgr_->GetTransaction(txn_id);
        DeleteNode(txn_id);
        // Set aborted
        txn->SetState(TxnState::kAborted);
        // Awake all sleep thread
        lock_table_[required_rec[txn_id]].cv_.notify_all();
      }
    }
  }
}

/**
 * Implement by chenjy
 */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (auto [t1, neighbor] : waits_for_) {
    for (auto t2 : neighbor) {
      // Add each edge
      result.emplace_back(t1, t2);
    }
  }
  // Sort result
  std::sort(result.begin(), result.end());
  return result;
}