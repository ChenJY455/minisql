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

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].insert(t2); }

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { waits_for_[t1].erase(t2); }

bool LockManager::HasCycle(txn_id_t &newest_tid_in_cycle) {
  // reset all variables before DFS
  revisited_node_ = INVALID_TXN_ID;
  visited_set_.clear();
  std::stack<txn_id_t>().swap(visited_path_);

  // get all txns
  std::set<txn_id_t> txn_set;
  for (const auto &[t1, vec] : waits_for_) {
    txn_set.insert(t1);
    for (const auto &t2 : vec) {
      txn_set.insert(t2);
    }
  }

  // starts from each txn to search cycle
  for (const auto &start_txn_id : txn_set) {
    if (DFS(start_txn_id)) {
      newest_tid_in_cycle = revisited_node_;
      while (!visited_path_.empty() && revisited_node_ != visited_path_.top()) {
        newest_tid_in_cycle = std::max(newest_tid_in_cycle, visited_path_.top());
        visited_path_.pop();
      }
      return true;
    }
  }

  newest_tid_in_cycle = INVALID_TXN_ID;
  return false;
}

void LockManager::DeleteNode(txn_id_t txn_id) {
  waits_for_.erase(txn_id);

  auto *txn = txn_mgr_->GetTransaction(txn_id);

  for (const auto &row_id : txn->GetSharedLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }

  for (const auto &row_id : txn->GetExclusiveLockSet()) {
    for (const auto &lock_req : lock_table_[row_id].req_list_) {
      if (lock_req.granted_ == LockMode::kNone) {
        RemoveEdge(lock_req.txn_id_, txn_id);
      }
    }
  }
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval_);
    {
      std::unique_lock<std::mutex> l(latch_);
      std::unordered_map<txn_id_t, RowId> required_rec;
      // build dependency graph
      for (const auto &[row_id, lock_req_queue] : lock_table_) {
        for (const auto &lock_req : lock_req_queue.req_list_) {
          // ensure lock_req has no XLock or SLock
          if (lock_req.granted_ != LockMode::kNone) continue;
          required_rec[lock_req.txn_id_] = row_id;
          for (const auto &granted_req : lock_req_queue.req_list_) {
            // ensure granted_req has XLock or SLock
            if (LockMode::kNone == granted_req.granted_) continue;
            AddEdge(lock_req.txn_id_, granted_req.txn_id_);
          }
        }
      }
      // break each cycle
      txn_id_t txn_id = INVALID_TXN_ID;
      while (HasCycle(txn_id)) {
        auto *txn = txn_mgr_->GetTransaction(txn_id);
        DeleteNode(txn_id);
        txn->SetState(TxnState::kAborted);
        lock_table_[required_rec[txn_id]].cv_.notify_all();
      }
      waits_for_.clear();
    }
  }
}

bool LockManager::DFS(txn_id_t txn_id) {
  // node revisited
  if (visited_set_.find(txn_id) != visited_set_.end()) {
    revisited_node_ = txn_id;
    return true;
  }

  // set visited flag & record visited path
  visited_set_.insert(txn_id);
  visited_path_.push(txn_id);

  // recursive search sibling nodes
  for (const auto wait_for_txn_id : waits_for_[txn_id]) {
    if (DFS(wait_for_txn_id)) {
      return true;
    }
  }

  // dfs pop
  visited_set_.erase(txn_id);
  visited_path_.pop();

  return false;
}

std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  std::vector<std::pair<txn_id_t, txn_id_t>> result;
  for (const auto &[t1, sibling_vec] : waits_for_) {
    for (const auto &t2 : sibling_vec) {
      result.emplace_back(t1, t2);
    }
  }
  std::sort(result.begin(), result.end());
  return result;
}
