#include "concurrency/lock_manager.h"

#include <iostream>

#include "common/rowid.h"
#include "concurrency/txn.h"
#include "concurrency/txn_manager.h"

void LockManager::SetTxnMgr(TxnManager *txn_mgr) { txn_mgr_ = txn_mgr; }

/**
 * TODO: Student Implement
 */
bool LockManager::LockShared(Txn *txn, const RowId &rid) {
  return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockExclusive(Txn *txn, const RowId &rid) {
  return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::LockUpgrade(Txn *txn, const RowId &rid) {
  return false;
}

/**
 * TODO: Student Implement
 */
bool LockManager::Unlock(Txn *txn, const RowId &rid) {
  return false;
}

/**
 * TODO: Student Implement
 */
void LockManager::LockPrepare(Txn *txn, const RowId &rid) {}

/**
 * TODO: Student Implement
 */
void LockManager::CheckAbort(Txn *txn, LockManager::LockRequestQueue &req_queue) {
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