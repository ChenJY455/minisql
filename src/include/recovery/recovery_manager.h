#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
  lsn_t checkpoint_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase persist_data_{};

  inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

  inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
 public:
  /**
    * Implement by chenjy
   */
  void Init(CheckPoint &last_checkpoint) {
    persist_lsn_ = last_checkpoint.checkpoint_lsn_;
    active_txns_ = last_checkpoint.active_txns_;
    data_ = last_checkpoint.persist_data_;
  }

  /**
    * Implement by chenjy
   */
  void RedoPhase() {
    for(const auto& log_iter: log_recs_) {
      if (log_iter.first < persist_lsn_) continue;
      LogRecPtr cur_log = log_iter.second;
      active_txns_[cur_log->txn_id_] = cur_log->lsn_;
      switch (cur_log->type_) {
        case LogRecType::kInvalid:
          break;
        case LogRecType::kInsert:
          data_[cur_log->key_] = cur_log->val_;
          break;
        case LogRecType::kDelete:
          if (data_[cur_log->key_] == cur_log->val_)
            data_.erase(cur_log->key_);
          else
            LOG(WARNING) << "Repeated deletion" << std::endl;
          break;
        case LogRecType::kUpdate:
          if (data_[cur_log->key_] == cur_log->val_)
            data_.erase(cur_log->key_);
          else
            LOG(WARNING) << "Old value has been modified" << std::endl;
          data_[cur_log->new_key_] = cur_log->new_val_;
          break;
        case LogRecType::kBegin:
          break;
        case LogRecType::kCommit:
          active_txns_.erase(cur_log->txn_id_);
          break;
        case LogRecType::kAbort:
          // Rollback
          lsn_t p_lsn = cur_log->prev_lsn_;
          while (p_lsn != INVALID_LSN) {
            auto p = log_recs_[p_lsn];
            RollBack(p);
            p_lsn = p->prev_lsn_;
          }
          active_txns_.erase(cur_log->txn_id_);
          break;
      }
    }
  }

  /**
    * Implement by chenjy
   */
  void UndoPhase() {
    for (const auto txn_iter : active_txns_) {
      lsn_t lsn = txn_iter.second;
      lsn_t lsn_p = lsn;
      while(lsn_p != INVALID_LSN) {
        LogRecPtr p = log_recs_[lsn_p];
        RollBack(p);
        lsn_p = p->prev_lsn_;
      }
    }
    // Clear active txn
    active_txns_.clear();
  }

  // used for test only
  void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

  // used for test only
  inline KvDatabase &GetDatabase() { return data_; }

 private:
  std::map<lsn_t, LogRecPtr> log_recs_{};
  lsn_t persist_lsn_{INVALID_LSN};
  ATT active_txns_{};
  KvDatabase data_{};  // all data in database

  /**
    * Implement by chenjy
   */
  void RollBack(const LogRecPtr& p) {
    switch (p->type_) {
      case LogRecType::kInsert:
        // Delete inserted one
        if(data_[p->key_] == p->val_)
          data_.erase(p->key_);
        else
          LOG(WARNING) << "Rollback unknown error" << std::endl;
        break;
      case LogRecType::kUpdate:
        // Remove update
        if(data_[p->new_key_] == p->new_val_)
          data_.erase(p->new_key_);
        else
          LOG(WARNING) << "Rollback unknown error" << std::endl;
        data_[p->key_] = p->val_;
        break;
      case LogRecType::kDelete:
        // Insert deleted one
        data_[p->key_] = p->val_;
        break;
      default:
        // Nothing to do.
        break;
    }
  }
};

#endif  // MINISQL_RECOVERY_MANAGER_H