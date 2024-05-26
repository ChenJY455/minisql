#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
  kInvalid,
  kInsert,
  kDelete,
  kUpdate,
  kBegin,
  kCommit,
  kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * Implement by chenjy
 */
struct LogRec {
  LogRec() = default;
  LogRec(LogRecType type, lsn_t lsn, lsn_t prev_lsn, txn_id_t txn_id,
         KeyType key, ValType val, KeyType new_key, ValType new_val)
    : type_(type),
      lsn_(lsn),
      prev_lsn_(prev_lsn),
      txn_id_(txn_id),
      key_(std::move(key)),
      val_(val),
      new_key_(std::move(new_key)),
      new_val_(new_val) {}

  LogRecType type_{LogRecType::kInvalid};
  lsn_t lsn_{INVALID_LSN};
  lsn_t prev_lsn_{INVALID_LSN};
  txn_id_t txn_id_{INVALID_TXN_ID};

  KeyType key_;
  ValType val_;

  KeyType new_key_;
  ValType new_val_;

  /* used for testing only */
  static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
  static lsn_t next_lsn_;
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * Implement by chenjy
 */
static LogRecPtr CreateLog(LogRecType type, txn_id_t txn_id,
                           [[maybe_unused]]const KeyType& key = "",     [[maybe_unused]]ValType val = 0,
                           [[maybe_unused]]const KeyType& new_key = "", [[maybe_unused]]ValType new_val = 0)
{
  lsn_t prev_lsn = INVALID_LSN;
  // Get and update pre_lsn
  auto prev_txn = LogRec::prev_lsn_map_.find(txn_id);
  // If find last txn, update lsn
  if (prev_txn != LogRec::prev_lsn_map_.end()) {
    prev_lsn = prev_txn->second;
    prev_txn->second = LogRec::next_lsn_;
  } else {
    // If not find, add new lsn
    LogRec::prev_lsn_map_.emplace(txn_id, LogRec::next_lsn_);
  }
  return std::make_shared<LogRec>(type, LogRec::next_lsn_++, prev_lsn, txn_id, key, val, new_key, new_val);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, const KeyType& ins_key, ValType ins_val) {
  return CreateLog(LogRecType::kInsert, txn_id, ins_key, ins_val);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, const KeyType& del_key, ValType del_val) {
  return CreateLog(LogRecType::kDelete, txn_id, del_key, del_val);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, const KeyType& old_key, ValType old_val,
                                 const KeyType& new_key, ValType new_val)
{
  return CreateLog(LogRecType::kUpdate, txn_id, old_key, old_val, new_key, new_val);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
  return CreateLog(LogRecType::kBegin, txn_id);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
  return CreateLog(LogRecType::kCommit, txn_id);
}

/**
 * Implement by chenjy
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
  return CreateLog(LogRecType::kAbort, txn_id);
}

#endif  // MINISQL_LOG_REC_H