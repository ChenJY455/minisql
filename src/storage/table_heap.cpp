#include "storage/table_heap.h"

/**
 * Implement by chenjy
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  if(row.GetSerializedSize(schema_) >= PAGE_SIZE) {
    LOG(WARNING) << "Tuple size too large" << endl;
    return false;
  }
  // Define iterator to find available page.
  int cur_id = first_page_id_;
  int prev_id = first_page_id_;
  while(cur_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_id));
    if(page == nullptr) {
      LOG(WARNING) << "TablePage has unavailable records" << endl;
      return false;
    }
//    page->WLatch();
    if(page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      // If insert successfully
//      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_id, true);
      return true;
    }
//  page->WUnlatch();
    prev_id = cur_id;
    buffer_pool_manager_->UnpinPage(prev_id, false);
    cur_id = page->GetNextPageId();
  }

  // If no page available, create new one
  page_id_t new_id;
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_id));
  page->Init(new_id, prev_id, log_manager_, txn);
//  page->WLatch();
  page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_);
//  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_id, true);

  // Update last page ptr
  auto pre_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(prev_id));
  pre_page->SetNextPageId(new_id);
  buffer_pool_manager_->UnpinPage(prev_id, true);
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * Implement by chenjy
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    LOG(WARNING) << "Page not exist" << endl;
    return false;
  }
  Row old_row(rid);
  page->WLatch();
  if(page->UpdateTuple(row, &old_row, schema_, txn, lock_manager_, log_manager_)) {
    // If update successfully
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(rid.GetPageId(),true);
    return true;
  }
  // If update fail
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(),false);
  return false;
}

/**
 * Implement by chenjy
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // Step2: Delete the tuple from the page.
  if(page == nullptr) {
    LOG(INFO) << "Page not exist" << endl;
    return;
  }
  page->WLatch();
  page->ApplyDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * Implement by chenjy
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    LOG(WARNING) << "Page not exist" << endl;
    return false;
  }
  if(page->GetTuple(row, schema_, txn,  lock_manager_)) {
    // If find
    buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
    return true;
  }
  buffer_pool_manager_->UnpinPage(row->GetRowId().GetPageId(), false);
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * Implement by chenjy
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t cur_page = first_page_id_;
  while(cur_page != INVALID_PAGE_ID){
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page));
    RowId first_rid;
    if(page->GetFirstTupleRid(&first_rid)){
      Row *first_row = new Row(first_rid);
      page->GetTuple(first_row, schema_, txn, lock_manager_);
      buffer_pool_manager_->UnpinPage(cur_page, false);
      return TableIterator(this, *first_row, txn);
    }
    buffer_pool_manager_->UnpinPage(cur_page,false);
    cur_page = page->GetNextPageId();
  }
  return End();
}

/**
 * Implement by chenjy
 */
TableIterator TableHeap::End() {
  return TableIterator(this, RowId(INVALID_PAGE_ID,0), nullptr);
}