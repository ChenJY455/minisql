#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * Implement by chenjy
 */
TableIterator::TableIterator(TableHeap *table_heap, const Row& row, Txn *txn):
  table_heap_(table_heap), row_(row), txn_(txn) {}

TableIterator::TableIterator(const TableIterator &other) = default;

TableIterator::~TableIterator() = default;

bool TableIterator::operator==(const TableIterator &itr) const {
  return row_.GetRowId() == itr.row_.GetRowId();
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(row_.GetRowId() == itr.row_.GetRowId());
}

const Row &TableIterator::operator*() {
  return row_;
}

Row *TableIterator::operator->() {
  return &row_;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if(itr == *this)
    return *this;
  table_heap_ = itr.table_heap_;
  row_ = itr.row_;
  txn_ = itr.txn_;
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  RowId rid = row_.GetRowId();
  page_id_t page_id = rid.GetPageId();
  RowId next_rid;
  auto page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
  if(page == nullptr){
    throw runtime_error("Page not exist");
    LOG(ERROR)<<"Page not exist"<<std::endl;
    return *this;
  }
  // If has next tuple. (This is not the last tuple)
  if(page->GetNextTupleRid(rid, &next_rid)){
    row_.SetRowId(next_rid);
    table_heap_->buffer_pool_manager_->UnpinPage(page_id, false);
    return *this;
  }
  // If last tuple already, turn to next page.
  auto pre_id = page_id;
  page_id = page->GetNextPageId();
  table_heap_->buffer_pool_manager_->UnpinPage(pre_id, false);
  while(page_id != INVALID_PAGE_ID) {
    page = reinterpret_cast<TablePage *>(table_heap_->buffer_pool_manager_->FetchPage(page_id));
    if(page == nullptr) {
      throw runtime_error("Page not exist");
      LOG(ERROR)<<"Page not exist"<<std::endl;
      return *this;
    }
    if(page->GetFirstTupleRid(&next_rid)) {
      row_.SetRowId(next_rid);
      table_heap_->buffer_pool_manager_->UnpinPage(page_id,false);
      return *this;
    }
    pre_id = page_id;
    page_id = page->GetNextPageId();
    table_heap_->buffer_pool_manager_->UnpinPage(pre_id, false);
  }
  // No tuple remained, return End().
  row_.SetRowId(RowId(INVALID_PAGE_ID,0));
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  const TableIterator temp(*this);
  ++(*this);
  return temp;
}
