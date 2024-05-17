#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Implement by chenjy
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
  SetSize(0);
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const {
  return next_page_id_;
}

void LeafPage::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
  if (next_page_id == 0) {
    LOG(ERROR) << "Fatal error";
  }
}

/**
 * Implement by chenjy
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 * 二分查找
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM) {
  int start = 0;
  int end = GetSize() - 1;
  int mid, cmp_res;
  int result = GetSize();
  while(start <= end) {
    mid = (start + end) >> 1;
    cmp_res = KM.CompareKeys(key, KeyAt(mid));
    if(cmp_res == 0) {
      // Equal
      result = mid;
      break;
    } else if(cmp_res < 0) {
      result = mid;
      end = mid - 1;
    } else {
      // If key > mid
      start = mid + 1;
    }
  }
  return result;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void LeafPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const {
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value) {
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void LeafPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index) { return {KeyAt(index), ValueAt(index)}; }

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Implement by chenjy
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM) {
  int size = GetSize();
  for(int i = 0; i < GetSize(); i++) {
    int cmp_res = KM.CompareKeys(key, KeyAt(i));
    if(cmp_res == 0) {
      // If find
      LOG(INFO) << "Insert key already exist" << endl;
      break;
    }
    else if(cmp_res > 0) {
      // If key not found, insert in the next place, and adjust following keys
      for(int j = size; j > i; j++) {
        SetKeyAt(j, KeyAt(j - 1));
        SetValueAt(j, ValueAt(j - 1));
      }
      SetKeyAt(i, key);
      SetValueAt(i, value);
      SetSize(++size);
      break;
    }
  }
  return size;
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient) {
  int size = GetSize();
  int half_size = (size + 1) / 2;
  CopyNFrom(PairPtrAt(size - half_size), size);
  recipient->CopyNFrom(PairPtrAt(size - half_size), half_size);
  SetSize(size - half_size);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size) {
  // The first key is not ignored
  PairCopy(PairPtrAt(0), src, size);
  SetSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM) {
  for(int i = 0; i < GetSize(); i++) {
    int cmp_res = KM.CompareKeys(key, KeyAt(i));
    if(cmp_res == 0) {
      // If found
      return true;
    }
  }
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * existed, perform deletion, otherwise return immediately.
 * NOTE: store key&value pair continuously after deletion
 * @return  page size after deletion
 */
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM) {
  int size = GetSize();
  for(int i = 0; i < GetSize(); i++) {
    int cmp_res = KM.CompareKeys(key, KeyAt(i));
    if(cmp_res == 0) {
      // If find
      for(int j = i; j < size - 1; j++) {
        SetKeyAt(j, KeyAt(j + 1));
        SetValueAt(j, ValueAt(j + 1));
      }
      SetSize(--size);
      break;
    }
    else if(cmp_res > 0) {
      // If key not found, return immediately
      break;
    }
  }
  return size;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient) {
  int size = GetSize();
  int recp_size = recipient->GetSize();
  PairCopy(recipient->KeyAt(recp_size), KeyAt(0),size);
  recipient->SetSize(recp_size + size);
  SetSize(0);
  recipient->SetNextPageId(GetNextPageId());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient) {
  int size = GetSize();
  int recp_size = recipient->GetSize();
  PairCopy(recipient->PairPtrAt(recp_size), PairPtrAt(0), 1);
  for(int i = 0; i < size - 1; i++) {
    SetKeyAt(i, KeyAt(i + 1));
    SetValueAt(i, ValueAt(i + 1));
  }
  SetSize(size - 1);
  recipient->SetSize(recp_size + 1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  SetKeyAt(size, key);
  SetValueAt(size, value);
  SetSize(size + 1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient) {
  int size = GetSize();
  int recp_size = recipient->GetSize();
  for(int i = recp_size; i > 0; i--) {
    recipient->SetKeyAt(i, recipient->KeyAt(i - 1));
    recipient->SetValueAt(i, recipient->ValueAt(i - 1));
  }
  PairCopy(recipient->PairPtrAt(0), PairPtrAt(size - 1), 1);
  SetSize(size - 1);
  recipient->SetSize(recp_size + 1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value) {
  int size = GetSize();
  for(int i = size; i > 0; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(0, key);
  SetValueAt(0, value);
  SetSize(size + 1);
}