#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * Implement by chenjy
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Implement by chenjy
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetKeySize(key_size);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return INVALID_PAGE_ID;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Implement by chenjy
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 * by scy
 * 做了一下修改，查找最后一个小于等于key的位置，方便搜索目标key的插入位置
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
  int left = 1;
  int right = GetSize()-1;
  int mid, comp_result;
  while(left <= right){
    mid = (left + right) / 2;
    comp_result = KM.CompareKeys(key,KeyAt(mid));
    if(comp_result == 0){
      left = mid + 1;
    }
    else if(comp_result < 0){
      right = mid - 1;
    }
    else{
      left = mid + 1;
    }
  }
  return ValueAt(left - 1);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Implement by chenjy
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 * NOTE: old_value's key < new_value's key
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  // The size of new root is 2
  SetSize(2);
  // root[0] is old value
  SetValueAt(0,old_value);
  // root[1] is new key and new value
  SetKeyAt(1,new_key);
  SetValueAt(1,new_value);
}

/*
 * Implement by chenjy
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
  int size = GetSize();
  int old_index = ValueIndex(old_value);
  if(old_index == -1) {
    LOG(ERROR) << "old value not exists" << endl;
    // throw invalid_argument("old value not exists");
    return size;
  }
  if(size == GetMaxSize()) {
    LOG(ERROR) << "Insertion overflow, splitting needed" << endl;
    return size;
  }
  for(int i = size; i > old_index + 1; i--){
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetKeyAt(old_index + 1, new_key);
  SetValueAt(old_index + 1, new_value);
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Implement by chenjy
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 * NOTE: 把后一半移走了，保留前一半
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int half_size = (size + 1) / 2; // half_size是移走的数量
  if(recipient->GetSize() + half_size > recipient->GetMaxSize()) {
    LOG(ERROR) << "Insertion overflow, splitting needed" << endl;
    return;
  }
  // KeyAt(i) function returns the beginning location of pair[i]
  recipient->CopyNFrom(PairPtrAt(size - half_size), half_size, buffer_pool_manager);
  IncreaseSize(-half_size);
}

/*
 * Implement by chenjy
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
  if(size > GetMaxSize()) {
    LOG(ERROR) << "Insertion overflow" << endl;
    return;
  }
  // The first key is not ignored
  PairCopy(PairPtrAt(0), src, size);
  for(int i = 0; i < size; i++) {
    auto page = buffer_pool_manager->FetchPage(ValueAt(i));
    if(page != nullptr) {
      auto node = reinterpret_cast<InternalPage *>(page->GetData());
      node->SetParentPageId(GetPageId());
      buffer_pool_manager->UnpinPage(ValueAt(i), true);
    } else {
      LOG(ERROR) << "Fatal error: Page not found" << endl;
    }
  }
  SetSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Implement by chenjy
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
  int size = GetSize();
  if(index >= size) {
    LOG(ERROR) << "Invalid argument: Index overflow" << endl;
    return;
  }
  for(int i = index; i < size - 1; i++){
    SetKeyAt(i,KeyAt(i + 1));
    SetValueAt(i,ValueAt(i + 1));
  }
  IncreaseSize(-1);
}

/*
 * Implement by chenjy
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
  SetSize(0);
  return ValueAt(0);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Implement by chenjy
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  int recp_size = recipient->GetSize();
  if(size + recp_size > recipient->GetMaxSize()) {
    LOG(ERROR) << "Insertion overflow: splitting needed" << endl;
    return;
  }
  // The first key should be replaced by middle key after merge.
  SetKeyAt(0, middle_key);
  PairCopy(recipient->PairPtrAt(recp_size), PairPtrAt(0), size);
  for(int i = 0; i < size + recp_size; i++) {
    auto page = buffer_pool_manager->FetchPage(recipient->ValueAt(i));
    if(page != nullptr) {
      auto node = reinterpret_cast<InternalPage *>(page->GetData());
      node->SetParentPageId(recipient->GetPageId());
      buffer_pool_manager->UnpinPage(recipient->ValueAt(i), true);
    } else {
      LOG(ERROR) << "Fatal error: Page not found" << endl;
    }
  }
  recipient->IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Implement by chenjy
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager) {
  if(GetSize() == GetMinSize() || recipient->GetSize() == recipient->GetMaxSize()) {
    LOG(ERROR) << "Move overflow / underflow" << endl;
    return;
  }
  // Copy middle key to move into recipient
  SetKeyAt(0, middle_key);
  // Change middle key by first key
  *middle_key = *KeyAt(1);
  recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
  Remove(0);
}

/*
 * Implement by chenjy
 * Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  if(size == GetMaxSize()) {
    LOG(ERROR) << "Insertion overflow: splitting needed" << endl;
    return;
  }
  // Set key and value
  SetKeyAt(size, key);
  SetValueAt(size, value);
  // Set page parent id
  auto page = buffer_pool_manager->FetchPage(value);
  if(page != nullptr) {
    auto node = reinterpret_cast<InternalPage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  } else {
    LOG(ERROR) << "Fatal error: Page not found" << endl;
  }
  IncreaseSize(1);
}

/*
 * Implement by chenjy
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
  int size = GetSize();
  if(size == GetMinSize() || recipient->GetSize() == recipient->GetMaxSize()) {
    LOG(ERROR) << "Move overflow / underflow" << endl;
    return;
  }
  // Key[0] will be moved to Key[1] later
  recipient->SetKeyAt(0, middle_key);
  recipient->CopyFirstFrom(KeyAt(size - 1), ValueAt(size - 1), buffer_pool_manager);
  // Change middle key by first key
  *middle_key = *KeyAt(size - 1);
  // Remove last pair
  Remove(size - 1);
}

/*
 * Implement by chenjy
 * Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
// 这里修改了函数参数表，是为了便于Key的插入
void InternalPage::CopyFirstFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
  SetKeyAt(0, key);
  for(int i = GetSize(); i > 0; i--) {
    SetKeyAt(i, KeyAt(i - 1));
    SetValueAt(i, ValueAt(i - 1));
  }
  SetValueAt(0, value);
  auto page = buffer_pool_manager->FetchPage(value);
  if(page != nullptr) {
    auto node = reinterpret_cast<InternalPage *>(page->GetData());
    node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(value, true);
  }
  IncreaseSize(1);
}