#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"
#include <page/header_page.h>

/**
 * TODO: Implement by ShenCongyu
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  if(leaf_max_size_ == 0)
    leaf_max_size_ = LEAF_PAGE_SIZE;
  if(internal_max_size_ == 0)
    internal_max_size_ = INTERNAL_PAGE_SIZE;
  //initialize root_page_id_
  auto index_root_pages = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  page_id_t root_page_id;
  if(index_root_pages->GetRootId(index_id_, &root_page_id)) {
    root_page_id_ = root_page_id;
  } else {
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);
}

void BPlusTree::Destroy(page_id_t current_page_id) {
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  if(IsEmpty())
    return false;
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false));
  int key_index = leaf_page->KeyIndex(key, processor_);
  if(key_index >= 0 && key_index < leaf_page->GetSize()) {//key_index is legal
    if(processor_.CompareKeys(leaf_page->KeyAt(key_index), key) == 0) {//found key is the same
      result.push_back(leaf_page->ValueAt(key_index));
      buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
      return true;
    }
  }
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if(IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  //ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)), buffer_pool_manager_);
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  //ask a new page from buffer pool manager
  page_id_t root_page_id;
  BPlusTreeLeafPage *new_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(root_page_id));
  if(new_leaf_page == nullptr) {
    LOG(ERROR) << "StartNewTree out of memory" << std::endl;
    throw std::bad_alloc();
    return;
  }
  root_page_id_ = root_page_id;
  UpdateRootPageId(1);
  new_leaf_page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  new_leaf_page->SetNextPageId(INVALID_PAGE_ID);
  new_leaf_page->Insert(key, value, processor_);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  BPlusTreeLeafPage *tree_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key, false));
  //ToString(reinterpret_cast<BPlusTreePage *>(tree_leaf_page), buffer_pool_manager_);
  int inserted_size = tree_leaf_page->Insert(key, value, processor_);
  if(inserted_size == -2) {
    buffer_pool_manager_->UnpinPage(tree_leaf_page->GetPageId(), false);
    return false;
  }
  if(inserted_size == leaf_max_size_) {
    //LOG(INFO) << "split" << std::endl;
    BPlusTreeLeafPage *new_leaf_page = Split(tree_leaf_page, transaction);
    InsertIntoParent(tree_leaf_page, new_leaf_page->KeyAt(0), new_leaf_page, transaction);
    //这里还要进入InsertIntoParent函数，所以还不用Unpin
    return true;
  }
  buffer_pool_manager_->UnpinPage(tree_leaf_page->GetPageId(), true);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 * 只做一次split不继续向上传递，向上继续split在insert函数中
 * split只是中间过程，所以还不用unpin
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  BPlusTreeInternalPage *tree_internal_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(new_page_id));
  if(tree_internal_page == nullptr) {
    LOG(ERROR) << "Split internal out of memory" << std::endl;
    throw std::bad_alloc();
    return nullptr;
  }
  tree_internal_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), internal_max_size_);
  node->MoveHalfTo(tree_internal_page, buffer_pool_manager_);
  return tree_internal_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  BPlusTreeLeafPage* tree_leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->NewPage(new_page_id));
  if(tree_leaf_page == nullptr) {
    LOG(ERROR) << "Split leaf out of memory" << std::endl;
    throw std::bad_alloc();
    return nullptr;
  }
  tree_leaf_page->Init(new_page_id, node->GetParentPageId(), node->GetKeySize(), leaf_max_size_);
  page_id_t next_id = node->GetNextPageId();
  node->MoveHalfTo(tree_leaf_page);
  node->SetNextPageId(tree_leaf_page->GetPageId());
  tree_leaf_page->SetNextPageId(next_id);
  return tree_leaf_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if(old_node->IsRootPage()) {
    page_id_t root_page_id;
    BPlusTreeInternalPage* new_root_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->NewPage(root_page_id));
    if(new_root_page == nullptr) {
      LOG(ERROR) << "InsertIntoParent out of memory" << std::endl;
      throw std::bad_alloc();
      return;
    }
    new_root_page->Init(root_page_id, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    UpdateRootPageId(0);
    old_node->SetParentPageId(root_page_id);
    new_node->SetParentPageId(root_page_id);
    root_page_id_ = root_page_id;
    buffer_pool_manager_->UnpinPage(root_page_id, true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  } else {
    BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
    int inserted_size = parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
    new_node->SetParentPageId(parent_page->GetPageId());
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    if(inserted_size == internal_max_size_) {
      BPlusTreeInternalPage *new_page = reinterpret_cast<BPlusTreeInternalPage *>(Split(parent_page, transaction));
      InsertIntoParent(parent_page, new_page->KeyAt(0), new_page, transaction);
      return;
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if(IsEmpty())
    return;
  BPlusTreeLeafPage *deletion_target = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key));

  /*std::cout << "deletion target:" << deletion_target->GetPageId() << std::endl;
  ToString(reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
  std::cout << std::endl;*/

  if(deletion_target == nullptr) {
    LOG(ERROR) << "didn't find deletion target" << std::endl;
    return;
  }
  int before_delete_size = deletion_target->GetSize();
  int after_delete_size = deletion_target->RemoveAndDeleteRecord(key, processor_);
  if(before_delete_size == after_delete_size) {
    LOG(ERROR) << "fail to delete the target" << std::endl;
    buffer_pool_manager_->UnpinPage(deletion_target->GetPageId(), false);
    return;
  }
  if(after_delete_size < deletion_target->GetMinSize()) {
    CoalesceOrRedistribute(deletion_target, transaction);
    //后续有对parent_page和deletion_target的更新操作，所以可以直接返回
    return;
  }
  BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(deletion_target->GetParentPageId()));
  if(parent_page != nullptr) {
    int index = parent_page->ValueIndex(deletion_target->GetPageId());
    parent_page->SetKeyAt(index, deletion_target->KeyAt(0));
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  }
  buffer_pool_manager_->UnpinPage(deletion_target->GetPageId(), true);
}

/** TODO:
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if(node->IsRootPage()) {
    if(AdjustRoot(node)) {
      buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
      buffer_pool_manager_->DeletePage(node->GetPageId());
      return true;
    }
    return false;
  } else {
    BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index = parent_page->ValueIndex(node->GetPageId());
    N *left_sibling = nullptr;
    N *right_sibling = nullptr;
    if(index > 0)
      left_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1)));
    if(index < parent_page->GetSize() - 1)
      right_sibling = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(index + 1)));
    if(left_sibling != nullptr) {
      if(right_sibling != nullptr)
        buffer_pool_manager_->UnpinPage(parent_page->ValueAt(index + 1), false);
      if(left_sibling->GetSize() + node->GetSize() >= node->GetMaxSize()) {
        Redistribute(left_sibling, node, 1);
      } else {
        Coalesce(node, left_sibling, parent_page, index, transaction);
      }
      buffer_pool_manager_->UnpinPage(parent_page->ValueAt(index - 1), true);
    } else if(right_sibling != nullptr) {
      if(right_sibling->GetSize() + node->GetSize() >= node->GetMaxSize()) {
        Redistribute(right_sibling, node, 0);
      } else {
        Coalesce(right_sibling, node, parent_page, index + 1, transaction);
      }
      buffer_pool_manager_->UnpinPage(parent_page->ValueAt(index + 1), true);
    }
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return true;
  }
}

/**
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  neighbor_node->MoveAllTo(node);
  parent->Remove(index);
  buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  if(parent->GetSize() < parent->GetMinSize()) {
    CoalesceOrRedistribute(parent, transaction);
  }
  return true;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  GenericKey *middle_key = parent->KeyAt(index);
  neighbor_node->MoveAllTo(node, middle_key, buffer_pool_manager_);
  parent->Remove(index);
  buffer_pool_manager_->DeletePage(neighbor_node->GetPageId());
  if(parent->GetSize() < parent->GetMinSize()) {
    CoalesceOrRedistribute(parent, transaction);
  }
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  //有两个结点，那么一定有更上一级的结点，所以parent_page不会是nullptr
  if(index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId()));
    int index_neighbor_node = parent_page->ValueIndex(neighbor_node->GetPageId());
    parent_page->SetKeyAt(index_neighbor_node, neighbor_node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(neighbor_node->GetParentPageId(), true);
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index_node = parent_page->ValueIndex(node->GetPageId());
    parent_page->SetKeyAt(index_node, node->KeyAt(0));
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
  }
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  if(index == 0) {
    BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(neighbor_node->GetParentPageId()));
    int index_neighbor_node = parent_page->ValueIndex(neighbor_node->GetPageId());
    GenericKey *middle_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent_page->SetKeyAt(index_neighbor_node, middle_key);
    buffer_pool_manager_->UnpinPage(neighbor_node->GetParentPageId(), true);
  } else {
    BPlusTreeInternalPage *parent_page = reinterpret_cast<BPlusTreeInternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId()));
    int index_node = parent_page->ValueIndex(node->GetPageId());
    GenericKey *middle_key = neighbor_node->KeyAt(neighbor_node->GetSize() - 1);
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent_page->SetKeyAt(index_node, middle_key);
    buffer_pool_manager_->UnpinPage(node->GetParentPageId(), true);
  }
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  //case 1:
  if(old_root_node->GetSize() == 1) {
    BPlusTreeInternalPage *internal_page = reinterpret_cast<BPlusTreeInternalPage *>(old_root_node);
    root_page_id_ = internal_page->ValueAt(0);
    BPlusTreePage *new_root_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
    new_root_page->SetParentPageId(INVALID_PAGE_ID);
    UpdateRootPageId(0);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    return true;
  }
  //case 2:
  if(old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, -1, true));
  page_id_t leaf_page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return IndexIterator(leaf_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(key));
  page_id_t leaf_page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return IndexIterator(leaf_page_id, buffer_pool_manager_, leaf_page->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  BPlusTreeLeafPage *leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(FindLeafPage(nullptr, -1, true));
  page_id_t leaf_page_id = leaf_page->GetPageId();
  while(leaf_page->GetNextPageId() != INVALID_PAGE_ID) {
    leaf_page = reinterpret_cast<BPlusTreeLeafPage *>(buffer_pool_manager_->FetchPage(leaf_page->GetNextPageId()));
    buffer_pool_manager_->UnpinPage(leaf_page_id, false);
    leaf_page_id = leaf_page->GetPageId();
  }
  buffer_pool_manager_->UnpinPage(leaf_page_id, false);
  return IndexIterator(leaf_page_id, buffer_pool_manager_, leaf_page->GetSize() - 1);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/**
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 * @param page_id       不知道干什么用的
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  if(IsEmpty())
    return nullptr;
  BPlusTreePage* tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_));
  BPlusTreeInternalPage* tree_internal_page;
  page_id_t lookup_temp;
  if(leftMost) {
    while(!tree_page->IsLeafPage()) {
      tree_internal_page = reinterpret_cast<BPlusTreeInternalPage *>(tree_page);
      lookup_temp = tree_internal_page->ValueAt(0);
      tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(lookup_temp));
      buffer_pool_manager_->UnpinPage(tree_internal_page->GetPageId(), false);
    }
  } else {
    while(!tree_page->IsLeafPage()) {
      tree_internal_page = reinterpret_cast<BPlusTreeInternalPage *>(tree_page);
      lookup_temp = tree_internal_page->Lookup(key, processor_);
      //LOG(INFO) << lookup_temp << std::endl;
      tree_page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(lookup_temp));
      buffer_pool_manager_->UnpinPage(tree_internal_page->GetPageId(), false);
    }
  }
  return reinterpret_cast<Page *>(tree_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = reinterpret_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(0));
  IndexRootsPage *index_roots_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if(insert_record) {
    index_roots_page->Insert(index_id_, root_page_id_);
    header_page->InsertRecord("Record No." + std::to_string(index_id_), root_page_id_);
  } else {
    index_roots_page->Update(index_id_, root_page_id_);
    header_page->UpdateRecord("Record No." + std::to_string(index_id_), root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(0, true);
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << " max size: " << leaf->GetMaxSize()
              << " size: " << leaf->GetSize() << std::endl;
    /*for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }*/
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId()
              << " max size: " << internal->GetMaxSize()
              << " size: " << internal->GetSize() << std::endl;
    /*for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }*/
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}