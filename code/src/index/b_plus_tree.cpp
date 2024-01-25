#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size)
{
  Page *page = buffer_pool_manager_->FetchPage(TREE_INDEX_META);

  IndexRootsPage *indexRootsPage = reinterpret_cast<IndexRootsPage *>(page->GetData());
  indexRootsPage->GetRootId(index_id, &root_page_id_); // 获取根节点的 id
  if (leaf_max_size == 0)
  { // undefined
    leaf_max_size_ = (PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(RowId)) - 1;
    internal_max_size_ = leaf_max_size_ + 1;
  }
  buffer_pool_manager_->UnpinPage(TREE_INDEX_META, false);
  buffer_pool_manager_->UnpinPage(root_page_id_, false);
}

void BPlusTree::Destroy(page_id_t current_page_id)
{
  if (current_page_id == INVALID_PAGE_ID)
  { // delete all
    BPlusTreePage *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    if (page->IsLeafPage())
    {
      DestroyAllLeaf(page);
    }
    else
    {
      DestroyInternalPage(page);
    }
    return;
  }
  BPlusTreePage *cur_page =
      reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if (cur_page->IsLeafPage())
  {
    if (cur_page->GetPageId() != INVALID_PAGE_ID)
    {
      InternalPage *parent =
          reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(cur_page->GetParentPageId())->GetData());
      int index = parent->ValueIndex(current_page_id);
      parent->Remove(index);
      if (index)
      {
        auto *leftSibling =
            reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(parent->ValueAt(index - 1))->GetData());
        leftSibling->SetPageType(IndexPageType::LEAF_PAGE);
        // because remove will make the index move left by 1
        leftSibling->SetNextPageId(parent->ValueAt(index));
      }
      else if (parent->GetSize() == 0)
        Destroy(parent->GetPageId());

      this->buffer_pool_manager_->DeletePage(current_page_id);
    }
  }
  else
  {
    DestroyInternalPage(cur_page);
  }
}

void BPlusTree::DestroyAllLeaf(BPlusTreePage *page)
{
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(page);
  while (leafPage->GetNextPageId() != INVALID_PAGE_ID)
  { // 将整块的叶子顺序删除
    LeafPage *nextPage =
        reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(leafPage->GetNextPageId())->GetData());
    nextPage->SetPageType(IndexPageType::LEAF_PAGE);
    Destroy(leafPage->GetPageId());
    leafPage = nextPage;
  }
  Destroy(leafPage->GetPageId());
}

void BPlusTree::DestroyInternalPage(BPlusTreePage *page)
{
  InternalPage *internalPage = reinterpret_cast<InternalPage *>(page);
  int size = internalPage->GetSize();
  for (int i = 0; i < size; i++)
  {
    auto *nextPage =
        reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internalPage->ValueAt(i))->GetData());
    // This is important!!! ortherwise the parent will be visited again in Destory
    nextPage->SetParentPageId(INVALID_PAGE_ID);
    Destroy(internalPage->ValueAt(i));
  }
  Destroy(internalPage->GetPageId());
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const { return this->root_page_id_ == INVALID_PAGE_ID; }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Transaction *transaction)
{
  if (this->IsEmpty())
  {
    return false;
  }
  Page *page = this->FindLeafPage(key, root_page_id_, false);
  LeafPage *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  RowId row;
  // Row row_;
  ASSERT(leaf_page != nullptr, "leaf_page is null!");
  // (this->processor_).DeserializeToKey(key, row_, processor_.key_schema_);
  // std::cout << "Getvalue" << (row_.GetField(0))->toString() << (row_.GetField(1))->toString() << std::endl;
  bool res = leaf_page->Lookup(key, row, this->processor_);
  // std::cout << "find" << res << std::endl;
  // std::cout << row.GetPageId() << " " << row.GetSlotNum() << std::endl;
  this->buffer_pool_manager_->UnpinPage(page->GetPageId(), false); // 没有对该页进行修改，不是脏页
  if(res)
    result.push_back(row);
  return res;
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
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Transaction *transaction)
{
  if (this->IsEmpty())
  {
    this->StartNewTree(key, value);
    return true;
  }
  // printf("hello\n");

  LeafPage *page = reinterpret_cast<LeafPage *>(this->FindLeafPage(key, this->root_page_id_, false));
  RowId new_value;
  if (page->Lookup(key, new_value, this->processor_))
  {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }
  page->Insert(key, value, this->processor_);
  if (page->GetSize() > page->GetMaxSize())
  {
    LeafPage *newleaf = this->Split(page, transaction);
    // for we have inserted the key into the page, and we are not sure where the key is
    this->InsertIntoParent(page, newleaf->KeyAt(0), newleaf, transaction);
  }
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}

/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value)
{
  // insert
  Page *page = this->buffer_pool_manager_->NewPage(root_page_id_);
  this->UpdateRootPageId(1);
  if (page == NULL)
    throw runtime_error("out of memory");

  LeafPage *root_node = reinterpret_cast<LeafPage *>(page->GetData());
  root_node->Init(page->GetPageId(), INVALID_PAGE_ID, processor_.GetKeySize(), this->leaf_max_size_);
  root_node->Insert(key, value, this->processor_);
  root_node->SetPageType(IndexPageType::LEAF_PAGE);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
  this->index_id_++;

}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */

bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Transaction *transaction)
{
  return this->Insert(key, value, transaction);
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */

BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Transaction *transaction)
{
  page_id_t page_Id;
  InternalPage *newLeaf = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->NewPage(page_Id));
  if (newLeaf == NULL)
  {
    throw runtime_error("out of memory");
  }
  newLeaf->Init(page_Id, node->GetParentPageId(), node->GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(newLeaf, this->buffer_pool_manager_);
  return newLeaf;
}

// 将一个叶子节点分成两半（没有排序）
BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Transaction *transaction)
{
  page_id_t page_Id;
  LeafPage *newLeaf = reinterpret_cast<LeafPage *>(this->buffer_pool_manager_->NewPage(page_Id));
  if (newLeaf == NULL)
  { // 建立新页失败
    throw runtime_error("out of memory");
  }
  newLeaf->Init(page_Id, node->GetParentPageId(), processor_.GetKeySize(), node->GetMaxSize());
  node->MoveHalfTo(newLeaf); // 各一半
  newLeaf->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(newLeaf->GetPageId());
  return newLeaf;
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
 //内部节点插入一个新的节点
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node,
                                 Transaction *transaction)
{
  if (old_node->GetParentPageId() == INVALID_PAGE_ID)
  {
    page_id_t root_Id;
    InternalPage *newroot = reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->NewPage(root_Id));
    if (newroot == NULL)
    {
      throw runtime_error("out of mem");
    }
    newroot->Init(root_Id, INVALID_PAGE_ID, old_node->GetKeySize(), internal_max_size_);
    root_page_id_ = newroot->GetPageId();
    this->UpdateRootPageId(0);
    newroot->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_Id);
    new_node->SetParentPageId(root_Id);

    buffer_pool_manager_->UnpinPage(root_Id, true);
    buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
    return;
  }
  // this is not root
  InternalPage *internalPage =
      reinterpret_cast<InternalPage *>(this->buffer_pool_manager_->FetchPage(old_node->GetParentPageId()));
  int index = internalPage->ValueIndex(old_node->GetPageId());
  internalPage->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  new_node->SetParentPageId(internalPage->GetPageId());
  if (internalPage->GetSize() > internalPage->GetMaxSize())
  {
    auto *newNode = this->Split(internalPage, transaction);
    GenericKey *temp;
    for (int i = 0; i < internalPage->GetSize(); i++) {
      BPlusTreePage *page1 =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(internalPage->ValueAt(i)));
      page1->SetParentPageId(internalPage->GetPageId());
      buffer_pool_manager_->UnpinPage(page1->GetPageId(), false);
    }
    for (int i = 0; i < newNode->GetSize(); i++) {
      BPlusTreePage *page1 =
          reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(newNode->ValueAt(i)));
      page1->SetParentPageId(newNode->GetPageId());
      buffer_pool_manager_->UnpinPage(page1->GetPageId(), false);
    }
    GenericKey *new_key = newNode->KeyAt(0);
    InsertIntoParent(internalPage, new_key, newNode, transaction);
  }
  buffer_pool_manager_->UnpinPage(new_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(old_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(internalPage->GetPageId(), true);
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
// 从b+树中删除 key，并从右边进行维护
void BPlusTree::Remove(const GenericKey *key, Transaction *transaction)
{
  vector<RowId> result;
  if (this->GetValue(key, result, transaction) == false)
    return;
  LeafPage *leafPage = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false));
  int index = leafPage->RemoveAndDeleteRecord(key, this->processor_);
  if (index != leafPage->GetSize())
  {
    CoalesceOrRedistribute(leafPage, transaction);
  }
  buffer_pool_manager_->UnpinPage(leafPage->GetPageId(), true);
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
// 删除之后的合并或重分配的维护
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Transaction *transaction)
{
  if (node->IsRootPage())
  {
    return this->AdjustRoot(node);
  }
  if ((node->IsLeafPage() && node->GetSize() >= node->GetMinSize()) ||
      (!node->IsLeafPage() && node->GetSize() > node->GetMinSize()))
    return false; // 上述情况删除后没有影响，无需合并
  page_id_t parent_page = node->GetParentPageId();
  InternalPage *parentNode = reinterpret_cast<InternalPage *>(
      this->buffer_pool_manager_->FetchPage(parent_page)->GetData()); // 获取父亲节点 if (parentNode == nullptr)
  {
    throw runtime_error("all page are pinned during CoalesceOrRedistribute");
  }
  // 获取当前节点在父节点中的下标位置
  int index = parentNode->ValueIndex(node->GetPageId());
  int sibling_Id;
  if (index == 0)
  {
    sibling_Id = parentNode->ValueAt(index + 1); // 此时只能找右边的兄弟
  }
  else
  {
    sibling_Id = parentNode->ValueAt(index - 1); // 左兄弟
  }
  auto *sibling_Page = reinterpret_cast<N *>(this->buffer_pool_manager_->FetchPage(sibling_Id)->GetData());
  if (sibling_Page == nullptr)
  {
    throw runtime_error("all page are pinned while CoalesceOrRedistribute");
  }
  if (sibling_Page->GetSize() + node->GetSize() > node->GetMaxSize())
  {
    this->Redistribute(sibling_Page, node, index);
    this->buffer_pool_manager_->UnpinPage(parent_page, true);
    return false;
  } // 进行合并
  if (index == 0)
  {
    this->Coalesce(node, sibling_Page, parentNode, index, transaction);
    this->buffer_pool_manager_->UnpinPage(parent_page, true);
    return false; // 此时node没有被删除
  }
  else
    this->Coalesce(sibling_Page, node, parentNode, index, transaction);
  this->buffer_pool_manager_->UnpinPage(parent_page, true);
  return true;
}

/*
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
// 两个节点融合，删除右边那一页，不用管index，传入已经不同了
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction)
{
  node->MoveAllTo(neighbor_node);
  neighbor_node->SetNextPageId(node->GetNextPageId());      // 更新链表
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true); // 删除前先解锁
  buffer_pool_manager_->DeletePage(node->GetPageId());      // 删除右边页
  int reIndex = index == 0 ? 1 : index;                     // 需要删除点的下标
  parent->Remove(reIndex);                                  // 从父亲节点中删除右边的节点
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  return this->CoalesceOrRedistribute(parent, transaction); // 递归维护 parent 节点
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Transaction *transaction)
{
  node->MoveAllTo(node, parent->KeyAt(index), buffer_pool_manager_);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true); // 删除前先解锁
  buffer_pool_manager_->DeletePage(node->GetPageId());      // 删除右边页
  int reIndex = index == 0 ? 1 : index;                     // 需要删除点的下标
  parent->Remove(reIndex);                                  // 从父亲节点中删除右边的节点
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  return this->CoalesceOrRedistribute(parent, transaction); // 递归维护 parent 节点
  return false;
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
// 当删除后需要从兄弟借节点时
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index)
{
  // 获取父亲节点，在更新子节点后也更新父节点
  InternalPage *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (parent == NULL)
  {
    throw runtime_error("all page are pinned during Redistribute");
  }
  if (index == 0)
  {
    neighbor_node->MoveFirstToEndOf(node);             // 把第一个节点移动到node的末尾
    parent->SetValueAt(1, neighbor_node->GetPageId()); // 更新第二个指针
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));      // 更新第一个键值
  }
  else
  {
    neighbor_node->MoveLastToFrontOf(node);       // 把最后一个节点移动到node的头
    parent->SetValueAt(index, node->GetPageId()); // 更新父亲节点中node的头
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true); // 释放父亲节点
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true); // 释放并更新内存
}
void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index)
{
  // 获取父亲节点，在更新子节点后也更新父节点
  InternalPage *parent =
      reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  if (parent == NULL)
  {
    throw runtime_error("all page are pinned during Redistribute");
  }
  if (index == 0)
  {
    neighbor_node->MoveFirstToEndOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetValueAt(1, neighbor_node->GetPageId()); // 更新第二个指针
    parent->SetKeyAt(1, neighbor_node->KeyAt(0));      // 更新第一个键值
  }
  else
  {
    neighbor_node->MoveLastToFrontOf(node, parent->KeyAt(index), buffer_pool_manager_);
    parent->SetValueAt(index, node->GetPageId()); // 更新父亲节点中node的头
    parent->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(parent->GetPageId(), true); // 释放父亲节点
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true); // 释放并更新内存
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
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node)
{
  if (old_root_node->IsLeafPage())
  {
    if (old_root_node->GetSize() == 0)
    { // 删除这个节点
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(0);
      return true;
    }
    else
      return false; // 不用调整
  }
  if (old_root_node->GetSize() == 1)
  {                                                               // 需要进行融合
    auto *page = reinterpret_cast<InternalPage *>(old_root_node); // 类型转换
    root_page_id_ = page->ValueAt(0);
    this->UpdateRootPageId(0); // 只有一个孩子，则这个孩子直接成为新的根
    auto *newRoot = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(root_page_id_)->GetData());
    if (page == nullptr)
    {
      throw runtime_error("all pages are pinned while AdjustRoot");
    }
    newRoot->SetParentPageId(INVALID_PAGE_ID);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
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
  GenericKey *key = processor_.InitKey();
  auto left_page = FindLeafPage(key, 0, true);
  auto *leaf = reinterpret_cast<LeafPage *>(left_page->GetData());
  page_id_t current_page_id = leaf->GetPageId();
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  return IndexIterator(current_page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low-key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto left_page = FindLeafPage(key);
  auto *leaf = reinterpret_cast<LeafPage *>(left_page->GetData());
  page_id_t current_page_id = leaf->GetPageId();
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  return IndexIterator(current_page_id, buffer_pool_manager_, leaf->KeyIndex(key, processor_));
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() { return IndexIterator(INVALID_PAGE_ID, buffer_pool_manager_, 0); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost)
{
  Page *curr_page = this->buffer_pool_manager_->FetchPage(this->root_page_id_);
  // Unpin after each update
  this->buffer_pool_manager_->UnpinPage(root_page_id_, false);
  BPlusTreePage *curr_node = reinterpret_cast<BPlusTreePage *>(curr_page->GetData());
  // Row row;
  // processor_.DeserializeToKey(key, row, processor_.key_schema_);
  // printf("%s", (row.GetField(0))->toString());
  // std::cout << "root size in find:" << curr_node->GetSize() << std::endl;
  while (!curr_node->IsLeafPage())
  {
    InternalPage *internal_node = reinterpret_cast<InternalPage *>(curr_node);
    page_id_t child_page_id;
    if (leftMost == true)
    {
      // if it is left most, no need to find key, just return the left most page
      child_page_id = internal_node->ValueAt(0);
    }
    else
    {
      // else, we have to find the key
      // printf("hhh\n");
      child_page_id = internal_node->Lookup(key, this->processor_);
      // printf("done %d\n", child_page_id);
    }
    Page *child_page = this->buffer_pool_manager_->FetchPage(child_page_id);
    ASSERT(child_page != nullptr, "child page is null!");
    BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    this->buffer_pool_manager_->UnpinPage(child_page_id, false);
    // 交换
    curr_page = child_page;
    curr_node = child_node;
  }
  // std::cout << "find leaf" << std::endl;
  return curr_page;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record)
{
  auto *IndexPage = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID));
  if (insert_record == 0) {
    IndexPage->Update(this->index_id_, this->root_page_id_);
  } else {
    IndexPage->Insert(this->index_id_, this->root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const
{
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage())
  {
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
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID)
    {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID)
    {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  }
  else
  {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink "; // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++)
    {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0)
      {
        out << inner->KeyAt(i);
      }
      else
      {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID)
    {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++)
    {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0)
      {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage())
        {
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
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const
{
  if (page->IsLeafPage())
  {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++)
    {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  }
  else
  {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++)
    {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++)
    {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check()
{
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned)
  {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}

