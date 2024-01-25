#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()
/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  // the size should be 0 at first
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetPageType(IndexPageType::INTERNAL_PAGE);
  this->SetKeySize(key_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index)
{
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key)
{
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const
{
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value)
{
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const
{
  for (int i = 0; i < GetSize(); ++i)
  {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

//pair<GenericKey *, page_id_t> *InternalPage::PairPtrAt(int index)
//{
//  pair<GenericKey *, page_id_t> key;
//  key.first = KeyAt(index);
//  key.second = ValueAt(index);
//  return &key;
//}
void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}
void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
   //memcpy(dest, src, pair_num *  INTERNAL_PAIR);
}



/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM)
{
  int max = this->GetSize() - 1, low = 1, center = 0; // 从第二个节点开始查找
  while (low <= max)
  {
    center = (low + max) / 2; // 二分法加速查找
    if (KM.CompareKeys(key, this->KeyAt(center)) < 0)
    { // 小于当前中间值
      if (center == 1 || KM.CompareKeys(key, this->KeyAt(center - 1)) >= 0)
      { // 大于前一个值
        return this->ValueAt(center - 1);
      }
      else
        max = center - 1;
    }
    else
    { // 大于当前键值
      if (center == max || KM.CompareKeys(key, this->KeyAt(center + 1)) < 0)
      { // 小于后一个键值
        return this->ValueAt(center);
      }
      else
        low = center + 1;
    }
  }
  return ValueAt(center);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value)
{
  this->SetValueAt(0, old_value);
  this->SetKeyAt(1, new_key);
  this->SetValueAt(1, new_value);
  this->SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value)
{
  int pos = ValueIndex(old_value);
  if (pos == -1)
  {
    ASSERT(false, "old value doesn't exist");
    return -1;
  }
  pos++;
  int i;
  for (i = this->GetSize() - 1; i >= pos; i--)
  {
    this->SetKeyAt(i + 1, this->KeyAt(i));
    this->SetValueAt(i + 1, this->ValueAt(i));
  }
  this->SetKeyAt(pos, new_key); // 插入新节点
  this->SetValueAt(pos, new_value);
  this->IncreaseSize(1);
  return this->GetSize();

}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager)
{
//  int size = this->GetSize();
//  recipient->CopyNFrom(pairs_off + INTERNAL_PAIR * (size - size / 2), int(size / 2), buffer_pool_manager);
//  IncreaseSize(-int(size / 2));
  //fix bug ,date 6.24. 2:32
  int index=GetMinSize();
  SetSize(index);
  recipient->CopyNFrom(PairPtrAt(index),GetMaxSize()-index,buffer_pool_manager);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 * 不做size检测
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager)
{
  // copy src into this
  this->PairCopy(pairs_off + this->GetSize() * INTERNAL_PAIR, src, size);
  for (int i = 0; i < size; i++)
  {
    // change the parent page id
    Page *child_page = buffer_pool_manager->FetchPage(ValueAt(this->GetSize() + i));
    BPlusTreePage *BNode = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    BNode->SetParentPageId(this->GetPageId());
    buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);
  }
  this->IncreaseSize(size);
}


/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index)
{
  for (int i = index + 1; i < this->GetSize(); i++)
  {
    this->SetKeyAt(i - 1, this->KeyAt(i));
    this->SetValueAt(i - 1, this->ValueAt(i));
  }
  this->IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild()
{
  page_id_t val = ValueAt(0);
  this->SetSize(0);
  return val;
}


/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager)
{
  // middle key stored in 0 pos? maybe not
  recipient->CopyLastFrom(middle_key, *reinterpret_cast<page_id_t *>(pairs_off + val_off), buffer_pool_manager);
  recipient->CopyNFrom(pairs_off + pair_size, GetSize() - 1, buffer_pool_manager);
  this->SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
// 有可能有问题，不知道理解的对不对，first指的估计是第0个
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
                                    BufferPoolManager *buffer_pool_manager)
{
  recipient->CopyLastFrom(middle_key, *reinterpret_cast<page_id_t *>(pairs_off + val_off), buffer_pool_manager);
  // recipient->CopyLastFrom(*reinterpret_cast<GenericKey **>(pairs_off + val_off + pair_size),
  // *reinterpret_cast<page_id_t *>(pairs_off + val_off + pair_size), buffer_pool_manager);
  this->Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager)
{
  Page *page = buffer_pool_manager->FetchPage(value);
  // Find the page that holds the new entry
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  bptp->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  int pos = this->GetSize();
  this->SetKeyAt(pos, key);
  this->SetValueAt(pos, value);
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key
 at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager)
{
  int last = GetSize() - 1;
  recipient->SetKeyAt(0, middle_key);
  // first is zero which is not used
  recipient->CopyFirstFrom(this->ValueAt(last - 1), buffer_pool_manager);
  this->Remove(last);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager)
{
  Page *page = buffer_pool_manager->FetchPage(value);
  BPlusTreePage *bptp = reinterpret_cast<BPlusTreePage *>(page->GetData());
  bptp->SetParentPageId(this->GetPageId());
  buffer_pool_manager->UnpinPage(value, true);
  for (int i = this->GetSize() - 1; i >= 0; i--)
  {
    this->SetKeyAt(i + 1, this->KeyAt(i));
    this->SetValueAt(i + 1, this->ValueAt(i));
  }
  this->SetValueAt(0, value);
  this->IncreaseSize(1);
}
