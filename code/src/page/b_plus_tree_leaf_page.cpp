#include "page/b_plus_tree_leaf_page.h"

#include <algorithm>

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(RowId))
#define key_off 0
#define val_off GetKeySize()
/*****************************************************************************
*HELPER METHODS AND UTILITIES
        ***************************************************************************** /

/**
 * TODO: Student Implement
 */
/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 * 未初始化next_page_id
 */
void LeafPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size)
{
  this->SetPageType(IndexPageType::LEAF_PAGE);
  this->SetSize(0);
  this->SetPageId(page_id);
  this->SetParentPageId(parent_id);
  this->SetMaxSize(max_size);
  this->SetNextPageId(INVALID_PAGE_ID);
  this->SetKeySize(key_size);
  this->next_page_id_ = INVALID_PAGE_ID;
  if (max_size == 0)
  {
    int size = LEAF_PAGE_SIZE;
    this->SetMaxSize(size);
  }
}

/**
 * Helper methods to set/get next page id
 */
page_id_t LeafPage::GetNextPageId() const { return next_page_id_; }

void LeafPage::SetNextPageId(page_id_t next_page_id)
{
  next_page_id_ = next_page_id;
  if (next_page_id == 0)
  {
    LOG(INFO) << "Fatal error";
  }
}

/**
 * TODO: Student Implement
 */
/**
 * Helper method to find the first index i so that pairs_[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
int LeafPage::KeyIndex(const GenericKey *key, const KeyManager &KM)
{
  int right = this->GetSize() - 1, center, left = 0;
  while (left <= right)
  {
    center = (left + right) / 2;
    if (KM.CompareKeys(key, this->KeyAt(center)) > 0)
    {
      if (center == this->GetSize() - 1 || KM.CompareKeys(key, this->KeyAt(center + 1)) <= 0)
      {
        return center + 1;
      }
      left = center + 1;
    }
    else
    {
      if (center == 0 || (KM.CompareKeys(key, this->KeyAt(center - 1)) > 0))
      {
        return center;
      }
      right = center - 1;
    }
  }
  return this->GetSize();
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *LeafPage::KeyAt(int index)
{
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

// // void BPlusTreeLeafPage::SetKeyAt(int index, GenericKey *key, const KeyManager &KM) {
// //   // ???这个函数无效了？？应该要做一段深拷贝
// //   // memcpy(pairs_off + index * pair_size + key_off, &key, sizeof(GenericKey *));
// //   // for (int i = 0; i < GetKeySize(); i++)
// //   // {
// //   //   *(reinterpret_cast<char *>(pairs_off + index * pair_size + key_off) + i) = *(reinterpret_cast<char *>(key)
// +
// //   i);
// //   //   // printf("%c %c\n", *(reinterpret_cast<char *>(pairs_off + index * pair_size + key_off) + i),
// //   //   *(reinterpret_cast<char *>(key) + i));
// //   // }
// //   printf("%d\n", GetKeySize());
// //   memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
// //   GenericKey *index_key = reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
// //   // Row row;
// //   // KM.DeserializeToKey(index_key, row, KM.key_schema_);
// //   // std::cout << (int)key << std::endl;
// //   // printf("%p\n", key);
// //   // std::cout << "key size" << *reinterpret_cast<double *>(index_key) << std::endl;
// //   // std::cout << "hhh" << (row.GetField(0))->toString() << (row.GetField(1))->toString() << std::endl;
// // }

void LeafPage::SetKeyAt(int index, GenericKey *key)
{
  // for (int i = 0; i < GetKeySize(); i++)
  // {
  //   *(reinterpret_cast<char *>(pairs_off + index * pair_size + key_off) + i) = *(reinterpret_cast<char *>(key) +i);
  // }
  // printf("size   %d\n", GetKeySize());
  // (GenericKey *)malloc(key_size_);
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

RowId LeafPage::ValueAt(int index) const
{
  return *reinterpret_cast<const RowId *>(pairs_off + index * pair_size + val_off);
}

void LeafPage::SetValueAt(int index, RowId value)
{
  *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off) = value;
}

void *LeafPage::PairPtrAt(int index) { return KeyAt(index); }

void LeafPage::PairCopy(void *dest, void *src, int pair_num)
{
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(RowId)));
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a. array offset)
 */
std::pair<GenericKey *, RowId> LeafPage::GetItem(int index)
{
  // replace with your own code
  GenericKey *key = *reinterpret_cast<GenericKey **>(pairs_off + index * pair_size + key_off);
  RowId rid = *reinterpret_cast<RowId *>(pairs_off + index * pair_size + val_off);
  return make_pair(key, rid);
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return page size after insertion
 */
int LeafPage::Insert(GenericKey *key, const RowId &value, const KeyManager &KM)
{
  int pos = KeyIndex(key, KM);
  // the key is already in the page
  // std::cout << "pos" << pos << "size" << GetSize() << std::endl;
  if (pos != GetSize() && KM.CompareKeys(key, this->KeyAt(pos)) == 0)
    return GetSize();
  for (int i = GetSize(); i > pos; i--)
  {
    // printf("in loop\n");
    this->SetKeyAt(i, this->KeyAt(i - 1));
    this->SetValueAt(i, this->ValueAt(i - 1));
  }
  this->SetKeyAt(pos, key);
  this->SetValueAt(pos, value);
  // Row row;
  // std::cout << "search in insertion:" << KM.CompareKeys(key, KeyAt(pos)) << std::endl;
  // KM.DeserializeToKey(KeyAt(pos), row, KM.key_schema_);
  // std::cout << "hhhh" << (row.GetField(0))->toString() << (row.GetField(1))->toString() << std::endl;
  this->IncreaseSize(1);
  return this->GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
void LeafPage::MoveHalfTo(LeafPage *recipient)
{
  int size = this->GetSize();
  recipient->CopyNFrom(pairs_off + (size - size / 2) * pair_size, size / 2);
  this->IncreaseSize(-size / 2);
}

/*
 * Copy starting from items, and copy {size} number of elements into me.
 */
void LeafPage::CopyNFrom(void *src, int size)
{
  this->PairCopy(this->data_ + pair_size * this->GetSize(), src, size);
  this->IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
bool LeafPage::Lookup(const GenericKey *key, RowId &value, const KeyManager &KM)
{
  // std::cout << "LookUp-leaf" << std::endl;
  int pos = this->KeyIndex(key, KM);
  // std::cout << pos << endl;
  if (pos == GetSize())
  {
    return false;
  }
  if (KM.CompareKeys(key, this->KeyAt(pos)) == 0)
  {
    // std::cout << "not exist " << pos << std::endl;
    value = this->ValueAt(pos);
    return true;
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
int LeafPage::RemoveAndDeleteRecord(const GenericKey *key, const KeyManager &KM)
{
  RowId rid;
  if (!this->Lookup(key, rid, KM))
  {
    return -1;
  }
  int pos = this->KeyIndex(key, KM);
  for (int i = pos; i < this->GetSize(); i++)
  {
    this->SetKeyAt(i, this->KeyAt(i + 1));
    this->SetValueAt(i, this->ValueAt(i + 1));
  }
  this->IncreaseSize(-1);
  return this->GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all key & value pairs from this page to "recipient" page. Don't forget
 * to update the next_page id in the sibling page
 */
void LeafPage::MoveAllTo(LeafPage *recipient)
{
  recipient->CopyNFrom(pairs_off, this->GetSize());
  recipient->SetNextPageId(this->GetNextPageId());
  this->IncreaseSize(-this->GetSize());
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page.
 *
 */
void LeafPage::MoveFirstToEndOf(LeafPage *recipient)
{
  recipient->CopyLastFrom(this->KeyAt(0), this->ValueAt(0));
  for (int i = 0; i < this->GetSize() - 1; i++)
  {
    this->SetKeyAt(i, this->KeyAt(i + 1));
    this->SetValueAt(i, this->ValueAt(i + 1));
  }
  this->IncreaseSize(-1);
}

/*
 * Copy the item into the end of my item list. (Append item to my array)
 */
void LeafPage::CopyLastFrom(GenericKey *key, const RowId value)
{
  int size = this->GetSize();
  this->SetKeyAt(size, key);
  this->SetValueAt(size, value);
  this->IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page.
 */
void LeafPage::MoveLastToFrontOf(LeafPage *recipient)
{
  recipient->CopyFirstFrom(this->KeyAt(this->GetSize() - 1), this->ValueAt(this->GetSize() - 1));
  this->IncreaseSize(-1);
}

/*
 * Insert item at the front of my items. Move items accordingly.
 *
 */
void LeafPage::CopyFirstFrom(GenericKey *key, const RowId value)
{
  for (int i = GetSize(); i > 0; i--)
  {
    this->SetKeyAt(i, this->KeyAt(i - 1));
    this->SetValueAt(i, this->ValueAt(i - 1));
  }
  this->SetKeyAt(0, key);
  this->SetValueAt(0, value);
  this->IncreaseSize(1);
}
