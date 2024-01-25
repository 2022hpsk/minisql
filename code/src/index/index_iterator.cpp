#include "index/index_iterator.h"

#include "index/basic_comparator.h"
#include "index/generic_key.h"

IndexIterator::IndexIterator() = default;

IndexIterator::IndexIterator(page_id_t page_id, BufferPoolManager *bpm, int index)
    : current_page_id(page_id), item_index(index), buffer_pool_manager(bpm)
{
  if (current_page_id != INVALID_PAGE_ID)
    page = reinterpret_cast<LeafPage *>(buffer_pool_manager->FetchPage(current_page_id)->GetData());
  else
    page = nullptr;
}

IndexIterator::~IndexIterator()
{
  if (current_page_id != INVALID_PAGE_ID)
    buffer_pool_manager->UnpinPage(current_page_id, false);
}

std::pair<GenericKey *, RowId> IndexIterator::operator*()
{
  Page *page = this->buffer_pool_manager->FetchPage(this->current_page_id);
  BPlusTreeLeafPage *node = reinterpret_cast<BPlusTreeLeafPage *>(page->GetData());
  GenericKey *key = node->KeyAt(this->item_index);
  RowId rid = node->ValueAt(this->item_index);
  return std::pair<GenericKey *, RowId>(key, rid);
}

IndexIterator &IndexIterator::operator++()
{
  Page *page = this->buffer_pool_manager->FetchPage(this->current_page_id);
  LeafPage *node = reinterpret_cast<LeafPage *>(page);
  int size = node->GetSize();
  // just Point to Next Pair in this Page
  if (this->item_index +1 < size)
  {
    this->buffer_pool_manager->UnpinPage(this->current_page_id, false);
    this->item_index++;
  }
  else
  {
    page_id_t NextPage = node->GetNextPageId();

    this->buffer_pool_manager->UnpinPage(this->current_page_id, false);
    // It means NextPage is the Last Page
    if (NextPage == INVALID_PAGE_ID)
    {
      this->current_page_id = INVALID_PAGE_ID;
      this->item_index = 0;
    }
    else
    {
      // Update Next Page
      this->current_page_id = NextPage;
      // Update Next Position
      this->item_index = 0;
    }
  }
  return *this;
}

bool IndexIterator::operator==(const IndexIterator &itr) const
{
  return current_page_id == itr.current_page_id && item_index == itr.item_index;
}

bool IndexIterator::operator!=(const IndexIterator &itr) const
{
  return !(*this == itr);
}
