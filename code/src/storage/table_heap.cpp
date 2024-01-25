#include "storage/table_heap.h"
#include <iostream>

/**
 * TODO: Student Implement
 * buffer_pool_manager掌管从内存与磁盘中的读取数据
 * 所有的被用过的数据页通过链表连在一起进行管理
 */
bool TableHeap::InsertTuple(Row &row, Transaction *txn)
{
  // first to examine whther the row is suitable to store in one page
  uint32_t row_size = row.GetSerializedSize(this->schema_);
  if (row_size > TablePage::SIZE_MAX_ROW)
  {
    return false;
  }
  // then to examine whether the pages in the dual-side list can hold the row
  ASSERT(this->first_page_id_ >= 0, "first_page_id<0");
  TablePage *page_to_check = reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(this->first_page_id_));
  this->buffer_pool_manager_->UnpinPage(page_to_check->GetPageId(), false);
  while (1)
  {
    if (page_to_check->InsertTuple(row, schema_, txn, lock_manager_, log_manager_))
    {
      int temp = page_to_check->GetPageId();
      // std::cout << "空页" << temp << " " << row.GetRowId().GetSlotNum() << std::endl;
      this->buffer_pool_manager_->UnpinPage(page_to_check->GetPageId(), true); // The page is dirty
      return true;
    }
    page_id_t NextPageId = page_to_check->GetNextPageId();
    if (NextPageId == INVALID_PAGE_ID) // all the page in the dual-side list have been checked
    {
      break;
    }
    ASSERT(NextPageId > 0, "NextPageId invalid");
    page_to_check = reinterpret_cast<TablePage *>(this->buffer_pool_manager_->FetchPage(NextPageId));
    this->buffer_pool_manager_->UnpinPage(page_to_check->GetPageId(), false);
  }
  // Because all the pages are not available, we have to new a page
  int new_page_id = INVALID_PAGE_ID;
  // the last page in the list haven't been pined
  this->buffer_pool_manager_->UnpinPage(page_to_check->GetPageId(), true);
  TablePage *New_Page = reinterpret_cast<TablePage *>(this->buffer_pool_manager_->NewPage(new_page_id));
  // this->buffer_pool_manager_->UnpinPage(New_Page->GetPageId(), false);
  if (!New_Page)
    return false;
  int temp = New_Page->GetPageId();
  // std::cout << "New" << temp << std::endl;
  // connect to the dual-side list
  New_Page->Init(new_page_id, page_to_check->GetPageId(), this->log_manager_, txn);
  New_Page->InsertTuple(row, this->schema_, txn, this->lock_manager_, this->log_manager_);
  buffer_pool_manager_->UnpinPage(New_Page->GetPageId(), true); // this is dirty
  page_to_check->SetNextPageId(new_page_id);
  buffer_pool_manager_->UnpinPage(page_to_check->GetPageId(), true); // this is dirty
  return true;
}

bool TableHeap::MarkDelete(const RowId &rid, Transaction *txn)
{
  // Find the page which contains the tuple.
  ASSERT(rid.GetPageId() >= 0, "Mark delete invalid");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr)
  {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  // std::cout << "succeed in marking delete" << rid.GetPageId() << " " << rid.GetSlotNum() << std::endl;
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Transaction *txn)
{
  // Find the page which contains the tuple.
  ASSERT(rid.GetPageId() >= 0, "update rid invalid");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the transaction.
  if (page == nullptr)
  {
    return false;
  }
  this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  // Otherwise, mark the tuple as deleted.
  Row old_row(rid);
  int result = page->UpdateTuple(row, &old_row, this->schema_, txn, lock_manager_, log_manager_);
  if (result == 2)
  {
    // If there is not enough space to update, we need to update via delete followed by an insert (not enough space).
    // std::cout << "no enough space" << std::endl;
    bool result = this->InsertTuple(row, txn);
    if (result)
    {
      this->MarkDelete(rid, txn);
      // this->ApplyDelete(rid, txn);
    }
    ASSERT(result, "Insert in update failed");
    this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), result);
    page->WUnlatch();
    return result;
  }
  else if (result == 0 || result == 1)
  {
    // if the slotnumber is invalid or the tuple is deleted
    this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    page->WUnlatch();
    return false;
  }

  row.SetRowId(rid);
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Transaction *txn)
{
  // Step1: Find the page which contains the tuple.
  ASSERT(rid.GetPageId() >= 0, "Applydelete invalid");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // std::cout << "succeed in applying delete" << rid.GetPageId() << " " << rid.GetSlotNum() << std::endl;
  // Step2: Delete the tuple from the page.
  page->ApplyDelete(rid, txn, this->log_manager_);
  this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Transaction *txn)
{
  // Find the page which contains the tuple.
  ASSERT(rid.GetPageId() >= 0, "roll back invalid");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Transaction *txn)
{
  ASSERT((row->GetRowId()).GetPageId() >= 0, "gettuple invalid");
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(row->GetRowId().GetPageId()));
  if (page == nullptr)
  {
    return false;
  }
  bool result = page->GetTuple(row, this->schema_, txn, this->lock_manager_);
  this->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  return result;
}

void TableHeap::DeleteTable(page_id_t page_id)
{
  if (page_id != INVALID_PAGE_ID)
  {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id)); // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  }
  else
  {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction *txn) {
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;
  if( !page->GetFirstTupleRid(&rid) ) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return End();
  }

  Row* tmp = new Row(rid);
  page->GetTuple(tmp,schema_, nullptr,lock_manager_);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  return {this,tmp,txn};
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  RowId rid = RowId(INVALID_PAGE_ID, 0);
  return TableIterator(this,new Row(rid), nullptr);
}


