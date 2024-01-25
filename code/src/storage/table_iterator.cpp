#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator() {}

TableIterator::TableIterator( TableHeap* table_heap, Row* row, Transaction* txn){
  row_ = row;
  table_heap_ = table_heap;
}

TableIterator::TableIterator(const TableIterator &other) {
  this->table_heap_ = other.table_heap_;
  this->row_ = other.row_;
}

TableIterator::~TableIterator() {}


const Row &TableIterator::operator*() {
  return *this->row_;
};

Row* TableIterator::operator->() {
  return row_;
};

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  this->row_ = itr.row_;
  this->table_heap_ = itr.table_heap_;
  return *this;
};

// ++iter
TableIterator &TableIterator::operator++() {
  auto bpm = table_heap_->buffer_pool_manager_;
  auto cur_page = reinterpret_cast<TablePage *>(bpm->FetchPage(row_->GetRowId().GetPageId()));
  RowId next_row_id;
  if( !cur_page->GetNextTupleRid(row_->GetRowId(), &next_row_id) )
    while( cur_page->GetNextPageId() != INVALID_PAGE_ID){
      auto next_page = reinterpret_cast<TablePage *>(bpm->FetchPage(cur_page->GetNextPageId()));
      bpm->UnpinPage(row_->GetRowId().GetPageId(), false);
      cur_page = next_page;
      if( cur_page->GetFirstTupleRid(&next_row_id)) break;
    }
  this->row_->SetRowId(next_row_id);
  bpm->UnpinPage(cur_page->GetTablePageId(), true);
  if (*this != table_heap_->End()) {
    row_->GetFields().clear();
    table_heap_->GetTuple(row_,nullptr);
  }
  return *this;
};

// iter++
const TableIterator TableIterator::operator++(int) {
  TableIterator old(*this);
  ++(*(this));
  return old;
};
