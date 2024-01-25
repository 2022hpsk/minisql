#ifndef MINISQL_TABLE_ITERATOR_H
#define MINISQL_TABLE_ITERATOR_H

#include "buffer/buffer_pool_manager.h"
#include "common/rowid.h"
#include "record/row.h"
#include "transaction/transaction.h"

class TableHeap;

class TableIterator {
 public:

  // you may define your own constructor based on your member variables
  explicit TableIterator();

  TableIterator( TableHeap* table_heap, Row* row, Transaction* txn);

  TableIterator(const TableIterator &other);

  virtual ~TableIterator();

  bool operator==(const TableIterator &itr) const{
    return this->table_heap_ == itr.table_heap_ && this->row_->GetRowId() == itr.row_->GetRowId() ;
  };


  bool operator!=(const TableIterator &itr) const {
    return !(*this == itr);
  };

  const Row &operator*();

  Row* operator->();

  TableIterator &operator=(const TableIterator &itr) noexcept;

  TableIterator &operator++();

  const TableIterator operator++(int);

 private:
  // add your own private member variables here
  TableHeap* table_heap_;
  Row* row_;
};

#endif  // MINISQL_TABLE_ITERATOR_H
