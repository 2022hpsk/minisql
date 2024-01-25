#include "storage/table_heap.h"
// 沿着TablePage构成的链表依次查找，直到找到第一个能够容纳该记录的TablePage（First Fit 策略）
bool TableHeap::InsertTuple(Row& row, Transaction* txn) {
    TablePage* curPage = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(first_page_id_));  // 根据 first_page_id 申请一页
    TablePage* nextPage;

    if (!curPage)  // 没有page了
        return false;

    // 如果插入失败就移到下一个page, 直到找到有足够空间的page为止
    while (curPage->InsertTuple(row, schema_, txn, lock_manager_, log_manager_) == false) {
        page_id_t nextID = curPage->GetNextPageId();
        if (nextID != INVALID_PAGE_ID) {                                                      // 如果还存在下一个page
            buffer_pool_manager_->UnpinPage(curPage->GetPageId(), false);                     // 取消固定curPage, 没有更改所以不需要写硬盘
            curPage = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(nextID));  // 在Fetch和New的时候会pin
        } else {
            page_id_t nextID;
            nextPage = reinterpret_cast<TablePage*>(buffer_pool_manager_->NewPage(nextID));
            if (nextPage == nullptr) {                                         // new失败了
                buffer_pool_manager_->UnpinPage(curPage->GetPageId(), false);  // 取消固定curPage, 没有更改所以不需要写硬盘
                return false;
            }
            // 给权限
            nextPage->WLatch();
            curPage->WLatch();

            curPage->SetNextPageId(nextID);                                   // curPage 指向 nextPage
            nextPage->Init(nextID, curPage->GetPageId(), log_manager_, txn);  // 初始化 nextPage

            // 收回权限
            curPage->WUnlatch();
            nextPage->WUnlatch();

            buffer_pool_manager_->UnpinPage(curPage->GetPageId(), true);  // 取消固定curPage, nextID changed
            curPage = nextPage;
        }
    }
    buffer_pool_manager_->UnpinPage(curPage->GetPageId(), true);  // 取消固定curPage, data_ changed
    return true;
}

bool TableHeap::MarkDelete(const RowId& rid, Transaction* txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
    // If the page could not be found, then abort the transaction.
    if (page == nullptr) {
        return false;
    }
    // Otherwise, mark the tuple as deleted.
    page->WLatch();
    page->MarkDelete(rid, txn, lock_manager_, log_manager_);
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
    return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(const Row& row, const RowId& rid, Transaction* txn) {
    return false;
}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId& rid, Transaction* txn) {
    // Step1: Find the page which contains the tuple.
    // Step2: Delete the tuple from the page.
}

void TableHeap::RollbackDelete(const RowId& rid, Transaction* txn) {
    // Find the page which contains the tuple.
    auto page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
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
bool TableHeap::GetTuple(Row* row, Transaction* txn) {
    return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
    if (page_id != INVALID_PAGE_ID) {
        auto temp_table_page = reinterpret_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
        if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
            DeleteTable(temp_table_page->GetNextPageId());
        buffer_pool_manager_->UnpinPage(page_id, false);
        buffer_pool_manager_->DeletePage(page_id);
    } else {
        DeleteTable(first_page_id_);
    }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Transaction* txn) {
    return TableIterator();
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
    return TableIterator();
}
