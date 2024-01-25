#include "buffer/buffer_pool_manager.h"

#include "glog/logging.h"
#include "page/bitmap_page.h"

static const char EMPTY_PAGE_DATA[PAGE_SIZE] = {0};

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager)
{
  pages_ = new Page[pool_size_];
  //replacer_ = new ClockReplacer(pool_size_);
  replacer_ = new LRUReplacer(pool_size_);
  for (size_t i = 0; i < pool_size_; i++)
  {
    free_list_.emplace_back(i);
  }
}

BufferPoolManager::~BufferPoolManager()
{
  for (auto page : page_table_)
  {
    FlushPage(page.first);
  }
  delete[] pages_;
  delete replacer_;
}

/**
 * TODO: Student Implement
 */
// Here稍微理一下，diskmanager有个metadata，后面跟着的是bitmap+数据页，这层是物理磁盘的
// 然后往上，有Bufferpool。bufferpool里的pages是缓存的物理数据，通过page_table完成pageid和frameid（也就是bufferpages里的id）完成映射
// freelist里存的应该就是pages中还能用的frameid，只有满了才有必要更新缓存区
// 通过lru的管理，可以把最老的frameid绑成新的物理数据页。当然，lru本身只是告诉我们哪个frame是最老的，实际替换是buffermanager做的。
Page *BufferPoolManager::FetchPage(page_id_t page_id)
{
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  frame_id_t frame_id;
  if ((this->page_table_).count(page_id) != 0)
  {
    frame_id = (this->page_table_)[page_id];
    // Pin the frame so that the unpin will make the frame exist at the front
    replacer_->Pin(frame_id);
    (this->pages_)[frame_id].pin_count_++;
    return this->pages_ + frame_id;
  }
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if ((this->free_list_).size() != 0)
  {
    frame_id = (this->free_list_).front();
    (this->free_list_).pop_front();
    (this->page_table_)[page_id] = frame_id;
    this->disk_manager_->ReadPage(page_id, (this->pages_)[frame_id].data_);
    // update page info
    (this->pages_)[frame_id].pin_count_ = 1;
    (this->pages_)[frame_id].page_id_ = page_id;
    (this->replacer_)->Pin(frame_id);
    return this->pages_ + frame_id;
  }
  else
  {
    // find from replacer
    if (!(this->replacer_)->Victim(&frame_id))
      return nullptr;
    (this->replacer_)->Pin(frame_id);
    // 2.     If R is dirty, write it back to the disk.
    if ((this->pages_)[frame_id].IsDirty())
    {
      (this->disk_manager_)->WritePage((this->pages_)[frame_id].GetPageId(), (this->pages_)[frame_id].GetData());
    }
    // 3.     Delete R from the page table and insert P.
    (this->page_table_).erase((this->pages_)[frame_id].GetPageId());
    (this->page_table_)[page_id] = frame_id;
    (this->disk_manager_)->ReadPage(page_id, (this->pages_)[frame_id].data_);
    // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
    (this->pages_)[frame_id].pin_count_ = 1;
    (this->pages_)[frame_id].page_id_ = page_id;
    return this->pages_ + frame_id;
  }
}

/**
 * TODO: Student Implement
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id)
{
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  page_id = 0;
  frame_id_t frame_id;
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  if ((this->free_list_).size() > 0)
  {
    frame_id = (this->free_list_).front();
    (this->free_list_).pop_front();
    page_id = this->disk_manager_->AllocatePage();
    ASSERT(page_id >= 0, "page_id in NewPage invalid");
  }
  else
  {
    if (!(this->replacer_)->Victim(&frame_id))
    {
      return nullptr;
    }
    // (this->replacer_)->Pin(frame_id);
    (this->page_table_).erase((this->pages_)[frame_id].page_id_);
    if ((this->pages_)[frame_id].IsDirty())
    {
      (this->pages_)[frame_id].is_dirty_ = false;
      this->disk_manager_->WritePage((this->pages_)[frame_id].page_id_, (this->pages_)[frame_id].data_);
    }
    page_id = this->disk_manager_->AllocatePage();
    ASSERT(page_id > 0, "page_id in NewPage invalid");
  }
  (this->replacer_)->Pin(frame_id);
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  (this->pages_)[frame_id].pin_count_ = 1;
  (this->pages_)[frame_id].page_id_ = page_id;
  // 4.   Set the page ID output parameter. Return a pointer to P.
  (this->page_table_)[page_id] = frame_id;
  return this->pages_ + frame_id;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::DeletePage(page_id_t page_id)
{
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  size_t frame_id;
  for (frame_id = 0; frame_id < pool_size_; frame_id++)
  {
    if ((this->pages_)[frame_id].page_id_ == page_id)
    {
      break;
    }
  }
  if (frame_id < pool_size_)
  {
    if ((this->pages_)[frame_id].pin_count_ != 0)
    {
      return false;
    }
  }
  else
  {
    return true;
  }
  this->DeallocatePage(page_id);
  (this->pages_)[frame_id].page_id_ = INVALID_PAGE_ID;
  (this->page_table_).erase(page_id);
  (this->pages_)[frame_id].ResetMemory();
  (this->free_list_).push_back(frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty)
{
  if ((this->page_table_).count(page_id) == 0)
    return false;
  frame_id_t frame_id = (this->page_table_)[page_id];
  if ((this->pages_)[frame_id].pin_count_ != 0)
    (this->pages_)[frame_id].pin_count_--;
  (this->pages_)[frame_id].is_dirty_ = is_dirty;
  (this->replacer_)->Unpin(frame_id);
  return true;
}

/**
 * TODO: Student Implement
 */
bool BufferPoolManager::FlushPage(page_id_t page_id)
{
  this->latch_.lock();
  if ((this->page_table_).count(page_id) == 0)
    return false;
  frame_id_t frame_id = (this->page_table_)[page_id];
  (this->disk_manager_)->WritePage(page_id, (this->pages_)[frame_id].data_);
  this->latch_.unlock();
  return true;
}

page_id_t BufferPoolManager::AllocatePage()
{
  page_id_t next_page_id = disk_manager_->AllocatePage();
  return next_page_id;
}

void BufferPoolManager::DeallocatePage(__attribute__((unused)) page_id_t page_id)
{
  disk_manager_->DeAllocatePage(page_id);
}

bool BufferPoolManager::IsPageFree(page_id_t page_id)
{
  return disk_manager_->IsPageFree(page_id);
}

// Only used for debug
bool BufferPoolManager::CheckAllUnpinned()
{
  bool res = true;
  for (size_t i = 0; i < pool_size_; i++)
  {
    if (pages_[i].pin_count_ != 0)
    {
      res = false;
      LOG(ERROR) << "page " << pages_[i].page_id_ << " pin count:" << pages_[i].pin_count_ << endl;
    }
  }
  return res;
}