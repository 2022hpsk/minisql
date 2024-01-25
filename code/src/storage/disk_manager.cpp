#include "storage/disk_manager.h"

#include <sys/stat.h>
#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file)
{
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open())
  {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path())
      std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open())
    {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close()
{
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed)
  {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data)
{
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data)
{
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::AllocatePage()
{
  // Find the extent that is not full yet
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  DiskFileMetaPage *MetaPage = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);
  uint32_t extent_id = 0;
  for (extent_id = 0; extent_id < MetaPage->num_extents_; extent_id++)
  {
    if (MetaPage->extent_used_page_[extent_id] != PageNum)
    {
      break;
    }
  }
  // update the num_allocated_pages
  MetaPage->num_allocated_pages_++;
  uint32_t page_offset;
  // If extent exists, then allocate the page using bitmap api
  if (extent_id < MetaPage->num_extents_)
  {
    char temp[PAGE_SIZE];
    ReadPhysicalPage((PageNum + 1) * extent_id + 1, temp);
    reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp)->AllocatePage(page_offset);
    ASSERT(page_offset >= 0, "page offset invalid");
    MetaPage->extent_used_page_[extent_id]++;
    WritePhysicalPage((PageNum + 1) * extent_id + 1, temp);
  }
  // If not exists, then new a extent
  else
  {
    MetaPage->num_extents_++;
    MetaPage->extent_used_page_[extent_id] = 1;
    BitmapPage<PAGE_SIZE> *newMap = new BitmapPage<PAGE_SIZE>();
    newMap->AllocatePage(page_offset);
    ASSERT(page_offset >= 0, "page offset invalid");
    WritePhysicalPage((PageNum + 1) * extent_id + 1, reinterpret_cast<char *>(newMap));
  }
  // We need return the page_id that is transparent to user(not the real physical id)
  ASSERT(extent_id * PageNum + page_offset >= 0, "page_id invalid");
  return (page_id_t)(extent_id * PageNum + page_offset);
}

/**
 * TODO: Student Implement
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id)
{
  // extract the page out
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id = logical_page_id / PageNum;
  uint32_t page_offset = logical_page_id % PageNum;
  char temp[PAGE_SIZE];
  ReadPhysicalPage((PageNum + 1) * extent_id + 1, temp);
  // deallocate it, return if fails
  bool result = true;
  result = reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp)->DeAllocatePage(page_offset);
  if (result == false)
  {
    return;
  }
  // update the num_allocated_pages_ and extent_used_page_
  WritePhysicalPage((PageNum + 1) * extent_id + 1, temp);
  DiskFileMetaPage *MetaPage = reinterpret_cast<DiskFileMetaPage *>(this->meta_data_);
  MetaPage->num_allocated_pages_--;
  MetaPage->extent_used_page_[extent_id]--;
  return;
}

/**
 * TODO: Student Implement
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id)
{
  // extract the page out
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  uint32_t extent_id = logical_page_id / PageNum;
  uint32_t page_offset = logical_page_id % PageNum;
  char temp[PAGE_SIZE];
  ReadPhysicalPage((PageNum + 1) * extent_id + 1, temp);
  // determine whether the page is free or not
  return reinterpret_cast<BitmapPage<PAGE_SIZE> *>(temp)->IsPageFree(page_offset);
}

/**
 * TODO: Student Implement
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id)
{
  size_t PageNum = BitmapPage<PAGE_SIZE>::GetMaxSupportedSize();
  return (PageNum + 1) * logical_page_id / PageNum + 1 + logical_page_id % PageNum + 1;
}

int DiskManager::GetFileSize(const std::string &file_name)
{
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data)
{
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_))
  {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  }
  else
  {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE)
    {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data)
{
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad())
  {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}