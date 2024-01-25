#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
// allocate a empty page and return the index of the page if succeeded
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset)
{
  ASSERT(next_free_page_ >= 0, "next_free_page_ in bit map invalid");

  // if the bit map is already full
  if (this->next_free_page_ >= MAX_CHARS * sizeof(char) * 8)
  {
    // ASSERT(false, "next_free_page_ too big");
    return false;
  }

  // calculate the page_offset, allocate the page
  page_offset = this->next_free_page_;
  int byte_offset = this->next_free_page_ / (sizeof(char) * 8);
  int bit_offset = this->next_free_page_ % (sizeof(char) * 8);
  unsigned char page_mask = ((unsigned char)128) >> bit_offset;
  bytes[byte_offset] = bytes[byte_offset] | page_mask;
  // update the next_free_page and page_allocated
  uint32_t i;
  for (i = this->next_free_page_ + 1; i < MAX_CHARS * 8; i++)
  {
    if (IsPageFree(i))
      break;
  }
  this->next_free_page_ = i;
  this->page_allocated_++;
  return true;
}

/**
 * TODO: Student Implement
 */
// reset the bit, and return false if the bit is reset already
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset)
{
  // get the state of the page
  int byte_offset = page_offset / (sizeof(char) * 8);
  int bit_offset = page_offset % (sizeof(char) * 8);
  unsigned char page_mask = ((unsigned char)128) >> bit_offset;
  int state = bytes[byte_offset] & page_mask;
  // return false if it's reset
  if (state == 0)
  {
    return false;
  }
  // change to the state
  bytes[byte_offset] = bytes[byte_offset] & (~page_mask);
  // update next_free_page, page_allocated
  this->next_free_page_ = this->next_free_page_ > page_offset ? page_offset : this->next_free_page_;
  this->page_allocated_--;
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const
{
  // extract the state of the page
  int byte_offset = page_offset / (sizeof(char) * 8);
  int bit_offset = page_offset % (sizeof(char) * 8);
  unsigned char page_mask = ((unsigned char)128 >> bit_offset);
  int state = bytes[byte_offset] & page_mask;
  // examine the state
  return state == 0 ? true : false;
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const
{
  unsigned char page_mask = ((unsigned char)128 >> bit_index);
  int state = bytes[byte_index] & page_mask;
  return state == 0 ? true : false;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;