#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : capacity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * If want to insert a new page that is not in the lru, victim first and than unpin the page
 * If want to call a page existing in the lru, pin it first and then unpin it
 */

/**
 * TODO: Student Implement
 */
// the least recently visited id should be returned by pointe frame_id
bool LRUReplacer::Victim(frame_id_t *frame_id)
{
  // first we need to judge whther there are enough frmae_ids to be deleted
  if ((this->lru_list_).size() == 0)
  {
    return false;
  }
  // then delete the frame_id from lru_list_ and lru_hash_
  *frame_id = (this->lru_list_).back();
  (this->lru_hash_).erase(*frame_id);
  (this->lru_list_).pop_back();
  return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id)
{
  // delete from the list and the hash table, so that no other threads can pin or delete it
  if (this->lru_hash_.count(frame_id) != 0)
  {
    auto it = (this->lru_hash_)[frame_id];
    (this->lru_list_).erase(it);
    (this->lru_hash_).erase(frame_id);
  }
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id)
{
  // determine whther the capacity is full and whether the frame is already in the buffer pool
  if ((this->lru_hash_).count(frame_id) != 0 || (this->lru_list_).size() == this->capacity)
  {
    return;
  }
  // insert the new page in the front of the list and map it into the hash table
  (this->lru_list_).push_front(frame_id);
  auto it = (this->lru_list_).begin();
  (this->lru_hash_).emplace(frame_id, it);
  return;
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size()
{
  return (this->lru_list_).size();
}



/*下面是clock lru 的实现*/

ClockReplacer::ClockReplacer(size_t num_pages) : capacity(num_pages),victim_num(0),clock_pointer(0){
  struct PageState page;
  page.isPin= true;
  page.recently_visited=true;
  for(int i=0;i<num_pages;i++){
    clock_vec.push_back(page);
  }
}

ClockReplacer::~ClockReplacer() = default;

// the least recently visited id should be returned by pointe frame_id
bool ClockReplacer::Victim(frame_id_t *frame_id)
{

  while(victim_num>0){
    //如果最近被访问，将状态改为false，然后跳过
    if (clock_vec[clock_pointer].recently_visited) {
      clock_vec[clock_pointer].recently_visited = false;
      clock_pointer=(clock_pointer+1)%(capacity);
      continue;
    }
    //如果被pin，直接跳过
    if (clock_vec[clock_pointer].isPin) {
      clock_pointer=(clock_pointer+1)%(capacity);
      continue;
    }
    //满足ispin==false 且 recently_visited==false
    clock_vec[clock_pointer].isPin = true;
    *frame_id =clock_pointer;
    victim_num--;
    clock_pointer=(clock_pointer+1)%(capacity);
    return true;
  }
  return false;//没有可以victim的了
}


void ClockReplacer::Pin(frame_id_t frame_id)
{
  if (clock_vec[frame_id].isPin==false) {
    clock_vec[frame_id].isPin = true;
    victim_num--;
  }
}

void ClockReplacer::Unpin(frame_id_t frame_id)
{

  if (clock_vec[frame_id].isPin) {
    clock_vec[frame_id].isPin = false;
    clock_vec[frame_id].recently_visited = true;
    victim_num++;
  }
}

//这里size表示里面还可以替换出来的数量！！
size_t ClockReplacer::Size()
{
  return victim_num;
}