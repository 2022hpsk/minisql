#ifndef MINISQL_LRU_REPLACER_H
#define MINISQL_LRU_REPLACER_H

#include <list>
#include <mutex>
#include <unordered_set>
#include <vector>
#include <unordered_map>

#include "buffer/replacer.h"
#include "common/config.h"

using namespace std;

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer
{
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  // add your own private member variables here
  size_t capacity;                                                      // The biggest volumn
  list<frame_id_t> lru_list_;                                           // The frame_ids which can be removed
  std::unordered_map<frame_id_t, list<frame_id_t>::iterator> lru_hash_; // the hash table
};


class ClockReplacer : public Replacer
{
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit ClockReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~ClockReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 private:
  struct PageState {
    bool recently_visited;
    bool isPin;
  };
  size_t capacity;    // The clock size
  size_t victim_num;  //number of pages that can be victim (not be pined )
  size_t clock_pointer;  //clock的指针
  std::vector<PageState> clock_vec;//下标为frame_id
};




#endif // MINISQL_LRU_REPLACER_H
