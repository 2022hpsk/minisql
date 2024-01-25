#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages)
    : capcity(num_pages) {}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t* frame_id) {
    if (lruList.empty())  // 空, 即所有页面都被锁了
        return false;
    frame_id_t ret = lruList.back();
    lruList.pop_back();
    hash.erase(ret);
    *frame_id = ret;
    return true;
}

/**
 * TODO: Student Implement
 */
// 将数据页固定使之不能被Replacer替换, Pin函数应当在一个数据页被Buffer Pool Manager固定时被调用；
void LRUReplacer::Pin(frame_id_t frame_id) {
    if (hash.count(frame_id)) {
        lruList.erase(hash[frame_id]);
        hash.erase(frame_id);
    }
}

/**
 * TODO: Student Implement
 */
// Unpin函数应当在一个数据页的引用计数变为0时被Buffer Pool Manager调用，使页帧对应的数据页能够在必要时被替换；
void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (hash.count(frame_id) || hash.size() >= capcity)
        return;
    lruList.push_front(frame_id);
    hash[frame_id] = lruList.begin();
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
    return hash.size();
}