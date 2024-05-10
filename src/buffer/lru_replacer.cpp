#include "buffer/lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * Implement by chenjy
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if(buffer_.empty())
    return false;
  *frame_id = buffer_.front();
  buffer_.pop_front();
  return true;
}

/**
 * Implement by chenjy
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  auto frame_ptr = find(buffer_.begin(), buffer_.end(), frame_id);
  if(frame_ptr == buffer_.end())
    return;
  else
    buffer_.erase(frame_ptr);
}

/**
 * Implement by chenjy
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  if(find(buffer_.begin(), buffer_.end(), frame_id) != buffer_.end())
    return;
  buffer_.push_back(frame_id);
}

/**
 * Implement by chenjy
 */
size_t LRUReplacer::Size() {
  return buffer_.size();
}