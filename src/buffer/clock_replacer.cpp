#include "buffer/clock_replacer.h"

CLOCKReplacer::CLOCKReplacer(size_t num_pages): capacity(num_pages){}

/**
 * Destroys the CLOCKReplacer.
 */
CLOCKReplacer::~CLOCKReplacer() = default;

/**
 * Implement by chenjy
 */
bool CLOCKReplacer::Victim(frame_id_t *frame_id){
  if(clock_list.empty())
    return false;
  auto iter = clock_list.begin();
  while(true) {
    // Find by circulation
    if(iter == clock_list.end())
      iter = clock_list.begin();
    if(clock_status[*iter] == 1)
      clock_status[*iter] = 0;
    else{
      // if clock_status[*iter] == 0
      *frame_id = *iter;
      clock_status.erase(*iter);
      clock_list.erase(iter);
      break;
    }
    iter++;
  }
  return true;
}

/**
 * Implement by chenjy
 */
void CLOCKReplacer::Pin(frame_id_t frame_id) {
  auto frame_ptr = find(clock_list.begin(), clock_list.end(), frame_id);
  if(frame_ptr == clock_list.end())
    return;
  clock_status.erase(frame_id);
  clock_list.erase(frame_ptr);
}

/**
 * Implement by chenjy
 */
void CLOCKReplacer::Unpin(frame_id_t frame_id) {
  if(find(clock_list.begin(), clock_list.end(), frame_id) != clock_list.end())
    return;
  clock_list.push_back(frame_id);
  clock_status.insert(pair(frame_id, 1));
}

/**
 * Implement by chenjy
 */
size_t CLOCKReplacer::Size() {
  return clock_list.size();
}