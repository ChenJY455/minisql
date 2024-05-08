#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * Implement by chenjy
 * Charset: UTF-8
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 如果可分配的页数到达上限，则分配失败
  if(page_allocated_ == GetMaxSupportedSize()) {
    std::cerr << "分配页数到达上限" << std::endl;
    return false;
  }
  uint32_t free_byte = next_free_page_ / 8;
  uint32_t free_bit = next_free_page_ % 8;
  uint32_t free_bit_mask = 1 << (7 - free_bit);
  auto max_page_size = GetMaxSupportedSize();
  /**
   * 将下一个可用的页标记为占用(1)
   * bytes数组里每个元素是8bits，代表8页，所以要找到对应byte对应的bit，并通过 |= 操作置1，并保证其他位不变
   */
  bytes[free_byte] |= free_bit_mask;
  page_offset = next_free_page_;
  /**
   * 寻找下一个可用的next_free_page
   * 此处循环如果进行到内存页末尾，会跳转到内存页开头寻找（因为next_free_page_之前的页可能被释放），直到遍历完整个内存页
   */
  for(auto i = next_free_page_ + 1; i != next_free_page_; i = (i + 1) % max_page_size) {
    if(IsPageFree(i)) {
      // 如果可用，则更新next_free_page_
      next_free_page_ = i;
      break;
    }
  }
  // 更新已分配的页数
  page_allocated_++;
  return true;
}

/**
 * Implement by chenjy
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  if(IsPageFree(page_offset)) {
    std::cerr << "将要释放的内存页未分配" << std::endl;
    return false;
  }
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  uint32_t bit_mask = 1 << (7 - bit_index);
  // 此处的异或操作将对应的位取反(置0)，其余位不变
  bytes[byte_index] ^= bit_mask;
  return true;
}

/**
 * Implement by chenjy
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;
  return IsPageFreeLow(byte_index, bit_index);
}

/**
 * Implement by chenjy
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  uint32_t bit_mask = 1 << (7 - bit_index);
  return (bytes[byte_index] & bit_mask) == 0;
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;