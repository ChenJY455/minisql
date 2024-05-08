#include "storage/disk_manager.h"

#include <sys/stat.h>

#include <filesystem>
#include <stdexcept>

#include "glog/logging.h"
#include "page/bitmap_page.h"

DiskManager::DiskManager(const std::string &db_file) : file_name_(db_file) {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
  // directory or file does not exist
  if (!db_io_.is_open()) {
    db_io_.clear();
    // create a new file
    std::filesystem::path p = db_file;
    if (p.has_parent_path()) std::filesystem::create_directories(p.parent_path());
    db_io_.open(db_file, std::ios::binary | std::ios::trunc | std::ios::out);
    db_io_.close();
    // reopen with original mode
    db_io_.open(db_file, std::ios::binary | std::ios::in | std::ios::out);
    if (!db_io_.is_open()) {
      throw std::exception();
    }
  }
  ReadPhysicalPage(META_PAGE_ID, meta_data_);
}

void DiskManager::Close() {
  std::scoped_lock<std::recursive_mutex> lock(db_io_latch_);
  WritePhysicalPage(META_PAGE_ID, meta_data_);
  if (!closed) {
    db_io_.close();
    closed = true;
  }
}

void DiskManager::ReadPage(page_id_t logical_page_id, char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  ReadPhysicalPage(MapPageId(logical_page_id), page_data);
}

void DiskManager::WritePage(page_id_t logical_page_id, const char *page_data) {
  ASSERT(logical_page_id >= 0, "Invalid page id.");
  WritePhysicalPage(MapPageId(logical_page_id), page_data);
}

/**
 * Implement by chenjy
 */
page_id_t DiskManager::AllocatePage() {
  auto meta_data_cast = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  if(meta_data_cast->GetAllocatedPages() == MAX_VALID_PAGE_ID) {
    LOG(WARNING) << "磁盘分配已达上限" << std::endl;
    return INVALID_PAGE_ID;
  }
  uint32_t MAX_EXTENT_NUM = (PAGE_SIZE - 8) / 4;
  for(uint32_t i = 0; i < MAX_EXTENT_NUM; i++){
    // 如果该区块有可用的页，那就插入
    if(meta_data_cast->GetExtentUsedPage(i) < BITMAP_SIZE) {
      char bitmap_data_[PAGE_SIZE];
      auto bitmap_data_cast = reinterpret_cast<BitmapPage<PAGE_SIZE>* >(bitmap_data_);
      uint32_t page_offset;
      // 由于位图没有逻辑页码，所以只能手动计算物理页码并读取
      page_id_t bitmap_index = i * (BITMAP_SIZE + 1) + 1;

      ReadPhysicalPage(bitmap_index, bitmap_data_);
      if(!bitmap_data_cast->AllocatePage(page_offset)) {
        // 理论上这种情况不会发生，因为之前已经检测过
        LOG(WARNING) << "该分区已满" << std::endl;
      }
      // 因为是指针cast，所以在bitmap_data_cast改变的同时，bitmap_data_也改变了，直接写回磁盘
      WritePhysicalPage(bitmap_index, bitmap_data_);
      // 更新meta_data参数
      meta_data_cast->num_allocated_pages_++;
      if(meta_data_cast->GetExtentUsedPage(i) == 0) {
        // 如果启用了新区块，区块数量加一
        meta_data_cast->num_extents_++;
      }
      meta_data_cast->extent_used_page_[i]++;
      return i * BITMAP_SIZE + page_offset;
    }
  }
}

/**
 * Implement by chenjy
 */
void DiskManager::DeAllocatePage(page_id_t logical_page_id) {
  auto meta_data_cast = reinterpret_cast<DiskFileMetaPage *>(meta_data_);
  char bitmap_data_[PAGE_SIZE];
  auto bitmap_data_cast = reinterpret_cast<BitmapPage<PAGE_SIZE>* >(bitmap_data_);
  page_id_t extent_index = logical_page_id / BITMAP_SIZE;
  page_id_t page_offset = logical_page_id % BITMAP_SIZE;
  page_id_t bitmap_index = extent_index * (BITMAP_SIZE + 1) + 1;

  ReadPhysicalPage(bitmap_index, bitmap_data_);
  if(!bitmap_data_cast->DeAllocatePage(page_offset)) {
    LOG(WARNING) << "磁盘释放失败" << std::endl;
  }
  WritePhysicalPage(bitmap_index, bitmap_data_);
  // 相关参数更新
  meta_data_cast->num_allocated_pages_--;
  meta_data_cast->extent_used_page_[extent_index]--;
  if(meta_data_cast->GetExtentUsedPage(extent_index) == 0) {
    // 如果一个区块空了，那使用的区块数目减一
    meta_data_cast->num_extents_--;
  }
}

/**
 * Implement by chenjy
 */
bool DiskManager::IsPageFree(page_id_t logical_page_id) {
  char bitmap_data_[PAGE_SIZE];
  auto bitmap_data_cast = reinterpret_cast<BitmapPage<PAGE_SIZE>* >(bitmap_data_);
  page_id_t extent_index = logical_page_id / BITMAP_SIZE;
  page_id_t page_offset = logical_page_id % BITMAP_SIZE;
  page_id_t bitmap_index = extent_index * (BITMAP_SIZE + 1) + 1;

  ReadPhysicalPage(bitmap_index, bitmap_data_);
  return bitmap_data_cast->IsPageFree(page_offset);
}

/**
 * Implement by chenjy
 * extent_index: 该逻辑页码属于第几组分区
 * page_index: 该逻辑页码在所属分区中的位置
 * BITMAP_SIZE: 每个分区支持的最大数据页数
 */
page_id_t DiskManager::MapPageId(page_id_t logical_page_id) {
  auto extent_index = logical_page_id / BITMAP_SIZE;
  auto page_index = logical_page_id % BITMAP_SIZE;
  return extent_index * (BITMAP_SIZE + 1) + page_index + 2;
}

int DiskManager::GetFileSize(const std::string &file_name) {
  struct stat stat_buf;
  int rc = stat(file_name.c_str(), &stat_buf);
  return rc == 0 ? stat_buf.st_size : -1;
}

void DiskManager::ReadPhysicalPage(page_id_t physical_page_id, char *page_data) {
  int offset = physical_page_id * PAGE_SIZE;
  // check if read beyond file length
  if (offset >= GetFileSize(file_name_)) {
#ifdef ENABLE_BPM_DEBUG
    LOG(INFO) << "Read less than a page" << std::endl;
#endif
    memset(page_data, 0, PAGE_SIZE);
  } else {
    // set read cursor to offset
    db_io_.seekp(offset);
    db_io_.read(page_data, PAGE_SIZE);
    // if file ends before reading PAGE_SIZE
    int read_count = db_io_.gcount();
    if (read_count < PAGE_SIZE) {
#ifdef ENABLE_BPM_DEBUG
      LOG(INFO) << "Read less than a page" << std::endl;
#endif
      memset(page_data + read_count, 0, PAGE_SIZE - read_count);
    }
  }
}

void DiskManager::WritePhysicalPage(page_id_t physical_page_id, const char *page_data) {
  size_t offset = static_cast<size_t>(physical_page_id) * PAGE_SIZE;
  // set write cursor to offset
  db_io_.seekp(offset);
  db_io_.write(page_data, PAGE_SIZE);
  // check for I/O error
  if (db_io_.bad()) {
    LOG(ERROR) << "I/O error while writing";
    return;
  }
  // needs to flush to keep disk file in sync
  db_io_.flush();
}