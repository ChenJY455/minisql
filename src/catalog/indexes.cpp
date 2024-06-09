#include "catalog/indexes.h"

IndexMetadata::IndexMetadata(const index_id_t index_id, const std::string &index_name, const table_id_t table_id,
                             const std::vector<uint32_t> &key_map)
    : index_id_(index_id), index_name_(index_name), table_id_(table_id), key_map_(key_map) {}

IndexMetadata *IndexMetadata::Create(const index_id_t index_id, const string &index_name, const table_id_t table_id,
                                     const vector<uint32_t> &key_map) {
  return new IndexMetadata(index_id, index_name, table_id, key_map);
}

uint32_t IndexMetadata::SerializeTo(char *buf) const {
  char *p = buf;
  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize index info.");
  // magic num
  MACH_WRITE_UINT32(buf, INDEX_METADATA_MAGIC_NUM);
  buf += 4;
  // index id
  MACH_WRITE_TO(index_id_t, buf, index_id_);
  buf += 4;
  // index name
  MACH_WRITE_UINT32(buf, index_name_.length());
  buf += 4;
  MACH_WRITE_STRING(buf, index_name_);
  buf += index_name_.length();
  // table id
  MACH_WRITE_TO(table_id_t, buf, table_id_);
  buf += 4;
  // key count
  MACH_WRITE_UINT32(buf, key_map_.size());
  buf += 4;
  // key mapping in table
  for (auto &col_index : key_map_) {
    MACH_WRITE_UINT32(buf, col_index);
    buf += 4;
  }
  ASSERT(buf - p == ofs, "Unexpected serialize size.");
  return ofs;
}

/**
 * TODO: Done by cww
 * Calculates the total size in bytes required to serialize the IndexMetadata object. This total includes the sizes
 * of various components of the index metadata.
 *
 * Components calculated are:
 * - The size of the index metadata magic number.
 * - The size of the index ID.
 * - The size of the index name, including the length of the name and the size to store the length.
 * - The size of the table ID.
 * - The size of the key count, which indicates how many keys are in the index.
 * - The size of the key map, which includes the size of each key mapped to columns in the table schema.
 *
 * Each of these sizes is added to compute the total serialized size.
 *
 * @return The total serialized size of the index metadata in bytes.
 */
uint32_t IndexMetadata::GetSerializedSize() const {
  uint32_t constexpr size_magic_num = sizeof(INDEX_METADATA_MAGIC_NUM); // Size of the index metadata magic number
  uint32_t constexpr size_index_id = sizeof(index_id_t); // Size of the index ID
  uint32_t const size_index_name = sizeof(uint32_t) + index_name_.length(); // Size of the index name (length + content)
  uint32_t constexpr size_table_id = sizeof(table_id_t); // Size of the table ID
  uint32_t constexpr size_key_count = sizeof(uint32_t); // Size of the key count
  uint32_t const size_keymaps = sizeof(uint32_t) * key_map_.size(); // Total size of the key map (number of keys * size of each key)

  uint32_t const size_ser = size_magic_num + size_index_id + size_index_name + size_table_id
                                + size_key_count + size_keymaps; // Total serialized size

  return size_ser; // Return the calculated serialized size
}

uint32_t IndexMetadata::DeserializeFrom(char *buf, IndexMetadata *&index_meta) {
  if (index_meta != nullptr) {
    LOG(WARNING) << "Pointer object index info is not null in table info deserialize." << std::endl;
  }
  char *p = buf;
  // magic num
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == INDEX_METADATA_MAGIC_NUM, "Failed to deserialize index info.");
  // index id
  index_id_t index_id = MACH_READ_FROM(index_id_t, buf);
  buf += 4;
  // index name
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string index_name(buf, len);
  buf += len;
  // table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // index key count
  uint32_t index_key_count = MACH_READ_UINT32(buf);
  buf += 4;
  // key mapping in table
  std::vector<uint32_t> key_map;
  for (uint32_t i = 0; i < index_key_count; i++) {
    uint32_t key_index = MACH_READ_UINT32(buf);
    buf += 4;
    key_map.push_back(key_index);
  }
  // allocate space for index meta data
  index_meta = new IndexMetadata(index_id, index_name, table_id, key_map);
  return buf - p;
}

Index *IndexInfo::CreateIndex(BufferPoolManager *buffer_pool_manager, const string &index_type) {
  size_t max_size = 0;
  uint32_t column_cnt = key_schema_->GetColumns().size();
  size_t size_bitmap = (column_cnt % 8) ? column_cnt / 8 + 1 : column_cnt / 8;
  // rid + column_cnt + bitmap
  max_size += 4 + sizeof(unsigned char) * size_bitmap;
  for (auto col : key_schema_->GetColumns()) {
    // length of char column
    if(col->GetType() == TypeId::kTypeChar)
      max_size += 4;
    max_size += col->GetLength();
  }

  if (index_type == "bptree") {
    if (max_size <= 8)
      max_size = 16;
    else if (max_size <= 24)
      max_size = 32;
    else if (max_size <= 56)
      max_size = 64;
    else if (max_size <= 120)
      max_size = 128;
    else if (max_size <= 248)
      max_size = 256;
    else {
      LOG(ERROR) << "GenericKey size is too large";
      return nullptr;
    }
  } else {
    return nullptr;
  }
  return new BPlusTreeIndex(meta_data_->index_id_, key_schema_, max_size, buffer_pool_manager);
}
