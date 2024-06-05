#include "catalog/table.h"

uint32_t TableMetadata::SerializeTo(char *buf) const {
  char *p = buf;
  uint32_t ofs = GetSerializedSize();
  ASSERT(ofs <= PAGE_SIZE, "Failed to serialize table info.");
  // magic num
  MACH_WRITE_UINT32(buf, TABLE_METADATA_MAGIC_NUM);
  buf += 4;
  // table id
  MACH_WRITE_TO(table_id_t, buf, table_id_);
  buf += 4;
  // table name
  MACH_WRITE_UINT32(buf, table_name_.length());
  buf += 4;
  MACH_WRITE_STRING(buf, table_name_);
  buf += table_name_.length();
  // table heap root page id
  MACH_WRITE_TO(page_id_t, buf, root_page_id_);
  buf += 4;
  // table schema
  buf += schema_->SerializeTo(buf);
  ASSERT(buf - p == ofs, "Unexpected serialize size.");
  return ofs;
}

/**
 * TODO: Done by cww
 * Calculates the total size in bytes required to serialize the TableMetadata object. This total includes the sizes
 * of various components of the table metadata.
 *
 * Components calculated are:
 * - The size of the table metadata magic number.
 * - The size of the table ID.
 * - The size of the table name, including the length of the name and the size to store the length.
 * - The size of the root page ID.
 * - The size of the table schema, which is determined by calling the schema's GetSerializedSize method.
 *
 * Each of these sizes is added to compute the total serialized size.
 *
 * @return The total serialized size of the table metadata in bytes.
 */
uint32_t TableMetadata::GetSerializedSize() const {
  uint32_t constexpr size_magic_num = sizeof(uint32_t); // Size of the table metadata magic number
  uint32_t constexpr size_table_id = sizeof(table_id_); // Size of the table ID
  uint32_t const size_table_name = sizeof(uint32_t) + table_name_.length(); // Size of the table name (length + content)
  uint32_t constexpr size_root_page_id = sizeof(page_id_t); // Size of the root page ID
  uint32_t const size_table_schema = schema_->GetSerializedSize(); // Size of the table schema

  uint32_t const size_ser = size_magic_num + size_table_id + size_table_name + size_root_page_id + size_table_schema; // Total serialized size

  return size_ser; // Return the calculated serialized size
}

/**
 *
 * @param heap Memory heap passed by TableInfo
 */
uint32_t TableMetadata::DeserializeFrom(char *buf, TableMetadata *&table_meta) {
  if (table_meta != nullptr) {
    LOG(WARNING) << "Pointer object table info is not null in table info deserialize." << std::endl;
  }
  char *p = buf;
  // magic num
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == TABLE_METADATA_MAGIC_NUM, "Failed to deserialize table info.");
  // table id
  table_id_t table_id = MACH_READ_FROM(table_id_t, buf);
  buf += 4;
  // table name
  uint32_t len = MACH_READ_UINT32(buf);
  buf += 4;
  std::string table_name(buf, len);
  buf += len;
  // table heap root page id
  page_id_t root_page_id = MACH_READ_FROM(page_id_t, buf);
  buf += 4;
  // table schema
  TableSchema *schema = nullptr;
  buf += TableSchema::DeserializeFrom(buf, schema);
  // allocate space for table metadata
  table_meta = new TableMetadata(table_id, table_name, root_page_id, schema);
  return buf - p;
}

/**
 * Only called by create table
 *
 * @param heap Memory heap passed by TableInfo
 */
TableMetadata *TableMetadata::Create(table_id_t table_id, std::string table_name, page_id_t root_page_id,
                                     TableSchema *schema) {
  // allocate space for table metadata
  return new TableMetadata(table_id, table_name, root_page_id, schema);
}

TableMetadata::TableMetadata(table_id_t table_id, std::string table_name, page_id_t root_page_id, TableSchema *schema)
    : table_id_(table_id), table_name_(table_name), root_page_id_(root_page_id), schema_(schema) {}
