#include "catalog/catalog.h"
#include <algorithm>

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Done by cww
 * Calculates the total size in bytes that the serialized representation of the catalog metadata would occupy.
 * This total includes the sizes of various components of the catalog metadata.
 *
 * Components calculated are:
 * - The size of the catalog metadata magic number.
 * - The size of the table ID.
 * - The size of the index ID.
 * - The size for all table metadata pages, which include table ID and page ID for each table metadata page.
 * - The size for all index metadata pages, which include table ID and page ID for each index metadata page.
 *
 * Each of these sizes are added to compute the total serialized size.
 *
 * @return The total serialized size of the catalog metadata in bytes.
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  uint32_t constexpr size_magic_num = sizeof(CATALOG_METADATA_MAGIC_NUM);
  uint32_t constexpr size_table_id = sizeof(table_id_t);
  uint32_t constexpr size_index_id = sizeof(index_id_t);
  uint32_t const size_table_meta_page = (sizeof( table_id_t) + sizeof(page_id_t)) * table_meta_pages_.size();
  uint32_t const size_index_meta_page = (sizeof(table_id_t) + sizeof(page_id_t)) * index_meta_pages_.size();

  uint32_t const size_ser = size_magic_num + size_table_id + size_index_id + size_table_meta_page + size_index_meta_page;

  return size_ser;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Done by cww
 * Constructs a new CatalogManager instance, responsible for managing catalog metadata in a database.
 * This constructor initializes the CatalogManager with instances of BufferPoolManager, LockManager, and LogManager.
 * It also handles the initialization or loading of the catalog metadata based on the provided `init` flag.
 *
 * @param buffer_pool_manager Pointer to the BufferPoolManager responsible for managing the buffer pool.
 * @param lock_manager Pointer to the LockManager for managing locks on database entities.
 * @param log_manager Pointer to the LogManager for logging database operations.
 * @param init Boolean flag indicating whether to initialize a new catalog or load an existing one.
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
// Initialize DBStorageEngine instance based on the `init` flag
  if (init) {
    // Create a new instance of CatalogMeta for initial setup
    catalog_meta_ = CatalogMeta::NewInstance();

    // Fetch the catalog meta page from the buffer pool using a predefined page ID
    Page *catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);

    // Serialize the newly created catalog metadata to the fetched page
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());

    // Initialize IDs for tables and indexes to zero for a new database
    next_table_id_ = 0;
    next_index_id_ = 0;

    // Unpin the catalog meta page after modifying it, marking it as dirty (true)
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, true);
  } else {
    // Fetch the catalog meta page from the buffer pool for an existing database
    Page *catalog_meta_page = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);

    // Check if the catalog meta page was successfully fetched
    if (catalog_meta_page == nullptr) {
      // Log an error and throw an exception if the page could not be fetched
      LOG(ERROR) << "Failed to fetch catalog meta page";
      throw runtime_error("Failed to fetch catalog meta page");
    }

    // Get the data from the catalog meta page
    char *meta_data = catalog_meta_page->GetData();

    // Deserialize the catalog metadata from the page data
    catalog_meta_ = CatalogMeta::DeserializeFrom(meta_data);

    // Unpin the catalog meta page without marking it as dirty (false)
    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);

    // Load existing table data from catalog meta
    for(auto const& [table_id, page_id]: catalog_meta_->table_meta_pages_){
      // Attempt to load each table using its ID and page ID
      if (LoadTable(table_id, page_id) != DB_SUCCESS) {
        // Log a warning if a table fails to load
        LOG(WARNING) << "Fail to load table data" << std::endl;
      }
    }

    // Load existing index data from catalog meta
    for(auto const& [index_id, page_id]: catalog_meta_->index_meta_pages_){
      // Attempt to load each index using its ID and page ID
      if (LoadIndex(index_id, page_id) != DB_SUCCESS) {
        // Log a warning if an index fails to load
        LOG(WARNING) << "Fail to load index data" << std::endl;
      }
    }

    // Update the next IDs for tables and indexes based on the loaded metadata
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Done by cww
 * Attempts to create a new table within the database catalog. This function handles the creation
 * of necessary data structures and storage mechanisms required to manage the table, including
 * serialization of metadata and updating the catalog.
 *
 * @param table_name The name of the new table to create. It must not already exist in the database.
 * @param schema Pointer to the TableSchema object describing the schema of the new table.
 * @param txn Pointer to the Transaction object representing the current transaction context.
 * @param table_info Reference to a pointer that will hold the newly created TableInfo object upon successful creation.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_ALREADY_EXIST if a table with the given name already exists.
 *    - DB_FAILED on failure to create the table or any related components.
 *    - DB_SUCCESS on successful creation of the table and all associated structures.
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info) {
  // Check if the table name already exists in the catalog
  if (table_names_.find(table_name) != table_names_.end()) {
    // Return error if table already exists
    return DB_TABLE_ALREADY_EXIST;
  }

  // Create a new table heap instance using provided managers and transaction details
  TableHeap *new_table = TableHeap::Create(buffer_pool_manager_, schema, txn, log_manager_, lock_manager_);
  if (!new_table) {
    // Return a generic failure error if the table heap could not be created
    return DB_FAILED;
  }

  // Request a new page from the buffer pool manager to store table metadata
  page_id_t table_meta_page_id;
  auto const table_meta_page = buffer_pool_manager_->NewPage(table_meta_page_id);
  if (!table_meta_page) {
    // Clean up the newly created table heap if the page could not be allocated
    delete new_table;
    // Handle page creation failure
    return DB_FAILED;
  }

  // Create table metadata object, serialize it, and store it in the new page
  auto const table_meta = TableMetadata::Create(next_table_id_, table_name, new_table->GetFirstPageId(), schema);
  table_meta->SerializeTo(table_meta_page->GetData());
  // Mark the page as dirty
  buffer_pool_manager_->UnpinPage(table_meta_page_id, true);

  // Update the catalog's internal structures with new table's metadata
  catalog_meta_->table_meta_pages_[next_table_id_] = table_meta_page_id;
  table_names_[table_name] = next_table_id_;

  // Initialize the TableInfo object for the new table
  table_info = TableInfo::Create();
  if (!table_info) {
    // Clean up in case the table info object could not be created
    buffer_pool_manager_->DeletePage(table_meta_page_id);  // Delete the metadata page
    delete new_table;  // Clean up the table heap
    return DB_FAILED;  // Handle table info creation failure
  }
  table_info->Init(table_meta, new_table);
  tables_[next_table_id_] = table_info;
  ++next_table_id_;

  // Update catalog metadata in the buffer pool
  auto const catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (!catalog_meta_page) {
    // Clean up the TableInfo object if the catalog metadata page couldn't be fetched
    delete table_info;
    return DB_FAILED;  // Handle catalog metadata page fetch failure
  }
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());
  // Write changes to disk
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  // Return success after all operations are completed successfully
  return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Retrieves the TableInfo object for a given table name from the database catalog.
 * This function searches the internal catalog structures to find and return the TableInfo
 * associated with the specified table name.
 *
 * @param table_name The name of the table to retrieve.
 * @param table_info Reference to a pointer that will be set to point to the TableInfo object if the table is found.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_NOT_EXIST if no table with the given name exists in the catalog.
 *    - DB_FAILED if the table is found but the corresponding TableInfo could not be retrieved (e.g., data inconsistency).
 *    - DB_SUCCESS if the table and its corresponding TableInfo are successfully retrieved.
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  // Try to find the table by its name in the catalog
  auto table_iter = table_names_.find(table_name);

  // Check if the table was found
  if (table_iter == table_names_.end()) {
    // If the table is not found, return an error indicating the table does not exist
    return DB_TABLE_NOT_EXIST;
  }

  // Retrieve the corresponding TableInfo using the ID obtained from table_names_
  auto info_iter = tables_.find(table_iter->second);

  // Check if the retrieval was successful
  if (info_iter == tables_.end()) {
    // If no TableInfo is found for the given ID, return an error (safety check)
    return DB_FAILED;
  }

  // Assign the found TableInfo to the output parameter
  table_info = info_iter->second;

  // Return success after successfully retrieving the TableInfo
  return DB_SUCCESS;
}


/**
 * TODO: Done by cww
 * Retrieves all TableInfo objects currently registered in the database catalog.
 * This function compiles a list of pointers to TableInfo objects representing each table
 * managed by the CatalogManager.
 *
 * @param tables A reference to a vector that will be populated with pointers to the TableInfo objects.
 *               The vector will be cleared and resized as necessary to hold all the TableInfo pointers.
 *
 * @return Always returns DB_SUCCESS as this function does not involve operations that can typically fail.
 *
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  // Reserve capacity to avoid multiple reallocations
  tables.reserve(tables_.size());

  // Use structured bindings to directly unpack the pair
  for (const auto& [key, table_info] : tables_) {
    tables.push_back(table_info);
  }

  return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Attempts to create a new index for a specified table in the database catalog. This function manages the creation
 * of index structures and metadata, ensuring that the index is properly registered within the catalog system.
 *
 * @param table_name The name of the table for which the index is to be created.
 * @param index_name The name of the new index.
 * @param index_keys A vector of strings representing the column names that will form the index keys.
 * @param txn Pointer to the Transaction object representing the current transaction context.
 * @param index_info Reference to a pointer that will hold the newly created IndexInfo object upon successful creation.
 * @param index_type The type of the index (e.g., "B+Tree", "Hash").
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_NOT_EXIST if no table with the given name exists in the catalog.
 *    - DB_INDEX_ALREADY_EXIST if an index with the given name already exists for the specified table.
 *    - DB_COLUMN_NAME_NOT_EXIST if one or more column names provided in index_keys do not exist in the table.
 *    - DB_FAILED on failure to allocate a new page or other internal failures.
 *    - DB_SUCCESS on successful creation of the index and all associated structures.
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const std::string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const std::string &index_type) {
    // Check if the specified table exists in the catalog
    auto const tableIter = table_names_.find(table_name);
    if (tableIter == table_names_.end()) {
        // Return error if table does not exist
        return DB_TABLE_NOT_EXIST;
    }

    // Check if the specified index already exists for the table
    if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
        // Return error if index already exists
        return DB_INDEX_ALREADY_EXIST;
    }

    // Retrieve the table ID from the iterator
    table_id_t const table_id = tableIter->second;

    // Get the columns of the table schema
    auto columns = tables_[table_id]->GetSchema()->GetColumns();

    // Map the index keys to the columns of the table
    std::vector<uint32_t> key_map;
    for (const auto& index_key : index_keys) {
        auto colIter = std::find_if(columns.begin(), columns.end(),
                                    [&index_key](const auto* col) { return col->GetName() == index_key; });
        // Check if index key corresponds to any column name
        if (colIter == columns.end()) {
            return DB_COLUMN_NAME_NOT_EXIST; // Return error if no matching column found
        }
        // Store the index of the column that matches the index key
        key_map.push_back((*colIter)->GetTableInd());
    }

    // Allocate a new page in the buffer pool to store the index metadata
    page_id_t index_page_id;
    auto const index_page = buffer_pool_manager_->NewPage(index_page_id);
    if (!index_page) {
        // Return error if the page could not be allocated
        return DB_FAILED;
    }

    // Create index metadata and serialize it to the newly allocated page
    IndexMetadata *index_meta = IndexMetadata::Create(next_index_id_, index_name, table_id, key_map);
    index_meta->SerializeTo(index_page->GetData());

    // Mark the page as dirty and unpin it
    buffer_pool_manager_->UnpinPage(index_page_id, true);

    // Create and initialize the IndexInfo object
    index_info = IndexInfo::Create();
    index_info->Init(index_meta, tables_[table_id], buffer_pool_manager_);

    // Store the index metadata page ID in the catalog metadata
    catalog_meta_->index_meta_pages_.emplace(next_index_id_, index_page_id);
    auto catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_->SerializeTo(catalog_meta_page->GetData());

    // Mark the catalog metadata page as dirty and unpin it
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

    // Add the new index to the index names map for the table
    auto& index_map = index_names_[table_name];
    index_map.emplace(index_name, next_index_id_);

    // Store the new IndexInfo object in the global index map and increment the next index ID
    indexes_.emplace(next_index_id_, index_info);
    ++next_index_id_;

    // Return success status
    return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Retrieves the IndexInfo object for a specified index of a given table from the database catalog.
 * This function searches the internal catalog structures to find and return the IndexInfo
 * associated with the specified index name and table.
 *
 * @param table_name The name of the table whose index is to be retrieved.
 * @param index_name The name of the index to retrieve.
 * @param index_info Reference to a pointer that will be set to point to the IndexInfo object if the index is found.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_INDEX_NOT_FOUND if the specified table or index does not exist in the catalog.
 *    - DB_SUCCESS if the index and its corresponding IndexInfo are successfully retrieved.
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  // Find the table entry in the index names map
  auto const tableIter = index_names_.find(table_name);
  if (tableIter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;  // Table not found in index map
  }

  // Find the specific index entry within the table
  auto const indexIter = tableIter->second.find(index_name);
  if (indexIter == tableIter->second.end()) {
    return DB_INDEX_NOT_FOUND;  // Index not found in table's index map
  }

  // Retrieve the IndexInfo using the found index ID
  index_info = indexes_.at(indexIter->second);
  if (!index_info) {
    return DB_INDEX_NOT_FOUND;  // IndexInfo not found, though index ID was present
  }

  return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Retrieves all IndexInfo objects for a given table from the database catalog. This function compiles a list
 * of pointers to IndexInfo objects representing each index associated with the specified table.
 *
 * @param table_name The name of the table whose indexes are to be retrieved.
 * @param indexes A reference to a vector that will be populated with pointers to the IndexInfo objects for all indexes of the specified table.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_INDEX_NOT_FOUND if no indexes are found for the specified table in the catalog.
 *    - DB_SUCCESS if all indexes for the table are successfully retrieved.
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  // Find the table in the map to see if it has any indices
  auto const tableIndicesIter = index_names_.find(table_name);
  if (tableIndicesIter == index_names_.end()) {
    return DB_INDEX_NOT_FOUND; // No indices found for the specified table
  }

  // Reserve space in the vector to improve efficiency
  indexes.reserve(tableIndicesIter->second.size());

  // Iterate over the index map of the found table to retrieve all index information
  for (const auto& [index_name, index_id] : tableIndicesIter->second) {
    // Safely access the index info using at(), which throws if the key is not found
    indexes.push_back(indexes_.at(index_id));
  }

  return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Deletes a specified table and all associated indexes from the database catalog. This function manages the removal
 * of the table's entries from the internal catalog structures and ensures all relevant metadata pages are deleted
 * from the storage.
 *
 * @param table_name The name of the table to be deleted.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_NOT_EXIST if no table with the given name exists in the catalog.
 *    - DB_FAILED if there are issues during the operation such as failing to fetch or delete pages.
 *    - DB_SUCCESS if the table and all its associated structures are successfully deleted.
 */
dberr_t CatalogManager::DropTable(const std::string &table_name) {
  // Check if the table exists in table_names_
  auto const table_itr = table_names_.find(table_name);
  if (table_itr == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;  // If the table does not exist, return DB_TABLE_NOT_EXIST
  }

  // Check if there are associated indexes for the table in index_names_
  auto const index_itr = index_names_.find(table_name);
  if (index_itr != index_names_.end()) {
    // Iterate through and drop each index associated with the table using structured bindings
    for (const auto &[index_name, index_id] : index_itr->second) {
      DropIndex(table_name, index_name);
    }
    index_names_.erase(index_itr); // Remove the index entry after deleting indexes
  }

  // Get the table ID from table_names_ and remove the table
  table_id_t const table_id = table_itr->second;
  table_names_.erase(table_itr);
  tables_.erase(table_id);

  // Fetch the catalog metadata page from the buffer pool manager
  auto const catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (!catalog_meta_page) {
    return DB_FAILED; // Return an appropriate error if fetching fails
  }

  // Delete the table's metadata page and update catalog_meta_
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[table_id]);
  catalog_meta_->table_meta_pages_.erase(table_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());

  // Unpin the catalog metadata page, marking it as dirty
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS; // Return DB_SUCCESS if all operations are successful
}

/**
 * TODO: Done by cww
 * Deletes a specified index for a given table from the database catalog. This function manages the removal
 * of the index's entries from the internal catalog structures and ensures the relevant metadata pages are deleted
 * from storage.
 *
 * @param table_name The name of the table to which the index belongs.
 * @param index_name The name of the index to be deleted.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_NOT_EXIST if the specified table does not exist in the catalog.
 *    - DB_INDEX_NOT_FOUND if the specified index does not exist for the table.
 *    - DB_FAILED if there are issues during the operation, such as failing to fetch or delete pages.
 *    - DB_SUCCESS if the index and all its associated structures are successfully deleted.
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  // Check if the table exists in the index map
  auto const table_it = index_names_.find(table_name);
  if (table_it == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;  // No such table
  }

  // Check if the index exists for the table
  auto& index_map = table_it->second;
  auto const index_it = index_map.find(index_name);
  if (index_it == index_map.end()) {
    return DB_INDEX_NOT_FOUND;  // No such index
  }

  // Retrieve the index ID, erase the index from the map, and remove index metadata
  index_id_t const index_id = index_it->second;
  index_map.erase(index_it);  // Remove index entry from the table's index map
  indexes_.erase(index_id);   // Remove index from global index list

  // Update catalog metadata
  auto const catalog_meta_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (!catalog_meta_page) {
    return DB_FAILED;  // Failed to fetch the catalog meta page
  }

  // Update and serialize the catalog meta information
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[index_id]);
  catalog_meta_->index_meta_pages_.erase(index_id);
  catalog_meta_->SerializeTo(catalog_meta_page->GetData());

  // Unpin the catalog meta page, marking it as dirty
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Flushes the catalog metadata page to disk. This function ensures that the in-memory catalog metadata
 * is written to persistent storage, maintaining consistency and durability of the catalog data.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_FAILED if the buffer pool manager fails to flush the catalog metadata page to disk.
 *    - DB_SUCCESS if the catalog metadata page is successfully flushed to disk.
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  // Assume buffer_pool_manager_->FlushPage returns a boolean indicating success or failure
  bool const isFlushed = buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);

  // Check the result of the FlushPage operation
  if (!isFlushed) {
    return DB_FAILED;  // Return an error if flushing the page failed
  }

  return DB_SUCCESS;  // Return success if the page was successfully flushed
}

/**
 * TODO: Done by cww
 * Loads a table into the database catalog from a specified page in storage. This function fetches the page, deserializes
 * the table metadata, and creates the necessary structures in memory to manage the table data.
 *
 * @param table_id The unique identifier for the table to load.
 * @param page_id The page identifier where the table's metadata is stored.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_FAILED if the table already exists, if there is a failure in fetching the page, deserializing metadata, or creating table structures.
 *    - DB_SUCCESS if the table is successfully loaded into the catalog.
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
    // Check if the table already exists in the system
    if (tables_.count(table_id) > 0) {
        LOG(WARNING) << "Attempt to load an already existing table with ID: " << table_id;
        return DB_FAILED;
    }

    // Fetch the page containing the table metadata from the buffer pool
    auto const metadata_page = buffer_pool_manager_->FetchPage(page_id);
    if (!metadata_page) {
        LOG(ERROR) << "Failed to fetch metadata page for table ID: " << table_id;
        return DB_FAILED;
    }

    // Use RAII to ensure the page is unpinned
    auto metadata_page_guard = std::unique_ptr<Page, std::function<void(Page*)>>(metadata_page, [this](Page* page) {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    });

    // Deserialize table metadata from the fetched page
    TableMetadata* table_metadata = nullptr;
    if (!TableMetadata::DeserializeFrom(metadata_page->GetData(), table_metadata)) {
        LOG(ERROR) << "Deserialization of table metadata failed for table ID: " << table_id;
        return DB_FAILED;
    }

    // Check if the table metadata has been correctly deserialized
    if (!table_metadata) {
        LOG(ERROR) << "Table metadata is null for table ID: " << table_id;
        return DB_FAILED;
    }

    // Create the table heap
    auto const table_heap = TableHeap::Create(buffer_pool_manager_, table_metadata->GetFirstPageId(), table_metadata->GetSchema(), log_manager_, lock_manager_);
    if (!table_heap) {
        LOG(ERROR) << "Failed to create TableHeap for table ID: " << table_id;
        return DB_FAILED;
    }

    // Create and initialize TableInfo using the Create method
    auto table_info = TableInfo::Create();
    if (!table_info) {
        LOG(ERROR) << "Failed to create TableInfo for table ID: " << table_id;
        return DB_FAILED;
    }
    table_info->Init(table_metadata, table_heap);

    // Register the new table
    table_names_.emplace(table_metadata->GetTableName(), table_id);
    tables_.emplace(table_id, table_info);

    return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Loads an index into the database catalog from a specified page in storage. This function fetches the page, deserializes
 * the index metadata, and creates the necessary structures in memory to manage the index data.
 *
 * @param index_id The unique identifier for the index to load.
 * @param page_id The page identifier where the index's metadata is stored.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_FAILED if the index already exists, if there is a failure in fetching the page, deserializing metadata, or creating index structures.
 *    - DB_SUCCESS if the index is successfully loaded into the catalog.
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
    // Check if the index already exists in the system
    if (indexes_.count(index_id) > 0) {
        LOG(WARNING) << "Index with ID: " << index_id << " already exists.";
        return DB_FAILED;
    }

    // Fetch the page containing the index metadata
    auto const index_metadata_page = buffer_pool_manager_->FetchPage(page_id);
    if (!index_metadata_page) {
        LOG(ERROR) << "Failed to fetch page with ID: " << page_id << " for index ID: " << index_id;
        return DB_FAILED;
    }

    // Use RAII to ensure the page is unpinned
    auto page_guard = std::unique_ptr<Page, std::function<void(Page*)>>(index_metadata_page, [this](Page* page) {
        buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    });

    // Deserialize index metadata from the fetched page
    IndexMetadata* index_metadata = nullptr;
    if (!IndexMetadata::DeserializeFrom(index_metadata_page->GetData(), index_metadata)) {
        LOG(ERROR) << "Deserialization failed for index metadata with index ID: " << index_id;
        return DB_FAILED;
    }

    // Validate and find the associated table using the table ID from the index metadata
    auto const table_iterator = tables_.find(index_metadata->GetTableId());
    if (table_iterator == tables_.end()) {
        LOG(ERROR) << "Table with ID: " << index_metadata->GetTableId() << " not found for index ID: " << index_id;
        return DB_FAILED;
    }

    // Create and initialize IndexInfo using the Create method
    auto const index_info = IndexInfo::Create();
    if (!index_info) {
        LOG(ERROR) << "Failed to create IndexInfo for index ID: " << index_id;
        return DB_FAILED;
    }
    index_info->Init(index_metadata, table_iterator->second, buffer_pool_manager_);

    // Register the new index name and ID in the index name map
    auto& table_index_map = index_names_[table_iterator->second->GetTableName()];
    table_index_map[index_metadata->GetIndexName()] = index_id;

    // Register the new index in the global index map
    indexes_[index_id] = index_info;

    return DB_SUCCESS;
}

/**
 * TODO: Done by cww
 * Retrieves the TableInfo object for a given table ID from the database catalog. This function searches
 * the internal catalog structures to find and return the TableInfo associated with the specified table ID.
 *
 * @param table_id The unique identifier for the table to retrieve.
 * @param table_info Reference to a pointer that will be set to point to the TableInfo object if the table is found.
 *
 * @return Returns a dberr_t enum value:
 *    - DB_TABLE_NOT_EXIST if no table with the given table ID exists in the catalog.
 *    - DB_SUCCESS if the table and its corresponding TableInfo are successfully retrieved.
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  // Search for the table with the given table_id in the tables_ container
  auto const table_iter = tables_.find(table_id);

  // If the table is not found, return DB_TABLE_NOT_EXIST error
  if (table_iter == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  // If the table is found, set the output parameter table_info to the found table info
  table_info = table_iter->second;

  // Return DB_SUCCESS to indicate the operation was successful
  return DB_SUCCESS;
}

