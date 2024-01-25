#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const
{
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_)
  {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_)
  {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf)
{
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
  for (uint32_t i = 0; i < table_nums; i++)
  {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++)
  {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const
{
  // ASSERT(false, "Not Implemented yet");
  // in case the page is invalid
  uint32_t len = table_meta_pages_.size();
  for (auto it : table_meta_pages_)
  {
    if (it.second == INVALID_PAGE_ID)
      len--;
  }
  len += index_meta_pages_.size();
  for (auto it : index_meta_pages_)
  {
    if (it.second == INVALID_PAGE_ID)
      len--;
  }
  len = len * (sizeof(uint32_t) + sizeof(int)) + 3 * sizeof(uint32_t);
  return len;
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager)
{
  //    ASSERT(false, "Not Implemented yet");
  if (init)
  {
    this->catalog_meta_ = CatalogMeta::NewInstance();
    this->next_table_id_ = 0;
    this->next_index_id_ = 0;
  }
  else
  {
    // else, we can just load the meta page
    Page *catalogMetaPage = buffer_pool_manager->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(catalogMetaPage->GetData());

    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();

    for (auto it = catalog_meta_->table_meta_pages_.begin(); it != catalog_meta_->table_meta_pages_.end(); it++)
    {
      if (it->second != INVALID_PAGE_ID)
      {
        LoadTable(it->first, it->second);
      }
    }

    for (auto it = catalog_meta_->index_meta_pages_.begin(); it != catalog_meta_->index_meta_pages_.end(); it++)
    {
      if (it->second != INVALID_PAGE_ID)
      {
        LoadIndex(it->first, it->second);
      }
    }

    buffer_pool_manager->UnpinPage(CATALOG_META_PAGE_ID, false);
  }
}

CatalogManager::~CatalogManager()
{
  /** After you finish the code for the CatalogManager section,
    you can uncomment the commented code. Otherwise it will affect b+tree test**/
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_)
  {
    delete iter.second;
  }
  for (auto iter : indexes_)
  {
    delete iter.second;
  }
  /**/
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema,
                                    Transaction *txn, TableInfo *&table_info)
{
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.count(table_name) != 0)
    return DB_TABLE_ALREADY_EXIST;

  table_id_t table_id = this->next_table_id_;
  this->next_table_id_++;
  page_id_t new_table_page_id;
  Page *new_table_page = buffer_pool_manager_->NewPage(new_table_page_id);
  catalog_meta_->table_meta_pages_[table_id] = new_table_page_id;
  catalog_meta_->table_meta_pages_[next_table_id_] = INVALID_PAGE_ID;
  TableMetadata *table_meta_data = TableMetadata::Create(table_id, table_name, new_table_page_id, schema->DeepCopySchema(schema));
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, schema->DeepCopySchema(schema), txn, log_manager_, lock_manager_);

  // 简单来说，就是通过几种方法建立table
  table_meta_data->SerializeTo(new_table_page->GetData());
  table_info=table_info->Create();
  table_info->Init(table_meta_data, table_heap);
  buffer_pool_manager_->UnpinPage(new_table_page_id, true);

  table_names_.emplace(table_name, new_table_page_id);
  tables_.emplace(new_table_page_id, table_info);

  if (table_meta_data != nullptr && table_heap != nullptr)
    return DB_SUCCESS;
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info)
{
  // ASSERT(false, "Not Implemented yet");
  if ((this->table_names_).count(table_name) == 0)
    return DB_TABLE_NOT_EXIST;
  page_id_t table_page_id = this->table_names_[table_name];
  if ((this->tables_).count(table_page_id) == 0)
    return DB_FAILED;
  table_info = (this->tables_)[table_page_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const
{
  // ASSERT(false, "Not Implemented yet");
  auto iter = tables_.begin();
  while (iter != tables_.end())
  {
    tables.push_back(iter->second);
    iter++;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Transaction *txn,
                                    IndexInfo *&index_info, const string &index_type)
{
  // ASSERT(false, "Not Implemented yet");
  // index_keys里存的是index是以哪几个column来建立索引的
  TableInfo *table_info;
  if (GetTable(table_name, table_info) != DB_SUCCESS)
    return DB_TABLE_NOT_EXIST;

  if (index_names_[table_name].count(index_name) != 0)
  {
    return DB_INDEX_ALREADY_EXIST;
  }

  // whether the columns of the index exists
  Schema *schema = table_info->GetSchema();
  vector<uint32_t> col_indexes;
  for (const auto &s : index_keys)
  {
    uint32_t column_index;
    if (schema->GetColumnIndex(s, column_index) != DB_SUCCESS)
      return DB_COLUMN_NAME_NOT_EXIST;
    col_indexes.push_back(column_index);
  }

  index_info = IndexInfo::Create();
  index_id_t index_id = this->next_index_id_;
  this->next_index_id_++;
  // create Metadata
  IndexMetadata *index_meta_data = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), col_indexes);
  index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
  page_id_t page_id;
  catalog_meta_->index_meta_pages_[index_id + 1] = INVALID_PAGE_ID;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr)
    return DB_FAILED;

  index_meta_data->SerializeTo(page->GetData());
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  buffer_pool_manager_->UnpinPage(page_id, false);

  indexes_.emplace(index_id, index_info);
  index_names_[table_name][index_name] = index_id;
  if (index_meta_data != nullptr)
    return DB_SUCCESS;
  return DB_FAILED;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const
{
  //find table
  if(index_names_.find(table_name)==index_names_.end())
    return DB_TABLE_NOT_EXIST;

  //find index
  auto indname_id=index_names_.find(table_name)->second;
  if(indname_id.find(index_name)==indname_id.end())
    return DB_INDEX_NOT_FOUND;

  //have found and return index_info
  index_id_t index_id=indname_id[index_name];
  index_info=indexes_.find(index_id)->second;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const
{
  // ASSERT(false, "Not Implemented yet");
  if (index_names_.size() == 0)
    return DB_SUCCESS;
  for (auto iter = index_names_.at(table_name).begin(); iter != index_names_.at(table_name).end(); iter++)
  {
    indexes.push_back(indexes_.at(iter->second));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name)
{
  // ASSERT(false, "Not Implemented yet");
  if (table_names_.count(table_name) == 0)
  {
    return DB_TABLE_NOT_EXIST;
  }
  table_id_t tableId = table_names_[table_name];
  tables_[tableId]->GetTableHeap()->DeleteTable();
  buffer_pool_manager_->DeletePage(catalog_meta_->table_meta_pages_[tableId]);

  catalog_meta_->table_meta_pages_.erase(tableId);
  table_names_.erase(table_name);
  tables_.erase(tableId);
  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name)
{
  if (table_names_.find(table_name) == table_names_.end()) return DB_TABLE_NOT_EXIST;
  // ASSERT(false, "Not Implemented yet");
  if (index_names_[table_name].count(index_name) == 0)
  {
    return DB_INDEX_NOT_FOUND;
  }

  index_id_t indexId = index_names_[table_name][index_name];
  buffer_pool_manager_->DeletePage(catalog_meta_->index_meta_pages_[indexId]);

  catalog_meta_->index_meta_pages_.erase(indexId);
  indexes_.erase(indexId);
  index_names_[table_name].erase(index_name);

  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const
{
  // ASSERT(false, "Not Implemented yet");
  // flush catalog meta page
  Page *page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);

  // flush all the table pages
  std::vector<TableInfo *> tables;
  this->GetTables(tables);
  for (auto table : tables)
  {
    Page *page = this->buffer_pool_manager_->FetchPage(table->GetRootPageId());
    table->table_meta_->SerializeTo(page->GetData());
    this->buffer_pool_manager_->UnpinPage(table->GetRootPageId(), true);
  }
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id)
{
  // ASSERT(false, "Not Implemented yet");
  Page *page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr)
  {
    return DB_FAILED;
  }
  TableInfo *tableInfo = TableInfo::Create();
  TableMetadata *tableMetaData = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), tableMetaData);
  if (tableMetaData == nullptr)
  {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_FAILED;
  }
  TableHeap *tableHeap = TableHeap::Create(buffer_pool_manager_, tableMetaData->GetFirstPageId(),
                                           tableMetaData->GetSchema(), log_manager_, lock_manager_);
  tableInfo->Init(tableMetaData, tableHeap);

  tables_.emplace(table_id, tableInfo);
  table_names_.emplace(tableMetaData->GetTableName(), table_id);

  buffer_pool_manager_->UnpinPage(page_id, false);

  return DB_SUCCESS;

}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id)
{
  try{
    string table_name="";
    string index_name="";
    page_id_t meta_page_id=0;
    std::vector<std::uint32_t> key_map{};

    meta_page_id=page_id;
    Page* meta_page=buffer_pool_manager_->FetchPage(meta_page_id);
    //deserial index meta
    IndexMetadata* index_meta_= nullptr;
    IndexMetadata::DeserializeFrom(meta_page->GetData(),index_meta_);
    //get index name from index meta
    index_name=index_meta_->GetIndexName();
    //get table id from index meta
    table_id_t table_id=index_meta_->GetTableId();

    //table info
    TableInfo* table_info_=tables_[table_id];
    table_name=table_info_->GetTableName();

    //index info
    IndexInfo* index_info=index_info->Create();
    index_info->Init(index_meta_,table_info_,buffer_pool_manager_);
    //table meta
    indexes_[index_id]=index_info;
    index_names_[table_name][index_name]=index_id;
    return DB_SUCCESS;
  }catch(exception e){
    return DB_FAILED;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info)
{
  // ASSERT(false, "Not Implemented yet");
  if ((this->tables_).count(table_id) == 0)
    return DB_FAILED;
  table_info = (this->tables_)[table_id];
  return DB_SUCCESS;
}
