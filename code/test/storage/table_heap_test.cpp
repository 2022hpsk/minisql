#include "storage/table_heap.h"
#include "storage/table_iterator.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, TableHeapSampleTest)
{
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++)
  {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    // std::cout << row.GetRowId().GetPageId() << std::endl;
    if (row_values.find(row.GetRowId().Get()) != row_values.end())
    {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    }
    else
    {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values)
  {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++)
    {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
  for (TableIterator i = table_heap->Begin(nullptr); i != table_heap->End(); i++)
  {

    //std::cout << i->GetRowId().GetPageId() << " " << i->GetRowId().GetSlotNum() << std::endl;
  }
}

TEST(TableHeapTest, myTableHeapSampleTest)
{
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++)
  {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    // std::cout << "insert row" << row.GetRowId().GetPageId() << " " << row.GetRowId().GetSlotNum() << std::endl;
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    // std::cout << "After inserting row " << row.GetRowId().GetPageId() << " " << row.GetRowId().GetSlotNum() << std::endl;

    // add test for update
    Fields *myfields = new Fields{Field(TypeId::kTypeInt, 188),
                                  Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                                  Field(TypeId::kTypeFloat, 19.99f)};
    Row row2(*myfields);
    // std::cout << "jjj" << row.GetRowId().GetPageId() << std::endl;
    ASSERT_TRUE(table_heap->UpdateTuple(row2, row.GetRowId(), nullptr));
    // std::cout << "hhh" << row.GetRowId().GetPageId() << std::endl;
    // std::cout << row2.GetRowId().GetPageId() << std::endl;
    ASSERT_TRUE(table_heap->MarkDelete(row2.GetRowId(), nullptr));
    table_heap->ApplyDelete(row2.GetRowId(), nullptr);
    // std::cout << "bu insert" << std::endl;
    ASSERT_TRUE(table_heap->InsertTuple(row2, nullptr));
    // std::cout << "insert again after deletion" << row2.GetRowId().GetPageId() << " " << row2.GetRowId().GetSlotNum() << std::endl;

    if (row_values.find(row2.GetRowId().Get()) != row_values.end())
    {
      std::cout << row2.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    }
    else
    {
      row_values.emplace(row2.GetRowId().Get(), myfields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values)
  {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    // std::cout << row.GetRowId().GetPageId() << std::endl;
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++)
    {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}

TEST(TableHeapTest, TableHeapIteratorTest) {
  // init testing instance
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;//map； key是
  std::vector<int64_t> rowid_vector;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {//插入所有的测试数据
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);//产生随机的测试数据
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {//如果map里面已经有这个row了，说明出了问题
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      rowid_vector.push_back(row.GetRowId().Get());//顺序插入rowid
      row_values.emplace(row.GetRowId().Get(), fields);//插入键值对，分别是rowid和对应的row的属性
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);


  for (TableIterator itr = table_heap->Begin(nullptr);itr != table_heap->End();itr++){//这里!=报warning我不理解
    size--;
    //按顺序用迭代器取出row，这里的顺序不是插入的顺序，因为是first fit
    //然后和迭代器中的比较
    Row row = *itr;
    Fields* field_in_map = row_values[row.GetRowId().Get()];
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(field_in_map->at(j)))<<"error size:"<<size;;
    }
    delete field_in_map;
    row_values.erase(row.GetRowId().Get());
  }
  delete disk_mgr_;
  //delete bpm_;
  ASSERT_EQ(size, 0);
}