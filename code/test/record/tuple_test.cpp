#include <cstring>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "page/table_page.h"
#include "record/field.h"
#include "record/row.h"
#include "record/schema.h"

char *chars[] = {const_cast<char *>(""), const_cast<char *>("hello"), const_cast<char *>("world!"),
                 const_cast<char *>("\0")};

Field int_fields[] = {
    Field(TypeId::kTypeInt, 188),
    Field(TypeId::kTypeInt, -65537),
    Field(TypeId::kTypeInt, 33389),
    Field(TypeId::kTypeInt, 0),
    Field(TypeId::kTypeInt, 999),
};
Field float_fields[] = {
    Field(TypeId::kTypeFloat, -2.33f),
    Field(TypeId::kTypeFloat, 19.99f),
    Field(TypeId::kTypeFloat, 999999.9995f),
    Field(TypeId::kTypeFloat, -77.7f),
};
Field char_fields[] = {Field(TypeId::kTypeChar, chars[0], strlen(chars[0]), false),
                       Field(TypeId::kTypeChar, chars[1], strlen(chars[1]), false),
                       Field(TypeId::kTypeChar, chars[2], strlen(chars[2]), false),
                       Field(TypeId::kTypeChar, chars[3], 1, false)};
Field null_fields[] = {Field(TypeId::kTypeInt), Field(TypeId::kTypeFloat), Field(TypeId::kTypeChar)};

TEST(TupleTest, FieldSerializeDeserializeTest)
{
  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 4; i++)
  {
    p += int_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 3; i++)
  {
    p += float_fields[i].SerializeTo(p);
  }
  for (int i = 0; i < 4; i++)
  {
    p += char_fields[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Field *df = nullptr;
  for (int i = 0; i < 4; i++)
  {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeInt, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(int_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(int_fields[4]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(int_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(int_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++)
  {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeFloat, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(float_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(float_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[1]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(float_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(float_fields[2]));
    delete df;
    df = nullptr;
  }
  for (int i = 0; i < 3; i++)
  {
    ofs += Field::DeserializeFrom(buffer + ofs, TypeId::kTypeChar, &df, false);
    EXPECT_EQ(CmpBool::kTrue, df->CompareEquals(char_fields[i]));
    EXPECT_EQ(CmpBool::kFalse, df->CompareEquals(char_fields[3]));
    EXPECT_EQ(CmpBool::kNull, df->CompareEquals(null_fields[2]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareGreaterThanEquals(char_fields[0]));
    EXPECT_EQ(CmpBool::kTrue, df->CompareLessThanEquals(char_fields[2]));
    delete df;
    df = nullptr;
  }
}

TEST(TupleTest, RowTest)
{
  TablePage table_page;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  std::vector<Field> fields = {Field(TypeId::kTypeInt, 188),
                               Field(TypeId::kTypeChar, const_cast<char *>("minisql"), strlen("minisql"), false),
                               Field(TypeId::kTypeFloat, 19.99f)};
  auto schema = std::make_shared<Schema>(columns);
  Row row(fields);
  table_page.Init(0, INVALID_PAGE_ID, nullptr, nullptr);
  table_page.InsertTuple(row, schema.get(), nullptr, nullptr, nullptr);
  RowId first_tuple_rid;
  ASSERT_TRUE(table_page.GetFirstTupleRid(&first_tuple_rid));
  ASSERT_EQ(row.GetRowId(), first_tuple_rid);
  Row row2(row.GetRowId());
  ASSERT_TRUE(table_page.GetTuple(&row2, schema.get(), nullptr, nullptr));
  std::vector<Field *> &row2_fields = row2.GetFields();
  ASSERT_EQ(3, row2_fields.size());
  for (size_t i = 0; i < row2_fields.size(); i++)
  {
    ASSERT_EQ(CmpBool::kTrue, row2_fields[i]->CompareEquals(fields[i]));
  }
  ASSERT_TRUE(table_page.MarkDelete(row.GetRowId(), nullptr, nullptr, nullptr));
  table_page.ApplyDelete(row.GetRowId(), nullptr, nullptr);
}

// my test for column Serialize&Deserialize!!
TEST(TupleTest, myColumnSerializeDeserializeTest)
{
  Column columns[] = {Column("column0", TypeId::kTypeInt, 0, false, false),
                      Column("column1", TypeId::kTypeFloat, 1, false, false),
                      Column("column2", TypeId::kTypeChar, 10, 2, false, false)};
  Column testcolumn("column3", TypeId::kTypeChar, 64, 3, true, true);

  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  for (int i = 0; i < 3; i++)
  {
    p += columns[i].SerializeTo(p);
  }
  // Deserialize phase
  uint32_t ofs = 0;
  Column *df = nullptr;
  for (int i = 0; i < 1; i++)
  {
    ofs += Column::DeserializeFrom(buffer + ofs, df);
    EXPECT_EQ(columns[i].GetName(), df->GetName());
    EXPECT_EQ(columns[i].GetLength(), df->GetLength());
    EXPECT_EQ(columns[i].GetTableInd(), df->GetTableInd());
    EXPECT_EQ(columns[i].GetType(), df->GetType());
    EXPECT_EQ(columns[i].IsNullable(), df->IsNullable());
    EXPECT_EQ(columns[i].IsUnique(), df->IsUnique());

    EXPECT_NE(testcolumn.GetName(), df->GetName());
    EXPECT_NE(testcolumn.GetLength(), df->GetLength());
    EXPECT_NE(testcolumn.GetTableInd(), df->GetTableInd());
    // EXPECT_NE(testcolumn.GetType(), df->GetType());
    EXPECT_NE(testcolumn.IsNullable(), df->IsNullable());
    EXPECT_NE(testcolumn.IsUnique(), df->IsUnique());
    delete df;
    df = nullptr;
  }
}

// my test for Schema Serialize&Deserialize!!
TEST(TupleTest, mySchemaSerializeDeserializeTest)
{
  std::vector<Column *> pcolumns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                    new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                    new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(pcolumns);
  std::vector<Column *> pcolumns2 = {new Column("id2", TypeId::kTypeInt, 0, false, false),
                                     new Column("name2", TypeId::kTypeChar, 64, 1, true, false),
                                     new Column("account2", TypeId::kTypeFloat, 2, true, false)};
  auto schema2 = std::make_shared<Schema>(pcolumns2);

  char buffer[PAGE_SIZE];
  memset(buffer, 0, sizeof(buffer));
  // Serialize phase
  char *p = buffer;
  p += schema->SerializeTo(p);
  char *p2 = p;
  p += schema2->SerializeTo(p);

  // Deserialize phase
  uint32_t ofs = 0;
  Schema *df = nullptr;

  // For schema
  ofs += Schema::DeserializeFrom(buffer + ofs, df);
  EXPECT_EQ(schema->GetColumn(0)->GetName(), df->GetColumn(0)->GetName());
  EXPECT_EQ(schema->GetColumn(1)->GetName(), df->GetColumn(1)->GetName());
  EXPECT_EQ(schema->GetColumn(2)->GetName(), df->GetColumn(2)->GetName());

  EXPECT_NE(schema2->GetColumn(0)->GetName(), df->GetColumn(0)->GetName());

  delete df;
  df = nullptr;

  // For schema2
  ofs += Schema::DeserializeFrom(p2, df);
  // std::cout << df << std::endl;
  // df->GetColumn(0)->GetName();
  // std::cout << "hh" << std::endl;
  EXPECT_EQ(schema2->GetColumn(0)->GetName(), df->GetColumn(0)->GetName());
  EXPECT_EQ(schema2->GetColumn(1)->GetName(), df->GetColumn(1)->GetName());
  EXPECT_EQ(schema2->GetColumn(2)->GetName(), df->GetColumn(2)->GetName());

  EXPECT_NE(schema->GetColumn(0)->GetName(), df->GetColumn(0)->GetName());

  delete df;
  df = nullptr;
}
