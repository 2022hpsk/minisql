#include "record/row.h"

/**
 * 从row开始写，理一理
 * Field中是有val和type_id的信息的，调用field的serializeto可以成功把val读到buf里，并返回type的大小
 * 每个type调用自己的serilaizeto可以把field的val类型读到buf里，并返回大小
 * kTypeInvalid = 0, kTypeInt 1, kTypeFloat 2, kTypeChar 3
 */

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const
{
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  // First to change all the pointers in the fields to the corresponding type
  char *buf_for_each = buf;
  // uint32_t head_size = ((this->fields_).size() / 8 + 1) + sizeof(int);
  // *reinterpret_cast<int *>(buf_for_each) = head_size;
  // buf_for_each += sizeof(int);
  // char *null_bit_map = buf_for_each;
  for (int i = 0; i < (this->fields_).size(); i++)
  {
    buf_for_each += (this->fields_)[i]->SerializeTo(buf_for_each);
    // if ((this->fields_)[i]->IsNull())
    // {
    //   // 0 is not null and 1 is null
    //   null_bit_map[i / 8] = null_bit_map[i / 8] | ((unsigned char)128 >> (i % 8));
    // }
  }
  // Then calculate the size of Row
  return buf_for_each - buf;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema)
{
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t len = schema->GetColumnCount();
  // std::cout << len << "hudsaf" << std::endl;
  // char *head = buf;
  // uint32_t head_size = *reinterpret_cast<int *>(head);
  // char *buf_for_each = buf + head_size;
  // char *null_bit_map = buf + sizeof(int);
  char *buf_for_each = buf;
  for (int i = 0; i < (int)len; i++)
  {
    // read each field from buf and store into a field
    Field *f;
    // 1 is null; 0 is not null
    // std::cout << "???" << schema->GetSerializedSize() << std::endl;
    buf_for_each += f->DeserializeFrom(buf_for_each, schema->GetColumn(i)->GetType(), &f, false);
    // if (null_bit_map[i / 8] & ((unsigned char)128 >> (i % 8)))
    // {
    //   f->SetIsNull(true);
    // }
    (this->fields_).push_back(f);
    // std::cout << "???" << std::endl;
  }
  return buf_for_each - buf;
}

uint32_t Row::GetSerializedSize(Schema *schema) const
{
  // std::cout << schema->GetColumnCount() << "-row schema " << fields_.size() << "-real schema" << std::endl;
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t size = 0;
  // uint32_t head_size = ((this->fields_).size() / 8 + 1) + sizeof(int);
  for (int i = 0; i < (int)schema->GetColumnCount(); i++)
  {
    size += (this->fields_)[i]->GetSerializedSize();
  }
  // return size + head_size;
  return size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row)
{
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns)
  {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}

