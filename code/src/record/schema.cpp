#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const
{
  // replace with your code here
  char *buf_for_each = buf;
  for (int i = 0; i < int((this->columns_).size()); i++)
  {
    buf_for_each += (this->columns_)[i]->SerializeTo(buf_for_each);
  }
  return buf_for_each - buf;
}

uint32_t Schema::GetSerializedSize() const
{
  // replace with your code here
  uint32_t size = 0;
  for (int i = 0; i < int((this->columns_).size()); i++)
  {
    size += (this->columns_)[i]->GetSerializedSize();
  }
  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema)
{
  // replace with your code here
  char *buf_for_each = buf;
  std::vector<Column *> columns;
  Column *column;
  while (*buf_for_each != 0)
  {
    buf_for_each += Column::DeserializeFrom(buf_for_each, column);
    columns.push_back(column);
  }
  schema = new Schema(columns, true);
  // std::cout << schema->GetColumn(0)->GetName() << std::endl;
  return buf_for_each - buf;
}

