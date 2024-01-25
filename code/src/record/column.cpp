#include "record/column.h"

#include "glog/logging.h"
#include <iostream>

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique)
{
  // ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type)
  {
  case TypeId::kTypeInt:
    len_ = sizeof(int32_t);
    break;
  case TypeId::kTypeFloat:
    len_ = sizeof(float_t);
    break;
  default:
    ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique)
{
  // ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
 * TODO: Student Implement
 */
uint32_t Column::SerializeTo(char *buf) const
{
  // replace with your code here
  char *pos = buf;
  // name is a varchar, so we have to use a length
  uint32_t name_len = (this->name_).size();
  memcpy(pos, &name_len, sizeof(uint32_t));
  pos += sizeof(uint32_t);
  (this->name_).copy(pos, name_len, 0);
  pos += name_len;
  memcpy(pos, &(this->type_), sizeof(TypeId));
  pos += sizeof(TypeId);
  memcpy(pos, &(this->len_), sizeof(uint32_t)); // len_, for varrchar
  // std::cout << "len:" << *reinterpret_cast<uint32_t *>(pos) << std::endl;
  pos += sizeof(uint32_t);
  memcpy(pos, &(this->table_ind_), sizeof(uint32_t)); // table_ind_, index
  pos += sizeof(uint32_t);
  memcpy(pos, &(this->nullable_), sizeof(bool));
  pos += sizeof(bool);
  memcpy(pos, &(this->unique_), sizeof(bool));
  pos += sizeof(bool);
  return pos - buf;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const
{
  // replace with your code here
  return (uint32_t)(sizeof(uint32_t) * 3 + sizeof(TypeId) + 2 * sizeof(bool) + (this->name_).size());
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column)
{
  // replace with your code here
  char *pos = buf;
  uint32_t name_len = MACH_READ_FROM(uint32_t, pos);
  pos += sizeof(uint32_t);
  std::string name;
  name.append(pos, name_len);
  pos += name_len;
  TypeId type = MACH_READ_FROM(TypeId, pos);
  pos += sizeof(TypeId);
  uint32_t len = MACH_READ_FROM(uint32_t, pos);
  // std::cout << "len:" << len << std::endl;
  pos += sizeof(uint32_t);
  uint32_t table_id = MACH_READ_FROM(uint32_t, pos);
  pos += sizeof(uint32_t);
  bool null_able = MACH_READ_FROM(bool, pos);
  pos += sizeof(bool);
  bool unique = MACH_READ_FROM(bool, pos);
  pos += sizeof(bool);
  column = new Column(name, type, len, table_id, null_able, unique);
  return pos - buf;
}
