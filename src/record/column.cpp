#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
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
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Implement by ShenCongyu
 */
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t serializeSize = 0;

  MACH_WRITE_UINT32(buf + serializeSize, COLUMN_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;

  MACH_WRITE_UINT32(buf + serializeSize, (uint32_t)name_.length());
  MACH_WRITE_STRING(buf + serializeSize + 4, name_);
  serializeSize += MACH_STR_SERIALIZED_SIZE(name_);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;
  //LOG(INFO) << "se " << name_ << std::endl;

  //LOG(INFO) << "se " << type_ << std::endl;
  MACH_WRITE_TO(TypeId, buf + serializeSize, type_);
  serializeSize += sizeof(TypeId);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;

  MACH_WRITE_UINT32(buf + serializeSize, len_);
  serializeSize += sizeof(uint32_t);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;

  MACH_WRITE_UINT32(buf + serializeSize, table_ind_);
  serializeSize += sizeof(uint32_t);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;

  MACH_WRITE_TO(bool, buf + serializeSize, nullable_);
  serializeSize += sizeof(bool);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;

  MACH_WRITE_TO(bool, buf + serializeSize, unique_);
  serializeSize += sizeof(bool);
  //LOG(INFO) << "size1 " << serializeSize << std::endl;
  return serializeSize;
}

/**
 * TODO: Implement by ShenCongyu
 */
uint32_t Column::GetSerializedSize() const {
  return sizeof(uint32_t) * 3 +
         MACH_STR_SERIALIZED_SIZE(name_) +
         sizeof(TypeId) +
         sizeof(bool) * 2;
}

/**
 * TODO: Implement by ShenCongyu
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t serializeSize = 0;

  uint32_t _COLUMN_MAGIC_NUM = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  if(_COLUMN_MAGIC_NUM != COLUMN_MAGIC_NUM)
    LOG(ERROR) << "column deserialize error" << std::endl;

  uint32_t _name_length = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  char _name_char_[_name_length + 1];
  memcpy(_name_char_, buf + serializeSize, _name_length);
  _name_char_[_name_length] = '\0';
  std::string _name_(_name_char_, _name_length);
  serializeSize += sizeof(char) * _name_length;

  TypeId _type_ = MACH_READ_FROM(TypeId, buf + serializeSize);
  serializeSize += sizeof(TypeId);

  uint32_t _len_ = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  uint32_t _table_ind_ = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  //LOG(INFO) << "size2 " << serializeSize << std::endl;

  bool _nullable_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);
  //LOG(INFO) << "size2 " << serializeSize << std::endl;

  bool _unique_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);
  //LOG(INFO) << "size2 " << serializeSize << std::endl;
  if(_type_ != TypeId::kTypeChar) {
    column = new Column(_name_, _type_, _table_ind_, _nullable_, _unique_);
  } else {
    column = new Column(_name_, _type_, _len_, _table_ind_, _nullable_, _unique_);
  }
  return serializeSize;
}
