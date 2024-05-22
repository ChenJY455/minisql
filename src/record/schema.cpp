#include "record/schema.h"

/**
 * TODO: Implement by ShenCongyu
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t serializeSize = 0;

  MACH_WRITE_UINT32(buf + serializeSize, SCHEMA_MAGIC_NUM);
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_TO(bool, buf + serializeSize, is_manage_);
  serializeSize += sizeof(bool);

  MACH_WRITE_UINT32(buf + serializeSize, (uint32_t)columns_.size());
  serializeSize += sizeof(uint32_t);

  for(auto ite = columns_.begin(); ite != columns_.end(); ite++) {
    serializeSize += (*ite)->SerializeTo(buf + serializeSize);
  }
  return serializeSize;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t serializeSize = 0;
  serializeSize += sizeof(uint32_t) * 2 + sizeof(bool);

  for(auto ite = columns_.begin(); ite != columns_.end(); ite++) {
    serializeSize += (*ite)->GetSerializedSize();
  }

  return serializeSize;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t serializeSize = 0;

  uint32_t _SCHEMA_MAGIC_NUM = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  if(_SCHEMA_MAGIC_NUM != SCHEMA_MAGIC_NUM)
    LOG(ERROR) << "schema deserialize error" << std::endl;

  bool _is_manage_ = MACH_READ_FROM(bool, buf + serializeSize);
  serializeSize += sizeof(bool);

  uint32_t _columns_num = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  std::vector<Column *> _columns_;
  for(uint32_t i = 0; i < _columns_num; ++i) {
    Column * _column_ = nullptr;
    serializeSize += Column::DeserializeFrom(buf + serializeSize, _column_);
    _columns_.push_back(_column_);
  }

  schema = new Schema(_columns_, _is_manage_);
  return serializeSize;
}