#include "record/row.h"

/**
 * TODO: Implement by ShenCongyu
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t serializeSize = 0;

  uint32_t fields_num = GetFieldCount();
  MACH_WRITE_UINT32(buf + serializeSize, fields_num);
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + serializeSize, rid_.GetPageId());
  serializeSize += sizeof(uint32_t);

  MACH_WRITE_UINT32(buf + serializeSize, rid_.GetSlotNum());
  serializeSize += sizeof(uint32_t);

  if(fields_num == 0)
    return serializeSize;

  uint32_t cnt = 0;
  uint32_t bitmap_size = (fields_num - 1) / 8 + 1;
  char *bitmap = new char[bitmap_size];
  memset(bitmap, 0, sizeof(bitmap));
  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    if((*ite)->IsNull()) {
      bitmap[cnt / 8] |= 1 << (cnt % 8);
    }
    cnt++;
  }
  MACH_WRITE_STRING(buf + serializeSize, std::string(bitmap));
  serializeSize += sizeof(char) * bitmap_size;

  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    serializeSize += (*ite)->SerializeTo(buf + serializeSize);
  }

  delete []bitmap;
  return serializeSize;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  fields_.resize(0);
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  
  uint32_t serializeSize = 0;

  uint32_t _fields_num = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  uint32_t _page_id = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);

  uint32_t _slot_num = MACH_READ_UINT32(buf + serializeSize);
  serializeSize += sizeof(uint32_t);
  rid_.Set(_page_id, _slot_num);

  if(_fields_num == 0)
    return serializeSize;

  uint32_t _bitmap_size = (_fields_num - 1) / 8 + 1;
  char *_bitmap = new char[_bitmap_size];
  memcpy(_bitmap, buf + serializeSize, _bitmap_size*sizeof(char));
  serializeSize += sizeof(char) * _bitmap_size;

  TypeId _type;
  Type type_manager();
  for(uint32_t k = 0; k < _fields_num; k++) {
    uint32_t i = k / 8;
    uint32_t j = k % 8;
    Field *_field_ = nullptr;
    _type = schema->GetColumn(k)->GetType();
    serializeSize += Type::GetInstance(_type)->DeserializeFrom(buf + serializeSize, &_field_, ((uint32_t)_bitmap[i] & (uint32_t)(1 << j)) == 1);
    fields_.push_back(_field_);
  }

  delete []_bitmap;
  return serializeSize;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  
  uint32_t serializeSize = 0;
  serializeSize += sizeof(uint32_t) * 3;

  if(GetFieldCount() == 0)
    return serializeSize;

  serializeSize += sizeof(char) * ((GetFieldCount() - 1) / 8 + 1);

  for(auto ite = fields_.begin(); ite != fields_.end(); ite++) {
    serializeSize += (*ite)->GetSerializedSize();
  }

  return serializeSize;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
