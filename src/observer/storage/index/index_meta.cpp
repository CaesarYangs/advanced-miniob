/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai.wyl on 2021/5/18.
//

#include "storage/index/index_meta.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/field/field_meta.h"
#include "storage/table/table_meta.h"
#include "json/json.h"

const static Json::StaticString FIELD_NAME("name");
const static Json::StaticString FIELD_FIELD_NAME("field_name");
const static Json::StaticString INDEX_NAME("index_name");
const static Json::StaticString INDEX_FIELD_NAMES("index_field_names");
const static Json::StaticString UNIQUE_OR_NOT("unique");

// 利用field vector初始化Indexmeta
RC IndexMeta::init(const char *name, std::vector<const FieldMeta *> fields)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  // 初始化IndexMeta
  name_ = name;
  field_.clear();
  for (const FieldMeta *field : fields) {
    field_.push_back(field->name());
  }
  return RC::SUCCESS;
}

// 两种不同的init方式，都可使用
RC IndexMeta::init(const char *name, std::vector<std::string> fields)
{
  if (common::is_blank(name)) {
    LOG_ERROR("Failed to init index, name is empty.");
    return RC::INVALID_ARGUMENT;
  }

  // 初始化IndexMeta
  name_  = name;
  field_ = fields;
  return RC::SUCCESS;
}

// 序列化索引文件
void IndexMeta::to_json(Json::Value &json_value) const
{
  json_value[INDEX_NAME]    = name_;
  json_value[UNIQUE_OR_NOT] = unique_;
  json_value[FIELD_FIELD_NAME] = get_field_names_str();
  // for (int i = 0; i < int(field_.size()); i++) {
  //   json_value[FIELD_FIELD_NAME][i] = field_.at(i);
  // }
}

// // 反序列化索引文件
// RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
// {
//   const Json::Value &name_value   = json_value[INDEX_NAME];
//   const Json::Value &unique_value = json_value[UNIQUE_OR_NOT];
//   // const Json::Value &fields_values = json_value[INDEX_FIELD_NAMES];
//   const Json::Value &field_value = json_value[FIELD_FIELD_NAME];
//   if (!name_value.isString()) {
//     LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
//     return RC::INTERNAL;
//   }

//   if (!unique_value.isBool()) {
//     LOG_ERROR("Unique is not a boolean. json value=%s", unique_value.toStyledString().c_str());
//     return RC::INVALID_ARGUMENT;
//   }

//   // 主要反序列化部分
//   std::vector<std::string> fields;
//   for (int i = 0; i < int(field_value.size()); i++) {
//     if (!field_value[i].isString()) {
//       LOG_ERROR("Field name of index [%s] is not a string. json value=%s",
//           name_value.asCString(),
//           field_value.toStyledString().c_str());
//       return RC::INVALID_ARGUMENT;
//     }

//     const FieldMeta *field = table.field(field_value[i].asCString());
//     if (nullptr == field) {
//       LOG_ERROR("Deserialize index [%s]: no such field: %s", name_value.asCString(), field_value[i].asCString());
//       return RC::SCHEMA_FIELD_MISSING;
//     }
//     fields.push_back(field->name());
//   }

//   return index.init(name_value.asCString(), fields);
// }

RC IndexMeta::from_json(const TableMeta &table, const Json::Value &json_value, IndexMeta &index)
{
  const Json::Value &name_value   = json_value[INDEX_NAME];
  const Json::Value &unique_value = json_value[UNIQUE_OR_NOT];
  const Json::Value &field_value  = json_value[FIELD_FIELD_NAME];

  if (!name_value.isString()) {
    LOG_ERROR("Index name is not a string. json value=%s", name_value.toStyledString().c_str());
    return RC::INTERNAL;
  }

  if (!unique_value.isBool()) {
    LOG_ERROR("Unique is not a boolean. json value=%s", unique_value.toStyledString().c_str());
    return RC::INVALID_ARGUMENT;
  }

  if (!field_value.isString()) {
    LOG_ERROR("Field names of index [%s] is not a string. json value=%s",
              name_value.asCString(), field_value.toStyledString().c_str());
    return RC::INVALID_ARGUMENT;
  }

  std::vector<std::string> fields;
  std::stringstream ss(field_value.asString());
  std::string field_name;
  while (std::getline(ss, field_name, ',')) {
    fields.push_back(field_name);
  }

  return index.init(name_value.asCString(), fields);
}

// 是否为唯一索引
const bool IndexMeta::is_unique() const { return unique_; }

// 返回要创建的索引列表数量
const int IndexMeta::field_count() const { return field_.size(); }

// 返回索引名
const char *IndexMeta::name() const { return name_.c_str(); }

// 单个情况下返回（或者unique-index）
const char *IndexMeta::field() const { return field_[0].c_str(); }

// 多个情况下返回
const std::vector<std::string> *IndexMeta::fields() const { return &field_; }

// const char *IndexMeta::field() const { return field_.c_str(); }

// 单一索引情况下降序排列，multi暂时不考虑多种排序情况
void IndexMeta::desc(std::ostream &os) const
{
  os << "index name=" << name_ << ", field=" << field_.at(0);

  for (unsigned long int i = 1; i < field_.size(); i++) {
    os << ',' << field_.at(i);
  }
}

void IndexMeta::show(std::ostream &os) const
{
  for (int i = 0; i < field_count(); i++) {
    os << name_ << " | " << (unique_ ? 0 : 1) << " | " << name_ << " | " << (i + 1) << " | " << field_.at(i)
       << std::endl;
  }
}

std::string IndexMeta::get_field_names_str() const
{
  std::stringstream ss;
  for (const auto &field_name : field_) {
    ss << field_name << ",";
  }
  std::string result = ss.str();
  if (!result.empty()) {
    result.pop_back();  // 移除最后一个逗号
  }
  return result;
}
