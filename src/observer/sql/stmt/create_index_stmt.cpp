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
// Created by Wangyunlai on 2023/4/25.
//

#include "sql/stmt/create_index_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "storage/table/table.h"

using namespace std;
using namespace common;

// 第一步，创建索引stmt，所有的前置条件在此判断：是否已存在同名索引，要创建的table和field是否存在
RC CreateIndexStmt::create(Db *db, const CreateIndexSqlNode &create_index, Stmt *&stmt)
{
  stmt = nullptr;

  // 获取要插入的表名
  const char *table_name = create_index.relation_name.c_str();
  if (is_blank(table_name) || is_blank(create_index.index_name.c_str()) || create_index.attribute_names.size() == 0) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, index name=%s, attribute name=%s",
        db, table_name, create_index.index_name.c_str(), create_index.attribute_names[0]);
    return RC::INVALID_ARGUMENT;
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // 获取要创建的索引列表
  // 创建数组field数组并根据sqlnode指定的field插入
  std::vector<const FieldMeta *> field_meta_list;
  for (long unsigned int i = 0; i < create_index.attribute_names.size(); i++) {
    const FieldMeta *tmp = table->table_meta().field(create_index.attribute_names[i].c_str());
    if (tmp == nullptr) {
      LOG_WARN("no such field in table. db=%s, table=%s, field name=%s", 
             db->name(), table_name, create_index.attribute_names[i].c_str());
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
    field_meta_list.push_back(tmp);
  }

  // 查找是否已存在该索引
  Index *index = table->find_index(create_index.index_name.c_str());
  if (nullptr != index) {
    LOG_WARN("index with name(%s) already exists. table name=%s", create_index.index_name.c_str(), table_name);
    return RC::SCHEMA_INDEX_NAME_REPEAT;
  }

  // create index stmt, use vector meta list
  stmt = new CreateIndexStmt(table, field_meta_list, create_index.index_name,create_index.is_unique);

  // test for each item in the CreateIndexStmt
  CreateIndexStmt *createIndexStmt = new CreateIndexStmt(table, field_meta_list, create_index.index_name,create_index.is_unique);
  for (long unsigned int i = 0; i < createIndexStmt->field_metas().size(); i++) {
    LOG_DEBUG("[[[[[[[[[[CreateIndexStmt::create]]]]]]]]]]: %d: %s is_unique:%d",i,createIndexStmt->field_metas()[i]->name(),createIndexStmt->is_unique());
  }

  return RC::SUCCESS;
}
