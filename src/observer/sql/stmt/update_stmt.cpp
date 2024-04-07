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
// Created by Wangyunlai on 2022/5/22.
//

#include "sql/stmt/update_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/table/table.h"

UpdateStmt::UpdateStmt(Table *table, const Value *values, FilterStmt *filter_stmt)
    : table_(table), values_(values), filter_stmt_(filter_stmt)
{}

/**
 * UpdateStmt: create update statement
 *
 * @param  {Db*} db               :
 * @param  {UpdateSqlNode} update :
 * @param  {Stmt*} stmt           :
 * @return {RC}                   :
 */
RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // check the sql input
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || 0 == update.value.length()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(update.value.length()));
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  LOG_DEBUG("TEST UPDATE FROM STMT: ");
  const Value *values = &update.value;
  // const TableMeta &table_meta = table->table_meta();
  // TODO check whether the field type and input type are matching
  // const int sys_field_num = table_meta.sys_field_num();

  // 获取要修改的表项信息
  Field *query_field = nullptr;

  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  const char      *field_name = update.attribute_name.c_str();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    // field_metas.push_back(Field(table, table_meta.field(i)));
    if(strcmp(table_meta.field(i)->name(),field_name)==0){
      query_field = new Field(table,table_meta.field(i));
      break;
    }
  }
  LOG_DEBUG("get field name: %s",field_name);
  if(query_field == nullptr){
    LOG_ERROR("failed to get existing field to change.");
    return RC::NOTFOUND;
  }
  LOG_DEBUG("get field name good: %s",field_name);

  // 创建filter
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));
  FilterStmt *filter_stmt = nullptr;

  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // TODO everything alright
  UpdateStmt *update_stmt = new UpdateStmt(table, values, filter_stmt);
  update_stmt->query_field_ = query_field;
  stmt = update_stmt;
  return RC::SUCCESS;
}
