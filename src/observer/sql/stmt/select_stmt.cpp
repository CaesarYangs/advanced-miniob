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
// Created by Wangyunlai on 2022/6/6.
//

#include "sql/parser/parse_defs.h"
#include "storage/field/field.h"
#include "sql/stmt/select_stmt.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/db/db.h"
#include "storage/table/table.h"
#include "sql/stmt/order_by_stmt.h"
#include <cstddef>

SelectStmt::~SelectStmt()
{
  if (nullptr != filter_stmt_) {
    delete filter_stmt_;
    filter_stmt_ = nullptr;
  }
}

static void wildcard_fields(Table *table, std::vector<Field> &field_metas)
{
  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    field_metas.push_back(Field(table, table_meta.field(i)));
  }
}

/**
 * @description: 将sql中select字段的关键信息：表名，字段，在哪个字段上做聚集进行包装并转化到Field对象中
 * @return {RC} 查询的状况
 */
static RC get_fields(bool is_aggre, std::vector<Field> &query_fields, const std::vector<RelAttrSqlNode> &attributes,
    const std::unordered_map<std::string, Table *> &table_map, const std::vector<Table *> &tables, const Db *db)
{
  if (is_aggre) {
    // 当前 aggre 查询的情况只有 attr
    // 默认只有一张表
    Table *table = tables.front();
    for (auto &attribute : attributes) {
      const auto attr_names = attribute.aggretion_node.attribute_names;
      const auto aggre_type = attribute.aggretion_node.aggre_type;

      if (attr_names.size() != 1) {
        LOG_WARN("query aggretion size is %d != 1", attr_names.size());
        return RC::INTERNAL;
      }

      const auto attr_name  = attr_names.front();  // 因为只有一个元素, 所有第一个就是当前查询的
      auto       field_meta = table->table_meta().field(attr_name.c_str());
      // 如果列名不等于1, 则错误

      if (attr_name == "*") {
        // 只有count(*)允许存在
        if (aggre_type != AGGRE_COUNT) {
          LOG_WARN("Aggregation type %s cannot match parameters '*'", aggreType2str(aggre_type).c_str());
          return RC::INTERNAL;
        }
        field_meta = table->table_meta().field(0);  // 默认在第一列做count(*)
      } else {
        // 查询的列名不存在
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), attr_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }
      }

      query_fields.push_back(Field(table, field_meta, aggre_type, attr_name));
    }
    return RC::SUCCESS;
  }

  // not aggre
  for (auto &relation_attr : attributes) {
    const auto table_name = relation_attr.relation_name;
    const auto field_name = relation_attr.attribute_name;
    const auto aggre_type = AGGRE_NONE;
    if (table_name == "*" || field_name == "") {
      LOG_WARN("no fields type err=%s.%s", table_name.c_str(), field_name.c_str());
      return RC::SCHEMA_FIELD_MISSING;
    }

    if (table_name == "" && field_name == "*") {
      for (const auto table : tables) {
        wildcard_fields(table, query_fields);
      }
    } else if (table_name == "" && field_name != "*") {  // field_name != "*"
      if (tables.size() != 1) {
        LOG_WARN("invalid. I do not know the attr's table. attr=%s", relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table           *table      = tables[0];
      const FieldMeta *field_meta = table->table_meta().field(relation_attr.attribute_name.c_str());
      if (nullptr == field_meta) {
        LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), relation_attr.attribute_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }
      query_fields.push_back(Field(table, field_meta));
    } else {  // table_name != "*"
      auto iter = table_map.find(table_name);
      if (iter == table_map.end()) {
        LOG_WARN("no such table in from list: %s", table_name.c_str());
        return RC::SCHEMA_FIELD_MISSING;
      }

      Table *table = iter->second;
      if (field_name == "*") {
        wildcard_fields(table, query_fields);
      } else {
        const FieldMeta *field_meta = table->table_meta().field(field_name.c_str());
        if (nullptr == field_meta) {
          LOG_WARN("no such field. field=%s.%s.%s", db->name(), table->name(), field_name.c_str());
          return RC::SCHEMA_FIELD_MISSING;
        }
        query_fields.push_back(Field(table, field_meta));
      }
    }
  }

  return RC::SUCCESS;
}

RC SelectStmt::create(Db *db, const SelectSqlNode &select_sql, Stmt *&stmt)
{
  // 主要修改部分——主要修改select的实现逻辑
  // new select stmt with the implementation of aggregation function

  if (nullptr == db) {
    LOG_WARN("invalid argument. db is null");
    return RC::INVALID_ARGUMENT;
  }

  // collect tables in `from` statement
  std::vector<Table *>                     tables;
  std::unordered_map<std::string, Table *> table_map;
  std::vector<OrderSqlNode>                orderbys = select_sql.orders;
  for (size_t i = 0; i < select_sql.relations.size(); i++) {
    // is table name valid
    const char *table_name = select_sql.relations[i].c_str();
    if (nullptr == table_name) {
      LOG_WARN("invalid table name. name index=%d", i);
      return RC::INVALID_ARGUMENT;
    }

    // is table existed
    Table *table = db->find_table(table_name);
    if (nullptr == table) {
      LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
      return RC::SCHEMA_TABLE_NOT_EXIST;
    }

    tables.push_back(table);
    table_map.insert(std::pair<std::string, Table *>(table_name, table));  // 用map作为存储介质
  }

  auto  &attributes      = select_sql.attributes;
  size_t aggregation_num = 0;
  for (auto &attribute : attributes) {
    if (attribute.aggretion_node.aggre_type != AGGRE_NONE) {
      aggregation_num++;
    }
  }

  // is attributes valid
  if (aggregation_num != 0 && aggregation_num != attributes.size()) {
    LOG_WARN("Aggregation field error");
    return RC::INTERNAL;
  }

  // get the fields (table_meta, (table_name, col_name))
  // 全部的关键信息都被存储到query_fields中，包括对每个字段上是否进行agg，进行何种agg，都存储起来，并准备下一步操作
  std::vector<Field> query_fields;
  RC rc = get_fields(static_cast<bool>(aggregation_num), query_fields, attributes, table_map, tables, db);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // success get all tables, query fields and clauses
  LOG_INFO("Got %d tables in FROM stmt and %d fields in QUERY stmt", tables.size(), query_fields.size());

  Table *default_table = nullptr;
  if (tables.size() == 1) {
    default_table = tables[0];
  }

  // create filter statement in `where` statement
  FilterStmt *filter_stmt = nullptr;
  rc                      = FilterStmt::create(db,
      default_table,
      &table_map,
      select_sql.conditions.data(),
      static_cast<int>(select_sql.conditions.size()),
      filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct filter stmt");
    return rc;
  }

  // create orderby stmt for ORDER BY
  // 创建ORDER_BY的STMT
  OrderByStmt *orderby_stmt = nullptr;
  rc                        = OrderByStmt::create(
      db, default_table, &table_map, orderbys.data(), static_cast<int>(orderbys.size()), orderby_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("cannot construct orderby stmt");
    return rc;
  }

  // everything alright
  SelectStmt *select_stmt = new SelectStmt();
  // TODO add expression copy
  select_stmt->tables_.swap(tables);
  select_stmt->query_fields_.swap(query_fields);
  select_stmt->filter_stmt_ = filter_stmt;
  select_stmt->order_stmt_  = orderby_stmt;
  stmt                      = select_stmt;
  return RC::SUCCESS;
}
