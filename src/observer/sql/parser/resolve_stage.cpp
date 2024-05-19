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
// Created by Longda on 2021/4/13.
//

#include <string.h>
#include <string>

#include "resolve_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "session/session.h"
#include "sql/stmt/stmt.h"

using namespace common;

RC ResolveStage::handle_request(SQLStageEvent *sql_event)
{
  RC            rc            = RC::SUCCESS;
  SessionEvent *session_event = sql_event->session_event();
  SqlResult    *sql_result    = session_event->sql_result();

  Db *db = session_event->session()->get_current_db();
  if (nullptr == db) {
    LOG_ERROR("cannot find current db");
    rc = RC::SCHEMA_DB_NOT_EXIST;
    sql_result->set_return_code(rc);
    sql_result->set_state_string("no db selected");
    return rc;
  }

  ParsedSqlNode *sql_node = sql_event->sql_node().get();
  Stmt          *stmt     = nullptr;

  rc = Stmt::create_stmt(db, *sql_node, stmt);
  if (rc != RC::SUCCESS && rc != RC::UNIMPLENMENT) {
    LOG_WARN("failed to create stmt. rc=%d:%s", rc, strrc(rc));
    sql_result->set_return_code(rc);
    return rc;
  }

  sql_event->set_stmt(stmt);

  return rc;
}

RC ResolveStage::alias_pre_process(SelectSqlNode *select_sql)
{
   
  if (!select_sql) return RC::SUCCESS;

  if (select_sql->attributes.size() == 0) {
    LOG_WARN("select attribute size is zero");
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS; 
  std::map<std::string, std::string> table2alias_map_tmp; ///< alias-->table_name
  std::map<std::string, int> alias_exist_tmp;   // 是否存在这个alias,防止重复

  LOG_TRACE("alias_pre_process=>>>>>>>>>>");
  for (size_t i= 0; i< select_sql->relations.size(); i++)
  {
    if (select_sql->table_alias[i].empty()){ //当前表没有别名（可能在后面的语句中定义别名
      if (!alias_exist_tmp[select_sql->relations[i]]){ //如果alias_exit中标记当前表没有alias
        if (!alias_exis[select_sql->relations[i]]) continue;  //如果当前表确实没有alias
      }
      if (alias_exist_tmp[select_sql->relations[i]]){
        select_sql->relations[i] = table2alias_map_tmp[select_sql->relations[i]]; 
        continue;
      }
      if (alias_exis[select_sql->relations[i]]){
        select_sql->relations[i] = table2alias_mp[select_sql->relations[i]]; 
        continue;
      }
    }
    
    if (alias_exist_tmp[select_sql->table_alias[i]]){
      return RC::SAME_ALIAS;
    }
    table2alias_map_tmp[select_sql->table_alias[i]] = select_sql->relations[i];
    alias_exist_tmp[select_sql->table_alias[i]] = 1;

    if (alias_exis[select_sql->table_alias[i]])continue;
    
    table2alias_mp[select_sql->table_alias[i]] = select_sql->relations[i];
    alias_exis[select_sql->table_alias[i]] = 1;
  }
  LOG_TRACE("table_name=>>>>>>>>>>%s   alias=>>>>>>%s", select_sql->relations[0],select_sql->table_alias[0]);



  for (RelAttrSqlNode &node : select_sql->attributes)
  {
      if (!field_exis[node.alias] && !node.alias.empty() && node.attribute_name != "*"){ //属性不存在别名，别名空，或者表示all
        field2alias_mp[node.alias] = node.attribute_name;
        field_exis[node.alias] = 1;
      }
      if (alias_exist_tmp[node.relation_name]){ //表有别名
        node.relation_name = table2alias_map_tmp[node.relation_name];
        LOG_TRACE("2if=>>>>>>>>>>%s", node.relation_name);
        continue;
      }
      if (alias_exis[node.relation_name]){ //表有别名
        node.relation_name = table2alias_mp[node.relation_name]; //属性的表名要更换成别名
        LOG_TRACE("3if=>>>>>>>>>>%s", node.relation_name);
      }
      
  }

  for (ConditionSqlNode &con_node : select_sql->conditions)
  {

    RelAttrSqlNode &node =con_node.left_attr;
    if (!field_exis[node.alias] && !node.alias.empty() && node.attribute_name != "*"){ //属性不存在别名，别名空，或者表示all
      field2alias_mp[node.alias] = node.attribute_name;
      field_exis[node.alias] = 1;
    }
    if (alias_exist_tmp[node.relation_name]){ //表有别名
      node.relation_name = table2alias_map_tmp[node.relation_name];
      LOG_TRACE("2if=>>>>>>>>>>%s", node.relation_name);
      continue;
    }
    if (alias_exis[node.relation_name]){ //表有别名
      node.relation_name = table2alias_mp[node.relation_name]; //属性的表名要更换成别名
      LOG_TRACE("3if=>>>>>>>>>>%s", node.relation_name);
    }
  }


  return RC::SUCCESS;
}

RC ResolveStage::handle_alias(SQLStageEvent *sql_event) {
  RC rc = RC::SUCCESS;
  field2alias_mp.clear();
  field_exis.clear();
  alias_exis.clear();
  table2alias_mp.clear();

  if (sql_event->sql_node()) { 
    SessionEvent *session_event = sql_event->session_event();
    SqlResult    *sql_result    = session_event->sql_result();
    SelectSqlNode* select_sql = nullptr;
    
    if (sql_event->sql_node()->flag == SqlCommandFlag::SCF_SELECT)
      select_sql = &sql_event->sql_node()->selection;
    

    rc = alias_pre_process(select_sql);
    if (OB_FAIL(rc)) {
      LOG_TRACE("failed to do select pre-process. rc=%s", strrc(rc));
      sql_result->set_return_code(rc);
      return rc;
    }
  }
  return rc;
}
