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

#include "optimize_stage.h"

#include "common/conf/ini.h"
#include "common/io/io.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/operator/logical_operator.h"
#include "sql/stmt/stmt.h"



using namespace std;
using namespace common;

RC OptimizeStage::handle_request(SQLStageEvent *sql_event)
{
  unique_ptr<LogicalOperator> logical_operator;

  RC                          rc = create_logical_plan(sql_event, logical_operator); 
  if (rc != RC::SUCCESS) {
    if (rc != RC::UNIMPLENMENT) {
      LOG_WARN("failed to create logical plan. rc=%s", strrc(rc));
    }
    return rc;
  }

  rc = rewrite(logical_operator); 
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to rewrite plan. rc=%s", strrc(rc));
    return rc;
  }

  rc = optimize(logical_operator, sql_event);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to optimize plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<PhysicalOperator> physical_operator;
  rc = generate_physical_plan(logical_operator, physical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to generate physical plan. rc=%s", strrc(rc));
    return rc;
  }

  sql_event->set_operator(std::move(physical_operator));

  return rc;
}

RC OptimizeStage::optimize(unique_ptr<LogicalOperator> &oper,SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  //根据session_event 找到session 再找到当前的数据库
  const char* zhifang = "relstatistics";
  SessionEvent *session_event = sql_event->session_event();
  Db * db =  session_event->session()->get_current_db();
  Table * relstatistics = db->find_table(zhifang);

  if(relstatistics != nullptr){
    RowTuple tuple_;
  bool readonly_ = false;
  RecordFileScanner record_scanner_;
  Record current_record_;
  Trx *trx = session_event->session()->current_trx();

  rc = relstatistics->get_record_scanner(record_scanner_, trx, readonly_);
  if (rc == RC::SUCCESS) {
    tuple_.set_schema(relstatistics, relstatistics->table_meta().field_metas());
  }

  bool filter_result = true;
  while (record_scanner_.has_next()) {
    rc = record_scanner_.next(current_record_);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    tuple_.set_record(&current_record_);

    int cell_num =tuple_.cell_num();
    Value table_name;
    Value record_num;
    rc = tuple_.cell_at(0,table_name);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    rc = tuple_.cell_at(4,record_num);
   
    if (filter_result) {
      sql_debug("get a tuple: !!!!!!!!!!!!!!!%s", tuple_.to_string().c_str());
      break;
    } else {
      sql_debug("a tuple is filtered: !!!!!!!!!!!! %s", tuple_.to_string().c_str());
      rc = RC::RECORD_EOF;
    }
  }

  }
  
  //验证是否读取到表
  // if(relstatistics ==nullptr){
  //   LOG_TRACE("NOT GOT TABLE!!!!!!!!");
  // }
  // const TableMeta tablemeta = relstatistics->table_meta();
  // LOG_TRACE("GOT TABLE!!!!!!!!:%s", relstatistics->table_meta().name());

  //完整读取表的信息
  // std::vector<std::vector<Value>> data_matrix_ = relstatistics->get_data_matrix();
   
  // if(!data_matrix_.empty())
  //   LOG_TRACE("GOT TABLEMATRIX!!!!!!!!:");

  //1.获取到所有table_get
  std::vector<TableGetLogicalOperator *> tablegets;
  rc = get_tablegets(oper, tablegets);
  if(rc != RC::SUCCESS){
    return rc;
  }
  //2.计算代价，写出连接代价函数

  //3.回溯计算最优排序

  //4.改写逻辑计划
  

  //

  return rc;
}

RC OptimizeStage::get_tablegets(unique_ptr<LogicalOperator> &oper,vector<TableGetLogicalOperator *> &tablegets){

  RC rc = RC::SUCCESS;
  for(auto &child: oper->children()){
    if(child->type()== LogicalOperatorType::TABLE_GET){
      auto table_get_oper = static_cast<TableGetLogicalOperator *>(child.get());
      tablegets.emplace_back(std::move(table_get_oper));
    }else{
       RC rc = get_tablegets(child, tablegets);
    }
  }
  return rc;
}

RC OptimizeStage::generate_physical_plan(
    unique_ptr<LogicalOperator> &logical_operator, unique_ptr<PhysicalOperator> &physical_operator)
{
  RC rc = RC::SUCCESS;
  rc    = physical_plan_generator_.create(*logical_operator, physical_operator);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
  }
  return rc;
}

RC OptimizeStage::rewrite(unique_ptr<LogicalOperator> &logical_operator)
{
  RC rc = RC::SUCCESS;
  bool change_made = false;
  do {
    change_made = false;
    rc          = rewriter_.rewrite(logical_operator, change_made);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to do expression rewrite on logical plan. rc=%s", strrc(rc));
      return rc;
    }
  } while (change_made);

  return rc;
}

RC OptimizeStage::create_logical_plan(SQLStageEvent *sql_event, unique_ptr<LogicalOperator> &logical_operator)
{
  Stmt *stmt = sql_event->stmt();
  if (nullptr == stmt) {
    return RC::UNIMPLENMENT;
  }

  return logical_plan_generator_.create(stmt, logical_operator,sql_event);
}
