/* Copyright (c) 2023 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/08/16.
//

#pragma once

#include <memory>

#include "storage/db/db.h"
#include "storage/table/table_meta.h"
#include "session/session.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/physical_operator.h"
#include "storage/record/record_manager.h"
#include "event/sql_event.h"
#include "event/session_event.h"



#include "common/rc.h"


class Stmt;
class CalcStmt;
class SelectStmt;
class FilterStmt;
class InsertStmt;
class DeleteStmt;
class ExplainStmt;
class UpdateStmt;
class OrderByStmt;
class LogicalOperator;
class AnalyzeStmt;
class SQLStageEvent;
class SessionEvent;
class Session;
class TableMeta;


class LogicalPlanGenerator
{
public:
  LogicalPlanGenerator()          = default;
  virtual ~LogicalPlanGenerator() = default;

  RC create(Stmt *stmt, std::unique_ptr<LogicalOperator> &logical_operator,SQLStageEvent *sql_event);

private:
  RC create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(SelectStmt *select_stmt, std::unique_ptr<LogicalOperator> &logical_operator,SQLStageEvent *sql_event);
  RC create_plan(FilterStmt *filter_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(InsertStmt *insert_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(UpdateStmt *update_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(DeleteStmt *delete_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(ExplainStmt *explain_stmt, std::unique_ptr<LogicalOperator> &logical_operator,SQLStageEvent *sql_event);
  RC create_plan(OrderByStmt *order_by_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_plan(AnalyzeStmt *analyze_stmt, std::unique_ptr<LogicalOperator> &logical_operator);
  RC create_tables(SQLStageEvent *sql_event,std::vector<Table *> tables);
};