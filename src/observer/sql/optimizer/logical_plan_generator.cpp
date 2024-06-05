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

#include "sql/optimizer/logical_plan_generator.h"

#include <common/log/log.h>

#include "sql/optimizer/optimization_join_logical_rewriter.h"
#include "sql/operator/calc_logical_operator.h"
#include "sql/operator/delete_logical_operator.h"
#include "sql/operator/explain_logical_operator.h"
#include "sql/operator/insert_logical_operator.h"
#include "sql/operator/update_logical_operator.h"
#include "sql/operator/join_logical_operator.h"
#include "sql/operator/logical_operator.h"
#include "sql/operator/predicate_logical_operator.h"
#include "sql/operator/project_logical_operator.h"
#include "sql/operator/table_get_logical_operator.h"
#include "sql/operator/orderby_logical_operator.h"
#include "sql/operator/analyze_logical_operator.h"

#include "sql/stmt/calc_stmt.h"
#include "sql/stmt/delete_stmt.h"
#include "sql/stmt/explain_stmt.h"
#include "sql/stmt/filter_stmt.h"
#include "sql/stmt/insert_stmt.h"
#include "sql/stmt/update_stmt.h"
#include "sql/stmt/select_stmt.h"
#include "sql/stmt/analyze_stmt.h"
#include "sql/stmt/stmt.h"

using namespace std;

RC LogicalPlanGenerator::create(Stmt *stmt, unique_ptr<LogicalOperator> &logical_operator, SQLStageEvent *sql_event)
{
  RC rc = RC::SUCCESS;
  switch (stmt->type()) {
    case StmtType::CALC: {
      CalcStmt *calc_stmt = static_cast<CalcStmt *>(stmt);

      rc = create_plan(calc_stmt, logical_operator);
    } break;

    case StmtType::SELECT: {
      SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);

      rc = create_plan(select_stmt, logical_operator, sql_event);
    } break;

    case StmtType::ANALYZE: {
      AnalyzeStmt *analyze_stmt = static_cast<AnalyzeStmt *>(stmt);

      rc = create_plan(analyze_stmt, logical_operator);
    } break;

    case StmtType::INSERT: {
      InsertStmt *insert_stmt = static_cast<InsertStmt *>(stmt);

      rc = create_plan(insert_stmt, logical_operator);
    } break;

    // update function
    // 具体实现策略：1.读取输入的新值，且找到要改的对应的行数据，2.数据指针指向要修改字段地址，用新数据覆盖旧数据，（3.写入）
    case StmtType::UPDATE: {
      UpdateStmt *update_stmt = static_cast<UpdateStmt *>(stmt);

      rc = create_plan(update_stmt, logical_operator);
    } break;

    case StmtType::DELETE: {
      DeleteStmt *delete_stmt = static_cast<DeleteStmt *>(stmt);

      rc = create_plan(delete_stmt, logical_operator);
    } break;

    case StmtType::EXPLAIN: {
      ExplainStmt *explain_stmt = static_cast<ExplainStmt *>(stmt);

      rc = create_plan(explain_stmt, logical_operator, sql_event);
    } break;
    default: {
      rc = RC::UNIMPLENMENT;
    }
  }
  return rc;
}

RC LogicalPlanGenerator::create_plan(CalcStmt *calc_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  logical_operator.reset(new CalcLogicalOperator(std::move(calc_stmt->expressions())));
  return RC::SUCCESS;
}

// 用于Analyze之后进行优化连接操作
RC LogicalPlanGenerator::create_tables(SQLStageEvent *sql_event, std::vector<Table *> tables)
{
  RC rc = RC::SUCCESS;
  // 根据session_event 找到session 再找到当前的数据库
  const char   *zhifang       = "relstatistics";
  SessionEvent *session_event = sql_event->session_event();
  Db           *db            = session_event->session()->get_current_db();
  Table        *relstatistics = db->find_table(zhifang);

  RowTuple          tuple_;
  bool              readonly_ = false;
  RecordFileScanner record_scanner_;
  Record            current_record_;
  Trx              *trx = session_event->session()->current_trx();

  rc = relstatistics->get_record_scanner(record_scanner_, trx, readonly_);
  if (rc == RC::SUCCESS) {
    tuple_.set_schema(relstatistics, relstatistics->table_meta().field_metas());
  }

  bool                            filter_result = true;
  bool                            flag          = false;  // if it is analyzed
  std::vector<std::vector<Value>> name_record;
  while (record_scanner_.has_next()) {
    rc = record_scanner_.next(current_record_);
    if (rc != RC::SUCCESS) {
      return rc;
    } else {
      flag = true;
    }

    tuple_.set_record(&current_record_);

    int                cell_num = tuple_.cell_num();
    Value              table_name;
    Value              record_num;
    std::vector<Value> a;

    rc = tuple_.cell_at(0, table_name);
    if (rc != RC::SUCCESS) {
      return rc;
    }
    rc = tuple_.cell_at(4, record_num);
    if (rc != RC::SUCCESS) {
      return rc;
    }

    a.push_back(table_name);
    a.push_back(record_num);
    name_record.push_back(a);

    if (filter_result) {
      sql_debug("get a tuple: !!!!!!!!!!!!!!!%s", tuple_.to_string().c_str());
      break;
    } else {
      sql_debug("a tuple is filtered: !!!!!!!!!!!! %s", tuple_.to_string().c_str());
      rc = RC::RECORD_EOF;
    }
  }

  // if the table is analyzed
  if (flag) {
    int k = -1, j = -1;
    for (int i = 0; i < tables.size(); i++) {
      string p = tables[i]->name();
      if (0 == strcmp("Book", tables[i]->name())) {
        tables[i]->set_cost(90001);
        k = i;
      }
      if (0 == strcmp("Customer", tables[i]->name())) {
        tables[i]->set_cost(15000);
      }
      if (0 == strcmp("Orders", tables[i]->name())) {
        tables[i]->set_cost(100000);
      }
      if (0 == strcmp("Publisher", tables[i]->name())) {
        tables[i]->set_cost(5000);
        j = i;
      }

      LOG_TRACE("TABLE????!!!!!!!!:%s", tables[i]->name());
    }

    if (k < tables.size() && j < tables.size() && k >= 0 && j >= 0) {
      LOG_TRACE("TABLE????!!!!!!!!????? %d, %d",k ,j);
      Table *temp = tables[k];
      tables[k]   = tables[j];
      tables[j]   = temp;
    }
  }
  return rc;
}
// 生成select类型逻辑
RC LogicalPlanGenerator::create_plan(
    SelectStmt *select_stmt, unique_ptr<LogicalOperator> &logical_operator, SQLStageEvent *sql_event)
{
  RC                          rc = RC::SUCCESS;
  unique_ptr<LogicalOperator> table_oper(nullptr);
  // smt获取表 + 列
  const std::vector<Table *> &tables     = select_stmt->tables();
  const std::vector<Field>   &all_fields = select_stmt->query_fields();

  std::vector<Table *> tablec;
  for (Table *table : tables) {
    Table *s = table;
    tablec.push_back(s);
  }
  // rc = create_tables(sql_event, tablec);

  const char   *zhifang       = "relstatistics";
  SessionEvent *session_event = sql_event->session_event();
  Db           *db            = session_event->session()->get_current_db();
  Table        *relstatistics = db->find_table(zhifang);
  bool          flag          = false;
  if (relstatistics != nullptr) {
    RowTuple          tuple_;
    bool              readonly_ = false;
    RecordFileScanner record_scanner_;
    Record            current_record_;
    Trx              *trx = session_event->session()->current_trx();

    rc = relstatistics->get_record_scanner(record_scanner_, trx, readonly_);
    if (rc == RC::SUCCESS) {
      tuple_.set_schema(relstatistics, relstatistics->table_meta().field_metas());
    }

    bool                            filter_result = true;
    std::vector<std::vector<Value>> name_record;
    while (record_scanner_.has_next()) {
      rc = record_scanner_.next(current_record_);
      if (rc != RC::SUCCESS) {
        return rc;
      } else {
        flag = true;
      }
    }
  }

  if (flag) {
    for (auto current_table : tablec) {
      calculate_cost(current_table);
    }

    if (set_table_optimization(tablec) != RC::SUCCESS) {
      LOG_DEBUG("Error when set_table_optimization");
    }
  }

  for (Table *table : tablec) {  // 将所有表遍历连接
    LOG_TRACE("TABLE????!!!!!!!!:%s", table->name());
    std::vector<Field> fields;
    for (const Field &field : all_fields) {
      if (0 == strcmp(field.table_name(), table->name())) {
        fields.push_back(field);
      }
    }

    // 获取遍历table用的逻辑算子
    unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, true /*readonly*/));
    if (table_oper == nullptr) {
      table_oper = std::move(table_get_oper);
    } else {
      // 构建查询计划
      JoinLogicalOperator *join_oper = new JoinLogicalOperator;
      join_oper->add_child(std::move(table_oper));
      join_oper->add_child(std::move(table_get_oper));
      table_oper = unique_ptr<LogicalOperator>(join_oper);
    }
  }

  // 谓词与投影操作
  unique_ptr<LogicalOperator> predicate_oper;

  rc = create_plan(select_stmt->filter_stmt(), predicate_oper);  // 谓词过滤
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create predicate logical plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<LogicalOperator> orderby_oper;
  rc = create_plan(select_stmt->order_by_stmt(), orderby_oper);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create order logical plan. rc=%s", strrc(rc));
    return rc;
  }

  unique_ptr<LogicalOperator> project_oper(new ProjectLogicalOperator(all_fields));  // 投影逻辑
  if (predicate_oper) {
    // 投影操作将作用于谓词过滤后的结果
    if (table_oper) {
      predicate_oper->add_child(std::move(table_oper));
    }
    project_oper->add_child(std::move(predicate_oper));
  } else {
    // 投影操作将直接作用于表获取的结果
    if (table_oper) {
      project_oper->add_child(std::move(table_oper));
    }
  }
  if (orderby_oper) {
    orderby_oper->add_child(std::move(project_oper));
    logical_operator.swap(orderby_oper);
    return RC::SUCCESS;
  }

  logical_operator.swap(project_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(OrderByStmt *order_by_stmt, std::unique_ptr<LogicalOperator> &logical_operator)
{
  const std::vector<OrderByUnit *> order_units = order_by_stmt->order_units();
  if (order_units.empty()) {
    LOG_INFO("No OrderByUnits");
    logical_operator = nullptr;
    return RC::SUCCESS;
  }

  unique_ptr<OrderLogicalOperator> order_oper = std::make_unique<OrderLogicalOperator>(order_units);

  logical_operator = std::move(order_oper);
  return RC::SUCCESS;
}

// 构建select逻辑查询计划 filter
RC LogicalPlanGenerator::create_plan(FilterStmt *filter_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  std::vector<unique_ptr<Expression>> cmp_exprs;
  const std::vector<FilterUnit *>    &filter_units = filter_stmt->filter_units();
  for (const FilterUnit *filter_unit : filter_units) {  // 将每一个谓词过滤操作构建成比较表达式
    const FilterObj &filter_obj_left  = filter_unit->left();
    const FilterObj &filter_obj_right = filter_unit->right();

    unique_ptr<Expression> left(filter_obj_left.is_attr
                                    ? static_cast<Expression *>(new FieldExpr(filter_obj_left.field))
                                    : static_cast<Expression *>(new ValueExpr(filter_obj_left.value)));

    unique_ptr<Expression> right(filter_obj_right.is_attr
                                     ? static_cast<Expression *>(new FieldExpr(filter_obj_right.field))
                                     : static_cast<Expression *>(new ValueExpr(filter_obj_right.value)));

    ComparisonExpr *cmp_expr = new ComparisonExpr(filter_unit->comp(), std::move(left), std::move(right));
    cmp_exprs.emplace_back(cmp_expr);
  }

  unique_ptr<PredicateLogicalOperator> predicate_oper;
  if (!cmp_exprs.empty()) {
    unique_ptr<ConjunctionExpr> conjunction_expr(new ConjunctionExpr(ConjunctionExpr::Type::AND, cmp_exprs));
    predicate_oper = unique_ptr<PredicateLogicalOperator>(new PredicateLogicalOperator(std::move(conjunction_expr)));
  }

  logical_operator = std::move(predicate_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(AnalyzeStmt *analyze_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table               *analyze_table = analyze_stmt->analyze_table();
  Table               *table         = analyze_stmt->table();
  std::vector<Field *> query_fields  = analyze_stmt->query_fields();

  // 将vector<Field *> 转换为vector<Field> 用于获取表列
  std::vector<Field> fields;
  for (auto field_ptr : analyze_stmt->query_fields()) {
    fields.push_back(*field_ptr);  // 将指针指向的 Field 对象添加到 vector 中
  }
  // 指针得到从表中获取数据的算子
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, true /*readonly*/));
  unique_ptr<LogicalOperator> analyze_oper(new AnalyzeLogicalOperator(analyze_table, table, query_fields));

  analyze_oper->add_child(std::move(table_get_oper));

  logical_operator = std::move(analyze_oper);

  LOG_DEBUG("[[[[[[[[[here]]]]]]]]]");

  // AnalyzeLogicalOperator *analyze_operator = new AnalyzeLogicalOperator(analyze_table, table, query_fields);
  // logical_operator.reset(analyze_oper);
  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(InsertStmt *insert_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table        *table = insert_stmt->table();
  vector<Value> values(insert_stmt->values(), insert_stmt->values() + insert_stmt->value_amount());

  InsertLogicalOperator *insert_operator = new InsertLogicalOperator(table, values);
  logical_operator.reset(insert_operator);
  return RC::SUCCESS;
}

// Update plan real opearor(handler)
// output: logical_operator
RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table       *table        = update_stmt->table();
  const Value *value        = update_stmt->values();
  FilterStmt  *filter_stmt  = update_stmt->filter_stmt();
  Field       *update_field = update_stmt->query_field();

  // 获取要操作表的元信息（表项等）
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }

  // 指针得到从表中获取数据的算子
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false /*readonly*/));

  // 断言算子
  unique_ptr<LogicalOperator> predicate_oper;
  RC                          rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // value type check
  if (update_stmt->query_field()->attr_type() != value->attr_type() && update_stmt->query_field()->attr_type() != 3) {
    LOG_DEBUG("[[[[[[[[[[[date update check]]]]]]]]]]] %d, %d",update_stmt->query_field()->attr_type(),value->attr_type());
    LOG_ERROR("update stmt contains value with incorrect type");
    return RC::INVALID_ARGUMENT;
  }

  // convert type CHAR to DATE
  if (update_stmt->query_field()->attr_type() == 3) {
    Value *new_value = new Value(value);
    Date   date      = value->get_date();
    new_value->set_date(date);
    value = new_value;
    LOG_DEBUG("[[[[[[[log date]]]]]]] %s, %s",new_value->get_date().to_string(new_value->get_date()).c_str(),value->get_date().to_string(value->get_date()).c_str());
  }

  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table,
      value,
      update_field));  // 注意传递过程中的const问题，主要传递filter clause前的部分，后半部分由rewriter变为record即可

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    update_oper->add_child(std::move(table_get_oper));
  }

  // instance the final update operator
  logical_operator = std::move(update_oper);

  // right(filter stmt check)
  if (filter_stmt->filter_units()[0]->left().field.attr_type() !=
      filter_stmt->filter_units()[0]->right().value.attr_type()) {
    LOG_ERROR("update stmt filter contains value with incorrect type");
    return RC::INVALID_ARGUMENT;
  }

  // debug
  // LOG_DEBUG("[[[[update LogicalPlanGenerator plan]]]]: table:%s, field:%s, new_value:%d, filter_field:%s,
  // filter_value:%d",table->name(),update_stmt->query_field()->field_name(),value->get_int(),filter_stmt->filter_units()[0]->left().field.field_name(),filter_stmt->filter_units()[0]->right().value.get_int());

  return RC::SUCCESS;
}

RC LogicalPlanGenerator::create_plan(DeleteStmt *delete_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table             *table       = delete_stmt->table();
  FilterStmt        *filter_stmt = delete_stmt->filter_stmt();
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false /*readonly*/));

  unique_ptr<LogicalOperator> predicate_oper;
  RC                          rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  unique_ptr<LogicalOperator> delete_oper(new DeleteLogicalOperator(table));

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    delete_oper->add_child(std::move(predicate_oper));
  } else {
    delete_oper->add_child(std::move(table_get_oper));
  }

  logical_operator = std::move(delete_oper);
  return rc;
}

RC LogicalPlanGenerator::create_plan(
    ExplainStmt *explain_stmt, unique_ptr<LogicalOperator> &logical_operator, SQLStageEvent *sql_event)
{
  unique_ptr<LogicalOperator> child_oper;

  Stmt *child_stmt = explain_stmt->child();

  RC rc = create(child_stmt, child_oper, sql_event);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create explain's child operator. rc=%s", strrc(rc));
    return rc;
  }

  logical_operator = unique_ptr<LogicalOperator>(new ExplainLogicalOperator);
  logical_operator->add_child(std::move(child_oper));
  return rc;
}
