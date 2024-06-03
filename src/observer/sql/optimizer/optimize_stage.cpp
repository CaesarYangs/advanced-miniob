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
#include <iostream>
#include <sstream>
#include <vector>
#include <regex>

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
    bool get_tuple = false;
    RecordFileScanner record_scanner_;
    Record current_record_;
    Trx *trx = session_event->session()->current_trx();

    rc = relstatistics->get_record_scanner(record_scanner_, trx, readonly_);
    if (rc == RC::SUCCESS) {
      tuple_.set_schema(relstatistics, relstatistics->table_meta().field_metas());
    }

    std::vector<std::vector<Value>> relstatistics_data;
    while (record_scanner_.has_next()) {
      rc = record_scanner_.next(current_record_);
      if (rc != RC::SUCCESS) {
        return rc;
      }
      std::vector<Value> t;
      tuple_.set_record(&current_record_);

      int cell_num =tuple_.cell_num();

      for (int i = 0; i < cell_num; i++) {
        //table_name,column_name,bucket,histogtram,record_num;
        Value feild;
        rc = tuple_.cell_at(i,feild);
        if (rc != RC::SUCCESS) {
          return rc;
        }
        t.push_back(feild);

      }
     
      LOG_TRACE("GOT TABLE!!!!!!!!:%s", t[0].to_string());
      relstatistics_data.push_back(t);
    }
    if(get_tuple){
      //1.获取到所有table_get
      std::vector<TableGetLogicalOperator *> tablegets;
      rc = get_tablegets(oper, tablegets);
      if(rc != RC::SUCCESS){
        return rc;
      }
      //2.计算代价存进算子，写出连接代价函数
      rc = calculate_cost(tablegets, relstatistics_data);

      //3.回溯计算最优排序
      rc  = sort_operator(tablegets);
    }

  }
  

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
RC OptimizeStage::calculate_cost(vector<TableGetLogicalOperator *> &tablegets, std::vector<std::vector<Value>> reltatistics_data){
  RC rc = RC::SUCCESS;
  for(TableGetLogicalOperator *tableget : tablegets){
    double chose_ratio = 1;
    std::vector<std::unique_ptr<Expression>> &exprs=tableget->predicates();
    std::unique_ptr<Expression> &expr = exprs.front();
    auto comparison_expr = static_cast<ComparisonExpr *>(expr.get());//转换成比较表达式子

    std::unique_ptr<Expression> &left_expr = comparison_expr->left(); //获取表算子的表名和列名
    FieldExpr *left_field_expr = static_cast<FieldExpr *>(left_expr.get());
    const char *left_table_name = left_field_expr->table_name();
    const char *left_field_name =  left_field_expr->field_name();

    std::unique_ptr<Expression> &right_expr = comparison_expr->right();
    ValueExpr *right_value_expr = static_cast<ValueExpr *>(right_expr.get());
    int right_value = right_value_expr->get_value().get_int();
    
    for(std::vector<Value> data: reltatistics_data){  //找寻匹配的直方图
      if(data[0].to_string()==left_table_name && data[1].to_string() ==left_field_name){
        int record_num = data[4].get_int();
        tableget->setRecordNum(record_num);
        int num;
        int relpages = 100;
        string str = data[3].to_string(); //直方图的数据
        std::regex reg("\\\\((\\\\d+),(\\\\d+)\\\\)");
        std::string tmp(str);
        std::vector<std::vector<int>> res;
        while (std::regex_search(tmp, reg)) {
            std::smatch match;
            std::string match_str = match.str();
            std::stringstream ss(match_str.substr(1, match_str.size() - 2));
            int a, b;
            ss >> a >> b;
            res.push_back({a, b});
            tmp = match.suffix().str();
        }

        auto it = std::find_if(res.begin(), res.end(),
                           [right_value](const std::vector<int> &interval) {
                               return interval[1] >= right_value;
                           });
        if (it != res.begin()) {
          num = it - res.begin() + 1;
        } else {
          num = data[2].get_int(); //不在区间内
        }
        chose_ratio = num / data[2].get_int() ;  //选择率计算


        //开始计算代价
        double seq_page_cost = 1.0;
        double random_page_cost = 4.0;
        double cpu_tuple_cost = 0.01;
        double cpu_index_tuple_cost = 0.005;
        double cpu_operator_cost = 0.0025;
       

        double full_scan_cost = (cpu_tuple_cost + cpu_operator_cost) * record_num + seq_page_cost * relpages;

        double temp_cost = chose_ratio*record_num*(cpu_tuple_cost+ seq_page_cost); 
        
        tableget->setCost(full_scan_cost + temp_cost);
      }
    }
  }

  return rc;
}

RC OptimizeStage::sort_operator(vector<TableGetLogicalOperator *> &tablegets){
    size_t n = tablegets.size();
    std::vector<std::vector<double>> dp(n, std::vector<double>(n, 0.0));
    std::vector<std::vector<size_t>> path(n, std::vector<size_t>(n, 0));

    // 初始化单个操作符的代价
    for (size_t i = 0; i < n; ++i) {
        dp[i][i] = tablegets[i]->getcost();
    }

    // 计算所有可能的子序列的代价
    for (size_t len = 2; len <= n; ++len) {
        for (size_t i = 0; i <= n - len; ++i) {
            size_t j = i + len - 1;
            dp[i][j] = std::numeric_limits<double>::max();
            for (size_t k = i; k < j; ++k) {
                double newCost = dp[i][k] + dp[k+1][j] + (tablegets[i]->getcost()+ tablegets[j]->getcost());
                if (newCost < dp[i][j]) {
                    dp[i][j] = newCost;
                    path[i][j] = k;
                }
            }
        }
    }

    // 重构最佳排列顺序
    std::vector<size_t> order(n);
    std::vector<bool> visited(n, false);
    size_t start = 0;
    for (size_t i = 0; i < n; ++i) {
        if (dp[start][i] < dp[start][i+1]) {
            order[i] = start;
            visited[start] = true;
            start = i + 1;
        }
    }

    // 填充剩余的元素
    size_t index = 0;
    for (size_t i = 0; i < n; ++i) {
        if (!visited[i]) {
            while (visited[index]) ++index;
            order[i] = index++;
        }
    }
  return RC::SUCCESS;
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
