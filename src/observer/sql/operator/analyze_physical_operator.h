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
// Created by WangYunlai on 2021/6/7.
//

#pragma once

#include "storage/record/record_manager.h"
#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include <vector>

class AnalyzeStmt;
class table;

/**
 * @brief 插入物理算子
 * @ingroup PhysicalOperator
 */
class AnalyzePhysicalOperator : public PhysicalOperator
{
public:
  AnalyzePhysicalOperator(Table *analyze_table, Table *table, std::vector<Field *> analyze_fields);

  virtual ~AnalyzePhysicalOperator() = default;

  PhysicalOperatorType type() const override { return PhysicalOperatorType::ANALYZE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table                          *analyze_table_ = nullptr;
  Table                          *table_         = nullptr;
  std::vector<Field *>            analyze_fields_;
  Trx                            *trx_ = nullptr;
  std::vector<std::vector<Value>> data_matrix_;
  std::vector<FieldMeta>          analyze_field_metas_;  // 全部使用的FieldMetas
};
