#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

/**
 * @brief Update逻辑算子
 * @ingroup LogicalOperator
 */
class UpdateLogicalOperator : public LogicalOperator
{
public:
  UpdateLogicalOperator(Table *table, const Value *values, Field *field);
  virtual ~UpdateLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::UPDATE; }

  Table *table() const { return table_; }
  const Value *values() const { return values_; }
  // Value *values() { return values_; }
  Field *field() const { return field_; }

private:
  Table *table_ = nullptr;
  const Value *values_;  // 为什么他们都不用地址传递，决定了，你这就用地址传递
  Field *field_ = nullptr;
};