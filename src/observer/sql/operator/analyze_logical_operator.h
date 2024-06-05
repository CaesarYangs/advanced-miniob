#pragma once

#include <vector>

#include "sql/operator/logical_operator.h"
#include "sql/parser/parse_defs.h"

class AnalyzeLogicalOperator : public LogicalOperator
{
public:
  AnalyzeLogicalOperator(Table *analyze_table ,Table *table, std::vector<Field *> query_fields);
  virtual ~AnalyzeLogicalOperator() = default;

  LogicalOperatorType type() const override { return LogicalOperatorType::ANALYZE; }

  Table               *table() const { return table_; }
  Table               *analyze_table() const { return analyze_table_; }
  std::vector<Field *> query_fields() { return query_fields_; }

private:
  Table               *analyze_table_ = nullptr;
  Table               *table_         = nullptr;
  std::vector<Field *> query_fields_;
};