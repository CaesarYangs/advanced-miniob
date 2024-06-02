#pragma once

#include <memory>

#include "common/rc.h"
#include "sql/expr/expression.h"
#include "sql/operator/logical_operator.h"
#include "sql/optimizer/rewrite_rule.h"

RC calculate_cost(Table* table);

RC set_table_optimization(std::vector<Table *> &table_list);