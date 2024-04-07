
#include "sql/operator/update_logical_operator.h"

UpdateLogicalOperator::UpdateLogicalOperator(Table *table, const Value *values, Field *field)
    : table_(table), values_(values), field_(field)
{}
