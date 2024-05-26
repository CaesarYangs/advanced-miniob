
#include "sql/operator/analyze_logical_operator.h"

AnalyzeLogicalOperator::AnalyzeLogicalOperator(Table *analyze_table, Table *table, std::vector<Field *> query_fields)
    : analyze_table_(analyze_table), table_(table), query_fields_(query_fields)
{}