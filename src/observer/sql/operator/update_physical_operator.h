#pragma once

#include "sql/operator/physical_operator.h"
#include "sql/parser/parse.h"
#include <vector>

class UpdateStmt;

/**
 * @brief 插入物理算子
 * @ingroup PhysicalOperator
 */
class UpdatePhysicalOperator : public PhysicalOperator
{
public:
  UpdatePhysicalOperator() {}
  UpdatePhysicalOperator(Table *table, const Value *values, Field *field);

  virtual ~UpdatePhysicalOperator();

  PhysicalOperatorType type() const override { return PhysicalOperatorType::UPDATE; }

  RC open(Trx *trx) override;
  RC next() override;
  RC close() override;

  Tuple *current_tuple() override { return nullptr; }

private:
  Table       *table_ = nullptr;
  const Value *values_;
  Field       *field_ = nullptr;
  Trx         *trx_   = nullptr;
  char        *data_;
};