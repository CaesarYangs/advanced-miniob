#pragma once

#include "common/rc.h"
#include "sql/parser/parse_defs.h"
#include "stmt.h"
#include "storage/db/db.h"
#include "storage/field/field.h"
#include <unordered_map>
#include <vector>

class Db;
class Table;
class FieldMeta;

class OrderByUnit
{
public:
  OrderByUnit()  = default;
  ~OrderByUnit() = default;
  void             set_table(Table *table);
  void             set_order(OrderType type);
  void             set_field_meta(const FieldMeta *&filed_meta);
  bool             get_asc() const { return is_asc; };
  const FieldMeta *get_fields() const { return &order_field; }
  Table           *get_table() const { return order_table_; }

private:
  Table    *order_table_;
  FieldMeta order_field;
  bool      is_asc = true;
};

/**
 * @brief 表示OrderBy的stmt
 */
class OrderByStmt : public Stmt
{
public:
  OrderByStmt() = default;
  virtual ~OrderByStmt();
  StmtType type() const override { return StmtType::ORDER_BY; }

public:
  static RC create(
      Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables, const OrderSqlNode *orderbys,
      int order_num, OrderByStmt *&stmt
  );
  static RC create_orderby_unit(
      Db *db, Table *default_table, std::unordered_map<std::string, Table *> *tables, const OrderSqlNode &orderby,
      OrderByUnit *&order_unit
  );

public:
  const std::vector<OrderByUnit *> &order_units() const { return order_units_; }

private:
  std::vector<OrderByUnit *> order_units_;
};