#pragma once

#include "common/rc.h"
#include "sql/stmt/stmt.h"

class Table;
class Db;
class Field;

class AnalyzeStmt : public Stmt
{
public:
  AnalyzeStmt(Table *table);
  AnalyzeStmt(Table *analyze_table, Table *table, std::vector<Field *> query_fields);
  AnalyzeStmt() = default;
  StmtType type() const override { return StmtType::ANALYZE; }

  static RC create(Db *db, const AnalyzeSqlNode &analyze_sql, Stmt *&stmt);

  Table               *analyze_table() const { return analyze_table_; }
  Table               *table() const { return table_; }
  std::vector<Field *> query_fields() const { return query_fields_; }

private:
  Table               *analyze_table_ = nullptr;
  Table               *table_         = nullptr;
  std::vector<Field *> query_fields_;
};