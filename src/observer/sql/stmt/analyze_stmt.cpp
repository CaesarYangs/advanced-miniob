#include "sql/stmt/analyze_stmt.h"
#include "common/log/log.h"
#include "storage/db/db.h"
#include "sql/stmt/filter_stmt.h"
#include "storage/table/table.h"

AnalyzeStmt::AnalyzeStmt(Table *table) : table_(table) {}

AnalyzeStmt::AnalyzeStmt(Table *analyze_table, Table *table, std::vector<Field *> query_fields)
    : analyze_table_(analyze_table), table_(table), query_fields_(query_fields)
{}

RC AnalyzeStmt::create(Db *db, const AnalyzeSqlNode &analyze_sql, Stmt *&stmt)
{
  // default table name
  const char *analyze_table_name = "relstatistics";

  // check the sql input
  const char *table_name = analyze_sql.relation_name.c_str();
  if (nullptr == db || nullptr == table_name) {
    LOG_WARN("invalid argument. db=%p, table_name=%p",
        db, table_name);
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  // check whether the table exists
  Table *analyze_table = db->find_table(analyze_table_name);
  if (nullptr == analyze_table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), analyze_table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  LOG_DEBUG("TEST ANALYZE FROM STMT: ");

  const TableMeta         &table_meta       = table->table_meta();
  const int                field_num        = table_meta.field_num();
  std::vector<std::string> sql_query_fields = analyze_sql.attribute_name;
  std::vector<Field *>     query_fields;

  // validate query fields
  bool is_match;
  for (std::string field_name : sql_query_fields) {
    is_match = false;
    for (int i = table_meta.sys_field_num(); i < field_num; i++) {
      if (strcmp(table_meta.field(i)->name(), field_name.c_str()) == 0) {
        LOG_DEBUG("[[[TEST]]] match from: %s",field_name);
        Field *query_field = new Field(table, table_meta.field(i));
        query_fields.push_back(query_field);
        is_match = true;
        break;
      }
    }
    if (!is_match) {
      LOG_DEBUG("[[[[TEST]]]] SCHEMA_FIELD_NOT_EXIST");
      return RC::SCHEMA_FIELD_NOT_EXIST;
    }
  }

  LOG_DEBUG("Got analyze table: %s",analyze_table->name());

  for (auto i : query_fields) {
    // print out field names
    LOG_DEBUG("Got field name: %s",i->field_name());
  }

  AnalyzeStmt *analyze_stmt = new AnalyzeStmt(analyze_table, table, query_fields);
  stmt                      = analyze_stmt;
  return RC::SUCCESS;
}