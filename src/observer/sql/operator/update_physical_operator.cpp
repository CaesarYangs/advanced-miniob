#include "sql/operator/update_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"

using namespace std;

// TODO#1
UpdatePhysicalOperator::UpdatePhysicalOperator(Table *table, const Value *values, Field *field)
    : table_(table), values_(values), field_(field)
{}

UpdatePhysicalOperator::~UpdatePhysicalOperator() {}

RC UpdatePhysicalOperator::open(Trx *trx)
{
  // test
  if (table_ != nullptr && field_ != nullptr) {

    LOG_DEBUG("[[[[[[[[[[[UpdatePhysicalOperator]]]]]]]]]]] table:%s, field:%s, value:%s",table_->name(),field_->field_name(),values_->get_string());
  }

  if (children_.empty()) {
    return RC::SUCCESS;
  }

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }

  trx_ = trx;

  return RC::SUCCESS;
}

RC UpdatePhysicalOperator::next()
{
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }

  PhysicalOperator *child = children_[0].get();
  while (RC::SUCCESS == (rc = child->next())) {
    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();
    LOG_DEBUG("[[[[[[[[[[[[[[[UpdatePhysicalOperator]]]]]]]]]]]]]]]");
    rc = trx_->update_record(table_, field_, values_, record);  // real update section
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to delete record: %s", strrc(rc));
      return rc;
    }
  }

  LOG_DEBUG("(([[UpdatePhysicalOperator not imp]]))");

  return RC::RECORD_EOF;
}

RC UpdatePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}
