#include "sql/operator/analyze_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include <iomanip>

using namespace std;

AnalyzePhysicalOperator::AnalyzePhysicalOperator(
    Table *analyze_table, Table *table, std::vector<Field *> analyze_fields)
    : analyze_table_(analyze_table), table_(table), analyze_fields_(analyze_fields)
{}

RC AnalyzePhysicalOperator::open(Trx *trx)
{

  LOG_DEBUG("[[[[[[[[[[[AnalyzePhysicalOperator open start]]]]]]]]]]]");
  if (children_.empty()) {
    LOG_DEBUG("[[[[[[[[[[[lalala]]]]]]]]]]]");
    return RC::SUCCESS;
  }

  // 1. get value from table
  std::vector<Record *> origianl_records;

  std::unique_ptr<PhysicalOperator> &child = children_[0];
  RC                                 rc    = child->open(trx);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to open child operator: %s", strrc(rc));
    return rc;
  }
  trx_ = trx;

  // 2. analyze and calculate
  std::vector<const Value *> analyzed_values;

  // 获取全部使用的FieldMetas
  const std::vector<FieldMeta> *field_metas = table_->table_meta().field_metas();  // table 原始field数据

  for (auto ana_field : analyze_fields_) {
    for (auto field : *field_metas) {
      if (strcmp(field.name(), ana_field->field_name()) == 0) {
        analyze_field_metas_.push_back(field);
        break;
      }
    }
  }

  for (auto i : analyze_field_metas_) {
    LOG_DEBUG("[[[[TEST]]]] name:%s, offset:%d",i.name(),i.offset());
  }

  // 3. insert into analyze table. 插入到统一的分析表中
  //   Record record;
  //   RC     rc = analyze_table_->make_record(static_cast<int>(analyzed_values.size()), analyzed_values[0],record);
  //   if (rc != RC::SUCCESS) {
  //     LOG_WARN("failed to make record. rc=%s", strrc(rc));
  //     return rc;
  //   }

  //   rc = trx->insert_record(analyze_table_, record);
  //   if (rc != RC::SUCCESS) {
  //     LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
  //   }
  return RC::SUCCESS;
}

RC AnalyzePhysicalOperator::next()
{
  LOG_DEBUG("PASS HERE");
  RC rc = RC::SUCCESS;
  if (children_.empty()) {
    return RC::RECORD_EOF;
  }
  LOG_DEBUG("PASS HERE2");

  PhysicalOperator *child = children_[0].get();

  while (RC::SUCCESS == (rc = child->next())) {

    Tuple *tuple = child->current_tuple();
    if (nullptr == tuple) {
      LOG_WARN("failed to get current record: %s", strrc(rc));
      return rc;
    }

    RowTuple *row_tuple = static_cast<RowTuple *>(tuple);
    Record   &record    = row_tuple->record();

    // 获取数据内容的核心函数
    rc = table_->get_record(record.rid(), record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to get record: %s", strrc(rc));
      return rc;
    }

    std::vector<Value> tuple_value;
    std::string        str;  // 在switch语句之前声明
    // TODO: 解决此处hardcode问题
    for (auto field : analyze_field_metas_) {
      Value v;
      //   LOG_DEBUG("Field Stat: name:%s, offset:%d, len:%d",field.name(),field.offset(),field.len());

      switch (field.type()) {
        case INTS:
          int data;
          memcpy(&data, record.data() + field.offset(), sizeof(int));
          v.set_int(data);
          LOG_DEBUG("data now: %d",data);
          break;

        case CHARS: {
          size_t max_len    = field.len();
          size_t actual_len = strnlen(record.data() + field.offset(), max_len);
          str.assign(record.data() + field.offset(), actual_len);
          if (actual_len == max_len && str.back() != '\0') {
            if (actual_len < str.max_size()) {
              str.push_back('\0');
            } else {
              str[actual_len - 1] = '\0';
            }
          }
          v.set_string(str.c_str());
          LOG_DEBUG("data now: %s", str.c_str());
          break;
        }

        case FLOATS:
          float float_data;
          memcpy(&float_data, record.data() + field.offset(), field.len());
          v.set_float(float_data);
          LOG_DEBUG("data now: %f",data);
          break;

        default: break;
      }
      tuple_value.push_back(v);
    }
    data_matrix_.push_back(tuple_value);

    LOG_DEBUG("TEST: record_len: %d,rid: %s",record.len(),record.rid().to_string().c_str());
  }

  LOG_DEBUG("record size: %d",data_matrix_.size());

  return RC::RECORD_EOF;
}

RC AnalyzePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}