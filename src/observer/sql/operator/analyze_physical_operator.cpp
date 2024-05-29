#include "sql/operator/analyze_physical_operator.h"
#include "sql/stmt/insert_stmt.h"
#include "storage/table/table.h"
#include "storage/trx/trx.h"
#include <iomanip>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>
#include <unordered_map>
#include <sstream>

using namespace std;

std::string        buildHistogram(const std::vector<std::vector<Value>> &data_matrix, int num_buckets);
std::vector<Value> reservoirSampling(const std::vector<std::vector<Value>> &data_matrix, int sample_size);
int                calculateSampleSize(int num_records);
std::string        storeHistogram(
           int num_buckets, std::vector<int> &histogram, std::vector<double> &bucket_boundaries, int num_records);

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

  // 数据读取完毕
  LOG_DEBUG("record size: %d",data_matrix_.size());

  // TODO 数据处理部分 —— 逐个列生成histogram
  for (int i = 0; i < analyze_fields_.size(); i++) {
    int         num_bucket = 10;
    std::string histogram  = buildHistogram(data_matrix_, num_bucket);

    // 分析好的数据插入到统一的记录表
    int                res_record_size = analyze_table_->table_meta().record_size();
    int                res_record_num  = analyze_table_->table_meta().field_num();
    std::vector<Value> res_values;

    Value v_table_name, v_column_name, v_bucket_num, v_histogtram, v_record_num;

    // 准备数据到最终的分析表中
    v_table_name.set_string(table_->name());
    v_column_name.set_string(analyze_fields_[i]->field_name());
    v_bucket_num.set_int(num_bucket);
    v_histogtram.set_string(histogram.c_str());
    v_record_num.set_int(data_matrix_.size());

    res_values.push_back(v_table_name);
    res_values.push_back(v_column_name);
    res_values.push_back(v_bucket_num);
    res_values.push_back(v_histogtram);
    res_values.push_back(v_record_num);

    // 创建record并写入
    Record res_record;
    rc = analyze_table_->make_record(static_cast<int>(res_record_num), res_values.data(), res_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to make record. rc=%s", strrc(rc));
      return rc;
    }

    rc = trx_->insert_record(analyze_table_, res_record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record by transaction. rc=%s", strrc(rc));
    }
  }

  return RC::RECORD_EOF;
}

RC AnalyzePhysicalOperator::close()
{
  if (!children_.empty()) {
    children_[0]->close();
  }
  return RC::SUCCESS;
}

// 生成直方图部分
std::string buildHistogram(const std::vector<std::vector<Value>> &data_matrix, int num_buckets)
{
  int                num_records = data_matrix.size();
  int                sample_size = calculateSampleSize(num_records);
  std::vector<Value> samples     = reservoirSampling(data_matrix, sample_size);
  std::sort(samples.begin(), samples.end());

  Value  min_value    = samples.front();
  Value  max_value    = samples.back();
  double value_range  = max_value.get_int() - min_value.get_int();
  double bucket_width = value_range / num_buckets;

  std::vector<double> bucket_boundaries(num_buckets + 1);
  bucket_boundaries[0] = min_value.get_int();
  for (int i = 1; i < num_buckets; ++i) {
    double boundary = min_value.get_int() + i * bucket_width;
    auto   it       = std::lower_bound(samples.begin(), samples.end(), Value(static_cast<int>(boundary)));
    if (it != samples.end()) {
      bucket_boundaries[i] = it->get_int();
    } else {
      bucket_boundaries[i] = max_value.get_int();
    }
  }
  bucket_boundaries[num_buckets] = max_value.get_int();

  std::vector<int> histogram(num_buckets, 0);
  int              current_bucket = 0;
  for (const Value &value : samples) {
    while (current_bucket < num_buckets - 1 && value.get_int() >= bucket_boundaries[current_bucket + 1]) {
      current_bucket++;
    }
    histogram[current_bucket]++;
  }

  std::string histogram_data = storeHistogram(num_buckets, histogram, bucket_boundaries, num_records);
  return histogram_data;
}

int calculateSampleSize(int num_records)
{
  if (num_records <= 1000) {
    return num_records;
  } else if (num_records <= 10000) {
    return 1000;
  } else if (num_records <= 100000) {
    return 5000;
  } else {
    return 10000;
  }
}

std::vector<Value> reservoirSampling(const std::vector<std::vector<Value>> &data_matrix, int sample_size)
{
  std::vector<Value> samples;
  int                num_records = data_matrix.size();

  unsigned                           seed = std::chrono::system_clock::now().time_since_epoch().count();
  std::default_random_engine         generator(seed);
  std::uniform_int_distribution<int> distribution(0, num_records - 1);

  for (int i = 0; i < sample_size; ++i) {
    int index = distribution(generator);
    samples.push_back(data_matrix[index][0]);
  }

  return samples;
}

void deleteExistingHistogram(const std::string &table_name, const std::string &column_name)
{
  // 删除已存在的直方图数据的逻辑,根据实际存储方式进行实现
  // 例如,从数据库中删除对应表和列的直方图记录
}

std::string storeHistogram(
    int num_buckets, std::vector<int> &histogram, std::vector<double> &bucket_boundaries, int num_records)
{
  std::stringstream ss;

  for (int i = 0; i < num_buckets; ++i) {
    ss << "(" << bucket_boundaries[i] << "," << bucket_boundaries[i + 1] << ")";
    if (i < num_buckets - 1) {
      ss << ",";
    }
  }

  return ss.str();
}