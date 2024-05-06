/* Copyright (c) 2021 Xie Meiyi(xiemeiyi@hust.edu.cn) and OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Meiyi & Wangyunlai on 2021/5/13.
//

#include <algorithm>
#include <limits.h>
#include <string.h>

#include "common/defs.h"
#include "common/lang/string.h"
#include "common/log/log.h"
#include "storage/buffer/disk_buffer_pool.h"
#include "storage/common/condition_filter.h"
#include "storage/common/meta_util.h"
#include "storage/index/bplus_tree_index.h"
#include "storage/index/index.h"
#include "storage/record/record_manager.h"
#include "storage/table/table.h"
#include "storage/table/table_meta.h"
#include "storage/trx/trx.h"
#include "storage/field/field.h"

RC handle_text_value(RecordFileHandler *record_handler, Value &value);
Table::~Table()
{
  if (record_handler_ != nullptr) {
    delete record_handler_;
    record_handler_ = nullptr;
  }

  if (data_buffer_pool_ != nullptr) {
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
  }

  for (std::vector<Index *>::iterator it = indexes_.begin(); it != indexes_.end(); ++it) {
    Index *index = *it;
    delete index;
  }
  indexes_.clear();

  LOG_INFO("Table has been closed: %s", name());
}

RC Table::create(int32_t table_id, const char *path, const char *name, const char *base_dir, int attribute_count,
    const AttrInfoSqlNode attributes[])
{
  if (table_id < 0) {
    LOG_WARN("invalid table id. table_id=%d, table_name=%s", table_id, name);
    return RC::INVALID_ARGUMENT;
  }

  if (common::is_blank(name)) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to create table %s:%s", base_dir, name);

  if (attribute_count <= 0 || nullptr == attributes) {
    LOG_WARN("Invalid arguments. table_name=%s, attribute_count=%d, attributes=%p", name, attribute_count, attributes);
    return RC::INVALID_ARGUMENT;
  }

  RC rc = RC::SUCCESS;

  // 使用 table_name.table记录一个表的元数据
  // 判断表文件是否已经存在
  int fd = ::open(path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (fd < 0) {
    if (EEXIST == errno) {
      LOG_ERROR("Failed to create table file, it has been created. %s, EEXIST, %s", path, strerror(errno));
      return RC::SCHEMA_TABLE_EXIST;
    }
    LOG_ERROR("Create table file failed. filename=%s, errmsg=%d:%s", path, errno, strerror(errno));
    return RC::IOERR_OPEN;
  }

  close(fd);

  // 创建文件
  if ((rc = table_meta_.init(table_id, name, attribute_count, attributes)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init table meta. name:%s, ret:%d", name, rc);
    return rc;  // delete table file
  }

  std::fstream fs;
  fs.open(path, std::ios_base::out | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", path, strerror(errno));
    return RC::IOERR_OPEN;
  }

  // 记录元数据到文件中
  table_meta_.serialize(fs);
  fs.close();

  std::string        data_file = table_data_file(base_dir, name);
  BufferPoolManager &bpm       = BufferPoolManager::instance();
  rc                           = bpm.create_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to create table %s due to init record handler failed.", data_file.c_str());
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;
  LOG_INFO("Successfully create table %s:%s", base_dir, name);
  return rc;
}

/**
 * Drop table 删除文件及其索引
 *
 * @param  {char*} path     :
 * @param  {char*} name     :
 * @param  {char*} base_dir :
 * @return {RC}             :
 */
RC Table::drop(const char *path, const char *name, const char *base_dir)
{
  RC rc = RC::SUCCESS;  // 声明返回值

  // 检查输入名称是否为空
  if (common::is_blank(name) || name == nullptr) {
    LOG_WARN("Name cannot be empty");
    return RC::INVALID_ARGUMENT;
  }
  LOG_INFO("Begin to drop table %s:%s", base_dir, name);

  // 找到数据文件并在buffer pool中关闭
  std::string data_file = std::string(base_dir) + "/" + name + TABLE_DATA_SUFFIX;
  rc                    = data_buffer_pool_->close_file();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to drop disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }
  // 将数据文件从buffer pool中删除
  rc = data_buffer_pool_->drop_file(data_file.c_str());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to drop disk buffer pool of data file. file name=%s", data_file.c_str());
    return rc;
  }

  // 删除索引文件
  for (std::vector<Index *>::size_type i = 0; i < indexes_.size(); i++) {
    std::string index_file = table_index_file(base_dir_.c_str(), name, indexes_[i]->index_meta().name());
    rc                     = reinterpret_cast<BplusTreeIndex *>(indexes_[i])->close();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to close disk buffer pool of index file. file name=%s", index_file.c_str());
      return rc;
    }
    rc = data_buffer_pool_->drop_file(index_file.c_str());
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to drop disk buffer pool of index file. file name=%s", index_file.c_str());
      return rc;
    }
  }

  // 真正删除该数据表
  int fd = ::unlink(path);
  if (-1 == fd) {
    return RC::IOERR_CLOSE;
  }

  return rc;  // success
}

RC Table::open(const char *meta_file, const char *base_dir)
{
  // 加载元数据文件
  std::fstream fs;
  std::string  meta_file_path = std::string(base_dir) + common::FILE_PATH_SPLIT_STR + meta_file;
  fs.open(meta_file_path, std::ios_base::in | std::ios_base::binary);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open meta file for read. file name=%s, errmsg=%s", meta_file_path.c_str(), strerror(errno));
    return RC::IOERR_OPEN;
  }
  if (table_meta_.deserialize(fs) < 0) {
    LOG_ERROR("Failed to deserialize table meta. file name=%s", meta_file_path.c_str());
    fs.close();
    return RC::INTERNAL;
  }
  fs.close();

  // 加载数据文件
  RC rc = init_record_handler(base_dir);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open table %s due to init record handler failed.", base_dir);
    // don't need to remove the data_file
    return rc;
  }

  base_dir_ = base_dir;

  // TODO-multiIndex 最后修改加载索引文件部分
  // https://github.com/luooofan/miniob-2022/commit/69a11aed8800b988e9a1910fd7929f46f62f86f7#diff-5f02ea5561ca0e44a0c24e9a0a2d5c3a1cc875fee6fdc4bba151abd72a7e9b9c
  const int index_num = table_meta_.index_num();
  for (int i = 0; i < index_num; i++) {
    const IndexMeta                *index_meta        = table_meta_.index(i);
    const std::vector<std::string> *index_field_names = index_meta->fields();
    std::vector<FieldMeta>         *field_metas       = new vector<FieldMeta>;

    for (unsigned long int i = 0; i < index_field_names->size(); i++) {
      const char      *field_name = index_field_names->at(i).data();
      const FieldMeta *field_meta = table_meta_.field(field_name);
      if (field_meta == nullptr) {
        LOG_ERROR("Found invalid index meta info which has a non-exists field. table=%s, index=%s, field=%s",
            name(),
            index_meta->name(),
            index_meta->field());
        // skip cleanup
        //  do all cleanup action in destructive Table function
        return RC::INTERNAL;
      }
      field_metas->push_back(*field_meta);
    }

    BplusTreeIndex *index      = new BplusTreeIndex();
    std::string     index_file = table_index_file(base_dir, name(), index_meta->name());

    rc = index->open(index_file.c_str(), *index_meta, *field_metas);
    if (rc != RC::SUCCESS) {
      delete index;
      LOG_ERROR("Failed to open index. table=%s, index=%s, file=%s, rc=%s",
                name(), index_meta->name(), index_file.c_str(), strrc(rc));
      // skip cleanup
      //  do all cleanup action in destructive Table function.
      return rc;
    }
    indexes_.push_back(index);
  }

  return rc;
}

RC Table::insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->insert_record(record.data(), table_meta_.record_size(), &record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }else{
      LOG_TRACE("Insert record FILEHandler insert_record agin!!!!!!!!!))))))))))=>>");
  }
  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      // LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
      //           name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      // LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
      //           name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

RC Table::visit_record(const RID &rid, bool readonly, std::function<void(Record &)> visitor)
{
  return record_handler_->visit_record(rid, readonly, visitor);
}

RC Table::get_record(const RID &rid, Record &record)
{
  int record_size = table_meta_.record_size();
  if (rid.init) {
    LOG_TRACE("rid init))))))))))=>>>");
    record_size = rid.over_len; 
  }
  char     *record_data = (char *)malloc(record_size);
  ASSERT(nullptr != record_data, "failed to malloc memory. record data size=%d", record_size);

  auto copier = [&record, record_data, record_size](Record &record_src) {
    memcpy(record_data, record_src.data(), record_size);
    record.set_rid(record_src.rid());
  };
  RC rc = record_handler_->visit_record(rid, true /*readonly*/, copier);
  if (rc != RC::SUCCESS) {
    free(record_data);
    LOG_WARN("failed to visit record. rid=%s, table=%s, rc=%s", rid.to_string().c_str(), name(), strrc(rc));
    return rc;
  }else{
    LOG_TRACE("get_text_record 11))))))))))=>>>");
  }

  record.set_data_owner(record_data, record_size);
  return rc;
}

RC Table::recover_insert_record(Record &record)
{
  RC rc = RC::SUCCESS;
  rc    = record_handler_->recover_insert_record(record.data(), table_meta_.record_size(), record.rid());
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Insert record failed. table name=%s, rc=%s", table_meta_.name(), strrc(rc));
    return rc;
  }

  rc = insert_entry_of_indexes(record.data(), record.rid());
  if (rc != RC::SUCCESS) {  // 可能出现了键值重复
    RC rc2 = delete_entry_of_indexes(record.data(), record.rid(), false /*error_on_not_exists*/);
    if (rc2 != RC::SUCCESS) {
      LOG_ERROR("Failed to rollback index data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
    rc2 = record_handler_->delete_record(&record.rid());
    if (rc2 != RC::SUCCESS) {
      LOG_PANIC("Failed to rollback record data when insert index entries failed. table name=%s, rc=%d:%s",
                name(), rc2, strrc(rc2));
    }
  }
  return rc;
}

const char *Table::name() const { return table_meta_.name(); }

const TableMeta &Table::table_meta() const { return table_meta_; }

RC Table::make_record(int value_num, const Value *values, Record &record)
{
  // 检查字段类型是否一致
  if (value_num + table_meta_.sys_field_num() != table_meta_.field_num()) {
    LOG_WARN("Input values don't match the table's schema, table name:%s", table_meta_.name());
    return RC::SCHEMA_FIELD_MISSING;
  }

  const int normal_field_start_index = table_meta_.sys_field_num();
  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field = table_meta_.field(i + normal_field_start_index);
    Value           &value = const_cast<Value &>(values[i]);
    if (field->type() != value.attr_type()) {
      if (!Value::convert(value.attr_type(), field->type(), value)) {
        LOG_ERROR("Invalid value type. table name =%s, field name=%s, type=%d, but given=%d", table_meta_.name(),
                  field->name(), field->type(), value.attr_type());
        return RC::SCHEMA_FIELD_TYPE_MISMATCH;
      }
      }
      if (!field->match(value)) {
      LOG_ERROR("Invalid value type. table name=%s, field name=%s, type=%d, but given=%d",
                table_meta_.name(), field->name(), field->type(), value.attr_type());
      return RC::SCHEMA_FIELD_TYPE_MISMATCH;
    }else{
      LOG_TRACE("Insert record. make_record))))))))))=>>>%s",value.get_string());
    }
  }

  // 复制所有字段的值
  int   record_size = table_meta_.record_size();
  char *record_data = (char *)malloc(record_size);

  //int column = table_meta_.field_num();

  for (int i = 0; i < value_num; i++) {
    const FieldMeta *field    = table_meta_.field(i + normal_field_start_index);
    Value &value = const_cast<Value &>(values[i]);
// 如果是字符串类型，则拷贝字符串的len + 1字节。
    if (value.attr_type() == TEXTS) {
      if (value.get_string().length() > 65535) {
        return RC::TEXT_OVERFLOW;
      }
      handle_text_value(record_handler_, value); //如果现在的字段是text,那么立马进行插入,并且将value的值变成rid指针
      record.add_offset_text(field->offset());
      record.set_if_text();
    }


    size_t           copy_len = field->len();
    if (field->type() == CHARS) {
      const size_t data_len = value.length();
      if (copy_len > data_len) {
        copy_len = data_len + 1;
      }
      if (field->type() == TEXTS) {
        copy_len = sizeof(RID); //拷贝RID大小，因为data就是RID
      }
    }
    
    memcpy(record_data + field->offset(), value.data(), copy_len);

    // // 根据字段类型打印字段值
    // switch (field->type()) {
    //   case INTS: {
    //     int val = *(int *)(record_data + field->offset());
    //     LOG_DEBUG("Field %d: %d\n", i, val);
    //     break;
    //   }
    //   case FLOATS: {
    //     float val = *(float *)(record_data + field->offset());
    //     LOG_DEBUG("Field %d: %.2f\n", i, val);
    //     break;
    //   }
    //   case CHARS: {
    //     char *val = record_data + field->offset();
    //     LOG_DEBUG("Field %d: %s\n", i, val);
    //     break;
    //   }
    // }
  }
  record.set_data_owner(record_data, record_size);
  return RC::SUCCESS;
}

RC Table::init_record_handler(const char *base_dir)
{
  std::string data_file = table_data_file(base_dir, table_meta_.name());

  RC rc = BufferPoolManager::instance().open_file(data_file.c_str(), data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open disk buffer pool for file:%s. rc=%d:%s", data_file.c_str(), rc, strrc(rc));
    return rc;
  }

  record_handler_ = new RecordFileHandler();

  rc = record_handler_->init(data_buffer_pool_);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to init record handler. rc=%s", strrc(rc));
    data_buffer_pool_->close_file();
    data_buffer_pool_ = nullptr;
    delete record_handler_;
    record_handler_ = nullptr;
    return rc;
  }

  return rc;
}

RC Table::get_record_scanner(RecordFileScanner &scanner, Trx *trx, bool readonly)
{
  RC rc = scanner.open_scan(this, *data_buffer_pool_, trx, readonly, nullptr);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("failed to open scanner. rc=%s", strrc(rc));
  }
  return rc;
}

// backup legacy create index function
// RC Table::create_index(Trx *trx, const FieldMeta *field_meta, const char *index_name)
// {
//   if (common::is_blank(index_name) || nullptr == field_meta) {
//     LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or attribute_name is blank", name());
//     return RC::INVALID_ARGUMENT;
//   }

//   IndexMeta new_index_meta;

//   RC rc = new_index_meta.init(index_name, *field_meta);
//   if (rc != RC::SUCCESS) {
//     LOG_INFO("Failed to init IndexMeta in table:%s, index_name:%s, field_name:%s",
//              name(), index_name, field_meta->name());
//     return rc;
//   }

//   // 创建索引相关数据
//   BplusTreeIndex *index      = new BplusTreeIndex();
//   std::string     index_file = table_index_file(base_dir_.c_str(), name(), index_name);

//   rc = index->create(index_file.c_str(), new_index_meta, *field_meta);
//   if (rc != RC::SUCCESS) {
//     delete index;
//     LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
//     return rc;
//   }

//   // 遍历当前的所有数据，插入这个索引
//   RecordFileScanner scanner;
//   rc = get_record_scanner(scanner, trx, true /*readonly*/);
//   if (rc != RC::SUCCESS) {
//     LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s",
//              name(), index_name, strrc(rc));
//     return rc;
//   }

//   Record record;
//   while (scanner.has_next()) {
//     rc = scanner.next(record);
//     if (rc != RC::SUCCESS) {
//       LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s",
//                name(), index_name, strrc(rc));
//       return rc;
//     }
//     rc = index->insert_entry(record.data(), &record.rid());
//     if (rc != RC::SUCCESS) {
//       LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
//                name(), index_name, strrc(rc));
//       return rc;
//     }
//   }
//   scanner.close_scan();
//   LOG_INFO("inserted all records into new index. table=%s, index=%s", name(), index_name);

//   indexes_.push_back(index);

//   /// 接下来将这个索引放到表的元数据中
//   TableMeta new_table_meta(table_meta_);
//   rc = new_table_meta.add_index(new_index_meta);
//   if (rc != RC::SUCCESS) {
//     LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
//     return rc;
//   }

//   /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
//   /// 这样可以防止文件内容不完整
//   // 创建元数据临时文件
//   std::string  tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
//   std::fstream fs;
//   fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
//   if (!fs.is_open()) {
//     LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
//     return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
//   }
//   if (new_table_meta.serialize(fs) < 0) {
//     LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
//     return RC::IOERR_WRITE;
//   }
//   fs.close();

//   // 覆盖原始元数据文件
//   std::string meta_file = table_meta_file(base_dir_.c_str(), name());

//   int ret = rename(tmp_file.c_str(), meta_file.c_str());
//   if (ret != 0) {
//     LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s).
//     "
//               "system error=%d:%s",
//               tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
//     return RC::IOERR_WRITE;
//   }

//   table_meta_.swap(new_table_meta);

//   LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, name());
//   return rc;
// }

// create_index核心函数：当前连接trx，field_list，index_name
RC Table::create_index(Trx *trx, int unique, std::vector<const FieldMeta *> field_meta_list, const char *index_name)
{
  // 合法性检查
  if (common::is_blank(index_name) || 0 == field_meta_list.size()) {
    LOG_INFO("Invalid input arguments, table name is %s, index_name is blank or attribute_name is blank", name());
    return RC::INVALID_ARGUMENT;
  }

  IndexMeta new_index_meta;

  RC rc = new_index_meta.init(index_name, field_meta_list, unique);  // 初始化IndexMeta
  if (rc != RC::SUCCESS) {
    LOG_INFO("Failed to init IndexMeta in table:%s, index_name:%s, field_size:%d", 
             name(), index_name, field_meta_list.size());
    return rc;
  }

  // 创建索引相关数据
  BplusTreeIndex *index      = new BplusTreeIndex(unique);
  std::string     index_file = table_index_file(base_dir_.c_str(), name(), index_name);  // 在磁盘创建索引文件

  // 写的不优雅：用于将 std::vector<const FieldMeta *>转化为 std::vector<FieldMeta>
  std::vector<FieldMeta> field_meta_list_non_const;
  for (const FieldMeta *meta : field_meta_list) {
    field_meta_list_non_const.push_back(*meta);
  }

  // TODO 创建索引核心调用部分
  rc = index->create(index_file.c_str(), new_index_meta, field_meta_list_non_const);
  if (rc != RC::SUCCESS) {
    delete index;
    LOG_ERROR("Failed to create bplus tree index. file name=%s, rc=%d:%s", index_file.c_str(), rc, strrc(rc));
    return rc;
  }

  // 遍历当前的所有数据，插入这个索引
  RecordFileScanner scanner;
  rc = get_record_scanner(scanner, trx, true /*readonly*/);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create scanner while creating index. table=%s, index=%s, rc=%s", 
             name(), index_name, strrc(rc));
    return rc;
  }

  Record record;
  while (scanner.has_next()) {
    rc = scanner.next(record);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to scan records while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      return rc;
    }
    LOG_DEBUG("[[[[[[[[[[[[[[unique index data]]]]]]]]]]]]]] data:%d",record.data());
    rc = index->insert_entry(record.data(), &record.rid());  // 插入索引节点
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to insert record into index while creating index. table=%s, index=%s, rc=%s",
               name(), index_name, strrc(rc));
      
      // data_buffer_pool_->close_file();
      data_buffer_pool_->drop_file(index_file.c_str());  // 删除临时创建的索引文件 此处连续操作会有内存泄露bug
      // data_buffer_pool_ = nullptr;
      return rc;
    }
  }
  scanner.close_scan();
  LOG_INFO("inserted all records into new index. table=%s, index=%s", name(), index_name);

  indexes_.push_back(index);

  /// 接下来将这个索引放到表的元数据中：写入index头信息道tablemeta json文件中
  TableMeta new_table_meta(table_meta_);
  rc = new_table_meta.add_index(new_index_meta);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to add index (%s) on table (%s). error=%d:%s", index_name, name(), rc, strrc(rc));
    return rc;
  }

  /// 内存中有一份元数据，磁盘文件也有一份元数据。修改磁盘文件时，先创建一个临时文件，写入完成后再rename为正式文件
  /// 这样可以防止文件内容不完整
  // 创建元数据临时文件
  std::string  tmp_file = table_meta_file(base_dir_.c_str(), name()) + ".tmp";
  std::fstream fs;
  fs.open(tmp_file, std::ios_base::out | std::ios_base::binary | std::ios_base::trunc);
  if (!fs.is_open()) {
    LOG_ERROR("Failed to open file for write. file name=%s, errmsg=%s", tmp_file.c_str(), strerror(errno));
    return RC::IOERR_OPEN;  // 创建索引中途出错，要做还原操作
  }
  if (new_table_meta.serialize(fs) < 0) {
    LOG_ERROR("Failed to dump new table meta to file: %s. sys err=%d:%s", tmp_file.c_str(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }
  fs.close();

  // 覆盖原始元数据文件
  std::string meta_file = table_meta_file(base_dir_.c_str(), name());

  int ret = rename(tmp_file.c_str(), meta_file.c_str());
  if (ret != 0) {
    LOG_ERROR("Failed to rename tmp meta file (%s) to normal meta file (%s) while creating index (%s) on table (%s). "
              "system error=%d:%s",
              tmp_file.c_str(), meta_file.c_str(), index_name, name(), errno, strerror(errno));
    return RC::IOERR_WRITE;
  }

  table_meta_.swap(new_table_meta);
  // table_meta_.show_index(std::cout);  // test
  LOG_INFO("Successfully added a new index (%s) on the table (%s)", index_name, name());
  return rc;
}

RC Table::delete_record(const Record &record)
{
  RC rc = RC::SUCCESS;
  // for (Index *index : indexes_) {
  //   rc = index->delete_entry(record.data(), &record.rid());
  //   ASSERT(RC::SUCCESS == rc, 
  //          "failed to delete entry from index. table name=%s, index name=%s, rid=%s, rc=%s",
  //          name(), index->index_meta().name(), record.rid().to_string().c_str(), strrc(rc));
  // }
  LOG_DEBUG("(((((RC Table::delete_record))))) test:%s",record.rid().to_string().c_str());
  rc = record_handler_->delete_record(&record.rid());
  return rc;
}

RC Table::update_record(Record &record)
{
  RC rc = RC::SUCCESS;
  // for (Index *index : indexes_) {
  //   // TODO#2 update indexes?
  // }
  LOG_DEBUG("(((((RC Table::update_record))))) test:%s",record.rid().to_string().c_str());
  return rc;
}

RC Table::update_record(Record &record, Field *field, const Value *value)
{
  RC rc = RC::SUCCESS;

  // LOG_DEBUG("(((((RC Table::update_record))))) test:%s, data:%s, field:%s,
  // value:%d",record.rid().to_string().c_str(),record.data(),field->field_name(),value->get_int());
  LOG_DEBUG("(((((RC Table::update_record))))) record_size:%d",table_meta_.record_size());

  // main update section
  rc = record_handler_->update_record(&record.rid(), record, field, value);
  // RC update_record(const char *data, int record_size, const RID *rid);

  // 更新索引
  if (rc == RC::SUCCESS) {
    rc = delete_entry_of_indexes(record.data(), record.rid(), true);
    if (rc != RC::SUCCESS) {
      LOG_PANIC("Failed to delete old index. table name=%s, rc=%d:%s", name(), rc, strrc(rc));
    }
    rc = insert_entry_of_indexes(record.data(), record.rid());
    if (rc != RC::SUCCESS) {
      LOG_PANIC("Failed to add new index. table name=%s, rc=%d:%s", name(), rc, strrc(rc));
    }
  }
  return rc;
}

RC Table::insert_entry_of_indexes(const char *record, const RID &rid)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->insert_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      break;
    }
  }
  return rc;
}

RC Table::delete_entry_of_indexes(const char *record, const RID &rid, bool error_on_not_exists)
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->delete_entry(record, &rid);
    if (rc != RC::SUCCESS) {
      if (rc != RC::RECORD_INVALID_KEY || !error_on_not_exists) {
        break;
      }
    }
  }
  return rc;
}

Index *Table::find_index(const char *index_name) const
{
  for (Index *index : indexes_) {
    if (0 == strcmp(index->index_meta().name(), index_name)) {
      return index;
    }
  }
  return nullptr;
}
Index *Table::find_index_by_field(const char *field_name) const
{
  const TableMeta &table_meta = this->table_meta();
  const IndexMeta *index_meta = table_meta.find_index_by_field(field_name);
  if (index_meta != nullptr) {
    return this->find_index(index_meta->name());
  }
  return nullptr;
}

// Index *Table ::find_index_by_field(std::vector<std::string> field) const{
//   for (Index &index : indexes_) {
//     if (field == *index) {
//       return &index;
//     }
//   }
// }

RC Table::sync()
{
  RC rc = RC::SUCCESS;
  for (Index *index : indexes_) {
    rc = index->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to flush index's pages. table=%s, index=%s, rc=%d:%s",
          name(),
          index->index_meta().name(),
          rc,
          strrc(rc));
      return rc;
    }
  }
  LOG_INFO("Sync table over. table=%s", name());
  return rc;
}

RC handle_text_value(RecordFileHandler *record_handler, Value &value) 
{
  RC rc = RC::SUCCESS;
  int MAX_SIZE = 8000; //记录存储的最大值
  char *datass = new char[value.text_data().length() + 1]; //Text的总长度
  strcpy(datass, value.text_data().c_str());
  const char *end = datass + value.text_data().length();  //从后往前存，加上一个本身的长度到达字符串末尾

  RID *rid = new RID; //最后的空指针
  while (end - datass > MAX_SIZE) {
    RID *new_rid = new RID;
    rc = record_handler->insert_text_record(end - MAX_SIZE,(size_t)MAX_SIZE, new_rid); //存进一个record中

    if (rc != RC::SUCCESS) {
      LOG_ERROR("Insert text chunk failed: %s", strrc(rc));
      return rc;
    }else{
      LOG_TRACE("Insert record.))))))))))=>>>%d",MAX_SIZE);
      return rc;
    }

    if (rid->init == true) {
      new_rid->next_RID = rid;
    }
    rid = new_rid;

    end -= MAX_SIZE;
  }

  RID *rid_last = new RID;
  rc = record_handler->insert_text_record(datass, (size_t)(end - datass), rid_last); //将最后剩下的存进去
  if (rc != RC::SUCCESS) {
      LOG_ERROR("Insert text chunk failed: %s", strrc(rc));
      return rc;
    }else{
      LOG_TRACE("Insert record  handle_text_value text_data().length()))))))))))=>>>%d",value.text_data().length());
    }

  if (rid->init == true) {
    rid_last->next_RID = rid; 
  }
  rid = rid_last; //存放链表头
  rid->text_value = value.text_data().length();
  value.set_data(reinterpret_cast<char *>(rid),sizeof(RID)); //将vaule的data设置成指针地址

  delete[] datass;
  return rc;
}
