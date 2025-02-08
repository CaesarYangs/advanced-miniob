# Basic Functions Implementation Documentation

## Date

1. Parse Part

语法(yacc文件)上增加type，value里增加DATE_STR值，以便语法解析器能够顺利解析date相关关键词和字段

yacc.y:
```cpp
type:
    INT_T      { $$=INTS; }
    | STRING_T { $$=CHARS; }
    | FLOAT_T  { $$=FLOATS; }
    | DATE_T  { $$=DATES; }
    ;
```

lex_sql.q:
```cpp
DATE   RETURN_TOKEN(DATE_T);
```

2. 设定与其对应的Value Type以及系统的处理流程

专门创建一个Date类型，用于处理全部相关的数据校验和转化

在Parse过程中生成新的Date变量时，就要完成所有的数据类型转换和校验操作，Date的底层存储是以int存储的
```cpp
Date::Date(const std::string &s)
{
  std::tm tm;
  bzero(&tm, sizeof(tm));
  char *tail = strptime(s.c_str(), "%F", &tm);          // 数据类型转换
  if (tail != s.c_str() + s.size() || !valid_tm(tm)) {  // 合法性判断
    value = -1;
  } else {
    value = (kSecondsInDay + mktime(&tm)) / kSecondsInDay;  // 时间跨度计算
  }
}
```

**Core: DATE的底层数据存储**
- 在MiniOB的实现过程中，使用的是天数存储法，即计算当前时间到某一个相同标志位的天数差来实现
- 原始的存储方式是通过将固定的日期字符串转化为按数位取模存储的一个int数字
  - Pros:
    - 相对更少的存储空间
    - 对于比较、加减计算等操作都更加友好，可以直接操作
    - 索引效率也会更好
  - Cons:
    - 可读性差，每次输出显示需要单独计算
    - 时间的天数可能超过8，16，32等位的长度，需要格外小心使用的具体数据类型
    - 潜在的时区问题

专用的合法性校验函数：
Pros:
- 代码并没有直接检查每个字段的范围，而是将原始 std::tm 结构体的值与经过 mktime 和 localtime 修正后的值进行比较。 如果 tm_mday, tm_mon, tm_year 和 tm_wday 这几个关键字段在修正前后没有改变，则认为原始时间是有效的。 

Cons:
- tm其实在经过查阅后发现已经不再作为C++开发中的常用项，主要是由于其过于依赖本地时区，这在分布式时代是非常不合适的。
```cpp

bool valid_tm(const std::tm &tm)
{
  auto cpy = tm;

  // this two step process would correct any out of range values which may be present in cpy

  // http://en.cppreference.com/w/cpp/chrono/c/mktime
  //    note: "The values in time are permitted to be outside their normal ranges."
  const auto as_time_t = std::mktime(std::addressof(cpy));

  // http://en.cppreference.com/w/cpp/chrono/c/localtime
  cpy = *std::localtime(std::addressof(as_time_t));

  return tm.tm_mday == cpy.tm_mday &&  // valid day
         tm.tm_mon == cpy.tm_mon &&    // valid month
         tm.tm_year == cpy.tm_year &&  // valid year
         tm.tm_wday == cpy.tm_wday;    // valid day of week
}
```

**Extension: MySQL中的DATE字段存储实现**
- DATE 类型在 MySQL 中占用 3 个字节的存储空间
- 3个字节可以存储年份 (需要1-2个字节，根据年份大小)，月份和日(各需要1个字节)。
  - Year: 占用 2 个字节 (16 bits)。 MySQL 可以存储从 1000 到 9999 的年份，因此需要 14 bits (2^14 = 16384，足够存储 1000-9999)。 剩下 2 bits 没有使用或者用于内部标志。
  - Month: 占用 4 bits。足以存储 1-12 的月份。
  - Day: 占用 5 bits。足以存储 1-31 的日期。


**References:**
- https://www.cnblogs.com/bigben0123/p/5607020.html


## Drop-table

1. Parse Part

yacc和lex中需要添加特定的匹配字符，并且要能够传递给后序的stmt
```cpp
%type <sql_node>            drop_table_stmt

drop_table_stmt:    /*drop table 语句的语法解析树*/
    DROP TABLE ID {
      $$ = new ParsedSqlNode(SCF_DROP_TABLE);
      $$->drop_table.relation_name = $3;
      free($3);
    };

DROP                                    RETURN_TOKEN(DROP);
TABLE                                   RETURN_TOKEN(TABLE);
```

2. Core Stmt Design

drop table stmt类设计：
- 目的是用于从parse阶段获取到真正要的参数信息（此处为table name）
```cpp
class DropTableStmt : public Stmt
{
public:
  DropTableStmt(const std::string &table_name) : table_name_(table_name) {}
  virtual ~DropTableStmt() = default;

  StmtType type() const override { return StmtType::DROP_TABLE; }

  const std::string &table_name() const { return table_name_; }

  static RC drop(Db *db, const DropTableSqlNode &drop_table, Stmt *&stmt);

private:
  std::string table_name_;
};
```

3. Execute Code
Drop Table操作无需进行Optimization，因此直接进入到Exec阶段

经典的三级操作：
- 上层仅负责与用户的stmt进行交互
- 中间的数据库部分负责逻辑表的管理和删除命令发出
- 下层负责真正的删除逻辑和操作

```cpp
RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt    *stmt    = sql_event->stmt();
  Session *session = sql_event->session_event()->session();
  ASSERT(stmt->type() == StmtType::DROP_TABLE,
      "drop table executor can not run this command: %d",
      static_cast<int>(stmt->type()));

  DropTableStmt *drop_table_stmt = static_cast<DropTableStmt *>(stmt);

  const char *table_name = drop_table_stmt->table_name().c_str();
  // Trx *trx = session->current_trx();
  RC rc = session->get_current_db()->drop_table(table_name);

  return rc;
}
```

数据库层面关于获取每个表以及其逻辑map的相关层：
```cpp
RC Db::drop_table(const char *table_name)
{
  RC rc = RC::SUCCESS;
  if (opened_tables_.count(table_name) == 0) {  // search map to look for exist tables
    LOG_WARN("%s no such table to drop.", table_name);
    return RC::SCHEMA_TABLE_EXIST;
  }

  // drop table meta_file & data_file
  std::string table_file = table_meta_file(path_.c_str(), table_name);  // get meta data
  Table      *table      = opened_tables_[table_name];                  // get table_data

  rc = table->drop(table_file.c_str(), table_name, path_.c_str());  // main operation section for dropping table
  if (rc != RC::SUCCESS) {
    delete table;  // recycle pointer addr
    return rc;
  }

  opened_tables_.erase(table_name);  // remove from map
  LOG_INFO("Drop table success. table name=%s", table_name);
  return RC::SUCCESS;
}
```
`opened_tables_.erase(table_name); `是为了防止数据库在管理已经打开的表时，出现资源未释放的情况

核心的底层table删除层：
```cpp
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
```

**Extension: 升级为异步删除操作**
- 需要将全部的erase和unlink等删除操作作为异步线程管理，并加入回调函数，设置好先后顺序
- 利用future和async实现先后的线程调用防止阻塞
  - 异步任务启动: std::async 用于启动一个异步任务。 std::launch::async 确保任务在一个新的线程中执行。 这样做的好处是 drop 操作不会阻塞调用线程，允许数据库继续处理其他请求。
  - std::future 和 std::promise (隐式使用): std::async 返回一个 std::future 对象，它代表异步操作的结果。 你可以使用 future.get() 来获取结果（这会阻塞直到结果可用）。 但是，在这里，我们使用 future 来在异步任务完成后执行回调函数，而不是直接阻塞等待结果。
  - 回调函数: Table::drop 现在接受一个 std::function<void(RC)> callback 参数。 这个回调函数会在异步任务完成后被调用，并传递删除操作的结果。 这允许调用者在删除操作完成后执行一些清理工作，例如从 opened_tables_ 中移除表。
  - 资源管理：使用std::shared_ptr来管理Table的生命周期，确保在异步删除完成之前，Table对象不会被释放。一旦完成，table_ptr 会在回调函数结束时自动释放 Table 对象的内存。

```cpp
// Table.cpp
RC Table::drop(const char *path, const char *name, const char *base_dir, std::function<void(RC)> callback) {
  // 使用 std::async 启动一个异步任务
  std::future<RC> future = std::async(std::launch::async, [this, path, name, base_dir]() {
    return drop_sync(path, name, base_dir);  // 在异步任务中执行实际的删除操作
  });

  // 在 future 完成后执行回调函数
  std::thread([future = std::move(future), callback = std::move(callback)]() {
    RC rc = future.get(); // 获取异步任务的结果
    callback(rc);          // 执行回调函数
  }).detach();            // 让线程独立运行，避免阻塞

  return RC::SUCCESS; // 立即返回，表示异步删除任务已启动
}
```

未来升级和思考点：
- 并发控制：因为当前的drop函数并不是atomic操作，因此一旦出现并发访问，很可能出现drop的过程中插入或查询等未完成现象


**MySQL中的Drop Table实现**
- 客户端请求解析和权限检查
- 元数据锁定
  - MySQL 需要锁定表的元数据，以防止并发修改。这是通过元数据锁 (MDL) 实现的。
  - 获取 MDL 锁的代码位于 sql/lock.cc。 具体的函数是 mysql_lock_tables() 和 mysql_unlock_tables()。 DROP TABLE 需要排他性的 MDL 锁。
- 关闭表句柄：
  - 如果表当前被任何会话打开，MySQL 需要关闭这些表句柄。
  - 这涉及到从服务器的表缓存中移除表。 表缓存管理代码位于 sql/table_cache.cc。
- 删除数据文件和索引文件：
  - MySQL 会根据表的存储引擎（例如 InnoDB、MyISAM）调用相应的删除函数。
  - InnoDB 使用事务来保证 DROP TABLE 的原子性。
  - InnoDB 会删除表空间（如果表是独立表空间）或从共享表空间中释放空间。
  - InnoDB 会更新内部数据字典。 相关代码位于 storage/innobase/handler/ha_innodb.cc 文件中的 ha_innodb::drop_table() 函数。
- 更新数据字典
  - 删除表后，MySQL 需要更新数据字典，以反映表的删除。
- 释放元数据锁

- ***思考：是否需要数据库来在execute中显示处理buffer pool的清理，抑或是通过LRU来专门管理buffer pool的删除操作？因为这与效率高度相关***

**References:**
- [mysql drop table过程](https://blog.csdn.net/guduwuzheguduwuzhe/article/details/84443868)
- https://bugs.mysql.com/bug.php?id=41158
- https://bugs.mysql.com/bug.php?id=56696
- https://www.percona.com/blog/performance-problem-with-innodb-and-drop-table/

## Update

1. Parse Part

```cpp
UPDATE                                  RETURN_TOKEN(UPDATE);

update_stmt:      /*  update 语句的语法解析树*/
    UPDATE ID SET ID EQ value where 
    {
      $$ = new ParsedSqlNode(SCF_UPDATE);
      $$->update.relation_name = $2;
      $$->update.attribute_name = $4;
      $$->update.value = *$6;
      if ($7 != nullptr) {
        $$->update.conditions.swap(*$7);
        delete $7;
      }
      free($2);
      free($4);
    }
    ;
```

2. Update Stmt设置

Update Stmt类设计
```cpp
class UpdateStmt : public Stmt
{
public:
  UpdateStmt(Table *table, const Value *values, FilterStmt *filter_stmt);
  UpdateStmt() = default;

  StmtType type() const override { return StmtType::UPDATE; }

public:
  static RC create(Db *db, const UpdateSqlNode &update_sql, Stmt *&stmt);

public:
  Table       *table() const { return table_; }
  Field       *query_field() const { return query_field_; }
  FilterStmt  *filter_stmt() const { return filter_stmt_; }
  const Value *values() const { return values_; }
  int          value_amount() const { return value_amount_; }

private:
  Field       *query_field_  = nullptr;
  Table       *table_        = nullptr;
  const Value *values_       = nullptr;
  FilterStmt  *filter_stmt_  = nullptr;
  int          value_amount_ = 1;
};
```

- 核心点在于获取到要修改的项，以及其所对应的Table Field，从而才能够获取到正确的指针长度，用来进行覆写
- 当前支持修改单一表项，因此一旦匹配直接break退出
- 通过创建的table_map来实现确定要操作的表，尤其是如果需要操作多表，这个unordered_map就能够作为高效的索引结构，上层仅需要寻找一次，下层就能够通过快速的kv操作获取到要处理的表指针/对象,aka filter stmt here
- 最后由table, values和最关键的filter_stmt合并创建一个update_stmt供下一步执行器调用

```cpp
RC UpdateStmt::create(Db *db, const UpdateSqlNode &update, Stmt *&stmt)
{
  // check the sql input
  const char *table_name = update.relation_name.c_str();
  if (nullptr == db || nullptr == table_name || 0 == update.value.length()) {
    LOG_WARN("invalid argument. db=%p, table_name=%p, value_num=%d",
        db, table_name, static_cast<int>(update.value.length()));
  }

  // check whether the table exists
  Table *table = db->find_table(table_name);
  if (nullptr == table) {
    LOG_WARN("no such table. db=%s, table_name=%s", db->name(), table_name);
    return RC::SCHEMA_TABLE_NOT_EXIST;
  }

  LOG_DEBUG("TEST UPDATE FROM STMT: ");
  const Value *values = &update.value;
  // const TableMeta &table_meta = table->table_meta();
  // TODO check whether the field type and input type are matching
  // const int sys_field_num = table_meta.sys_field_num();

  // 获取要修改的表项信息
  Field *query_field = nullptr;

  const TableMeta &table_meta = table->table_meta();
  const int        field_num  = table_meta.field_num();
  const char      *field_name = update.attribute_name.c_str();
  for (int i = table_meta.sys_field_num(); i < field_num; i++) {
    // field_metas.push_back(Field(table, table_meta.field(i)));
    if(strcmp(table_meta.field(i)->name(),field_name)==0){
      query_field = new Field(table,table_meta.field(i));
      break;
    }
  }
  LOG_DEBUG("get field name: %s",field_name);
  if(query_field == nullptr){
    LOG_ERROR("failed to get existing field to change.");
    return RC::NOTFOUND;
  }
  LOG_DEBUG("get field name good: %s",field_name);

  // 创建filter
  std::unordered_map<std::string, Table *> table_map;
  table_map.insert(std::pair<std::string, Table *>(std::string(table_name), table));
  FilterStmt *filter_stmt = nullptr;

  RC rc = FilterStmt::create(
      db, table, &table_map, update.conditions.data(), static_cast<int>(update.conditions.size()), filter_stmt);
  if (rc != RC::SUCCESS) {
    LOG_WARN("failed to create filter statement. rc=%d:%s", rc, strrc(rc));
    return rc;
  }

  // TODO everything alright
  UpdateStmt *update_stmt = new UpdateStmt(table, values, filter_stmt);
  update_stmt->query_field_ = query_field;
  stmt = update_stmt;
  return RC::SUCCESS;
}
```

3. Logical Plan Creation

Update需要进行优化的原因：
- 本质上用到了查询操作，有一个where子句，需要在查询过程中更加高效地实现搜索过滤
- Update本质上是操作数据，也就不可避免的需要利用到常见操作来优化执行性能，如索引选择、谓词下推、连接重排序等

Update操作的逻辑计划流程：
- 核心是提取从stmt中拿到的filter并创建各种逻辑算子
  - table_get_oper
  - predicate_oper：这个算子和select中使用的是相同的，目的都是为了查询
    - Predicate 算子表示根据 WHERE 子句中的条件进行过滤的操作。
  - update_oper：核心的update执行算子
- 中间进行下推操作
- 核心：构建逻辑计划树
  - 如果存在 Predicate 算子（即有 WHERE 子句），则将 TableGetLogicalOperator 作为 Predicate 算子的子节点，并将 Predicate 算子作为 UpdateLogicalOperator 算子的子节点。
  - 如果不存在 Predicate 算子（即没有 WHERE 子句），则直接将 TableGetLogicalOperator 作为 UpdateLogicalOperator 算子的子节点。
  - 将 UpdateLogicalOperator 算子作为逻辑计划的根节点。
    - unique_ptr只能够移动，不能够复制的操作在此体现出来，代码中使用的是move
  - 

```cpp
RC LogicalPlanGenerator::create_plan(UpdateStmt *update_stmt, unique_ptr<LogicalOperator> &logical_operator)
{
  Table       *table        = update_stmt->table();
  const Value *value        = update_stmt->values();
  FilterStmt  *filter_stmt  = update_stmt->filter_stmt();
  Field       *update_field = update_stmt->query_field();

  // 获取要操作表的元信息（表项等）
  std::vector<Field> fields;
  for (int i = table->table_meta().sys_field_num(); i < table->table_meta().field_num(); i++) {
    const FieldMeta *field_meta = table->table_meta().field(i);
    fields.push_back(Field(table, field_meta));
  }

  // 指针得到从表中获取数据的算子
  unique_ptr<LogicalOperator> table_get_oper(new TableGetLogicalOperator(table, fields, false /*readonly*/));

  // 断言算子
  unique_ptr<LogicalOperator> predicate_oper;
  RC                          rc = create_plan(filter_stmt, predicate_oper);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  // value type check
  if (update_stmt->query_field()->attr_type() != value->attr_type() && update_stmt->query_field()->attr_type() != 3) {
    LOG_DEBUG("[[[[[[[[[[[date update check]]]]]]]]]]] %d, %d",update_stmt->query_field()->attr_type(),value->attr_type());
    LOG_ERROR("update stmt contains value with incorrect type");
    return RC::INVALID_ARGUMENT;
  }

  // convert type CHAR to DATE
  if (update_stmt->query_field()->attr_type() == 3) {
    Value *new_value = new Value(value);
    Date   date      = value->get_date();
    new_value->set_date(date);
    value = new_value;
    LOG_DEBUG("[[[[[[[log date]]]]]]] %s, %s",new_value->get_date().to_string(new_value->get_date()).c_str(),value->get_date().to_string(value->get_date()).c_str());
  }

  unique_ptr<LogicalOperator> update_oper(new UpdateLogicalOperator(table,
      value,
      update_field));  // 注意传递过程中的const问题，主要传递filter clause前的部分，后半部分由rewriter变为record即可

  if (predicate_oper) {
    predicate_oper->add_child(std::move(table_get_oper));
    update_oper->add_child(std::move(predicate_oper));
  } else {
    update_oper->add_child(std::move(table_get_oper));
  }

  // instance the final update operator
  logical_operator = std::move(update_oper);

  // right(filter stmt check)
  if (filter_stmt->filter_units()[0]->left().field.attr_type() !=
      filter_stmt->filter_units()[0]->right().value.attr_type()) {
    LOG_ERROR("update stmt filter contains value with incorrect type");
    return RC::INVALID_ARGUMENT;
  }

  return RC::SUCCESS;
}
```


1. Physical Plan Creation

```cpp
case LogicalOperatorType::UPDATE: {
      return create_plan(static_cast<UpdateLogicalOperator &>(logical_operator), oper);
    } break;
```

物理算子构建
- 核心：子节点算子处理
  - 获取 UpdateLogicalOperator 的子节点（即逻辑算子）。
  - 如果存在子节点，则递归调用 create 函数来处理子节点，生成相应的物理算子。
  - child_physical_oper 是一个指向生成的物理算子的 unique_ptr。
  - 这段代码体现了从逻辑计划树到物理计划树的递归转换过程。
- 构建物理计划树
  - 如果存在子物理算子（即 child_physical_oper 不为空），则将子物理算子作为 UpdatePhysicalOperator 算子的子节点。
  - 这段代码将各个物理算子连接起来，构建物理计划树。

```cpp
RC PhysicalPlanGenerator::create_plan(UpdateLogicalOperator &update_oper, unique_ptr<PhysicalOperator> &oper)
{
  Table       *table  = update_oper.table();
  const Value *values = update_oper.values();
  Field       *field  = update_oper.field();

  vector<unique_ptr<LogicalOperator>> &child_opers = update_oper.children();

  unique_ptr<PhysicalOperator> child_physical_oper;

  RC rc = RC::SUCCESS;
  if (!child_opers.empty()) {
    LogicalOperator *child_oper = child_opers.front().get();
    rc                          = create(*child_oper, child_physical_oper);
    if (rc != RC::SUCCESS) {
      LOG_WARN("failed to create physical operator. rc=%s", strrc(rc));
      return rc;
    }
  }

  LOG_DEBUG("[[[[[[[[[[[[[[TEST update physical PhysicalPlanGenerator]]]]]]]]]]]]]] field_name:%s, new_value:%s",update_oper.field()->field_name(),update_oper.values()->get_string());

  oper = unique_ptr<PhysicalOperator>(new UpdatePhysicalOperator(table, values, field));

  if (child_physical_oper) {
    oper->add_child(std::move(child_physical_oper));
  }

  return rc;
}
```

**逻辑计划&物理计划**
- 逻辑计划是数据库查询执行的抽象表示。 它描述了需要执行的操作，以及这些操作之间的关系，而不指定如何执行这些操作。 换句话说，逻辑计划关注的是“做什么”，而不是“怎么做”。
  - 逻辑计划通常由一系列逻辑算子 (Logical Operators) 组成，每个算子代表一个逻辑操作
  - TableGet (Scan)：从表中读取数据。
  - Filter (Predicate)：根据条件过滤数据。
  - Join：连接两个或多个表的数据。
  - Project：选择要输出的列。
  - Aggregate：对数据进行分组和聚合。
  - Sort：对数据进行排序。
  - Update: 更新表中的数据
- 作用：
  - 查询表示： 提供了一种结构化的方式来表示 SQL 查询。
  - 优化基础： 作为查询优化的基础，通过应用各种优化规则，可以生成多个等价的逻辑计划，然后选择最佳的一个。
  - 与存储引擎解耦： 不依赖于特定的存储引擎，可以在不同的存储引擎上执行。

- 物理计划是数据库查询执行的具体实现。 它描述了如何执行逻辑计划中的每个操作，包括使用哪个索引、使用哪种连接算法、以什么顺序执行操作等。
  - 物理计划由一系列物理算子 (Physical Operators) 组成，每个算子代表一个具体的执行步骤。 物理算子是逻辑算子的具体实现
    - IndexScan：使用索引扫描表。
    - TableScan：全表扫描。
    - NestedLoopJoin：嵌套循环连接。
    - HashJoin：哈希连接。
    - SortMergeJoin：排序合并连接。
- 作用
  - 查询执行： 作为查询执行的蓝图，数据库系统按照物理计划的指示执行查询。
  - 性能优化： 通过选择最佳的物理算子和执行顺序，可以最大限度地提高查询性能。
  - 存储引擎适配： 将逻辑计划转换为特定存储引擎可以理解和执行的物理计划。

5. Core Update Physical Operator

最核心的Update物理算子实现：
- open
  - 递归地调用子节点的 open 函数，确保整个物理计划树都被打开。
  - 保存 Trx 指针，以便在 next 函数中使用。
  - Trx* trx 是事务对象，用于控制更新操作的原子性、一致性、隔离性和持久性 (ACID)。 UpdatePhysicalOperator 需要在事务的上下文中执行更新操作，以确保数据的完整性。
- next: 所有的迭代核心
  - 从子节点获取数据，并更新满足条件的行。驱动整个更新操作的执行。
  - 目标是找到对应的那一条/几条record，以执行后序操作
  - 

```cpp
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
```

**核心update函数**

table级别upate：

```cpp
RC Table::update_record(Record &record, Field *field, const Value *value)
{
  RC rc = RC::SUCCESS;

  // LOG_DEBUG("(((((RC Table::update_record))))) test:%s, data:%s, field:%s,
  // value:%d",record.rid().to_string().c_str(),record.data(),field->field_name(),value->get_int());
  LOG_DEBUG("(((((RC Table::update_record))))) record_size:%d",table_meta_.record_size());

  // main update section
  rc = record_handler_->update_record(&record.rid(), record, field, value);

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
```

Record级别更新操作：核心目标是找到并让对应的record记录的page被load进buffer_pool中，等待下一步操作，否则没有被载入内存，无法继续操作
```cpp
RC RecordFileHandler::update_record(const RID *rid, Record &record, Field *field, const Value *value)
{
  RC rc = RC::SUCCESS;

  RecordPageHandler page_handler;
  if ((rc = page_handler.init(*disk_buffer_pool_, rid->page_num, false /*readonly*/)) != RC::SUCCESS) {
    LOG_ERROR("Failed to init record page handler.page number=%d. rc=%s", rid->page_num, strrc(rc));
    return rc;
  }

  // main update memory operation func
  rc = page_handler.update_record(rid, field, value, &record);

  return rc;
}
```

Page级别直接更新内存操作：
- 更新record
  - 计算复制长度： 计算要复制的长度 copy_len，取输入长度和字段最大长度中的较小者。 这样做可以防止缓冲区溢出。 这里为了保证字符串有结束符，特意让copy_len最大只能去到max_field_len - 1
  - 复制数据： 使用 memcpy 将 value->data() 中的数据复制到 rec->data() 的指定偏移量处。
  - 添加空字符： 在复制后的字符串末尾添加空字符 \0，确保字符串正确终止。 这对于字符串类型的字段非常重要。
- 更新memory
  - 获取记录数据指针： 调用 get_record_data(rid->slot_num) 函数获取数据页中记录的起始地址。
  - 复制数据： 使用 memcpy 将 rec->data() 中的数据复制到数据页的指定地址处。
  - 标记数据页为脏页： 调用 frame_->mark_dirty() 函数将数据页标记为脏页。 脏页需要被写回磁盘，以保证数据的持久性。

```cpp
RC RecordPageHandler::update_record(const RID *rid, Field *field, const Value *value, Record *rec)
{
  ASSERT(readonly_ == false, "cannot update record from page while the page is readonly");

  if (rid->slot_num >= page_header_->record_capacity) {
    LOG_ERROR("Invalid slot_num %d, exceed page's record capacity, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::INVALID_ARGUMENT;
  }

  Bitmap bitmap(bitmap_, page_header_->record_capacity);
  if (!bitmap.get_bit(rid->slot_num)) {
    LOG_ERROR("Invalid slot_num:%d, slot is empty, page_num %d.", rid->slot_num, frame_->page_num());
    return RC::RECORD_NOT_EXIST;
  }

  // 在memory中对应的Field中写入新Value
  // 更新指定字段: 当前方案为 1.取出已有record，2.在mem
  // pile上写新数据到record中，3.flush整个record到原始指针处（不写单个field了）

  LOG_DEBUG("[[[[[[[RC RecordPageHandler::update_record]]]]]]] test:%d, value_len:%d",field->meta()->len(), strlen(value->data()));

  // 更新value
  // ATTENTION!!!:地址越界问题：字符串情况下，value的长度只会是字符串的长度，因此如果直接memcpy
  // meta长度的话，直接就越界了 memcpy(rec->data() + field->meta()->offset(), value->data(), field->meta()->len());

  // 获取字段最大长度和输入值的长度
  size_t max_field_len = field->meta()->len();   // 字段的最大长度
  size_t input_len     = strlen(value->data());  // 输入值的实际长度

  // 计算要复制的长度，取输入长度和字段最大长度中的较小者
  size_t copy_len = (input_len < max_field_len) ? input_len : max_field_len - 1;
  memcpy(rec->data() + field->meta()->offset(),
      value->data(),
      copy_len);                                           // 将数据复制到记录中，确保不会超过字段的最大长度
  rec->data()[field->meta()->offset() + copy_len] = '\0';  // 在复制后的字符串末尾添加空字符，确保字符串正确终止

  // 更新memory
  char *record_data = get_record_data(rid->slot_num);  // 奥卡姆剃刀
  memcpy(record_data, rec->data(), page_header_->record_real_size);
  frame_->mark_dirty();

  return RC::SUCCESS;
}
```

**bug fixed eec6b78 第一个解决的地址越界问题**
- 地址越界问题：在更新字符串情况下，value的长度只会是字符串的长度，因此如果直接memcpy meta长度的话，地址直接就越界了

**Extension:**
- 空值处理： 你需要考虑如何处理空值 (NULL)。 你可以使用一个特殊的位来标记字段是否为空。
- 并发控制： 如果多个事务同时更新同一条记录，可能会发生冲突。 你需要使用锁或其他并发控制机制来避免数据不一致。 通常，数据库系统会使用行级锁来控制对记录的并发访问。
- 原子性： 需要确保更新操作的原子性。 如果在更新过程中发生错误，需要回滚所有已经执行的操作。
- 性能： 可以考虑使用批量更新等技术来提高性能。
  - Write-Ahead Logging (WAL)： 先将修改操作写入日志，然后再将修改后的数据写回磁盘。 这种策略可以提高系统的性能和可靠性。
  - Copy-on-Write (COW)： 在修改数据之前，先复制一份数据页的副本，然后在副本上进行修改。 修改完成后，将副本替换原始数据页。 这种策略可以提高系统的并发性能。

**数据page从buffer_pool中写回**
- 当需要将修改后的数据持久化到磁盘时，通常情况下，数据库系统会将整个脏页写回磁盘，而不是只写回修改的单个记录。
- 在修改record时，需要准确修改每一条记录的内存地址指针，但是在mark dirty时，则需要以page为单位从buffer_pool中写回，以保持page header的正确性和完整性，同时这样也复合OS对内存的管理，更加方便的同时性能也会随着批量操作而被优化。

**MySQL中Update的大致实现流程**
- SQL 解析和权限检查
- 查询优化：
  - MySQL 的查询优化器会生成多个可能的执行计划，并选择成本最低的一个。
  - 优化器会考虑以下因素：
    - 索引选择： 选择最佳索引来加速 WHERE 子句的过滤。
    - 表扫描方式： 选择全表扫描还是索引扫描。
    - 连接顺序： 如果 UPDATE 语句涉及多个表（通过子查询），则选择最佳的连接顺序。
- 存储引擎接口调用
  - UPDATE 操作的核心是调用存储引擎的 update_row 函数
  - ha_innodb::update_row() 函数位于 storage/innobase/handler/ha_innodb.cc。
  - InnoDB 使用行级锁来控制并发访问。
  - InnoDB 使用多版本并发控制 (MVCC) 来实现读写并发:允许多个事务同时读取和修改同一行数据，而不会相互阻塞。
    - 当一个事务更新一行时，InnoDB 会：
      - 创建一个该行的副本（旧版本）。
      - 将旧版本的数据写入 undo 日志，并将 DB_ROLLBACK_PTR 指向该 undo 日志记录。
      - 修改原始行的数据，并将 DB_TRX_ID 设置为当前事务的 ID。
    - 当一个事务读取一行时，InnoDB 会根据以下规则选择可见的版本：
      - 如果行的 DB_TRX_ID 小于或等于当前事务的隔离级别所需的快照版本，则该行可见。
      - 如果行的 DB_TRX_ID 大于当前事务的隔离级别所需的快照版本，则检查 DB_ROLLBACK_PTR 指向的 undo 日志。如果 undo 日志记录的事务 ID 小于或等于当前事务的快照版本，则使用 undo 日志中的数据恢复到可见版本。
  - Redo 日志记录了所有对数据页的修改操作
  - Undo 日志记录了每个修改操作的逆操作
  - InnoDB 会根据需要更新索引。
- 缓冲池
  - MySQL 使用缓冲池来缓存数据页和索引页。
  - 当需要读取或修改数据时，MySQL 首先会检查缓冲池中是否存在相应的页。
  - 如果页存在于缓冲池中，则直接从缓冲池中读取或修改数据。
  - 如果页不存在于缓冲池中，则需要从磁盘读取数据，并将数据放入缓冲池。
- Write-Ahead Logging (WAL)：
  - InnoDB 采用 Write-Ahead Logging (WAL) 策略，即先将修改操作写入 Redo 日志，然后再将修改后的数据写回磁盘。
  - WAL 策略可以保证事务的持久性和可靠性。

**References**
- https://juejin.cn/post/6951716399412674596

