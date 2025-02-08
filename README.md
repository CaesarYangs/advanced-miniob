# Advanced-Miniob

An advanced implementation and deep thinking about MiniOB and database management systems.

## Indexes
1. [Basic:Date, Drop and Update](./docs/Basic.md)

**task list:**

- [x] Basic
  - [x] date
  - [x] drop-table
  - [x] update
  - [x] 文档
- [ ] Storage&Index
  - [ ] text
  - [ ] null
  - [ ] multi-index
  - [ ] varchar
  - [ ] unique-index
  - [ ] hash-index
  - [ ] 文档
- [ ] Query
  - [ ] aggregation-func
  - [ ] like
  - [ ] group-by
  - [ ] expression
  - [ ] alias
  - [ ] order-by
  - [ ] sub-query
  - [ ] 文档
- [ ] Optimization
  - [ ] Cost-based-optimization
  - [ ] AI-enabled-optimization
  - [ ] 文档

## System Structure

整体系统从`handle_sql`开始作为每一条sql语句的处理流程入口，一共有四个stage：
- query_cache_stage
- parse_stage
- resolve_stage
- optimize_stage
- execute_stage

```cpp
RC SqlTaskHandler::handle_sql(SQLStageEvent *sql_event)
{
  RC rc = query_cache_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do query cache. rc=%s", strrc(rc));
    return rc;
  }

  rc = parse_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do parse. rc=%s", strrc(rc));
    return rc;
  }

  rc = resolve_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do resolve. rc=%s", strrc(rc));
    return rc;
  }

  rc = optimize_stage_.handle_request(sql_event);
  if (rc != RC::UNIMPLENMENT && rc != RC::SUCCESS) {
    LOG_TRACE("failed to do optimize. rc=%s", strrc(rc));
    return rc;
  }

  rc = execute_stage_.handle_request(sql_event);
  if (OB_FAIL(rc)) {
    LOG_TRACE("failed to do execute. rc=%s", strrc(rc));
    return rc;
  }

  return rc;
}
```

## Resources

[GitHub - oceanbase/miniob: MiniOB is a compact database that assists developers in understanding the fundamental workings of a database.](https://github.com/oceanbase/miniob)

[MiniOB 简介 - MiniOB](https://oceanbase.github.io/miniob/)

[数据库基础理论课程 - MiniOB](https://oceanbase.github.io/miniob/lectures/index.html)