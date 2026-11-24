# 保护区巡护补拍取数接口

按唯一的识别记录号录入带标动物的观测，并按片区与起止日统计
「只在一天被拍到」和「至少两天被拍到」的个体数，供巡护组排补拍计划。

## 启动

### docker compose（推荐）

```bash
docker compose up --build
```

服务监听 <http://localhost:8000>，交互式文档在 <http://localhost:8000/docs>。
SQLite 文件位于命名卷 `resighting-data` 的 `/data/resighting.db`，重启容器后数据保留。

### 本地运行

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --port 8000
```

默认数据库为 `data/resighting.db`，可用环境变量 `DB_PATH` 覆盖。

## 接口

| 方法 | 路径 | 用途 |
|---|---|---|
| `POST` | `/observations` | 按 `recognition_id` 录入；同一记录号再次提交即更正三项属性 |
| `PUT` | `/observations/{recognition_id}` | 同上，记录号在路径中 |
| `PATCH` | `/observations/{recognition_id}` | 只更正提供的字段 |
| `GET` | `/observations/{recognition_id}` | 读取单条记录 |
| `GET` | `/observations` | 分页明细，可按片区 / 耳标 / 起止日筛选 |
| `GET` | `/stats/districts/{district}` | 单片区统计 |
| `GET` | `/stats/districts` | 全部片区统计（可用 `district` 只取一个） |
| `GET` | `/stats` | `/stats/districts` 的等价入口 |
| `POST` | `/migration/summaries` | 按 `{district, ear_tag}` 迁移一个组合（`step:true` 只跑一块） |
| `POST` | `/migration/summaries/step` | 只推进一个迁移写事务（≤128 个日期） |
| `POST` | `/migration/summaries/{district}/{ear_tag}` | 路径传参的迁移入口（`?step=true`） |
| `GET` | `/migration/summaries` | 各组合迁移状态（可按片区/耳标筛选） |
| `GET` | `/migration/summaries/{district}/{ear_tag}` | 单个组合迁移状态 |
| `GET` | `/health` | 健康检查 |

迁移接口都能直接在 <http://localhost:8000/docs> 的「汇总迁移」分组里
填片区和耳标逐个执行；`/admin/migrate-summary`、`/migrate`、
`/summaries/{district}/{ear_tag}/migrate` 等是兼容别名。

录入的请求体：

```json
{
  "recognition_id": "REC-0001",
  "observed_on": "2026-03-01",
  "district": "北沟片区",
  "ear_tag": "ET-1024"
}
```

可更正的三项是 `observed_on`、`district`、`ear_tag`；`recognition_id` 是唯一键，不可更正。

统计返回：

```json
{
  "district": "北沟片区",
  "start_date": "2026-03-01",
  "end_date": "2026-03-31",
  "one_day_count": 12,
  "multi_day_count": 5,
  "total_individuals": 17,
  "summary_rows_used": 23,
  "state_version": 41
}
```

起止日都包含。同一片区内同一耳标同一天被拍到多次只算一天。

## 实现要点

* **两种汇总格式。**
  * 旧格式 `tag_summaries`：每个 `(片区, 耳标)` 一行，`day_counts` 是按日
    排序的 `[日期, 次数]` JSON 数组，是旧 SQLite 卷里唯一的汇总。
  * 新格式 `tag_day_counts` + `summary_registry`：每个 `(片区, 耳标, 日期)`
    一行日汇总，另有一条定长的跨度/状态行。**新建组合直接写新格式**，
    一次录入或更正只读取、改写当天那一行和该定长状态行，实际汇总读写量
    与耳标的历史天数无关（不是只保证 SQL 条数不变）。
* **按天去重发生在写入时。** 同一天多次拍摄只增加当天的 `capture_count`，
  不会产生新的天；统计不读明细、不做 `GROUP BY` 或逐日归并，区间天数用
  排序日上的两次二分（旧格式）或区间内的日汇总行（新格式）。
* **统计同时覆盖两种格式。** 一条 `UNION ALL ... LIMIT 1024` 取组合级跨度
  行，再在同一个读快照内取新格式的区间日行；返回字段、复拍口径与
  **1024 条组合汇总上限**都不变，超限仍返回 422。
* **免回填启动 + 逐个在线迁移。** 新版只做附加式建表，旧卷直接启动、旧汇总
  继续供数。`POST /migration/summaries` 按片区 + 耳标迁移单个组合：
  1. 先建立 `dual` 双写状态，此后该组合的录入/更正同时写两边；
  2. 无写锁地读取、整理旧 JSON 并按 **每事务最多 128 个日期**分块；
  3. 逐块 `BEGIN IMMEDIATE` 幂等 upsert，块间释放锁，写入可插入提交；
  4. 末块校验新旧完全一致后，同一事务把状态切到 `new` 并删除旧 JSON 行。
  中途失败/进程退出时该块回滚，旧汇总继续供数；重试从 `cursor_day` 继续，
  校验通过才切换，双写保证期间已提交的观测一条不丢。
* **跨格式更正原子生效。** 一次更正的旧、新两个 `(片区, 耳标)` 作用域各自
  按当前格式路由（旧 JSON / 双写 / 新日行），明细与两边汇总、状态版本仍在
  同一个写事务内提交，任一失败整体回滚，原数仍可查到。
* **事务一致。** 查询在一个 WAL 读取快照内完成，明细与汇总来自同一已提交
  状态；响应的 `state_version` 可判断读写对应的是哪一次已提交状态。

### 试跑脚本

```bash
python scripts/trial_migration.py
```

会在临时目录构造一个只有旧 JSON 汇总的 SQLite 卷，依次核对：免回填启动、
保存读写量不随天数增长、128 天分块、迁移中改期/录入、注入故障后的回滚与
断点重试、切换后实际表改动，以及与明细暴力重算的一致性。

## 测试

```bash
pytest
```

覆盖：同日多拍、改期后跨天、起止日包含、片区隔离、路由级更正、
1024 条上限（含新旧格式混合）、统计不读明细、更正读取量不随历史增长、
**新建组合保存载荷不随历史天数增长**、失败保存回滚、并发写入期间读到旧的
已提交状态，以及旧卷免回填启动、128 天分块、迁移中改期、中断/切换失败
重试、跨格式更正与随机差分一致性（见 `tests/test_migration.py`）。
