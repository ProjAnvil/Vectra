# 图数据库能力调研：边周期衰减、节点属性检索与类 SQL 查询语言

**日期**：2026-10-14
**性质**：外部产品调研 + Vectra 落地映射
**承接**：[docs/design/2026-10-14-durability-write-model.md](../design/2026-10-14-durability-write-model.md) 的 RDBMS / 图引擎调研线。该篇解决"用什么引擎存"，本篇回答图数据库的三个具体能力问题。
**调研方法**：4 个并行调研（Vectra 仓库现状 / 边衰减机制 / 节点属性检索 / 查询 DSL），信息以 2026-10 官方文档为准，无法核实的结论保留"未证实"标注。

---

## 背景与三个问题

corpit（服务依赖图）与 scv（Spring 代码分析）两个场景向图存储提出三个问题：

| # | 问题（原始表述） | 业界标准术语 | 章节 |
|---|---|---|---|
| Q1 | 边上能否挂一个"consumer 函数"，对两节点间的边做**周期性数值衰减** | trigger / stored procedure / scheduled job / TTL / **read-time decay** | §2 |
| Q2 | 节点能否**自带属性**（scv 中 bean 的构造型 controller/service），并被一条检索语句快速检出 | node property + secondary index（属性图模型） | §3 |
| Q3 | 图能否被一个**类 SQL 的 DSL** 快速检索 | Cypher / GQL / SQL-PGQ / GSQL 等图查询语言 | §4 |

---

## TL;DR

1. **Q1（边衰减）**："consumer 函数"在业界没有这个叫法，对应物是 **trigger（写事件触发）/ 存储过程 / 定时任务（scheduled job）/ TTL**。关键结论：**没有任何主流图数据库内置"周期性对边跑数值函数"的调度器**。唯一有原生 trigger 的是 Memgraph（仅写事件驱动、不能定时）；TigerGraph 有最强的服务端边批计算（存储过程 + 累加器）但定时靠外部 cron；PostgreSQL + Apache AGE 是唯一 trigger + pg_cron 双全的。行业的主流答案是 **lazy 读时衰减 + 低频压实（hybrid）**：指数衰减 `w·e^(−λ·Δt)` 是可交换计算，根本不需要周期任务（§2.2）。
2. **Q2（节点属性）**：可以，这是属性图模型的看家本领——但**命门是二级索引而不是属性本身**（Neptune、Kùzu 均无二级索引，属性过滤退化为扫描，是两个现成反例）。Vectra 现状：节点已有 `Props map[string]any` 和字符串属性索引（`IndexProperty` / `LookupByProperty`），scv 的 `type=controller` 字符串等值检索**现状已支持**；缺口在数值型属性值与组合过滤（§3.4）。
3. **Q3（类 SQL DSL）**：图查询语言在 2024 年起已收敛到 ISO 双标准——**GQL（ISO/IEC 39075:2024，SQL 之后近 40 年第一个新 ISO 数据库语言）** 与 **SQL:2023 Property Graph Queries（SQL/PGQ）**，两者模式匹配语义一致。2025–2026 新入场者（Microsoft Fabric Graph、Google Spanner Graph、蚂蚁 TuGraph 4.0）清一色选 GQL/PGQ；Gremlin 无新系统采用。自研 DSL 的共识路线是 **openCypher/GQL 子集（MATCH–WHERE–RETURN）**（§4.4）。
4. **市场提示**：2025–2026 图数据库市场动荡明显——Kùzu 归档（团队进 Apple）、Dgraph 两度易主、ArangoDB v4.0 反而**移除**了服务端定时任务、NebulaGraph 社区版停滞在 3.8。"不押注外部图库、自研嵌入式轻量引擎"的路线在当下反而更稳（§5）。

---

## 1. 术语澄清：你说的 "consumer 函数" 叫什么

| 场景 | 术语 | 含义 | 典型实现 |
|---|---|---|---|
| RDB | **trigger（触发器）** | 写事件驱动的服务端回调 | PostgreSQL `CREATE TRIGGER`、MySQL trigger |
| RDB | **stored procedure（存储过程）** | 预编译、可 INSTALL 的服务端程序 | Oracle/PG PL/pgSQL、TigerGraph `INSTALL QUERY` |
| RDB | **scheduled job（定时任务）** | 周期执行的后台作业 | `pg_cron`、MySQL `EVENT`、ArangoDB Foxx queue（已弃用） |
| 图库 | **database trigger** | 对节点/边增删改事件的 Cypher 回调 | **Memgraph（唯一原生）**、Neo4j APOC trigger |
| 图库 | **TTL** | 按时间戳**硬过期删除**，不做数值计算 | NebulaGraph `TTL_COL`/`TTL_DURATION`、ArangoDB TTL index |
| 图库 | **CDC stream** | 变更日志的拉模式消费者 | Neptune Streams |
| 通用 | **read-time (lazy) decay** | 读时按 `last_updated` 现算衰减值 | Reddit/HN 排名、流式时间衰减聚合（TIDE） |

要点：**"周期性对边跑数值函数"这个能力，图数据库行业普遍不做进内核**——要么写事件 trigger 记账 + 外部 cron 批量重写，要么干脆读时惰性计算。

---

## 2. Q1：对边做周期性数值衰减

### 2.1 各产品服务端计算机制对比

| 产品 | trigger | 存储过程/服务端计算 | 定时能力 | 周期衰减的推荐姿势 | 2026 状态 |
|---|---|---|---|---|---|
| **Neo4j** | ❌ 原生无；APOC `apoc.trigger.install`（2025.06 起 `add` 改名 `install`，admin-only，写事件驱动） | APOC 过程 | `apoc.periodic.repeat`（进程内作业，非持久化调度器；`apoc.periodic.iterate` 已在 Cypher 25 弃用，替代为 `CALL {} IN CONCURRENT TRANSACTIONS`） | trigger 只记 `last_updated`；衰减读时算 + 外部 cron 压实 | 活跃，最成熟 |
| **Memgraph** | ✅ **唯一原生** `CREATE TRIGGER … ON (CREATE\|UPDATE\|DELETE)() OF (NODE\|EDGE) AFTER COMMIT EXECUTE <openCypher>`，持久化 | Cypher | ❌ 无定时语义 | trigger 在边写入时维护 `w`/`last_updated`，衰减读时算 | 活跃（3.0，转向 GraphRAG 叙事；公司规模小） |
| **TigerGraph** | ❌ | ✅ **最强**：`CREATE QUERY` + `INSTALL QUERY` 编译为 C++ 并生成 REST 端点；`ACCUM` 累加器批量算边 | ❌ 无内置 cron（社区通行做法：crontab 调 REST） | `decay_edges()` 查询（`ACCUM e.@w = e.w * exp(-λ*Δt)` 写回）+ 外部 crontab | 活跃（2025-07 私募收购） |
| **ArangoDB** | ❌ | Foxx 微服务（JS） | 3.12 有 Foxx queues / `tasks.registerPeriodic`，**已弃用且 v4.0（2026 GA）整体移除**——行业风向：后台任务移出数据库 | 3.12 可用 recurring job 跑 AQL 批量 `UPDATE`；v4.0 起必须外置调度 | 活跃，BSL 1.1 |
| **NebulaGraph** | ❌ | ❌ | ❌；但有 **TTL**（`TTL_COL="last_ts" TTL_DURATION=3600`，**硬过期删边，不做数值衰减**） | TTL 做死边清理；数值衰减靠外部 cron 跑 nGQL `UPDATE EDGE` 或读时算 | 社区版停 3.8（2023-10）；v5 仅企业版 |
| **Dgraph** | ❌ | lambda（仅请求驱动 webhook） | ❌ | 只能外置 cron | 活跃，但两度易主（见 §5） |
| **Kùzu** | ❌ | UDF 走客户端 API（无 SQL 级 `CREATE FUNCTION`） | ❌（嵌入式，"服务端"就是宿主进程） | 宿主进程 timer 周期跑 Cypher `SET r.w = …` | **2025-10 仓库归档**（团队进 Apple） |
| **Neptune** | ❌ | ❌（Streams 是 CDC 拉模式，官方明说不会主动触发 Lambda） | EventBridge Scheduler + Lambda（外置） | eager：EventBridge 定时调 Lambda 批量更新 | 活跃 |
| **PostgreSQL + AGE（RDB 基线）** | ✅ 原生 | ✅ PL/pgSQL | ✅ `pg_cron` | **唯一双全**：trigger 在边写入时把旧权折算到当前时刻；pg_cron 增量 `UPDATE … SET w = w*exp(−λ*Δt) WHERE last_updated < now()−interval '1h'` | 活跃 |

注：AGE 的图就是 PG schema，每种 label 一张表、边是带 `start_id/end_id` 的表行，所以 RDB 的 trigger/pg_cron 直接可用（绕过 Cypher 直接 UPDATE `_ag_label_*` 表属社区实践，官方支持口径未证实）。

### 2.2 工程界的三种衰减模式

衰减公式（指数衰减，corpit 的"调用强度随时间冷却"即此类）：

```
w_eff(t) = w_last · e^(−λ · (t − t_last))
```

| 模式 | 做法 | 优点 | 缺点 | 代表 |
|---|---|---|---|---|
| **Eager（定时重写）** | cron 周期 `w = w·e^(−λΔt)` 写回 | 读路径零开销；权重可索引可排序 | 写放大（多数边白写）；精度受限于调度周期 | 传统批处理 |
| **Lazy（读时现算）** | 只存 `w` + `t_last`，读时现算 `w_eff` | **零后台写**；精度连续 | 不能按 `w_eff` 建索引；删死边要扫 | Reddit hot / HN 排名（读时算时间惩罚）、TIDE |
| **Hybrid（惰性 + 压实）** | 读时惰性 + 低频后台压实（固化写回 + 删 `w_eff < θ` 的死边，只碰脏边） | 兼得精度与清理；压实频率可很低 | 一次可控写放大；需 `t_last` 可过滤 | **Redis 的 lazy + active expiration（教科书实现）** |

关键数学性质：`w·e^(−λΔt)` 是**可交换的折叠计算**——新事件到来时先折旧再加新（`w' = w·e^(−λΔt) + Δw`），与先加后折结果一致。这就是 lazy 模式成立的根基，也是排名系统的标准做法。

### 2.3 行业结论

1. **没有主流图数据库内置周期调度器**；有 trigger 的（Memgraph、APOC）只覆盖"写时记账"，不覆盖"定时衰减"。
2. **趋势是把后台作业移出数据库**：ArangoDB v4.0 直接移除了数据库内定时机制；Neo4j 弃用 `apoc.periodic.iterate` 转向显式并发事务语法。
3. 数值衰减的**通用解是 lazy + 低频压实**，把"周期"从数据库职责里拿掉。

### 2.4 对 Vectra 的启示

Vectra 现状：边是 `Edge{ID, Src, Dst, Label, Props map[string]any}`（[vectra.go:48](../../vectra.go)），无数值权重一等字段（权重是 props 约定，README 已有 `"weight": 2` 示例）；`UpsertEdge` 已被定位为"更新 weight-like props（关系强度、事件计数）的入口"（[vectra.go:167](../../vectra.go)）；全库无任何 decay/ttl 代码。

建议采纳 **lazy + 压实**，而不是引入 trigger/cron 子系统（与行业趋势相反）：

1. **约定（或一等支持）数值边属性对 `(w, t_last)`**——corpit 每次调用强度 +1 时走 `UpsertEdge`，引擎在写入时自动折旧：`w_new = w_old·e^(−λΔt) + Δw`，`t_last = now`。
2. **读时衰减参数**：遍历/查询 API（`neighbors`/`bfs`/`Query()`）接受可选 `λ`（或 half-life），返回时对数值边属性现算 `w_eff`；也支持按 `w_eff` 阈值过滤。Seal 后的只读 CSR 上零额外写入，与现有"内存只读图"模型完全契合。
3. **压实 API**：`CompactEdges(halfLife, θ)` 把衰减固化写回并删除 `w_eff < θ` 的死边——可以搭 Seal/snapshot 的便车执行（反正整图快照重写，写放大不新增）。
4. **不做**：数据库内 trigger、内置 cron。若上层（corpit 服务）想要 eager 语义，调用 `CompactEdges` 即可，调度权留给宿主。

---

## 3. Q2：节点自带属性 + 快速检索

### 3.1 先选模型：属性图，不是 RDF

| 维度 | 属性图（Neo4j/Nebula/AGE/Vectra…） | RDF 三元组库（GraphDB/Jena…） |
|---|---|---|
| 属性表达 | 节点内嵌 key-value | 属性=一条三元组（值是 literal），与"边"同构 |
| "节点"概念 | 一等实体（id + label + 属性聚合） | 隐式实体（靠主语 URI 聚合，无边界） |
| 等值过滤 | `WHERE n.type='controller'`，索引直接命中 | 三元组模式 `?s :stereotype "controller"`，literal index 加速 |
| 边带属性 | 原生 | 需 reification（4 条三元组）或 RDF-star |
| 向量生态 | 多数 2024–2025 已原生 | 通常外置 connector |

scv/corpit 的需求（节点带属性 + 快速过滤 + 可能的向量）落在属性图一侧；RDF 适合本体互操作、OWL 推理、SPARQL 联邦那些场景。**Vectra 已是属性图模型，方向正确。**

### 3.2 各产品属性与索引能力

| 产品 | 属性模型 | 属性二级索引 | "type=controller" 检索 | 原生向量 |
|---|---|---|---|---|
| **Neo4j** | label + 任意 kv | range/text/lookup/full-text/point，6 类 | ✅ 快（range 索引） | ✅ 5.11+（HNSW），2026.01 起支持预过滤的 filtered vector search（按官方路线图，细节未逐字核验） |
| **NebulaGraph** | tag 强 schema | native index（需 `REBUILD`；与 TTL 互斥） | ✅ 快（`LOOKUP`） | 仅企业版 v5.1+；社区版无 |
| **ArangoDB** | JSON 文档 | persistent/inverted/ArangoSearch | ✅ 快 | ✅ 3.12（需开关；`storedValues` 索引内过滤）；BSL 1.1 |
| **TigerGraph** | 强 schema 列 | 主键哈希 + 4.x `ALTER VERTEX … ADD INDEX`（单属性） | ✅ 快（建索引后） | TigerVector |
| **Dgraph** | predicate 即属性 | `@index(hash/exact/term/trigram…)` | ✅ 快（hash 最优） | ✅ v24+ `@index(vector(cosine))` |
| **Memgraph** | label + kv | `CREATE INDEX ON :Label(prop)` | ✅ 快 | ✅ `CREATE VECTOR INDEX` |
| **Neptune DB** | label + kv | ❌ **无用户索引**（靠内置 statement index + 扫描） | ❌ 慢（未锚定 id 的属性过滤） | 仅 Neptune Analytics 有 |
| **Kùzu** | node table 强 schema | ❌ **无用户二级索引**（仅 PK；`WHERE` 是扫描） | ⚠️ 扫描（列存较快） | extension（HNSW） |
| **GraphDB (RDF)** | 三元组 + literal | literal index + 外置 Lucene/ES | ⚠️ 中 | 外置 |
| **PG + AGE** | 行 + agtype JSON | PG B-tree 表达式索引（SQL 侧建） | ✅ 快（天然） | pgvector 同库共存 |

代表写法（同一件事，三种风格）：

```cypher
// Neo4j / Memgraph / AGE（Cypher）
CREATE INDEX bean_type_idx IF NOT EXISTS FOR (b:Bean) ON (b.type);
MATCH (b:Bean) WHERE b.type = 'controller' RETURN b;
```

```ngql
// NebulaGraph
CREATE TAG INDEX bean_type_idx ON bean(type(16));
REBUILD TAG INDEX bean_type_idx;
LOOKUP ON bean WHERE bean.type == "controller" YIELD id(vertex) AS vid;
```

```sql
-- PostgreSQL + AGE（属性点查天然快，RDB 基线）
CREATE INDEX bean_type_idx ON spring_graph."Bean" ((properties ->> 'type'));
SELECT * FROM cypher('spring_graph', $$ MATCH (b:Bean) WHERE b.type = 'controller' RETURN b $$) AS (b agtype);
```

### 3.3 反例的教训

Neptune（无二级索引）和 Kùzu（无二级索引且项目已归档）说明：**"属性图"这个名字本身不保证属性点查快——二级索引才是这个需求的命门**。对低基数过滤键（stereotype/type 这类），业界做法是建倒排/哈希索引（参照 Dgraph 的 hash tokenizer、向量库的 metadata filter），而不是指望图遍历顺路过滤。

### 3.4 Vectra 现状与缺口

现状（均已实现）：

- 节点：`Node{ID, Key, Label, Props map[string]any}`（[vectra.go:41](../../vectra.go)）——scv 的 `type=controller/service` 直接放 `Props` 即可，`AddNode` 幂等写、同 key 重写即更新。
- 属性索引：`IndexProperty(key)` 在 Seal 时建索引（[vectra.go:509](../../vectra.go)），`LookupByProperty(key, value)` 等值检索（[vectra.go:572](../../vectra.go)），MCP 工具 `lookup_property` 已暴露。

缺口：

1. **索引值只支持 string**：`indexEntry{val string, id uint32}`（[internal/engine/engine.go:39](../../internal/engine/engine.go)）。数值型属性（corpit 的调用强度、scv 的 LOC 之类）无法走索引，也不支持范围查询。建议把索引值类型化（string/int64/float64/bool），Seal 时按属性实际类型分桶。
2. **无组合过滤**：`type=controller AND pkg=com.acme.order` 需要多属性组合/交集合流（现有 `NodeFilter` 在遍历时后过滤，可先做"多属性索引求交集"的快路径）。
3. **无向量**：仓库名叫 Vectra，但目前是纯依赖图引擎（全库无 embedding 存储代码）。若 corpit/scv 将来要"向量 + type 过滤"混合检索，需另立项（参照 Neo4j filtered vector search、ArangoDB `storedValues` 的预过滤设计，而非后过滤）。

---

## 4. Q3：类 SQL 的查询 DSL

### 4.1 语言全景（2026）

| 语言 | 血缘 | 类 SQL 程度 | 范式 | 标准化 | 主要支持产品 |
|---|---|---|---|---|---|
| **ISO GQL** | ISO/IEC 39075:2024；融合 openCypher + PGQL + GSQL + G-CORE | 中（保留 WHERE/ORDER BY/LIMIT 子句，无 SELECT-FROM 投影） | 声明式模式匹配 | ✅ ISO 已出版 | Neo4j（Cypher 25/26 收敛中）、**MS Fabric Graph**、**Spanner Graph**、TuGraph 4.0、NebulaGraph EE、TigerGraph（实现中） |
| **SQL/PGQ** | SQL:2023 第 16 部分（PGQL 后裔） | **高（真 SQL 扩展）** | 声明式（SQL 内嵌图模式） | ✅ ISO 已出版 | Oracle 23ai/26ai、DuckDB+DuckPGQ、Spanner Graph；PG 主线补丁评审中（AGE 实现的是 openCypher，**不算** PGQ） |
| **Cypher/openCypher** | Neo4j → 开放规范 v9，正"为 GQL 铺路" | 中 | 声明式模式匹配 | 事实标准 | Neo4j、Neptune、Memgraph、FalkorDB、AGE、AgensGraph、ArcadeDB、TuGraph |
| **GSQL** | TigerGraph 自研 | 高（SELECT-FROM-WHERE；"ANSI SQL 超集"的营销说法不成立，官方口径是 SQL-like） | 声明式块 + 累加器过程体（图灵完备） | ❌ 厂商私有 | 仅 TigerGraph |
| **nGQL** | NebulaGraph 自研 | 中（GO/LOOKUP/YIELD 关键词像 SQL，管道组合是独特心智模型） | 双轨（原生 + MATCH） | ❌ | 仅 NebulaGraph |
| **Gremlin** | Apache TinkerPop | 低（函数链，无 SQL 文本面） | **命令式遍历**（优化器自由度小） | Apache 规范 | Neptune、JanusGraph、Cosmos DB、HugeGraph（均为存量系统） |
| **AQL** | ArangoDB 自研 | 中（FOR/FILTER/COLLECT） | 声明式，多模型统一 | ❌ | 仅 ArangoDB |

### 4.2 同一查询的六种写法

需求：*名为 X 的 controller，1..2 跳内调用的所有 service 及边*。

```cypher
// Cypher / openCypher（Neo4j、Memgraph、AGE、FalkorDB…）
MATCH (c:Controller {name:'X'})-[e:CALLS*1..2]->(s:Service)
RETURN c.name, s.name, e
```

```sql
-- ISO GQL（与 PGQ 共享模式匹配语义；属性过滤写在模式内）
MATCH (c:Controller WHERE c.name = 'X') -[:CALLS]->{1,2} (s:Service)
RETURN c.name AS controller, s.name AS service
```

```sql
-- SQL/PGQ（Oracle 23ai、DuckDB DuckPGQ、Spanner Graph）
SELECT svc.service, COUNT(e) AS calls
FROM GRAPH_TABLE (app_graph
     MATCH (c IS Controller WHERE c.name = 'X') -[e IS CALLS]->{1,2} (s IS Service)
     COLUMNS (s.name AS service)) AS svc
GROUP BY svc.service;
```

```sql
-- GSQL（TigerGraph；累加器范式）
result = SELECT t FROM Controller:s -(CALLS:e1)- :m -(CALLS:e2)- Service:t
         WHERE s.name == cname
         ACCUM @@edges += e1, @@edges += e2;
PRINT result;
```

```sql
-- nGQL（NebulaGraph 原生；LOOKUP 定位 + GO 遍历，管道衔接）
LOOKUP ON Controller WHERE Controller.name == "X" YIELD id(vertex) AS vid
| GO 1 TO 2 STEPS FROM $-.vid OVER CALLS
  YIELD $$.Service.name AS service;
```

```groovy
// Gremlin（命令式：一步步告诉 traverser 怎么走）
g.V().has('Controller','name','X').repeat(outE('CALLS').inV()).times(2).emit()
  .hasLabel('Service').dedup().path()
```

### 4.3 2026 年的选型共识

1. **新系统在向 GQL/openCypher 靠拢，Gremlin 无新增支持者**（观察性结论，非严格统计）：openCypher 项目官方改口径为"为 GQL 铺路"；Neo4j 承诺"让 Cypher 成为 GQL 的一种实现"；2024–2026 新入场者（Fabric Graph、Spanner Graph、TuGraph 4.0）全部选 GQL/PGQ。
2. **"类 SQL"诉求的官方答案是 SQL/PGQ**：会 SQL 的用户零成本（Oracle 23ai、DuckDB 的范本）；且 PGQ 与 GQL 模式匹配语义同源，"GQL 为主 + SQL 风格入口"两条腿不冲突。
3. **可参考的公开先例**：蚂蚁 TuGraph-DB 同时支持 openCypher 与 ISO GQL；TuGraph Analytics 的"SQL+GQL 融合语法"（SELECT-FROM-WITH-MATCH-RETURN，把 MATCH-RETURN 当 SQL 的数据子视图）——与 Vectra"向量+图、类 SQL 检索"的诉求最接近的公开设计。
4. Gremlin 路线与"类 SQL、可被优化器自由改写"的目标相悖；GSQL 式纯 SQL 关键词表达力够但生态孤立（单厂商）。

### 4.4 对 Vectra 的启示

- **v1 做 openCypher/GQL 子集**：`MATCH (模式)-[:LABEL*1..n]->(模式) WHERE … RETURN … [ORDER BY …] [LIMIT n]`。现有 `neighbors`/`bfs`/`shortest_path`/`reachable`/`lookup_property` 恰好就是这个子集的物理算子——DSL 层做的是"模式匹配 → 现有遍历原语的编译"，不需要新执行引擎。
- 挂载点：REST `POST /v1/query` 的超集 + 新 MCP 工具（如 `query_gql`），AI agent 侧收益最大（Cypher/GQL 是 LLM 训练语料最丰富的图语言，Fabric 已在做 NL2GQL）。
- 长期可选：`GRAPH_TABLE` 风格的 SQL 嵌入入口（服务 corpit 里写 SQL 的场景），语义与 GQL 子集同源，可复用同一套模式匹配。
- **不建议**：自造全新语法（生态孤立刻舟求剑，GSQL/nGQL 都是前车之鉴）；Gremlin 风格命令式 API。

---

## 5. 附：2025–2026 图数据库市场风险速览

选型外部图库前值得知道的事（也是自研路线的间接论据）：

| 产品 | 事件 | 影响 |
|---|---|---|
| Kùzu | 2025-10 仓库归档，团队被 Apple 收编；社区 fork（Ladybug 等）成熟度未证实 | 嵌入式图库最接近的参照物退出市场 |
| Dgraph | 2023-11 并入 Hypermode（曾关停 Cloud）；2025-10 转手 Istari Digital | 两次易主，长期路线不可预测 |
| ArangoDB | v4.0（2026 GA）移除 Foxx queues/tasks（数据库内定时）；3.12 起改 BSL 1.1 | "数据库内跑后台任务"被厂商自己证伪 |
| NebulaGraph | 社区版停在 3.8（2023-10）；v5 向量/GQL 仅企业版 | 开源版实际维护冻结 |
| TigerGraph | 2025-07 被 Cuadrilla Capital 私募收购 | 仍在运营，路线待观察 |
| RedisGraph | 2025-01 EOL → FalkorDB 接棒 | 又一个图库退场案例 |
| Neo4j / Memgraph / PG+AGE | 活跃 | 相对稳妥的外部选项 |

---

## 6. 综合建议（Vectra 路线图草案）

按对 corpit / scv 的直接收益排序：

| 优先级 | 能力 | 内容 | 依托现状 |
|---|---|---|---|
| **P0**（corpit） | 边衰减 lazy 模式 | 数值边属性 `(w, t_last)` 约定；`UpsertEdge` 写入折叠 `w' = w·e^(−λΔt) + Δw`；遍历/查询返回时支持 `λ` 参数现算 `w_eff`；`CompactEdges(θ)` 压实清理死边（搭 Seal/snapshot 便车） | `UpsertEdge` 已是 weight-like props 入口（[vectra.go:167](../../vectra.go)） |
| **P0**（scv） | 属性索引类型化 | 索引值从 string 扩展到 int64/float64/bool + 范围查询；多属性索引求交集 | `IndexProperty`/`LookupByProperty`（[vectra.go:509](../../vectra.go), [vectra.go:572](../../vectra.go)）；`indexEntry`（[internal/engine/engine.go:39](../../internal/engine/engine.go)） |
| **P1** | openCypher/GQL 子集 DSL | `MATCH–WHERE–RETURN` + `*1..n` 量词路径 + ORDER BY/LIMIT；挂 `POST /v1/query` 超集与 MCP 工具 | 现有遍历原语即物理算子 |
| **P2** | 向量能力（若需要） | HNSW + 属性预过滤（metadata filter），需先确认 corpit/scv 的 embedding 需求 | 全新模块，当前无任何向量代码 |
| 不做 | trigger / 内置 cron 子系统 | 与行业趋势相反（ArangoDB v4.0 已移除同类机制）；宿主进程自己调度 `CompactEdges` 即可 | — |

---

## 7. 主要来源

**边衰减 / trigger / 定时**：[APOC Triggers](https://neo4j.com/docs/apoc/current/background-operations/triggers) · [APOC 变更清单](https://neo4j.com/docs/apoc/current/deprecations-and-additions) · [Memgraph Triggers](https://memgraph.com/docs/fundamentals/triggers) · [TigerGraph Create and Install Queries](https://docs.tigergraph.com/gsql-ref/4.3/querying/create-and-install-queries) · [ArangoDB tasks（v4.0 移除）](https://docs.arango.ai/arangodb/stable/develop/javascript-api/tasks) · [ArangoDB 弃用清单](https://docs.arango.ai/arangodb/stable/release-notes/deprecated-and-removed-features) · [NebulaGraph TTL](https://docs.nebula-graph.io/3.8.0/3.ngql-guide/8.clauses-and-options/ttl-options) · [Neptune Streams](https://docs.aws.amazon.com/neptune/latest/userguide/streams.html) · [AGE Graphs（边=表行）](https://age.apache.org/age-manual/master/intro/graphs.html) · [pg_cron](https://github.com/citusdata/pg_cron) · [Redis lazy+active expiration](https://redis.io/docs/latest/commands/expire/) · [PG-Triggers 论文（arXiv 2307.07354）](https://arxiv.org/html/2307.07354v3) · [TIDE（时间衰减图）](https://humming80.github.io/papers/tide.pdf) · [HN 排名逆向](http://www.righto.com/2013/11/how-hacker-news-ranking-really-works.html)

**节点属性 / 索引**：[Neo4j 索引](https://neo4j.com/docs/cypher-manual/current/indexes/performance-indexes/create-indexes) · [Neo4j 向量索引](https://neo4j.com/docs/cypher-manual/current/indexes/search-indexes/vector-indexes) · [NebulaGraph LOOKUP](https://docs.nebula-graph.io/3.6.0/3.ngql-guide/7.general-query-statements/5.lookup/) · [NebulaGraph v5.1 企业版向量](https://nebula-graph.io/posts/nebulagraph-enterprise-v5-1-embeds-vector-search-for-ai-grade-data-fusion) · [ArangoDB 索引选型](https://docs.arango.ai/arangodb/stable/indexes-and-search/indexing/which-index-to-use-when) · [TigerGraph Schema 4.3](https://www.tigergraph.com/docs/gsql-ref/4.3/ddl-and-loading/defining-a-graph-schema) · [Dgraph @index](https://docs.dgraph.io/dql/predicate-indexing) · [Memgraph 向量](https://memgraph.com/docs/querying/vector-search) · [Neptune statement indexes](https://docs.aws.amazon.com/neptune/latest/userguide/feature-overview-storage-indexing.html) · [Kùzu 二级索引讨论 #3117](https://github.com/kuzudb/kuzu/issues/3117) · [GraphDB 存储](https://graphdb.ontotext.com/documentation/11.5/storage.html) · [RDF vs 属性图](https://neo4j.com/blog/knowledge-graph/rdf-vs-property-graphs-knowledge-graphs)

**查询语言**：[ISO/IEC 39075:2024](https://www.iso.org/standard/76120.html) · [gqlstandards.org](https://www.gqlstandards.org/) · [Neo4j GQL 新闻稿](https://neo4j.com/press-releases/gql-standard) · [Neo4j Cypher→GQL 路线](https://neo4j.com/blog/cypher-and-gql/cypher-path-gql) · [Neo4j GQL conformance](https://neo4j.com/docs/cypher-manual/current/appendix/gql-conformance) · [Oracle 23ai SQL/PGQ](https://blogs.oracle.com/database/property-graphs-in-oracle-database-23ai-the-sql-pgq-standard) · [DuckPGQ](https://duckpgq.org) / [CIDR 论文](https://www.cidrdb.org/cidr2023/papers/p66-wolde.pdf) · [Postgres PGQ 补丁综述](https://pgweekly.github.io/en/2026/07/sql-property-graph-queries-pgq.html) · [Spanner Graph GA](https://cloud.google.com/blog/products/databases/spanner-graph-is-now-ga) · [MS Fabric Graph](https://learn.microsoft.com/en-us/fabric/graph/graph-database) · [opencypher.org](https://opencypher.org/) · [GSQL 官网](https://www.tigergraph.com/gsql) · [nGQL 概览](https://docs.nebula-graph.io/3.6.0/3.ngql-guide/1.nGQL-overview/1.overview) · [TinkerPop](https://tinkerpop.apache.org) · [AQL 遍历](https://docs.arango.ai/arangodb/stable/aql/graph-queries/traversals) · [TuGraph 4.0](https://tugraph-db.readthedocs.io/zh-cn/v4.0.0/2.introduction/3.what-is-tugraph.html) · [TuGraph SQL+GQL 融合](https://www.cnblogs.com/fanzhidongyzby/p/18098628/ChatTuGraph) · [PuppyGraph Gremlin vs Cypher](https://www.puppygraph.com/blog/gremlin-vs-cypher)

**市场动态**：[Kùzu 归档](https://github.com/kuzudb/kuzu) · [Kùzu 后继分析](https://gdotv.com/blog/kuzu-legacy-embedded-graph-database-landscape) · [Dgraph 归入 Hypermode](https://discuss.dgraph.io/t/dgraph-labs-is-becoming-part-of-hypermode-dgraph-blog/19040) · [TigerGraph 收购公告](https://www.tigergraph.com/blog/tigergraph-accelerates-enterprise-ai-infrastructure-innovation-with-strategic-investment-from-cuadrilla-capital) · [RedisGraph EOL / FalkorDB](https://docs.falkordb.com/operations/migration/redisgraph-to-falkordb) · [NebulaGraph 2025 回顾](https://nebula-graph.io/posts/nebula-2025-year-in-review-charting-a-new-era-of-graph-intelligence-and-ai-convergence)
