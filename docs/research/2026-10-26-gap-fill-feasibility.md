# 补齐功能空洞：五项缺口的落地可行性调研

**日期**：2026-10-26
**性质**：代码侧落点取证 + 外部设计参考
**承接**：功能对照表调研（上一 session，对照 Vectra v0.11.4 与 Neo4j/Memgraph/FalkorDB/Kùzu）锁定的空洞清单：Cypher 写子句、全文搜索、唯一性约束、WITH 管道、REST 鉴权。本篇回答"每一项补起来要动哪里、怎么动、难度多大"。
**基准**：main@cd43731（v0.11.4）。所有 file:line 以此为准。
**调研方法**：5 路并行（internal/cypher 查询层架构 / 写路径与校验点 / 索引层 / server+MCP 鉴权面 / 外部设计参考：openCypher 写语义、嵌入式 FTS、约束与鉴权模式）。

---

## TL;DR

1. **两项架构红利先说清**（结论：架构站在补课这边）：
   - **三源补偿让全文索引免费正确**：`lookupNodesOf`（vectra.go:3190-3239）的候选 = base 索引命中 ∪ 全部层记录，再过墓碑 carve + `match` 重过滤。任何驻留 base 的索引在未合并层之下自动正确——索引陈旧是良性的。FTS 不需要发明增量更新机制，复用类型化索引的"Seal 时全量重建"生命周期即可。
   - **单写者 + 幂等 upsert 使 MERGE 免锁原子**：直接模式只有一个合法写者 goroutine，wrapper 模式全序排队；且 AddNode 按逻辑 key 幂等、UpsertEdge 按三元组幂等。Cypher MERGE 没有任何 TOCTOU 窗口——Neo4j 为 MERGE 上的并发锁（两端点排他锁）在单写者引擎里根本不存在这个问题。
2. **难度/风险排序**（从轻到重）：**鉴权（最小）< 全文搜索 < WITH 管道 ≈ Cypher 写子句 < 唯一约束（最重）**。前两项互相独立、可立即立项；后三项共享一个地基（AST 从固定两字段结构改为子句列表 + scope-chain 校验），应一次重构、分期使用。
3. **全文搜索设计收敛**：两路独立调研（代码侧落点 + 外部方案）殊途同归——**Seal 时构建的 ngram 排序切片**，复用类型化索引生命周期与查询补偿。n 取 **2（bigram）**而非 3：pg_bigm 的存在本身证明 trigram 对 CJK 与 1-2 字关键词太粗，而用户场景含中文文本（无空格，unicode61 式分词产生整串长 token，不可用）。内存红线：约 24-30 B/条 (gram,id)，1M 节点 × 100 字符 ≈ 3-5 GB，**必须 opt-in 按 (label, property) 声明**。
4. **鉴权形态收敛**：v1 = 静态 bearer token（`-auth-token-file`，空=保持现状开放，存量测试零改动），豁免 `/v1/health`，Join/httpPost 附带同一 token，MCP 的 Python/TS 代理加 `VECTRA_TOKEN`。v2 蓝本 = rqlite 形态（basic-auth 凭证文件 + 权限三元组 + HTTPS + 节点间独立证书）。**边界必须文档化**：raft 节点间 TCP 是明文，HTTP 鉴权永远护不到复制流量，网络隔离是前提，mTLS 后置。
5. **上轮对照表有一处勘误**：v0.11.4 **没有 SKIP**（`ReturnClause` 只有 `Star/Items/OrderBy/Limit`，ast.go:46-51；全 repo 无 SKIP）。此外 WHERE 表达式的缺口比对照表显示的更深：无算术、无 `$` 参数、无 `IS [NOT] NULL`、无 `=~` 正则、无函数调用，列表字面量只能作 IN 的右操作数（详见 §0）。
6. **建议立项顺序**：① 鉴权（独立，天级）→ ② FTS bigram（独立，低风险）→ ③ 子句列表地基 + row 标量绑定（WITH 与写子句共享）→ ④ WITH/UNWIND/OPTIONAL MATCH → ⑤ Cypher 写子句（CREATE/SET/DELETE/DETACH DELETE → MERGE）→ ⑥ 唯一约束（写语义稳定之后）。

---

## 0. 新发现的勘误与小缺口（对照表之外）

上轮对照表之外，本轮代码取证发现的事实修正与新增缺口：

| # | 发现 | 证据 |
|---|---|---|
| E1 | **无 SKIP**。上轮写"LIMIT/SKIP"有误 | ast.go:46-51；internal/cypher 全目录 grep SKIP 无果 |
| E2 | WHERE 表达式无算术（lexer 无 `+`）、无字符串拼接、无 `IS [NOT] NULL`、无函数调用、**无 `$param` 参数机制** | lexer.go:201-257, 16-44；parser.go:626-658 |
| E3 | 列表字面量只能作 IN 右操作数，其他位置求值为 null | project.go:426-427 |
| E4 | `RETURN *, count(*)` 可解析但 validate 拒绝（半实现） | parser.go:104-116 vs 740-749 |
| E5 | REST `/v1/join` 是**无认证的集群管理端点**（可达 leader HTTP 口即可加 voter）；`/debug/vars` 暴露 expvar；raft 节点间 TCP 明文无互认 | api.go:290-322, 76；node.go:136 |
| E6 | `DeleteEdge` 按 (src,dst,label) 删**全部**平行边；删除单条特定平行边（Cypher DELETE 持有 EdgeID 时）今日无法表达 | vectra.go:1279 |
| E7 | Cypher 对数值范围谓词**不下推**：`WHERE n.w > 3` 逐行求值；`LookupNumberRange` 只有公共 Go API 在用 | exec.go:103-110；vectra.go:3149 |
| E8 | 无 UpdateNode/SetProperty 原语：属性变更 = 整体替换（读 props → merge → AddNode 回写） | delta.go:264/318/420 |
| E9 | AddNode 无任何结构校验（label 可空、属性类型不查）；writer_hook.go 不是校验钩子，是写者 goroutine 身份纪律 | vectra.go:77-94；writer_hook.go:38-43 |

这些小缺口（SKIP、算术、参数机制、范围下推）与五个大项同域，立项时应作为"查询语言补课"的一部分顺带评估，单独不值一个 release。

---

## 1. REST 鉴权（最小工作量，完全独立）

### 现状（代码事实）

- **零认证零 TLS**：`server/**` 与 `mcp/**` grep `tls|Authorization|BasicAuth|Bearer|password` 无业务命中（仅 OCC snapshot token 命名）。`srv.ListenAndServe()` 明文（server/cmd/vectra-server/main.go:69）。
- **路由单一注册点**：全部端点在 `(*Node).Handler()` 的一个 `http.ServeMux` 上（server/api.go:56-80，Go 1.22 method+path 模式）。无框架、无中间件链、无 CORS——包一层就是全部工作量。
- 写端点（leader-only，raft Apply）：`POST /v1/batch`、`/v1/txn`、`/v1/vector/indexes`、`DELETE /v1/vector/indexes/{name}`、**`POST /v1/join`（集群管理）**。读端点：health/stats/await/nodes/neighbors/query/cypher/reachable/path/cycles/vector。另 `POST /v1/query` 虽是 POST 但只读。
- 配置面只有 6 个 flag（main.go:22-27：`-id -http -raft -data -join -seal-interval`），无 env 解析；库侧 `server.Config`（node.go:47-59）。
- Join 无凭证：`httpPost(seedHTTP+"/v1/join")` 明文裸发（node.go:22-29, 236）；raft `NewTCPTransport` 明文（node.go:136）。
- MCP：Go 版进程内嵌引擎（不经 HTTP），默认 stdio（mcp/cmd/vectra-mcp/main.go:46），`-http` 模式挂 `/mcp`；Python/TS 版是 MCP→REST 代理，当前只发 `Content-Type` 头（mcp/python/vectra_mcp/client.py:17；mcp/ts/src/index.ts:17）。

### 设计参考（外部）

- **rqlite**（同款产品结构的直接对标）：HTTP Basic Auth + 每节点 JSON 凭证文件（`-auth`，username/password/perms 三元组，权限含 query/execute/join/backup 等粒度，`*` 用户=匿名权限）+ `-join-as` 节点加入凭证 + HTTPS（`-http-cert/-http-key`，可 mTLS）+ 节点间独立 X.509（`-node-cert/-node-key/-node-ca-cert`）+ 热重载。全部 std-lib 可实现。
- **DuckDB httpserver 扩展**：单一静态凭证（basic / X-API-Key / 无）——证明静态 token 是可接受的低端形态，严肃部署前置代理。

### 落地方案（裁定）

- **v1（推荐）**：静态 bearer token。改动点：`Handler()` 返回前或 main.go:51 包一层 ~30-60 行中间件（`subtle.ConstantTimeCompare`）；新 flag `-auth-token-file`（空 = 保持现状，server/api_test.go、cluster_test.go、bench harness 全部零改动）；豁免 `GET /v1/health`；`Node.Join`/`httpPost` 附带同一 token 使 `/v1/join` 在集群内可用（集群 token = 客户端 token，同一信任域，v0.11 可接受并文档化）；Python/TS MCP 代理加 `VECTRA_TOKEN` env。
- **必须文档化的边界**：raft 复制流量（node.go:136 的 TCP）仍明文——HTTP 鉴权永远护不到它，`-raft` 必须绑内网接口；mTLS 是后续项而非 v1。
- **v2 蓝本**：rqlite 形态（凭证文件 + 权限分级 + HTTPS + 节点间证书分离）。

**难度裁定：小（天级）；风险：低**（零引擎接触，单一 choke point，空配置=行为不变的兼容性保证）。

---

## 2. 全文搜索（bigram ngram 索引，复用类型化索引生命周期）

### 现状（代码事实）

- 类型化属性索引 = **三个排序平面切片**（`strs/nums/bools`，按 (value, id) 联合排序，二分查找），驻留 `Snapshot.indexes`（internal/engine/engine.go:40-65），**只在 Seal 时全量重建**（engine.go:181-220）；层上只有声明（`Layer.indexKeys`，delta.go:97,142）。
- **关键机制——三源补偿**：查询侧 `lookupNodesOf`（vectra.go:3190-3239）的候选集 = base 索引命中 ∪ **全部层记录**，再 carve 节点墓碑，最后用调用方 `match` 谓词重过滤。索引只负责裁剪 base 扫描，正确性由补偿兜底。未密封的"pending"声明退化为全 base 扫描（vectra.go:3215-3219，typed_index_test.go:72-87 有测试）。
- 字符串匹配现状：`CONTAINS / STARTS WITH / ENDS WITH` 是逐行 `strings.Contains` 等（project.go:456-471），全扫 + 过滤，无候选收窄、无提前终止。**仓库内没有任何 ngram/倒排/分词代码**（grep 证实）。
- 声明持久化管道现成：`IndexProperty` → WAL IndexDecl 帧（vectra.go:2996-3026）→ 快照 `store.Data.IndexKeys`（internal/store/store.go:99-107, 252-255）——两者都有字段扩展空间。

### 设计参考（外部）

- **SQLite FTS5**：每事务一个小 b-tree 段、层级增量合并（automerge/crisismerge）——这是多写者磁盘库的复杂度来源；external-content 表形态（索引引用外部存储）与 Vectra 需要的"索引指向节点存储"同构。
- **pg_trgm / pg_bigm**：trigram 对 CJK 与 1-2 字关键词太粗，pg_bigm 专为日文/CJK 做 2-gram——**n=2 的直接依据**。
- **unicode61 式分词对中文不可用**：无空格中文整串成一个 token。bigram 是 CJK 场景下兼顾子串匹配与短关键词的正确粒度。
- **bleve（36 个直接依赖）/ bluge（~15 个）**：均非 std-lib-only，违背零依赖红线——手写的正当性确认。dgryski/go-trigram 是 trigram→ID-set 极简先例。

### 落地方案（裁定）

- **形态**：`bigram` 排序切片（`grams []gramEntry{gram string, id uint32}`），在 `engine.Seal` 现有桶循环旁构建（engine.go:181-220 同型）；查询侧克隆 `LookupByProperty` 为 `LookupByContains(key, substr)`，复用 `lookupNodesOf` 通道（换 baseIndex 函数：bigram posting 交集 → `strings.Contains` 验证 → 同一 `match` 过滤）。**正确性免费继承**——这是本架构独有的红利。
- **声明粒度**：`IndexFullText(label?, property)` opt-in——内存模型决定不能默认开启（1M 节点 × 100 字符 ≈ 3-5 GB；40 字符值约 0.7-1 KB/节点）。
- **增量更新：不需要**。Seal 全量重建 + 查询补偿即架构答案；ID 重编号在 seal 时发生所以天然无害。Clone/Export 需要深拷贝新切片（与 strs 桶同等待遇）。
- **Seal 成本**：O(T log T)，T=总 gram 数——大库 Seal 变长，需在 bench 门里加一条。
- 后续可选：`CONTAINS` 谓词在 `anchorCandidates`（exec.go:446-487）的下推接线（WHERE 语义今日已正确，纯性能项）。

**难度裁定：中偏低；风险：低**（改动局部：一个 Seal 桶 + 一个 lookup 克隆；风险在内存与 Seal 时长，不在正确性）。

---

## 3. Cypher 写子句（CREATE / SET / DELETE / DETACH DELETE / MERGE）

### 现状（代码事实）

- **查询层严格只读**：executor 只持 `*engine.Snapshot`（方法全为 getter，engine/engine.go:227-375）或只读 `Source` 接口（exec.go:49-60）；无任何写句柄。AST 是 `Query{Match, Return}` 固定两字段（ast.go:5-8），parser 只接受 MATCH 开头（parser.go:76, 163-165）。
- **调用链**：`server/api.go:524` / `mcp/server.go:750` → `(*Graph).Cypher`（cypher.go:31）→ `flushPend`（vectra.go:893）→ `view.Load()` → `cypherRunViewOf`（view.go:302-307）→ `cypher.Run/RunView`（exec.go:72/84）。
- **写 API 覆盖盘点**（每个子句对到哪个现有 API）：

| 子句 | 现有覆盖 | 缺口 |
|---|---|---|
| CREATE (n:L {..}) | AddNode（vectra.go:1171） | — |
| CREATE (a)-[:E]->(b) | AddNode 先建端点 + AddEdge（1203，端点缺失硬失败） | 变量绑定顺序编排 |
| MERGE 节点 | AddNode 按 key 幂等 upsert | **created-vs-matched 信号**（ON CREATE/ON MATCH SET 需要） |
| MERGE 边 | UpsertEdge 按三元组 fold（1235；delta.go:379-425）——单调用原子读写写 | 同上 |
| SET n.p = v | 读（Batch 外）→ merge → AddNode 回写 | 无 per-property 原语（E8）；无 RemoveProp |
| DELETE 边 | DeleteEdge 按三元组删全部平行边（1279） | **单条平行边删除不可表达**（E6） |
| DETACH DELETE 节点 | DeleteNode 级联删边（1324）——精确匹配 | — |

- **写执行器需要的东西**：写句柄接口（AddNode/AddEdge/UpsertEdge/DeleteEdge/DeleteNode + 属性合并），经 `RunWrite` 式入口注入 executor；或走 Changeset/`applyChangeset`（vectra.go:1505）在 `g.Batch` 内缓冲整语句原子应用。
- **可见性陷阱**：Batch 内查询看到 pre-batch 代（vectra.go:1550-1552；flushPend 守卫 901-903）。SET 必须在进 Batch 前读 props，或写执行器改用 writer 私有镜像（`nodeLive` vectra.go:916-925、builder 记录 delta.go:211-229——它们看得到 batch 局部状态；UpsertEdge 的 fold 判定就是这么做的）。
- **MERGE 原子性裁定（无 TOCTOU）**：直接模式单写者 goroutine 契约（vectra.go:251-254）+ wrapper 模式全序队列 ⇒ 跨调用无交错写者；即使两个并发客户端都读到"不存在"，两次创建都是按逻辑 key/三元组的幂等 upsert，第二次折叠到第一次。**Neo4j 为并发 MERGE 的两端点排他锁在单写者引擎里无对应物**。
- **ID 纪律**：Cypher CREATE 必须经 AddNode/AddEdge 取号（`nextNode/nextEdge` 棘轮，vectra.go:296-297），绝不自铸 ID；EdgeID 在 Seal 间不稳定（合并重编号），Cypher 边引用应走 key（Changeset wire 本来就是 key 寻址的，wal.go:74-76）。

### 设计参考（外部）

- **Neptune 先例**：2021 年 openCypher 先只读、写子句后补——两段式发布有业界先例。
- **语义要点（Neo4j 手册为基准）**：CREATE 按入站行逐行管道化（UNWIND $rows CREATE 是批量导入第一姿势）；MERGE 全模式匹配或全模式创建，多匹配时行为=MATCH 绑定全部；无向 MERGE 匹配双向、创建确定左→右；MERGE on null 属性报错；SET 的 `=`（整体替换，缺键即删）/`+=`（增量，null 值=删属性）两种形态；DELETE 对带边节点报错、DETACH DELETE 级联。
- **FalkorDB 的坑（要避）**：eager 算子（CREATE/SET/DELETE/MERGE/聚合）完全先于 LIMIT 执行——`UNWIND [1,2,3] CREATE ... LIMIT 1` 在 FalkorDB 建 3 个节点。Vectra 的写执行器必须让写算子尊重上游 LIMIT。
- **读后写可见性**：Kùzu 明确文档化了"更新子句的变量对紧随的 RETURN 可见"——Vectra 需要写执行器用 writer 镜像回读绑定。

### 落地方案（裁定）

- **最小集**（MATCH → CREATE/SET/DELETE/DETACH DELETE，单段管道，无 WITH）：中等难度。SET 走"Batch 外读 + merge + 回写"，CREATE 走 AddNode/AddEdge 编排，DELETE 走 DeleteNode/DeleteEdge。
- **MERGE + ON CREATE/ON MATCH SET**：需要小 API 扩展——AddNode/UpsertEdge 返回 created-vs-matched（或写执行器经 `nodeLive`/镜像预查）。
- **前置依赖**：AST → 子句列表重构（§4 地基，两者共享）；E6（单条平行边删除）建议作为写子句 phase 的伴生 API 项。
- **错误语义**：Batch 无回滚（首错停、已应用条目保留，writer.go:491-497）——必须作为文档化契约而非隐藏行为。

**难度裁定：中等；风险：中**（语义面广但每件都有现成 API 骨架；主要风险在可见性编排与 WAL/Changeset 演化纪律）。

---

## 4. WITH / UNWIND / OPTIONAL MATCH（查询管道补课）

### 现状（代码事实）

- 执行器是**树走式、物化集合解释器**：`runStatement`（exec.go:102-117）三段硬编码——matchRows → 内联 WHERE 过滤 → project。每段完全物化 `[]row` 后进下一段。无迭代器/Volcano。
- 行模型：`row{pos uint32; nodes map[string]uint32; edges map[string]uint32; nodesShared/edgesShared bool}`（exec.go:127-136）——**只绑 node/edge ID，无标量**；COW 纪律（exec.go:148-187）+ 存储后不再变的不变量（exec.go:119-126），由 exec_cow_test.go / exec_alloc_test.go 守护，且有零分配基线（CHANGELOG 记录的 perf 成果）。
- 聚合已是 stage 形状：`projectAgg`（project.go:74-155）多键分组 + DISTINCT + collect——"差一步终结化的 WITH 聚合"。
- 逗号 pattern 的共享变量 join（exec.go:405-428）就是第二个 MATCH 在 WITH 之后的挂接点。

### 落地方案（裁定）

- **WITH（难的那件，~300-450 LOC，中风险）**：四步——AST 子句列表化；parser 线性序列改循环（parseReturnItem/parseOrderKey/LIMIT 解析直接复用）；validate 改 scope-chain（现只读 `q.Match`，parser.go:689-792）；`runStatement` 改 stage 循环 + project/projectAgg 返回行而非终结。**核心阻塞**：`row` 需要加 `vals map[string]any`（标量绑定，`WITH s.key AS k` 所需）——eval/scalarOf/itemValue/dedupKey/validate 全要认识它；节点变量穿过 WITH 时重新播种 `nodes[var]=NodeVal.ID`（itemValue 已产出 NodeVal，project.go:342）；`pos` frontier 在投影行上无意义，MATCH-after-WITH 必须容忍。
- **UNWIND（~100-150 LOC，低风险）**：row.vals 存在后就是每行一循环展开列表绑定别名。注意 encodeVal 的列表回退是近似的（project.go:288-292），DISTINCT/分组键遇列表需留意。
- **OPTIONAL MATCH（~80-120 LOC，低风险）**：逐 base 行跑 runPath、空结果保留原行（模式变量不绑定）；未绑定变量的求值已是 null 安全的（project.go:340-347, 397-401）。需测 cartesian-with-null 语义与 maxRows/dedup 交互。
- **风险集中在 WITH 一处**：COW 行不变量 + row 标量化 + scope 校验三道脆弱缝同时动，零分配基线不能回归。UNWIND/OPTIONAL MATCH 是广义 row 上的边际便宜件。

**难度裁定：WITH 单项中等；全套 ~500-750 LOC，主导风险=WITH 的行模型改造。**

---

## 5. 唯一性约束（(label, property) 唯一）

### 现状（代码事实）

- **写路径今日从不咨询任何索引**：AddNode 只碰 WAL + builder + 向量摄入（vectra.go:1171-1199）。仓库内唯一性机制只有边三元组 fold（delta.UpsertEdgeFold）。
- **可行性根基**：writer 私有状态 = 在窗 Builder 记录（delta.go:211-229）+ 已发布镜像 `g.nodes`（每 publish 调和，vectra.go:1093-1125；rebase 后 `RepairAfterRebase` 重建，merge.go:189-206）——这是完整可见视图，写时检查不需要动读路径。读者侧 `lookupNodesOf` 也能答但每次分配 map，太重。
- **缺件清单**：(1) 声明加 unique 标志（store.Data.IndexKeys 与 WAL IndexDecl 帧都有空间，store.go:99-107）；(2) writer 私有 value→key(s) 映射，在 AddNode/publish/repair 三处维护；(3) DeleteNode/改值时的值释放（节点墓碑 delete→revive 清墓碑语义要联动，delta.go:313-317）；(4) 声明时存量校验扫描（Neo4j 式：有重复即整体失败并报重复项）；(5) 错误语义——Batch 无回滚，违约中途=部分应用，必须文档化；(6) WAL replay 与 writer replay 都复用根写方法，约束检查放方法入口即天然覆盖两条 replay 路径（最佳挂点：各根写方法入口 AddNode:1178 等；次选 wrapper-only 的 applyBatch，但会漏 replay）。

### 设计参考（外部）

- **Neo4j**：唯一性约束必须有范围索引背书（创建约束即建同名索引），写时同步检查，违约抛 `Schema.ConstraintValidationFailed`；对存量冲突数据，约束创建先失败。唯一性是社区版功能，存在性/类型约束是企业版——Vectra 对齐社区版粒度即可。
- **Kùzu**：schema 级 PRIMARY KEY + 自动索引。Vectra 是无 schema 模型，走 Neo4j 式"约束即索引+写时检查"更贴合。
- **单写者红利**：Neo4j 的并发锁问题（唯一性检查的竞态）在 Vectra 不存在——写时检查天然免锁。

### 落地方案（裁定）

形态：`DeclareUnique(label, property)`（或 IndexProperty 加 unique 变体）+ writer 私有 value→key 映射 + 写方法入口检查 + 声明时存量校验。**难度裁定：中偏高；风险：中**——触碰仓库里不变量最密集的 publish/rebase/repair 区域（C1 重编号、镜像修复、值释放生命周期），**必须 spec 级设计，不能当补丁打**。建议排在写子句之后（写语义先稳定，约束检查再叠上去，且 Cypher 写路径与 Go API 写路径要同时受约束）。

---

## 6. 依赖关系与建议顺序

```
鉴权 ──────────── 独立，随时（天级）
FTS bigram ────── 独立，随时（低风险）
                    │
子句列表地基 ────┬─→ WITH/UNWIND/OPTIONAL MATCH（~500-750 LOC）
(AST 重构+scope) └─→ Cypher 写子句（最小集→MERGE）
                                   │
                                   └→ 唯一约束（写语义稳定后，spec 级）
```

- **①② 可并行立项**，互不接触，都不动引擎核心不变量。
- **③ 是一次性重构**：AST 子句列表 + parser 循环化 + scope-chain 校验 + `row.vals` 标量绑定。WITH 与写子句都踩它，做一次、两处受益。先 WITH 后写、或先最小写后 WITH 皆可；考虑到迁移场景的杀手用法是 `UNWIND ... CREATE`（Neo4j 批量导入第一姿势），**建议 WITH/UNWIND 先行**，写子句落地即有完整管道可用。
- **⑥ 唯一约束最后**：它改变写路径语义（错误返回、部分应用契约），且必须同时约束 Go API 与 Cypher 写两条路——在写子句定型前做会返工。
- 版本建议：①② 可合并一个 minor（如 v0.12.0），③④⑤ 构成"查询语言现代化"线（v0.13+），⑥ 单独 minor。E 项小缺口（SKIP/算术/参数/范围下推）随 ④ 顺带评估。
- 与既有红线的交互：FTS 的 Seal 时长增长需进 bench 门；WITH 的 row 改造不能回归零分配基线（exec_alloc_test）；唯一约束触碰 C1 rebase 相关不变量。

## 7. 下一步

按 SDD 流程推进：本调研为 spec 输入。建议从 ①（鉴权）或 ②（FTS）启动第一个 implementation spec——两者范围清晰、外部参考已收敛、无引擎不变量风险。③ 开始前需单独一轮设计（子句列表 AST 的具体形状、scope 规则、row.vals 与 COW 的共存方案）。
