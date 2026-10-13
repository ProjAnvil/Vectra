# Spec: Vectra 交互式 OLTP 事务层（v0.4.0）

> 状态：rev2（已合入代码审计 + 工业先例核对结论，待 plan 审计）
> 日期：2026-10-13
> 依据：两轮 subagent 调研（学术前沿 GTX/HDCC/TuskFlow 等 + 代码集成点盘点）+ 一轮 spec 审计（PASS with conditions，处置见 §7.5）+ 工业先例核对（etcd/FDB/badger/gob/HTTP 语义）
> 先例同构：token ≈ etcd mod_revision / FDB read version / badger readTs；per-key lastMod 表 ≈ badger oracle；被拒事务消耗 raft 日志但不推进版本 = etcd STM 行为（etcd 失败 Txn 走 raft、不递增 revision）

## 1. 背景与目标

Vectra 当前写路径是**批处理**：客户端组装 `Batch`（server/batch.go:41-47）→ Raft 日志 → `fsm.Apply` 确定性重放（server/fsm.go:93-144）→ `live` 图（永不密封）→ 周期性 `Clone+Seal` 发布为不可变 CSR 快照（fsm.go:190-220）。每个 Batch 原子且全局全序，天然可串行化，但**没有跨 Batch 的交互式事务**：客户端无法"读-判断-写"而不被并发写打脸。

本 spec 定义**服务端交互式事务**：

- 客户端事务 = 在某个**已发布快照**上执行若干读，然后提交一个携带**读写集 + 快照序号 token** 的 Batch；
- leader 在 `fsm.Apply` 临界区内做 **OCC 后向验证**（冲突检测退化为 per-key 版本比较，Aria VLDB'20 同族）；
- 默认隔离级别 **快照隔离（SI）**：只验证写集，first-committer-wins（对标 PostgreSQL REPEATABLE READ / S3 If-Match；比 badger/FDB 默认 OCC 更严——盲写也参与写-写冲突检测）；可选 **可串行化（SER）**：追加验证读集（反依赖检测，等价于 GTX SIGMOD'25 的 SER 读集验证分层）；
- 冲突事务被**确定性拒绝**（HTTP 409 + 冲突键列表），客户端 full-jitter 指数退避后重试。

**不做的事（明确出范围）**：
- 不改根模块公共 API（嵌入式模式仍为单写者、由使用者外部串行化；见 §9）；
- 不改密封 CSR 快照的不可变 lock-free 读契约（engine.Snapshot 零改动）；
- 不做分布式多日志（HDCC VLDB'25 的双日志交错问题在单 Raft 日志下不存在）；
- 不做锁、不做 escrow（单写者 apply 点天然串行——TuskFlow"确定性调度替代存储层锁"同思路）；
- MCP 面不动：injectBatch 直写 `svc.live`（mcp/server.go:239-273），不构造 server.Batch，与本设计零交集。

## 2. 核心机制

### 2.1 版本表（fsm 旁路表）

`fsm` 新增字段（server/fsm.go:53-74 区域）：

```go
type edgeVerKey struct{ Src, Dst, Label string } // 与 Batch 键寻址同源（batch.go:29-36）

// verTable maps logical keys to the raft Index of the log entry that last
// modified them. Maintained only inside the Apply critical section, so it
// evolves deterministically from the same log on every replica.
nodeVer map[string]uint64
edgeVer map[edgeVerKey]uint64
```

**版本口径 = raft 日志 Index**（`l.Index`）。**有效写集（effective write set）**定义为：给定 Batch 与当前 live 图，在 Apply 临界区内确定性枚举出的"本条日志将实际改变其状态的键全集"，验证与 bump 共用同一枚举：

| Batch 操作 | 有效写集键 |
|---|---|
| `NodeUpsert{Key}` | nodeVer[Key]（属性 + 存在性） |
| `EdgeUpsert{Src,Dst,Label}` | nodeVer[Src]、nodeVer[Dst]（**邻接区 = 图上间隙锁的 OCC 等价物**：该节点邻接集变了，读过它邻接的事务必须能检测到）+ edgeVer[{Src,Dst,Label}]（边属性） |
| `EdgeDelete{Src,Dst,Label}` | 同上三个键 |
| `NodeDeletes{Key}` | nodeVer[Key] + **级联边双端**：对 live 中该节点当前关联的每条边（提交前用根模块 `EdgesOf(Key, Both)` 确定性枚举，O(度数)），bump 对端 nodeVer 与 edgeVer（**审计 P1-3 修复**：级联删除改变对端邻接，漏 bump = SER 假阴性） |

枚举规则（审计 B.2 结论，安全侧闭合）：**验证与 bump 都用"请求键 + 可计算级联键"的全集**（无条件 bump，即使操作最终 skipped——如引用不存在节点；只会多拒不会少拒，且免除"skip 判定与 bump 二选一"的实现分歧风险）。被拒事务不 bump 任何键。

### 2.2 快照 token 与读路径（审计 P1-2 修复：原子束发布）

- `fsm` 新增 `appliedIndex uint64`（Apply 内 = `l.Index`）。
- **`published` 改为单原子束**：`pubState atomic.Pointer[publishedState]`，`publishedState{graph *vectra.Graph, seq uint64}`；publishOnce 在现有 `pubMu` + `publishedVer` 单调保护内（fsm.go:204-211）一次 Store 图与 seq（seq = 采样时的 appliedIndex，与 f.version 同一临界区采样）。
- **不变式（必须 -race 测试）**：读侧 `graph()` 一次 Load 拿到 (graph, seq)，seq **永不大于**该图实际包含的最后一条日志 Index（token 偏旧无害：只会多拒；偏新有害：读旧图按新口径提交——原子束从结构上排除偏新）。
- 暴露：所有查询响应加 `X-Vectra-Snapshot: <seq>` 头；`GET /v1/stats` 增加 `appliedIndex`、`publishedSeq`（publishedAt 先例 api.go:230）。
- SI 快照读 = 读已发布图（有界陈旧 ≤ SealInterval + 发布时延，node.go:6-8 口径不变；token 精确刻画读到哪个点）。

### 2.3 Batch v3：事务载荷

`server/batch.go` 按演进规则加字段，`batchVersion 2→3`（该常量当前只写不读，无检查逻辑——保持现状，作为数据版本号）：

```go
type EdgeRef struct{ Src, Dst, Label string }

type TxnGuard struct {
    Snapshot  uint64    // token：读时所依据的 publishedSeq
    SER       bool      // false = SI（只验写集）；true = SER（加验读集）
    ReadNodes []string  // 读过属性或邻接的节点键（§2.7 覆盖表）
    ReadEdges []EdgeRef // v0.4.0 预留：当前 HTTP 面无返回边属性的生产者端点
}

// Batch 新增字段：Txn *TxnGuard // nil = 自动提交（现行为不变）
```

**gob 演进纪律（官方文档确认，写入代码注释）**：只加**导出**字段；**禁止重命名**既有字段（gob 按字段名匹配，无字段号，改名 = 静默丢数据）；**禁止改类型**（含 int↔uint 符号性，直接 decode error）；零值字段不上线。**语义约定**：`Txn == nil` 才是自动提交；非 nil 的 `TxnGuard`（含 `Snapshot:0`）一律验证——`Snapshot:0` 表示"以空图基线读取"，与同键的任何既有写冲突（严格解释，杜绝零值歧义被用于绕过验证）。

**上限（审计 P2-7）**：`len(ReadNodes) > 100_000` → 400 拒收（临界区内验证成本与持锁时长上限）；409 的 conflicts 列表截断至前 100 条 + 总数。

### 2.4 Apply 内验证

`fsm.Apply` 在 `f.mu.Lock()`（fsm.go:99）之后、replay 循环（:100）之前：

```
if b.Txn != nil:
    wNodes, wEdges = effectiveWriteSet(b, live)   // §2.1 全集，确定性枚举
    conflicts = []
    for k in wNodes:            if nodeVer[k] > b.Txn.Snapshot { conflicts += k }
    for e in wEdges:            if edgeVer[e] > b.Txn.Snapshot { conflicts += e }
    if b.Txn.SER:
        for k in b.Txn.ReadNodes ∪ wNodes: if nodeVer[k] > Snapshot { conflicts += k }
        for e in b.Txn.ReadEdges ∪ wEdges:  if edgeVer[e] > Snapshot { conflicts += e }
    if conflicts:
        return ApplyResult{Committed: false, Conflicts: 截断(conflicts)}
        // 不改 live、不 bump、metrics 不计（审计 P2-6）
// 通过 → replay + bump 有效写集（lastMod = l.Index）
```

要点：
- 验证在 bump 之前，本事务自己的写不会与自己冲突；
- **被拒事务不改任何状态但消耗一条 raft 日志**（etcd 先例：失败 Txn 走 raft apply、不递增 revision）；所有副本从同一日志推导同一版本表 ⇒ 同一结论（确定性）；
- 验证 + bump 为 O(|读写集|) map 操作，临界区内无锁等待，典型交互事务（键数 <100）微秒级；
- `ApplyResult` 扩展 `Committed bool`（零值 false——**注意**：现有成功路径必须显式置 true）与 `Conflicts []string`；node.go:294-301 的 switch 分支天然兼容。

### 2.5 快照持久化（审计 P0-1 / P1-4 / P1-5 修复）

**外层框架**：fsm 快照字节 = `u32 graphLen + graphBytes + "VCTX" + u16 fmtVer + gob{NodeVer, EdgeVer, AppliedIndex}`。新 Restore 按长度切分：图段独立走 `ImportSnapshot`（旧 CRC 语义不受影响），VCTX 段恢复版本表与 appliedIndex。

**捕获点**：`fsm.Snapshot()` 在与 `live.Clone()` 相同的 `f.mu.RLock` 临界区内**深拷贝**两 map 并采样 appliedIndex（v1.8.0 中快照元数据 index 与 `FSM.Snapshot()` 调用在 FSM 线程原子对齐；`Persist` 在快照线程稍后执行，期间 Apply 可继续推进——所以只允许 Persist 序列化已捕获副本）。

**旧格式快照（无 VCTX 框架）→ Restore 返回 error，拒绝安装**。理由（P0-1 根因）： followers 独立计算接受/拒绝，任何"表状态对同一日志前缀不一致"都会造成 FSM 状态永久分歧且无告警；旧格式快照空表 + 任意基线，无法与 VCTX 表或日志推导表对齐。升级 runbook（写入 README）：**全量升级 0.4.0 → 升级后的 leader 拍新快照（VCTX）→ 旧快照作废**；拒绝安装表现为节点无法追平、显式报错（loud failure），操作员重播种。旧二进制同样读不了新快照（框架前缀），滚动升级期间须避免旧节点落入快照追赶区——小部署建议停机升级（§6）。

### 2.6 HTTP API

新增 `POST /v1/txn`（复用 handleBatch 骨架，server/api.go:117-159）：

- 请求体 = Batch JSON（含 `txn` 字段）；上限沿 64MB + §2.3 读集上限；
- 成功：`200 {committed: true, counts…, index: <本条日志 Index>}`；
- 确定性拒绝：`409 {committed: false, conflicts: [≤100], conflictTotal: N}`（CouchDB/S3 条件写先例；**不带 Retry-After**——业界惯例 409 非该头场景，README 规定客户端 full-jitter 指数退避：`sleep = random(0, min(cap, base·2^attempt))`，base≈5ms、cap≈500ms）；
- 非 leader：现有 503 + `X-Vectra-Leader` 不动；
- `/v1/batch` 与 `txn:null` 自动提交行为与 v0.2.x 完全一致。

### 2.7 隔离级别论证与读集覆盖表

**SI**：T 在 token p 的发布图上读（含所有 lastMod ≤ p 的写）；提交时若写集有 lastMod > p ⇒ 有并发事务在 p 后提交过该键 ⇒ 拒绝。无脏读、快照不可变。注：本档验写集（first-committer-wins，盲写也拒），强于 badger/FDB 默认（盲写不冲突）——文档明示。

**SER**：加验读集 = rw-反依赖检测；单日志后向验证下提交事务依赖图无环（经典 OCC backward validation for serializability；GTX SIGMOD'25 同分层）。比 PostgreSQL SIREAD 谓词锁便宜——"谓词"降维为节点邻接键。

**逐端点读集覆盖表**（审计 B.3 补齐；响应内容决定键集）：

| 端点 | ReadNodes 必须记录 | SER 可覆盖 |
|---|---|---|
| `GET /v1/nodes/{key}` | {key} | ✅ |
| `GET /v1/neighbors` | {key} ∪ **全部邻居键**（响应含邻居完整 props） | ✅ |
| `POST /v1/query` | 起点集 ∪ 全部到达键（未展开节点记存在性，多记邻接是安全超集） | ✅ |
| `GET /v1/reachable` | {from} ∪ 全部返回键（闭包完备） | ✅ |
| `GET /v1/path` | 不可覆盖（双向 BFS 依赖整个展开前沿，幻影更短路） | ❌ SI-only |
| `GET /v1/cycles`、`GET /v1/stats` | 全图操作 | ❌ SI-only |

客户端使用 SI-only 端点的事务不得声明 SER（服务端无法验证）——文档约定，服务端不强制。

**残余限制（如实写入 README）**：读有界陈旧；read-your-writes 靠提交响应的 index 轮询 stats 等 publishedSeq ≥ index（§2.8）。

### 2.8 read-your-writes（审计降级决议）

v0.4.0 **不实现 await 长轮询端点**（pubMu/ready 非广播原语，改造成本高）：README 文档化轮询模式（提交后轮询 `/v1/stats` 的 publishedSeq）。未来可加 `sync.Cond` 监听器（§9）。

## 3. 性能预期与基准

- 验证成本 O(|读写集|) map 查询；对自动提交批吞吐预期无可测量影响（基准验证）。
- **已知回归面（如实记录）**：`fsm.Snapshot()` 的 map 深拷贝在 RLock 内、表无 GC（§9）——快照体积与 RLock 时长随"历史写过键数"增长；小图目标规模（≤百万键）下可接受，基准需含快照路径观测项。
- 基准（复用 server/internal/bench）：新增负载 `txnsi`/`txnser`——N worker"读 k 邻居 + upsert 1 节点 + 提交"，k∈{1,4,16}，测吞吐与 abort 率；对照自动提交基线。
- 验收线：自动提交回退 <2%；SI 低冲突（k=1）吞吐 ≥ 基线 80%。

## 4. 测试计划（矩阵，含审计 D 节 7 类补测）

| 类别 | 用例 |
|---|---|
| 单元 | Batch v3 gob roundtrip；TxnGuard 解码；**gob 演进纪律回归**（新编码 → 去掉新字段的旧结构体解码不报错、数据不丢）；快照框架切分（u32 长度 + VCTX、坏 CRC、坏框架、纯图段） |
| fsm 冲突矩阵 | nil Txn 直通；SI 写-写冲突（后提交者拒）；SI 不同键并行皆过；SER 读-写反依赖拒、SI 同场景过（写偏斜示例）；被拒不改 live/不 bump/metrics 不计；**有效写集含级联键**（NodeDeletes 级联后 SER 读对端被拒——审计 P1-3 场景） |
| 确定性 | 同一日志序列两个 fsm 实例 Committed/Conflicts 全同；被拒条目二次 Apply（重放）仍拒且表不变；committed 条目重放 bump 幂等（同 Index） |
| 快照一致性 | **snapshot→restore→重放 == 不间断副本的接受/拒绝序列**（P1-4 核心）；Persist 期间并发 Apply 不改已捕获表；**旧格式快照 Restore 报错**（P0-1）；Restore 后 appliedIndex 连续、重放被拒条目不翻转 |
| 读侧不变式 | 并发 publish + 查询：响应 seq ≤ 图内容实际 index（-race，P1-2） |
| api | /v1/txn 200/409/400（读集超限）/503；X-Vectra-Snapshot 头；stats 暴露两序号；conflicts 截断 |
| 集成 | 3 节点 testCluster：并发冲突事务全集群恰好一个提交、各副本结论一致；failover 后冲突判定不漂移 |
| 并发 | TestFSMConcurrentApplyAndRead 扩展事务负载；-race 全绿 |
| 基准 | §3 负载 + 自动提交回归对照 |

## 5. 覆盖率要求

README 口径（`go test -cover ./...` 按包/模块）：server 模块不低于现值 83.8%（目标 ≥85%）；根模块 92.2% 与 internal 各包**不得回退**（本设计根模块与 engine 零改动）；README 双语 Coverage 行按实测如实更新。

## 6. 兼容性与发布

- **版本基线更正（审计 P2-8）**：当前工作树版本串为 **0.2.1**（mcp/server.go、ts×3、lock×2、pyproject 共 6 处；注意 git 历史存在 0.3.0→0.2.1 的回落，发布前核对 tag 现状）。目标 v0.4.0。
- Batch gob 向前兼容（旧节点跳过新字段——官方语义）；`batchVersion` 常量 2→3 只作数据标记，不新增检查逻辑（现状本就无检查，审计 P2-9 措辞更正）。
- **快照格式跨版本约束**：旧二进制读不了 VCTX 框架快照、新二进制拒绝旧快照 → 滚动升级约束写入 README（建议小部署停机升级；升级后 leader 先拍新快照）。
- MCP 表述更正（审计 P2-9）：injectBatch 不构造 server.Batch，无 Txn 概念，不受影响。
- release notes：新增 /v1/txn、X-Vectra-Snapshot、409 语义、快照格式变更（含升级 runbook）、SI 档位强度说明。

## 7. 审计结论与处置（rev2 变更记录）

| 审计发现 | 处置 |
|---|---|
| P0-1 旧格式快照恢复分歧 | §2.5 拒绝旧快照 + 升级 runbook |
| P1-2 读侧 token 偏新竞态 | §2.2 原子束 publishedState + 不变式测试 |
| P1-3 级联删除漏 bump 对端 | §2.1 有效写集含级联键（EdgesOf 确定性枚举） |
| P1-4 VCTX 捕获点在 Persist（错） | §2.5 改为 Snapshot() RLock 内深拷贝 |
| P1-5 追加段破坏 CRC/跨版本 | §2.5 u32 长度外层框架 + §6 升级约束 |
| B.3 读集覆盖缺口 | §2.7 逐端点表；path/stats/cycles 标 SI-only |
| P2-6/7 metrics 与上限 | §2.4 metrics 只计 committed；§2.3 双上限 |
| P2-8/9 版本基线与措辞 | §6 更正 |
| await 端点 | §2.8 降级为文档化轮询 |
| gob 纪律 / 409 惯例 / SI 档位说明 | §2.3 / §2.6 / §2.7（工业核对 agent） |

## 8. 先例对照

GTX（SIGMOD'25，arXiv:2405.01418）分层验证同构；HDCC（PVLDB 18(5)）确定性+交互共存问题在单日志下消失；TuskFlow（PVLDB 18(12)）确定性调度替代锁；Aria（PVLDB'20）读写集相交检查；**etcd v3**（mod_revision/STM 失败耗日志不耗 revision）；**FoundationDB**（read version + resolver 区间判定，SIGMOD'21）；**badger**（oracle readTs + ErrConflict）；**bbolt**（单写者 + mmap 不可变快照读）；LiveGraph/GART/Mammoths（上轮调研，邻接版本化 = 图上间隙锁等价物）。

## 9. 未来工作（不在 v0.4.0）

根模块嵌入式事务（按 key 寻址版本表，盘点报告 §3.1）；版本表 GC；hub 度数 escrow；await 长轮询（sync.Cond 监听器）；MCP 事务工具；混合纪元 barrier 方案（若未来需要在线滚动升级快照兼容）。

## 10. 参考

学术：GTX/HDCC/TuskFlow/Aria/LSMGraph/RadixGraph/Oze/Ddi（第一轮 agent 报告，含 URL）；工业：etcd docs+stm.go、FDB developer guide+SIGMOD'21 论文、badger txn.go、bbolt README、encoding/gob doc.go、CouchDB/S3 条件写、AWS Exponential Backoff and Jitter（第二轮 agent 报告）；代码集成点：Explore agent 盘点报告（行号基于当前 main）；本文件 §7 审计报告（agent_8619…）。
