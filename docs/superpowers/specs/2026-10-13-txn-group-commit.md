# Spec: TxnGroup — 事务组提交（v0.5.0）

> 状态：rev2（已合入 spec 审计结论：P0-1 slot 约定、P1-2 batcher 边界、P1-3 混布破坏面、P1-4 appliedIndex/metrics 规则与 effectiveWriteSet 禁令；composite 编码数学经对抗推演确认成立）
> 日期：2026-10-13
> 依据：组提交调研（hashicorp/raft v1.8 机制、etcd/Calvin/Aria/MySQL/PG 先例、YugabyteDB 单条日志多写先例）+ 增量发布调研（GART/LSMGraph/Aspen，作未来工作）+ v0.4.0 基准（docs/benchmarks/2026-10-13-oltp-txn.md）

## 1. 问题与目标

v0.4.0 实测：交互式事务在 S 档（即时发布）16 并发仅 ~542 提交/s。瓶颈不在 OCC 验证（fsm 级 AutoCommit 34.9µs/事务，见 server/txn_bench_test.go 基准，理论 ~2.9 万/s），而在**每事务一条 raft 日志一次 fsync + 单写者串行**。hashicorp/raft v1.8 的 leader loop 有机会主义 group commit（drain 循环一次 `StoreLogs` 批量落盘），**但无时间窗口**——闭环负载下到达彼此错开，批退化为 1（Task 0 实测确认，§5）。

**目标**：leader 侧事务合并器（batcher）把并发到达的 `/v1/txn` 请求在微秒级窗口内攒成**一条 raft 日志（多个子事务）**，一次 fsync 提交一批。验收线：S 档 16 worker 提交 QPS ≥ v0.4.0 的 **5×**（≥2,700/s），p99 ≤ 窗口 + fsync + RTT 预算（~30ms）；单请求延迟无回归（p50 增量 < 1ms）。

**出范围**：增量发布（见 §9）；fsync 放松/异步落盘；多领导者；`/v1/batch` 自动提交路径行为不变。

## 2. 先例对照（调研结论）

- **YugabyteDB**：leader 把多个并发写请求合成一个 write batch、追加为一条 raft 日志——本 spec 同型。
- **etcd**：per-request Register/Trigger 结果路由 + 入 raft 前做 payload 上限与解码校验 + commit-apply 差距背压（ErrTooManyRequests）——本 spec 的路由/防炸/背压模板。
- **Aria/Calvin/BOHM**：批内按 TID（槽位）全序验证，与逐条按同一日志序提交**逐次等价**；禁止批内重排。
- **PG commit_delay/commit_siblings、MySQL binlog group commit**：自适应窗口（≥k 个等待者才开窗，批满即走）；社区窗口经验 1-3ms；CoCo 实测吞吐收益在 ~50ms 走平、p99≈epoch——窗口取曲线左端。
- **hashicorp/raft**：`CommitTimeout` 是空闲兜底心跳**不是**批量窗口；`BatchApplyCh`/`BatchingFSM`（v1.3+）可让非批量流量在 FSM 侧合批（本 spec 可选任务）。

## 3. 核心设计

### 3.1 载荷格式：Batch v4 — `Group []TxnItem`

`server/batch.go` 按 gob 只加字段规则演进（batchVersion 3→4）：

```go
// TxnItem is one client transaction inside a group entry. Field semantics
// identical to the v0.4 single-txn Batch (same op sets, same TxnGuard).
type TxnItem struct {
    Nodes       []NodeUpsert
    Edges       []EdgeUpsert
    EdgeDeletes []EdgeDelete
    NodeDeletes []string
    Txn         *TxnGuard // nil = auto-commit item (batcher never produces this; kept for format symmetry)
}

// Batch 新增字段：Group []TxnItem
// 约束（入口处校验）：Group 与 v0.4 单事务字段（Nodes/Txn 等）互斥——
// Group != nil 时其余操作字段必须为空；len(Group) ∈ [2, 256]。
```

- **slot 约定（审计 P0-1 钉死）**：slot = Group 内下标，**0 基**（0..255），与 Go range 一致；`len(Group) ≤ 256`；不存在"整批 bump"路径，slot 0 不保留（单事务条目与组条目经互斥校验不可能共享同一 raft index，slot 0 双语义无碰撞）。
- 旧节点 gob 解码 Group 条目：跳过未知字段后得到全字段空批——**这不是 no-op，见 §7 的 token-unsafe 分歧**；版本混布不支持。
- 槽位序 = 执行序 = 确定性序。

### 3.2 版本编码：composite version（正确性核心，审计重点）

组内多个子事务共享同一 `l.Index`，若都按 v0.4 的 plain index bump：

1. **组内冲突漏检**：item2 验证时 item1 的 bump 值 == l.Index，被 v0.4 的自排除规则（"忽略 == 自身 Index 的表项"）隐藏 → 同键两个子事务都提交，破坏与逐条提交的等价性；
2. **重放非幂等**：双重 Apply 时无法区分"自己的键"与"同组前序 item 的键"。

**方案**：版本表值从 plain index 改为 **composite = (raftIndex << 8) | slot**，slot ∈ [0, 255]；单事务路径（`/v1/batch` 与 v0.4 式单 txn 条目）一律 slot 0，组内 item 用其 0 基下标。约束：raftIndex < 2^56（天然成立）；`len(Group) ≤ 256`。

- **可见性判定**：`visible(snapshot) ⇔ (v >> 8) ≤ Snapshot`；等价地 `conflict ⇔ (v >> 8) > Snapshot`（v0.4 的 `v > Snapshot` 语义被精确继承：plain 值迁移后同样成立）。
- **自排除精确化**：item 验证时忽略的表项从"== 自身 Index"改为 **`(v >> 8) == l.Index && (v & 0xFF) == 自身 slot`**——组内前序 item（slot 不同）正确冲突，重放幂等保住（自己的键 slot 匹配被忽略）。
- **迁移**：VCTX 格式版本 2。0.5 Restore 读到 v1 快照（plain 值）时全部 `×256`（确定性迁移，所有副本一致）；0.5 重放 0.4 时代的日志条目时按新编码 bump（同一日志前缀在所有 0.5 副本上推导出同一表——确定性保持）。0.4 拒绝 VCTX v2 快照（fmtVer 检查已存在）。
- 单事务路径的 `appliedIndex`/publishedSeq/token **口径不变**（plain index），对外协议零变化。

### 3.3 fsm.Apply 的组处理

`Group != nil` 时按槽位顺序逐项执行（复用现有 effectiveWriteSet/validateTxn/重放循环，无新语义）：

```
for slot, item := range b.Group:            // slot 0 基；effectiveWriteSet 必须在循环内
                                             // 对"前序 item 已应用后的 live"逐 item 计算——
                                             // 禁止循环前批量预计算（审计 P1-4 红线：级联
                                             // 枚举依赖 live，预计算会破坏与逐条日志序的等价）
    wNodes, wEdges = effectiveWriteSet(item)
    if item.Txn != nil:
        conflicts = validateTxn(item.Txn, wNodes, wEdges, slot)  // §3.2 的比较与自排除
        if conflicts != nil:
            results[slot] = ApplyResult{Committed:false, Index:l.Index, Conflicts, ConflictTotal}
            conflictsPending++                                     // TxnConflicts 在锁外统一累加（v0.4 口径）
            continue                                              // 拒绝的 item 不写图不 bump；
                                                                   // 后续 item 必须能看见前序已提交 item
                                                                   // 的 bump——这正是等价性机制
    replayItem(item)                                              // 现有四个重放循环按 item 粒度
    bump(wNodes, wEdges, (l.Index<<8)|slot)
    results[slot] = ApplyResult{Committed:true, Index:l.Index, ...counts}
```

**item.Txn == nil（伪造 raft 条目可达）**：确定性处理为自动提交 item（应用 + bump，不验证）——与 v0.4 Txn=nil 语义一致，batcher 正常流量不产生它。

**appliedIndex / metrics 规则（审计 P1-4 钉死）**：
- `appliedIndex.Store(l.Index)` **恒推进**（含全拒组）——RYW 轮询语义（publishedSeq ≥ commit index）不因全拒组悬挂；与 v0.4 被拒单事务不推进的差异有意识接受（v0.4 被拒条目客户端根本拿不到 commit index，无 RYW 依赖；组内被拒 item 同样返回 Index 但全拒组若有客户端在轮询该 index——恒推进消除歧义）。注：v0.4 单事务被拒路径保持现状不改（历史条目重放语义不变）。
- `AppliedBatches` 仅当该条目 **≥1 个 item 提交** 时 +1（继承 v0.4"只计提交条目"口径）；`TxnCommits` 按提交 item 计；`TxnConflicts` 按被拒 item 计，锁外累加。
- dirty/version/wake 与 v0.4 相同口径（有任何状态变化才置 dirty）。

**等价性论证**（审计验证点）：组内按槽位序"验证→应用→bump"与逐条 raft 日志按同序提交，在版本表与 live 图上逐项产生相同转移——Calvin/Aria 的确定性批执行同款论证；重放（同条目再 Apply）因 §3.2 自排除而逐项幂等。

### 3.4 Leader 侧 batcher（server 新组件）

```
txnBatcher:
  入队: handleTxn 新增入队前 IsLeader 预检（503 + hint；审计 P2-5：该预检是新增代码，v0.4 无此路径）
        → 解码+上限校验（etcd 先例：坏 payload 不入 raft）→ item + per-slot future 入有界队列(4096)
  攒批: 自适应窗口——队列长度 < 2 时零等待立即派发（PG commit_siblings 式门槛，单请求延迟无回归）；
        ≥2 时等待 min(2ms, 批满) —— 批满 = 256 items 或整组编码 4MB
  派发: encodeBatch(Batch{Version:4, Group:items}) → n.r.Apply 一次
        派发 timeout = min(队列内各 item 剩余 ctx deadline)，下限 1s；入队时已超期的 item
        逐槽位直接返回 ErrTimeout、不入批
  回填: fut.Response() = []ApplyResult（按槽位）→ 逐 future 投递；整批 raft 错误按 v0.4 语义分派：
        ErrNotLeader → 所有槽位 503 + X-Vectra-Leader + Retry-After；ErrTimeout → 504 TIMEOUT
        （504-后-事务可能仍提交——与 v0.4 同源；OCC 使盲目重提安全：重提会因自己已写入的键 409）
  背压: 队列满 → 503 TXN_OVERLOADED + Retry-After（etcd ErrTooManyRequests 先例）
  兜底: batcher goroutine 必须 recover 并向全部在队 future 返回内部错误后重建/退出（防悬挂）
  超大单 item: 编码后 > 4MB 的 item 不入批，走 v0.4 单事务直发路径（Group 下限 2 由此闭合）
```

- **非 leader**：handleTxn 现有 503 快路径在入队前（不变）。
- **单 item 兜底**：窗口到点只有 1 个 item → 仍走 Group（len=1 放宽下限）？**否**——走 v0.4 单事务 Batch（Txn 字段）快路径，格式回归零风险；batcher 只在 ≥2 时产 Group。`len(Group) ∈ [2,256]` 约束由此闭合。
- 窗口/上限/队列容量均为 Config 常量起步（不做运行时可调，YAGNI）；记入 metrics。

### 3.5 可观测

expvar 新增：`vectra_txn_group_batches`（派发的组条目数）、`vectra_txn_group_size_total`（Σ批大小，供分布）、`vectra_txn_window_wait_ns`（累计窗口等待）、复用 raft-boltdb `logsPerBatch` 验证 raft 侧批形态。验收时用现有 txnsi/txnser 负载直接对比 v0.4.0 数字。

## 4. 诊断先行（Plan Task 0，调研强调）

实现前先证实"批=1"假设：在 bench 或 dev 模式暴露 raft-boltdb `logsPerBatch`/`numTx` 统计，S 档 16 worker 跑 txnsi 记录 raft StoreLogs 批分布。若批已 >1 而 TPS 仍 500，停止本设计、重诊瓶颈（FSM bolt fsync / 上游锁），把结论写回 spec。**这是闸门任务，不许跳过。**

### 4.1 Task 0 实测结果（2026-10-13，feat/txn-group-commit @ dd2d094）：闸门触发，实现停止

- **测量方法**：bench http 模式在 `server.New` 之前安装 go-metrics inmem sink（10s 间隔/1min 保留，全局实例替换黑洞默认 sink），工作负载循环前后双快照差分取 txnsi 压测段 `raft.boltdb.logsPerBatch` 聚合（metric 源头 raft-boltdb v2.4.2 `bolt_store.go:217`，每次 `StoreLogs` 一个样本；方法论经独立评审对照 go-metrics/raft-boltdb 源码逐层核验）。
- **数据**（S 档 / http / 16 worker / txnsi / 10s / seed=42，两次运行互证，0 err / 0 OCC 冲突）：

| run | logsPerBatch 段均值 | samples | txnsi 提交 QPS | p50 |
|---|---|---|---|---|
| 1 | **5.51** | 1,055 | **578.5**（5,785 ops） | 26.55ms |
| 2 | **5.69** | 1,047 | **593.1**（5,931 ops） | 25.99ms |

- **自洽校验**：Σlogs ≈ 5.51×1055 ≈ 5,813 ≈ 压测段提交事务数（含 ~19 笔单 worker warmup）——每事务恰 1 条 raft 日志、~1,055 次 `StoreLogs`，批均值 5.5 可信（两次运行偏差 <4%）。
- **判定**：段均值 >1.5 且 TPS 仍 ~500，两个停止条件**同时成立**——"批=1"假设被推翻。leader 的机会主义 drain 批已经把 ~5.5 条已提交日志合入每次 `StoreLogs`（~100 批/s × ~5.5 条/批 ≈ 550 事务/s）：fsync 摊销已天然发生，吞吐上限落在**每批流水线成本**（批周期 ~10ms：FSM apply 侧 bolt fsync / apply-发布串行点 / 上游单写者），而非日志批稀疏。
- **后果**：§1 的瓶颈模型（"每事务一条 raft 日志一次 fsync"为主瓶颈）不成立，按闸门规则 **v0.5.0 TxnGroup 组提交停止实现**（Task 1-7 不派发）。
- **待重诊**（瓶颈在别处）：FSM apply 侧 bolt fsync 计时与批周期分解、apply/发布路径串行点、上游锁竞争 profile。重诊结论决定下一步形态：修正后的组提交设计（若能进一步摊薄每批周期）、§8 的 raft 调参 + BatchingFSM 路线，或 FSM 持久化层改造。原始输出存 `.superpowers/sdd/2026-10-13-txn-group-commit/task-0-bench-output*.txt`。

### 4.2 Task 0b 重诊结果（2026-10-14）：瓶颈为 dev 机 fsync 延迟 + 客户端 RYW 轮询，组提交机制结构性冗余

三路独立测量（完整数据与原始输出：`.superpowers/sdd/2026-10-13-txn-group-commit/task-0b-rediagnosis.md`；S 档/http/16w/txnsi/10s/seed42 三次运行互证，偏差 <4%）：

1. **bolt StoreLogs 微基准**（M5 Pro/APFS）：每次调用 ≈ **7ms，与批大小基本无关**（1→256 条仅 +14%）——单次 bolt 提交+fsync 固定延迟主导。批周期 ~10ms = 7ms StoreLogs + ~3ms 流水线其余；97 批/s × 7ms ≈ 68% leader-loop 占空比，批率已贴 fsync 上限。
2. **客户端相位分解**（p50 ≈ 26.5ms）：read 0.65ms（2.5%）+ commit 15.6ms（59%，= 半周期排队 ~5ms + 一个完整批周期 ~10ms）+ **RYW 等待 11.1ms（42%，= 10ms 客户端轮询量化 + ~1-3ms 发布滞后）**。三相位和与闭环保守律 TPS = 16/26.5ms ≈ 604 均与实测吻合（578-594）。
3. **算术判定**：2,700/s @16w 需每 op ≤5.93ms，**低于本机 fsync 下限 7ms**——即使 read 与 RYW 为零，16/7ms = 2,286/s。任何保持每派发周期一次持久 fsync 的设计（即任何保持久性的设计）在本硬件上不可达。drain 周期（10ms）在闭环保守负载下自然收集全部到达（预测 16×10/26.5 ≈ 6.0/批，实测 5.5-5.7 ✓）；**2ms 显式窗口 < 自然周期，攒不出更大的批——TxnGroup 的核心机制在本 regime 结构性冗余**（组提交只在日志速率受限的 regime 有增益，本负载是延迟受限）。

**结论与建议**（按 §4.1 的三岔决策，三者皆非——瓶颈不在此三者）：
- **撤回 Task 1-7**（维持不派发），本 spec 保留为有据负结果（Task 0 + 0b 双证据）；
- v0.5.0 重定范围为：(a) **RYW await/长轮询端点**（§9 未来工作上提——最大单点杠杆：11.1ms → ~2ms，TPS → ~950/s，+60%，无正确性风险）；(b) **Linux 级存储重定基线**后再决定是否需要任何服务端攒批（生产 NVMe fsync 0.2-1ms → 批周期 ~1-3ms，配 await 后 ~4,000/s，5× 验收线在生产级存储上可达）；
- 重定范围决策属发布门，升级用户定夺。

## 5. 测试计划（矩阵，含审计 P2-6 补项）

| 类别 | 用例 |
|---|---|
| 单元 | TxnItem/Group gob roundtrip；0.4 结构体解码 Group 条目（跳过→空批）；Group 与单事务字段互斥校验（400）；len/字节上限 |
| composite 版本 | 组内同键两 item：slot 小者提交、大者拒（等价逐条）；跨条目（组后新条目）token 判定用 `v>>8`；重放幂等（组条目二次 Apply **逐 item 决策+终态**同果——counts 允许合法漂移，如 NodesDeleted→NodesSkipped）；**slot 边界**：满 256 组、首尾 slot 同键、首次+重放；**单事务→组混合序**（e1@i 单事务写 k → e2@i+1 组内双同键 item，拒/提交两分支 × 重放）；**混合纪元 A/B**：v1 快照 Restore(×256) vs 更早快照+完整重放，双副本对同一后续组条目逐 item 判定一致 |
| fsm 组矩阵 | 组内 SI 写写冲突；组内 SER 反依赖；拒后不 bump、后续 item 能看见前序已提交 bump；counts/metrics 聚合（含全拒组：appliedIndex 恒推进、AppliedBatches 不加、后续 token 可用）；空 Group 拒收；组内 Txn=nil item 的确定性处理；**slot-0 条目（/v1/batch）与 Group 条目交错混流**的副本一致性 |
| batcher | 单请求零窗口直发（延迟回归断言：p50 增量 <1ms）；≥2 触发窗口聚合（可注入时钟）；批满即走（256/4MB）；队列满 503；丢主→全槽位 503+hint；超时→504（且 504-后盲目重提→409，OCC 去重性质）；超大单 item（>4MB）走直发；入队时已超期 item 逐槽 ErrTimeout；panic recover 排空 future；future 槽位路由不串 |
| 集成 | 3 节点：16 并发 txnsi 经 batcher，副本间 nodeVer/live 一致；failover 后组提交继续；/v1/batch 与 /v1/txn 混流 |
| 基准/验收 | **Task 0 诊断闸门**：S 档 16w txnsi 下 raft-boltdb logsPerBatch 分布证实批=1（若批>1 停止实现、重诊瓶颈并回写 spec）；S 档 16w txnsi/txnser 对比 v0.4.0（≥5×，即 ≥2,700/s）；p50 单请求增量 <1ms；p99 ≤30ms；fsm 级 BenchmarkFSMGroupApply（批 64） |

## 6. 覆盖率与发布

- server 覆盖率 ≥ 86.9%（现值）不回退；README 双语 Transactions 节补组提交说明与基准；版本串 0.4.0→0.5.0（mcp/server.go、ts/package.json、ts/src/index.ts、package-lock×2、pyproject，发布前逐处核对）。
- release notes：性能特性 + **集群版本混布不支持（token-unsafe 分歧，见 §7）** + VCTX v2（0.4 拒绝 0.5 快照）。

## 7. 升级约束（如实文档，审计 P1-3 强化）

0.4→0.5 **必须停机全量升级**（不允许滚动），混布的破坏面比"分歧"更严重：

- **0.4 副本遇 Group 条目 = token-unsafe 分歧**：gob 跳过未知字段后 0.4 得到全空批，其 Apply **并非 no-op**——照常推进 `dirty/version/appliedIndex/AppliedBatches` 并以 `publishedSeq = l.Index` 发布（fsm.go Txn==nil 路径无早退）。后果：0.4 follower 的 token 跳过了组条目的写入——客户端从 0.4 读得 token=idx、携此 token 向 0.5 leader 提交同键事务，验证 `(idx>>8)=idx > idx` 为假 → **通过并提交**（对从未看见的状态盲写，SI 被击穿），且 0.4 FSM 永久缺这批写入。
- **快照回灌方向**：0.5 领导拍的 VCTX v2 快照，0.4 节点 Restore 拒绝（fmtVer 检查）→ 安装失败、盘上存有 v2 快照的 0.4 节点启动即拒——与 0.3→0.4 的 re-seed runbook 同类运维后果，README 必须并列写明。
- 旧（0.4 时代）日志条目由 0.5 统一按新编码 bump——同一日志前缀在所有 0.5 副本推导出同一表，确定性保持。
- 升级前 0.4 签发的 plain token 在升级后使用语义兼容（`(v>>8) > s ⇔ v > s` 对 plain 值成立）。

## 8. 明确否决的备选

- **只调 raft 旋钮**（MaxAppendEntries/BatchApplyCh/CommitTimeout）：无时间窗口的 drain 批在闭环负载下不可控；作为可选增强（BatchingFSM 实现）放 P2，不作主路径。
- **多条日志共享 fsync**：等价于内置机制，批大小不可设计。
- **组内重排优化**：破坏与逐条提交的等价性论证，禁止。

## 9. 未来工作（不入 v0.5.0）

**增量发布**（M/L 档 RYW 吞吐的下一个杠杆，发布成本 O(V+E)→O(Δ)）：推荐路线 = GART 式分段 CSR + 每顶点 epoch→offset 表（读 2.5-3.1× 优于 LiveGraph、税=每顶点一次表查找）；备选 = Aspen/immer 式结构共享持久化（唯一 100% 保住"读者无锁无版本过滤"契约，代价是邻接扫描 1.5-3× 税需原型实测）。SCC 等全图派生结构走"受影响 SCC 诱导子图局部重建 + 低频全量校准"。另：版本表 GC、await 长轮询、MCP 事务工具。

## 10. 参考

YugabyteDB raft/write batch 文档；etcd v3_server/raft 源码与性能文档；Calvin SIGMOD'12；Aria SIGMOD'20；BOHM arXiv:1412.2324；CoCo epoch 研究（VLDB）；PG WAL 配置；MySQL WL#5223/binlog group commit；hashicorp/raft v1.8 raft.go/config.go/fsm.go、GH-341/364/445/541/579；raft-boltdb v2 bolt_store.go；GART ATC'23；LSMGraph SIGMOD'25；Aspen PLDI'19；SMDG；LLAMA ICDE'15；Teseo PVLDB'21；LiveGraph PVLDB'20；HAL 定量对比（版本过滤税）。两份调研报告全文见本会话（subagent agent_2995…/agent_d70f…）。
