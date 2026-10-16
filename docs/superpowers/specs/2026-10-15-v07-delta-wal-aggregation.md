# v0.7 设计文档（rev2，审计后修订）：持续写 delta 层 + 嵌入式 WAL + Cypher 聚合 + 极简锁并发模型

日期：2026-10-15（rev1 同日，经系统架构师审计后修订，R1-R14 全部落实）
状态：待用户确认 rev2 后进入 plan
前置调研：[2026-10-15-v07-engine-architecture.md](../../research/2026-10-15-v07-engine-architecture.md) · [2026-10-14-durability-write-model.md](../design/2026-10-14-durability-write-model.md)

## 0. 决策记录

| # | 决策 | 依据 |
|---|---|---|
| D1 | 单引擎 + delta overlay，不做多引擎 | 四路调研一致（rev1 §1） |
| D2 | ①②③全进 v0.7；任务顺序 ②→③→①，每任务独立可发布；退路判据 = §7 验收线（尤其导入回归线 R5/R12） | 用户决策 + 审计附条件 |
| D3 | v0.7 delta 只上嵌入式，server 保持 clone 模型 | merge 会重编号（R1）→ 副本间 ID/顺序漂移；server 转正唯一前置 = delta 节点 ID 由 raft log index 派生 |
| D4 | core 保持零锁、零 goroutine、零 channel；"event loop" = opt-in 包装层的单写者 goroutine + 有界 channel | Pebble/Badger/etcd 公共解。**附条件（审计）**：①直接模式必须补成组提交边界（R5：显式 `Batch()` API + WAL opt-out）；②误用检测（R7：包装层不外露裸 `*Graph`，发 `WriteHandle`；debug 构建断言 writer 令牌） |
| D5 | core 公共写 API 同步语义；core **只被** writer goroutine（包装模式）或单一外部 goroutine（直接模式）调用——队列/channel 只存在于包装层 | 厘清 rev1 §2.3 与 D4 的表述冲突 |

## 1. 目标与非目标

同 rev1（①写后立读、②聚合、③WAL、锁预算收紧）。非目标补充：delta 期间 `Cycles()` 强一致以外的遍历强一致语义争议已消除（见 §3.3 分族语义）。

## 2. 并发模型

### 2.1-2.2 结论与读路径（同 rev1）

Go 不需要 evloop 框架；读 = atomic.Pointer 世代发布（RCU-in-Go），零锁保持。加固：**并存 delta 层数上限 = frozen + active ≤ 2**（超限触发 write stall，见 2.3）；旧 base 世代由慢读者持有的问题不靠 merge 解决，文档写明 + `GOMEMLIMIT` 兜底。

### 2.3 写路径（rev3：微层不可变设计，消除 P1-1 竞态空洞）

**核心不变量：已发布数据永不就地修改。** writer 攒批后把整批物化为一个**不可变微层（micro-layer）**，经 `atomic.Pointer` 发布新世代 `{base, layers[]}`；reader Load 后无锁读全不可变数据。后台 merger 把 `base ∪ layers` 折叠成新 base，发布 `{newBase, layers=nil}`。

```
直接模式：调用方 goroutine 即写者；每写方法级 op 记录器攒帧，g.Batch(fn) 边界物化一个微层+一帧 WAL
包装模式：WriteHandle.Submit(op) → writeCh（有界 cap 4096，满默认阻塞）
  writer goroutine（唯一写者）：
    攒批（Badger doWrites 式）→ 分配 LSN → WAL 追加（SyncMode 决定 fsync；组提交）
    → 物化本批为不可变 micro-layer → append 到 layers（copy-on-write 切片头）→ 世代发布（atomic.Pointer）
    → 逐 op 回 replyCh
    len(layers) ≥ 层上限（默认 64）且 merger 在飞 → write stall（channel 等待，无新锁）
  merger goroutine：
    Seal(base ∪ layers) → checkpoint（可选，仅导出 merge 完成后的不可变 base）→ 发布 {newBase, nil}
    失败：当前世代保持可读、退避重试、连续失败回退同步 merge
```

- 读可见性 = `base ∪ layers[]`（多微层联读；点查族逐层查+墓碑遮蔽，遍历族经 View 合并行流）。merger 在飞期间同样成立（layers 已不可变）。
- 代价模型：每 Submit 批一次 O(batch) 微层物化 + 一次指针发布；层数由 merger 阈值回收（边数累计或层数），层上限防读者遍历过长。
- 队列选型/背压/异步变体同 rev1；**误用检测（R7）**：包装激活后非 writer 裸写返回 `ErrNotWriter`（debug 构建经 runtime.Stack 解析 goroutine 令牌，仅 debug 开销；release 构建文档警告）；`WriteHandle` 为包装模式唯一写入口。

### 2.4 锁预算（同 rev1 清单；无新增项——stall 用 channel 等待实现，不加锁）

## 3. delta 层设计（rev2 修订）

### 3.1 结构（rev3：微层列表取代 active/frozen 双层）

- 世代 = `genView{base *engine.Snapshot; layers []*delta.Layer; lsn uint64}`；`delta.Layer` 不可变（物化后冻结）。边/节点/墓碑结构同 rev2（edges 追加区 + key 寻址墓碑 + override 节点 + pending 标签/索引）；**edgeIdx 仅索引层内记录（R2）**，base 实体写经 CSR 行扫描寻址（O(deg)）。
- **EdgeID 编号空间（审计 P2-6）**：世代内 `EdgeID = base 边数棘轮 + 层偏移 + 层内下标`，genView 维护棘轮，跨 merge 可重排（同 ID 口径 R1）；delta 新节点 ID = base.NodeCount + 层偏移 + 层内序号（保稠密）。
- merge 阈值：层数（默认 64）或累计边数（默认 64K）；**层上限触顶且 merger 在飞 → write stall**。
- 标签 pending 段 merge 时 LabelID 重映射（净新增）。`Compact()` 公共 API（spec §6）= 显式 merge + deep compaction 重排。

### 3.2 读可见性（rev2：V1 升格为必做，分族语义定死）

| 查询族 | 写后可见性 |
|---|---|
| 点查族（NodeByKey/NodeByID/EdgeByID/NodeCount/EdgeCount/EdgesOf/LookupBy*） | **立即**（三源联读；delta 命中走 delta、miss 走 base、tombstone 遮蔽；`EdgesOf` 未密封分支为模板） |
| 遍历族（BFS/ShortestPath/Cypher） | **立即**，经 `View`（base∪frozen∪active 双源行迭代；engineSnapshot 接口形状；**V1 升格为 v0.7 必做**——csr.Adjacency 热循环改造 ~13 处） |
| Reachable | 立即；delta 非空时退回点级 BFS（超节点加速仅在 merge 边界生效；该回退代码路径已存在） |
| Cycles | delta 非空时触发一次性 merge 后作答（低频诊断查询） |

"写查询永远合法"就此成立且无歧义（rev1 矛盾消除，R3）。

### 3.3 merge

freeze（O(delta) 浅拷）→ 后台 `engine.Seal(unionRecords)`（纯函数零改动）→ 翻转发布。`Graph.sealed` 溶解：`Seal()` = 显式 merge+checkpoint；`ErrSealed/ErrNotSealed` 不再返回、sentinel 保留一版。

**IndexProperty 分级（R14）**：会话中段的新索引声明 pending 到下次 merge 生效（生效前 `LookupBy*` 沿用旧索引 + 点查族 delta override 线扫兜底）；MCP 自动索引在每次 merge 时收割 pending。

## 4. 嵌入式 WAL（rev2 修订）

- **帧编码定死（R9）**：整帧 JSON（`Changeset` 及其子结构带 json tag）；server raft 流维持 gob 双轨。**gob 兼容（审计 P1-3）**：core 定义 `Changeset{Version, Nodes, Edges, EdgeDeletes, NodeDeletes}`（字段名与现 server.Batch 完全一致），server `Batch` 改为 `struct { vectra.Changeset; Txn *TxnGuard }` 嵌入——gob 对嵌入结构打平编码，**线格式与旧日志双向兼容**（gob 解码忽略未知字段），fsm/api 仅需编译适配无行为变化。
- 段文件：目录布局 `<dir>/wal-<N>.log`（预分配 64MB、回卷 salt 递增）；manifest `<dir>/manifest.json`（快照文件名、段号、已含 LSN），temp+rename 原子写。
- **单一排序规则（R8）**：writer→merger→checkpoint 全部经 merger goroutine 串行化（merge 完成与 checkpoint 在同一所有权域），manifest LSN = 精确导出边界；writer 只追加 WAL 与应用 delta。checkpoint 写停顿（数百 ms~秒级@大图）发生在 merger 上，不阻塞 writer（写只受 WAL 追加约束）——与 etcd "快照并发写、只锁日志截断" 同构。
- **Checkpoint 触发**：WAL 自上次 checkpoint ≥4MB、或 Close()、或显式 Seal()。**导出边界（审计 P2-14）**：常规 checkpoint 仅导出 merge 完成后的不可变 base（merger 所有权域内，manifest LSN = 该 base 恰含的 LSN）；Close 场景先 drain 写者、执行最终 merge 后导出。恢复 = Load 快照 → 逐帧重放（幂等）→ torn tail 首错即停；**对账测试：重放首帧 lsn == manifest.lsn + 1**。
- **SyncMode{Always, Interval, Off}（R5 补直接模式）**：直接模式下 WAL 帧按显式 `g.Batch(func(g *Graph))` 边界成组（一组一帧一次 fsync；Batch 外的单写 = 一帧一 fsync，文档写明代价）；**WAL opt-out**：`Options{WAL: WALOff}`（SCV 纯导入路径零 WAL 代价，行为=v0.6 快照模式）。
- **Close 语义（R10）**：WALOn → Close 必 checkpoint（含未 merge delta；checkpoint 失败返回 error 且不删 WAL——下次启动可再恢复）；WALOff → Close 仅停后台；包装层 Close 先关 writeCh、drain、统一回 `ErrClosed`；Closed 后 Submit/写返回 `ErrClosed`，查询继续合法（读最后世代）。

## 5. Cypher 聚合（rev2 修订）

同 rev1 函数集与 Neo4j 语义。**补充（R13）**：
- `RETURN *` 与聚合互斥（解析期报错，对齐 Neo4j）。
- **ORDER BY 引用聚合别名进 v1**（`RETURN s, count(*) AS n ORDER BY n DESC`——最高频用法；经现有 OrderKey.Name 别名匹配，零新机制）。裸聚合表达式进 ORDER BY 留 v2。
- `maxGroups` 对齐 maxRows（1M 量级）。

## 6. API/兼容影响（rev2 修订）

| 原 API | 新行为 |
|---|---|
| `Seal()` | merge + checkpoint（显式） |
| `ErrSealed/ErrNotSealed` | 不再返回；sentinel 保留一版 |
| `Save/SaveFile` | **无条件导出当前逻辑视图**（三源 union；不再强制 sealed）（R4） |
| `Load/LoadFile` | 快照 + WAL 重放（有 wal 目录时）+ 首代发布 |
| `BFS/Reachable/ShortestPath/Cypher/LookupBy*/EdgesOf/NodeByKey...` | 全部不再有 sealed 前置门（R4：10+ 处门移除，行为按 §3.2 分族表） |
| 新增 | `Options{WAL, SyncMode, WALDir}`、`g.Batch(fn)`、`Compact()`、包装层 `WriteHandle{Submit, Close}`、`ErrClosed/ErrNotWriter` |
| store 格式 | 快照 v3 不变；旁路 wal 目录对旧版本不可见无害；新版本读旧目录 = 无 wal 直接快照加载 |
| MCP | `seal` 工具语义顺延；可见性分级文档同步；server 模块 Batch 上提后编译面小改、行为零变 |

## 7. 测试与验收（rev2 修订，R12）

正确性（同 rev1 delta/WAL/聚合/并发四组 + 补）：三源联读一致性、write stall 行为、merger 失败重试与回退、checkpoint 边界精确性（manifest LSN 对账）、Batch 成组帧数断言、WALOff 模式行为=v0.6、Close 失败保留 WAL 可二次恢复、`ErrNotWriter`/`ErrClosed`、`RETURN *`+聚合报错、ORDER BY 聚合别名。

性能验收线（全部给出测量点）：
| # | 线 | 测量点 |
|---|---|---|
| A1 | merge 后遍历 p50 劣化 ≤5% | 同 Seal 管线基准（校验性） |
| A2 | **merge 进行中** 查询 p99 ≤ 平时 2x | 100K/1M 两档，后台 merge 压测 |
| A3 | 写后点查 p99 < 1ms（100K）/ < 4ms（1M） | 三源联读路径 |
| A4 | **WAL Off 档导入吞吐 vs v0.6 回归 ≤5%**（SCV 生死线） | inject 基准 |
| A5 | WAL Always + `Batch()` 成组：≥ 50K ops/s（NVMe 组提交） | 嵌入式 bench（新 harness，对照 `feat/txn-group-commit` 的 Apply 吞吐口径） |
| A6 | merge 时长 + 摊销比（每千写触发一次 merge 的开销占比 <10%） | 阈值可调基准 |
| A7 | 崩溃恢复 time-to-ready：100K+WAL(4MB) < 3s | kill -9 后重启 |
| A8 | 读路径零新增同步原语 | 静态断言限定范围：engine/delta/wal 新代码无 `sync.Mutex/RWMutex` import（既有 traverse 的 sync.Pool/atomic 属并行 BFS 合法使用，豁免）+ 基准 |

## 8. 任务顺序

②聚合（独立、先行）→ ③WAL（Batch 上提 core + JSON 帧格式 + Options/Batch API + checkpoint/恢复）→ ①delta（层结构 + 三源点查 + View 遍历 + merge/stall + 包装层 WriteHandle/writer/merger）→ MCP/API/README（可见性分级、Close、WAL 选项）→ 全量回归 + 基准报告 `docs/benchmarks/2026-10-15-delta-wal.md`。每任务独立绿灯；D2 退路判据 = A4/A2/A5。

## 9. 风险登记（rev2）

| 风险 | 等级 | 缓解 |
|---|---|---|
| csr.Adjacency 三源 View 热循环改造（13 处，V1 必做） | 高 | 集中在 internal/traverse + internal/cypher 两包的邻接访问抽象；BFS 先行、ShortestPath/Cypher 跟进；A1/A2 线守护 |
| **merge 内存峰值（R11 数字化）**：1M/10M base ≈4.1GB，merge 峰值 ≈ 旧 base + 新 base + union 中间 + Seal scratch ≈ 9-11GB；GOGC=100 下 RSS 可近 live-set 2x | 高 | 文档标注机器规格要求（≥16GB for 1M 档）；merge 前临时 `debug.SetGCPercent(50)`（选项化）；heap 峰值纳入 A6 报告；分段 merge 留 v0.8 |
| 误用（多 goroutine 裸写 = WAL 帧交错 = 恢复后数据分歧） | 高 | R7：WriteHandle 唯一入口 + debug 令牌断言 + `ErrNotWriter` |
| delta 期间 SCC 派生能力缺失（Reachable 超节点、Cycles） | 中 | §3.2 分族：点级回退 / 一次性 merge |
| gob/JSON 类型漂移 | 低 | 整帧 JSON（R9） |
| 公共 API 语义漂移 | 中 | §6 新行为表 + README 迁移说明 + sentinel 保留一版 |
| 恢复时长（逻辑日志重放 ∝ 变更数） | 中 | A7 线 + checkpoint 阈值即恢复上界（4MB WAL）；文档明示 |

## 10. 实现期勘误 I（2026-10-15，T6 收口时记录）

实现过程中与本 spec 字面表述不一致的落地点，行为契约以本节为准：

1. **§4 gob 嵌入假设错误 → 落地为扁平字段 + 类型别名（T2 裁决）。** spec 假设 "gob 对嵌入结构打平编码"；实际 encoding/gob 不打平嵌入结构（嵌入值编码为单一嵌套字段），任何 embed 形态至少单向破坏旧 raft 日志。落地：server `Batch` 保留扁平遗留字段表，条目类型别名到 core 类型（同形同名），线格式与 pre-0.7 日志双向兼容（TestBatchGobLegacyRoundtrip 背书）；与 core `vectra.Changeset` 的互转走逐字段赋值（`ChangesetOf`）。
2. **§4 checkpoint 顺序 → 水位快照机制（T2 修复轮）。** spec 的 "常规 checkpoint 仅导出 merge 完成后的不可变 base + manifest LSN = 精确导出边界" 落地为：checkpoint 从不原地写 `path`，而是写水位快照 `<db 去扩展名>-<lsn>.db` 并以 manifest 原子指名；SaveFile 与 manifest 落地之间的崩溃窗口由"上一份未动水位 + 全量 WAL 重放"关闭（已 checkpoint 的帧绝不会被应用两次——AllowParallel 追加与删除不幂等）。对账不变量保持：重放首帧 lsn == manifest.lsn + 1。
3. **§4 组提交 fsync 表述 → 混合批按 kind 拆帧（T5 Important 2）。** spec 的 "一组一帧一次 fsync" 仅在批内 op 序按 Changeset kind 有序时成立；kind 回退批（如先删后加同键）在每次 kind 回退处切帧，多帧各自保序重放。层仍按 Batch 边界一对一（多帧一层的例外已写入 Graph doc）。
4. **§4 "WAL ≥4MB 自动 checkpoint" 的最终裁量（T6/A.2）：已实现，落点在 writer 循环而非 merger。** `writer.Config.CheckpointBytes`（默认 4 MiB，负值关闭）：writer 的 afterBatch 钩子在有效帧字节达到阈值时于**写者 goroutine 上**执行完整 Seal 管线（merge + 水位快照 + manifest + WAL reset）——对免锁读者不可见，Submit 观察到一次 checkpoint 形态的写停顿。这与 spec §4 "writer→merger→checkpoint 全部经 merger goroutine 串行化" 的表述偏离（checkpoint 与 merge 同属 Seal 的所有权域，merger 侧 PublishMerge 是视图级 rebase、无 checkpoint 能力）；失败语义为粘性：错误经 Close 上报、触发器自禁用（显式 Compact/Close 仍可用），避免故障盘把每批变成重试风暴。直接模式（无包装层）不设自动触发，显式 Seal/Compact/Close 触发（文档化）。
5. **§3.1/§6 Compact 与 deep compaction 的 R1 吸收口径。** spec 的 "`Compact()` = 显式 merge + deep compaction 重排"：落地中 merge 本身即稠密重建节点/边 ID（R1），不存在额外的 "deep" 阶段——`Compact()` 与 `Seal()` 跑同一管线、语义等价（T5 裁决 7，godoc 已写明）。

## 11. 实现期勘误 II（2026-10-16，Task 7 hardening 收尾时记录）

Task 7（吸收窗口 + View 缓存 + 比例阈值）落地后与本 spec 字面表述不一致的落地点，行为契约以本节为准（接 §10）：

1. **API 形名勘误（终审 M1）**：后台 rebase 后的镜像重建入口是 `Graph.RepairAfterRebase()`（由 internal/writer 的 applyBatch 在每批之前调用），不是 "WriteHandle" 上的方法；包装层的类型名是 `writer.Handle`（`writer.Attach(g, cfg)` 构造）。`Options{WALMode, WALDir}` 是 `Open` 的参数（快照式 `Load/LoadFile` 不做 WAL 重放——只有 `Open` 恢复会话；`Load/LoadFile` 将快照合并为初始 base，可继续写但无日志）。
2. **§2.3/§4 逐 op 物化 → 吸收窗口（A4 hardening）**：spec 的 "每个未分组写物化一个微层" 落地为**直接模式吸收窗口**：未分组逐 op 写累积进一个打开的 builder 窗口，按 flush 触发物化为**一个**层——读 API 入口（写后立读保持即时：直接模式下读者即写者，经 owner-token 守卫）、Batch/Seal/Compact/Cycles/Close 边界、容量上限（边类 op 64K 边记录、其余 4096 op）。WAL 记录器**不**延迟：逐 op 帧与其 fsync 契约不变，吸收的只是层物化（N 帧 → 1 层；层 lsn = flush 时已记录的最大帧 lsn，checkpoint 水位不变量 "快照含 ≤lsn 全部帧" 保持）。包装模式不开窗口（写者走 Batch），其读者跳过 flush（mid-batch 读者看到批前世代 = 既有原子批语义）。
3. **§3.1 merge 阈值 → 尺寸比例触发（A6 hardening）**：spec 的 "MergeEdges 累计层边数触发 + MaxLayers 层帽触发" 落地为**体积比例规则**：后台 fold 触发 = 累计层边数 ≥ max(MergeEdges, base.EdgeCount)——折叠间隔与基座同量级（每次 fold O(base+layers)，定长间隔随基座增长必然重新超标）；`MergeEdges`（默认 64K）语义变为**下限**（小图仍按量子折叠）；`MaxLayers` 保留为深栈兜底的硬层帽 + write-stall 线（默认 64 → 2048——小层帽作为实际触发器会以 层数×批尺寸 的定长间隔折叠，结构性违反上述契约）。遍历读放大由**世代级 View 缓存**缓解（engineView 按世代指针键控、每世代构建一次，消灭 T4 遗留的每查询重算）。
