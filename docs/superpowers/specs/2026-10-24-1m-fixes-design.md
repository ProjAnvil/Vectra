# 1M 实测三项发现修复：设计（2026-10-24）

来源：`docs/benchmarks/2026-10-23-vector-1m.md` 的三个候选立项。业主
指令：先修复，性能复测后置；业界论证（DeepWiki：Qdrant/Weaviate）
+ 简单实验先行。

## 业界论证结论（设计依据）

- **Qdrant**：恢复一律 WAL 重放；未优化点启动时从 WAL 重建。WAL 截
  断由"各段最小已持久化版本"的水位线把门——**未持久化的工作永远
  保留日志**。优化器全程不阻塞写路径（ProxySegment COW + post-flush
  action：旧段删除推迟到新段刷盘确认之后）。
- **Weaviate**：HNSW 用 commit log 增量持久化（每次图变更先写日志）；
  压缩/维护全部异步（cyclemanager）；fsync 只在日志轮转，不逐批。
  干净关停=停后台周期+释放内存，无大刷盘。
- **共同点**：① 结构构建工作的持久化要么增量（commit log）、要么
  由水位线保证不丢（WAL 不超前截断）；② 全量压缩永不阻塞写者；
  ③ 延迟-recall 是调用方旋钮，业界无自动 ef、无绝对延迟承诺。

## 发现 → 修复映射

| 发现 | 根因 | 修复 |
|---|---|---|
| B1：轻量 Close 后重开恢复 970,751 条冻结积压，ready 2h57m | 打包工作只在压缩点持久化；收敛后的 pack 成果不落盘，Close 轻量档不刷新森林 | 项 1：Close 时按 pack 漂移刷新森林缓存 |
| 压缩档停顿 p50 11.47s / max 26.27s（12 次） | 压缩档全流水线在写者 afterBatch 内联执行 | 项 2：压缩档异步化（pin 不可变视图后台导出 + Reset 推迟为写者 post-action） |
| ef64 p99 12.16ms 超 §6.3 的 10ms 线 22% | 预算线与规模的匹配；业界无自动 ef | 项 3：ef 扫描实验（现存 1M 库）定夺——数据支持则修订 spec 预算线，否则另立优化弧 |

## 项 1：Close 时按 pack 漂移刷新森林缓存

**根因链**（实测+代码确认）：森林缓存在压缩点由 `writeVecForest` 用
`st.Encode` 全量编码（段+overlay 整体）。收敛后的 pack/merge 发生在
压缩点之后、Close 之前——这些成果只存在于内存。轻量 Close 不写森林
→ 磁盘森林停留在旧压缩点（冻结大积压）→ 重开按旧森林恢复 →
重付打包。1M 直测：970,751 条、2h57m30s。

**方案**（选定；否决项见后。builder-check 四问的裁定见文末"审查裁定"）：

1. `vecIndex` 增加 pack 漂移计数：`packCount`（在 `drain` 的
   `CommitPack` 成功时于 `vi.mu` 下递增——那是全树唯一的
   overlay→segment 工作；`publish()` 每批都调，不能用作信号）与
   `forestPacks`（上次成功写森林时的计数）。`writeVecForest` 编码时
   在锁内取 `n := packCount`；写成功后再加锁，当且仅当
   `packCount == n` 才置 `forestPacks = n`（编码期间发生的 pack 不丢
   标记——写下的森林不含它）。
2. Close 路径的轻量 checkpoint 之后，若任一索引 `packCount !=
   forestPacks` → 以当前 LSN 调 `writeVecForest`（temp+fsync+rename，
   原子性纪律沿用）。**挂点恰好两个 close 形入口**：`Graph.Close`
   的轻量分支 + `Handle.Close` 的收尾 Seal（走内部
   `sealForClose`；公共 `Seal()` 与中途卷 seal 一律不刷——否则 1M
   摄入 34 次轻 seal 各写 2.87GB，成本模型崩）。刷新的森林 LSN ≥
   manifest LSN，落在 v0.11.2 放宽后的加载门内（"forest LSN ≥ base
   LSN"）；重放的重复条目由 `State.Add` 的 vid 复用 + `Pack` 的 vid
   去重（"newest wins"）消化——该语义已被现行"陈旧森林+链重放"路
   径使用，非新面。
3. 崩溃窗口：刷新中途崩溃 → 撕裂/旧森林 → 加载门 CRC/水位拒绝 →
   走重建路径（既有纪律，spec §2.3）。

**成本**：Close 从 O(1) 变为"有漂移才付一次森林写"（1M ≈ 2.87GB ≈
6-15s；无写入就 close 的库保持 O(1)）。这是把"关停保住已做的工作"
变成确定性行为——与 Qdrant 水位线"不超前截断"同一精神：要么工作
落盘，要么丢弃工作的记录还在。

**否决项**：
- 压缩档改为"只冻结已打包段、overlay 走重放"——链只覆盖压缩点之后
  的帧，压缩点之前的未打包向量只有快照 props 可回灌=全量背填重打包，
  B1 场景救不了（该工作本就没做过，无从保留）；且改动森林编解码与
  加载门语义，面更大。
- 周期性（每 N pack）刷新森林——能把崩溃窗口也界住（对齐 Weaviate
  commit log 精神），但本项先修实测到的 Close 路径；周期刷新列为
  后续候选（见"遗留"）。

## 项 2：压缩档异步化（写者不再内联跑全量流水线）

**现状**：`volumeCheckpoint`（写者 afterBatch）→ `g.Seal()`（写者
线程）→ `checkpoint` → `compactCheckpoint`：`SaveFile`（2.88GB）→
manifest → `writeVecForest`（2.87GB）→ `Reset` 全部内联，Submit 排队
等完 → 1M 实测停顿 11.5-26.3s。

**关键可行性**（代码确认）：`unionRecords()` 走 `g.view.Load()`——
**不可变已发布视图**，任意 goroutine 对 pin 住的视图调用安全；
`flushPend` 在 writerAttached 时为 no-op；`encodeVecForest` 以
`vi.mu` 串行（可与写者并发，锁内取的是当下状态——森林是缓存，新于
pin 视图无害，水位用取到的 LSN）。

**方案**（ProxySegment 的 checkpoint 等价物。核心契约：**后台只做慢
的字节（快照+森林），一切 manifest 与 WAL 变更走写者侧快速完成步**）：

1. 写者线程只做快的部分：`flushPending`（发布）→ pin
   `(view, lsn, packCounts)` → 派后台压缩 flight（同一时刻至多一个
   在飞）。写者立即返回，Submit 不再等全量写。**在飞期间抑制一切卷
   检查点**（轻档也要抑制——后台 manifest 落地前的一次轻 manifest
   会把 Snapshot/RetainFrom 拉回旧 base+旧链首，与完成步的段删除组
   合出"manifest 指向已删段"的窗口）：`volumeCheckpoint` 见在飞即
   返回。WAL 增长上界 = 在飞时长 × 摄入率（1M 实测口径 ≈ 26s ×
   5,938/s ≈ 480MB）；安全阀：`WALSize` 超过硬上限
   （`CheckpointBytes*4 + 1GiB`）时 Submit 阻塞等完成（Qdrant 式背
   压）。
2. 后台 flight（goroutine）：在 pin 住的不可变视图上
   `view.unionRecords()` → 编码写快照（temp+fsync+rename）→
   `writeVecForest`（现 LSN，`vi.mu` 串行——森林是缓存，新于 pin 视
   图无害）。**不写 manifest、不碰 WAL。**
3. **完成步（写者线程**，flight 结束后由下一次 afterBatch 或 Close
   执行；manifest 必须写者侧写——`Segment` 字段要当前活跃段号，后台
   拿不到安全值）：
   - 静默情形（`LastLSN() == pin 的 lsn`，无新帧）：与今天的压缩档
     完全一致——manifest{新快照, L, 当前段} → `Reset(L)` → 清理旧
     快照。
   - 忙情形（`LastLSN() > L`，飞行期间有新帧；`Reset(L)` 会删掉
     L 之后的已 fsync 帧=丢数据，禁止）：写 v2 形 manifest{新快照,
     L, Segment: 当前头, RetainFrom: 首个含 >L 帧的段}，**先落
     manifest 再删段**（崩溃在中间=多段在场，lsn 门槛跳过 ≤L 帧，
     无损）；然后删除"全部帧 ≤ L"的段（新 WAL 原语
     `DropSegmentsCoveredBy(L)`：单写者维护段→maxLSN，只删整段被覆
     盖的；跨段的保留——段粒度保留在 RetainFrom 语义里本就表达不
     了段内分裂，整段粒度是现有词汇）。忙情形后保留字节=飞行窗口
     的帧量，下次触发自然续跑。
4. 崩溃矩阵：快照写一半→旧 manifest+完整链照常重放；manifest 已落
   →新 base + 链按门槛重放（段删除若未发生，多余 ≤L 帧被跳过）；
   森林任一时刻撕裂 → 加载门拒 → 重建。无新增静默丢失面。
5. `g.Seal()`/`Handle.Close()` 语义：显式 `Seal()` 保持同步（公共
   API 不变——调用方语义是"此刻落盘"）；Close 时若 flight 在飞，
   join 后在写者上跑完成步（Close 停顿=flight 剩余时间，与今天同步
   压缩同量级）。

**否决项**：
- 森林写单独异步、快照仍内联——只砍一半停顿，类不变。
- 压缩阈值随 N 缩放——降低频率但单次更贵，重放字节上升，方向错。
- "飞行后 LastLSN>L 就跳过 Reset"——持续摄入下链永不释放、磁盘无界
  增长（builder-check 指出），故有忙情形的 v2 重锚+段粒度删除。

## 项 3：ef 预算（实验定夺，见 ef-sweep 结果落档）

`bench/vector_ef_sweep_test.go`（VECTRA_EF_SWEEP=1）在现存 1M 库上扫
ef 40/48/56/64/80/96 的 recall/p50/p99。判定规则：
- 若存在 ef' 使 recall@10 ≥ 0.97 且 p99 ≤ 10ms → 修复=生产自适应 ef
  公式调整（`max(64, 2·log₂N)` 的常数/形状）+ spec §6.3 预算线加
  ef'-条件；
- 若不存在 → spec 修订案（预算线按实测重定，如"1M 级 p99 ≤ 15ms"）
  + 引擎搜索优化另立弧（SIMD 点积/布局，业界差距 2-3x 存在）。

## 非目标

- 不改 HNSW 算法本体、不改 store 格式（快照仍 v6、森林格式不变）。
- 不做周期性森林刷新（遗留候选：把崩溃重付窗口也界住）。
- 不动查询路径的热点优化（若项 3 判定为优化弧，另立项）。

## 影响面

- `internal/vector/state.go`：无（Pack 去重语义已备）。
- `vectra_vector.go`：vecIndex 增计数字段；writeVecForest 增漂移标记。
- `vectra.go`：checkpoint 分档路径拆分（close 刷新 + 异步压缩的
  prepare/persist/post 三段）；Close/Seal 语义保持。
- `internal/writer/writer.go`：afterBatch 触发改为派发后台压缩；
  Close join。
- 测试：项 1 的"漂移后轻量 Close → 重开仅尾"行为测试（20k 级、秒级
  运行）；项 2 的崩溃窗口矩阵（快照写一半/manifest 前后/森林前后/
  Reset 排队中）+ 停顿对比断言（小库上验证写者不等全量写：用大 payload
  慢化写盘不可行→用注入点把后台阶段人为延迟，断言 Submit 未阻塞）。

## 遗留（本设计不做）

- 周期性森林刷新（崩溃窗口界住：重付 ≤ N pack 的量）。
- 1M 级性能复测（业主明示后置）。
- 引擎搜索优化弧（若项 3 判定需要）。

## 审查裁定（builder-check 四问，2026-10-24）

1. **Reset 与并发追加**（最重要）：采纳"完成步二分"协议——静默情形
   Reset；忙情形 v2 形 manifest 重锚 + 段粒度删除已覆盖段（先
   manifest 后删段）。否决"跳过 Reset"（链无界）与"Reset 到当前
   LSN"（快照盖不住即丢数据）。
2. **轻 seal 与 WAL 增长**：在飞期间抑制一切卷检查点；增长上界=飞行
   时长×摄入率（实测口径 ≈480MB），另设硬上限安全阀（4×CP+1GiB，
   超限 Submit 背压等完成）。
3. **项 1 挂点**：恰好两个 close 形入口（Graph.Close 轻分支 +
   Handle.Close 收尾 Seal 的内部 sealForClose）；中途 seal 不刷。
4. **漂移信号**：`drain` 的 `CommitPack` 成功计数（publish 每批都
   调，不能用作信号——builder-check 证伪原前提）。
