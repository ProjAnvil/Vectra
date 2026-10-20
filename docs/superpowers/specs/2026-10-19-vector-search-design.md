# 向量检索设计：内嵌纯 Go HNSW 异步二级索引

- 日期：2026-10-19
- 状态：Draft v3（同日 subagent 审计，六条必修已落，见第 15 节审计响应）
- 关联文档：
  - `2026-10-14-typed-index-cypher.md`（类型化索引与 Cypher 接入先例）
  - `2026-10-15-v07-delta-wal-aggregation.md`（微层 / WAL / fold 模型）
  - `2026-10-17-background-folder-design.md`（后台 folder 单飞契约）
  - 2026-10-19 产品分析（@ProductManager）：Agent 记忆为主线，向量为必需品
  - 2026-10-19 spec 审计（code-reviewer subagent）：结论 APPROVE_WITH_CHANGES

## 0. 背景与产品定位

产品结论（来自产品分析报告）：

- Vectra 定位为"图领域的 SQLite"。首要客户是 AI Agent 开发者。
- Agent 记忆的主查询是"语义相似的过往 + 图扩展上下文"。
- 当前索引只做精确匹配。没有语义召回，MCP 边车只能按 key 查实体。
- 因此向量检索是 Agent 路线的必需品，不是加分项。

产品化设三道门：

- **门 1（采用信号）**：MCP 边车有真实使用（下载量、issue、外部案例）。
- **门 2a（算法门，Phase 0）**：spike 算法基准过关。
- **门 2b（集成门，Phase 1）**：集成基准不劣化。

本 spec 覆盖全部技术设计与分阶段门控。实现顺序由第 11 节定义。

## 1. 目标 / 非目标

### 目标

1. 节点属性可存向量（`[]float32`），可建向量索引，可做 top-k 近邻查询。
2. 纯 Go 自研 HNSW，保持零第三方依赖。
3. 复用既有架构：WAL 持久化、微层、后台 folder、零锁读。
4. 写路径不被索引构建阻塞（writer 只追加，不建图）。
5. 已提交的向量立即可查（KNN 读己之写）。
6. Go API 与 MCP 工具面先行，Cypher 语法后行。

### 非目标

1. 不做服务端向量库功能面（分片、复制、租户隔离）。
2. 不做磁盘向量索引（IVF、DiskANN 类）。
3. 不做嵌入生成。调用方提供向量。
4. Phase 1 不做 Cypher 向量语法。pre-filter 不做（第 13 节裁决）。
5. 不支持一个索引多种度量。一个索引建定时锁定度量。

## 2. 关键决策

| 编号 | 决策 | 理由 |
|---|---|---|
| D1 | 索引形态 = 分层 segment 森林 + overlay；顶层 segment 即 base | 与微层/fold 同构。增量合并无全量重建风暴。读侧全部不可变，零锁成立 |
| D2 | 纯 Go 自研，`internal/vector` 包 | 零依赖是既有哲学。核心约 1500 行 |
| D3 | 索引是派生数据，v1 启动重建，v2 索引文件 | 重建复用 WAL 真理源，v1 少一条持久化路径 |
| D4 | WAL/store 的 Changeset 与 IndexDecl 均为 JSON 编码；索引摄入统一走类型归一层 | 纠正 v2 的 gob 误记。JSON round-trip 会把 `[]float32` 变 `[]any`，必须归一 |
| D5 | writer 只追加 overlay 与 WAL，folder 负责建图 | 写路径 p99 不受索引影响 |
| D6 | 向量死亡集为独立结构（VID 空间的 chunked bitmap） | edge KillSet 是边 ID 专用；实现可复用，实例不复用 |
| D7 | KNN 读己之写由 overlay 线性扫保证 | 最新提交永远在 overlay 里 |
| D8 | 结果为近似，顺序不保证 | ANN 本性。Neo4j 同款语义，文档明示 |
| D9 | 向量索引主键 = key → VID 独立稳定空间 | NodeID 在 Seal 时 dense 重编号、删除后复活复用。key 稳定，VID 由索引单调分配 |
| D10 | 向量折叠走独立单飞车道，不与图层 fold 共用 flight | 全量/大合并挂共道会触发 write stall，违背 D5 |

## 3. 数据模型

### 3.1 向量索引声明

```go
type VectorIndexDecl struct {
    Name           string  // 索引名，库内唯一
    Prop           string  // 节点属性名，该属性存 []float32
    Dim            int     // 维度，建定时锁定；上限 4096
    Metric         string  // "cosine" | "l2" | "dot"
    M              int     // HNSW 每层最大邻居数，默认 16
    EfConstruction int     // 建图搜索宽度，默认 200（efc=100 试验过：构建 -23% 但 100k recall@ef32 降至 0.9940，弃用）
}
```

### 3.2 属性、归一层与查询类型

- 节点属性值新增支持 `[]float32`。
- **类型归一层（D4，强制）**：JSON round-trip 后该属性解码为 `[]any`（元素 float64）。索引摄入统一调用：

```go
// 三条摄入路径共用：实时写、WAL 重放、store 加载。
// 接受 []float32 或 []any(float64)；产出防御性拷贝的新 []float32。
// 维度不符、元素非数值、含 NaN/Inf → 返回 false，调用方跳过并记 warning。
func NormalizeVec(v any, dim int) ([]float32, bool)
```

- 同一属性在不同节点上可以是普通值。索引只收留归一成功的向量。
- 查询结果：

```go
type VectorHit struct {
    Key   string
    Score float64 // cosine/l2/dot 原始度量值
}
```

### 3.3 WAL 与 store 扩展

- WAL 的 Changeset 与 IndexDecl 帧均为 JSON payload（`wal.go` Append/AppendIndex 用 `json.Marshal`）。`[]float32` 经 JSON 落盘后靠第 3.2 节归一层恢复。768 维向量 WAL 体积约 7.2KB（JSON 文本），预算见第 12 节。
- `wal.IndexDecl` 增加 `VectorIndexes []VectorIndexDecl` 字段。JSON 解码默认忽略未知字段，前向兼容成立。帧类型不变。
- `wal.Frame` 结构与全部重放消费点（`vectra.go` 的 `fr.IndexKeys` 循环、store 加载路径）同步透出 `VectorIndexes`。
- store 格式 v3 → v4：`store.Data` 增加声明与向量（v1 走 props JSON 同款通道 + 归一层；Phase 2 可选独立二进制 section）。v3 文件可读，读后无向量索引。**v4 文件被 v3 二进制打开是硬失败**（trailing-garbage 校验），风险表已列。

## 4. HNSW 引擎（internal/vector）

### 4.1 主键与结构

- **VID（D9）**：每个索引维护 `key → vid` 映射，vid 为包内单调分配的 uint32。key 删除后复活时复用原 vid。Seal 重编号、NodeID 复活复用均不影响 vid 空间。
- 多层邻接图。每节点层级由随机数决定，种子可注入，测试可复现。
- 距离度量：cosine（写入时归一化，比较用内积）、l2（平方欧氏）、dot（原始内积）。

### 4.2 索引形态（D1，核心设计）

一个向量索引在任意时刻由三部分组成，全部不可变（发布后只读）：

| 部分 | 内容 | 构建 | 生命周期 |
|---|---|---|---|
| overlay | 未进任何 segment 的已提交向量（目标 ≤ 2048 条） | writer 追加（只存数组，不建图） | 打包时清空 |
| segment 森林 | 分层小 HNSW。层 i 容量 = 2048·4^i，每层 ≤ 3 个 | folder 把 overlay 打包成 L0；层内凑满 4 个合并为 1 个并晋升 | 构建完只读 |
| base | 顶层 segment 的别名 | 层层晋升的自然结果 | 只读；无独立全量重建路径 |

规则：

- **归并优先级**：overlay > segment；同 vid 出现在多个 segment 时，创建序（seq）新的获胜，旧的丢弃。
- **死亡集（D6）**：vid 空间的 chunked bitmap。key 删除 → vid 入死亡集，归并时过滤。key 复活 → 清死亡集条目，新向量进 overlay，seq 归并压制旧层残留。
- **层内合并**：同层第 4 个 segment 到位即合并该层全部 4 个（取各段 live 向量重建一个 HNSW，成本 O(层容量)），产物晋升上层。1M 向量约 5 层，单向量生命周期摊销约 3.4 次重建（写放大 4.42x，级联模型实测口径 2026-10-19；pair-wise 为 8.19x）。
- **死亡压实**：单 segment 内死亡 vid 占比 > 30% 时，单独重建该 segment（O(该段容量)）。全量重建仅手动维护入口（未来 API），常规路径不存在。
- 查询 = 全部 live segment 搜索 + overlay 线性扫，按 seq 归并 top-k。live segment 数量上界约 15（5 层 × 3）。

这个形态与"sealed base + micro layers + size-tiered fold"逐点同构。读侧永远持有不可变状态，零锁读成立。触发与成本的数量级匹配由 Phase 0 基准复核。

### 4.3 插入与搜索

- 插入（仅 folder 打包/合并/压实时）：贪心下降定位，宽度 EfConstruction 搜索候选，双向建链，修剪到 M。
- 搜索：参数 efSearch。默认自适应：`max(64, c·log2(索引向量数))`，常数 c 由 spike 的 recall–ef 曲线拟合后锁定。查询时可显式覆盖。硬下界 `ef ≥ k`。
- 死亡 vid 在搜索展开与结果归并两处过滤，并计数。

### 4.4 删除与复活

- 删除节点 / 覆盖向量属性 / 删除属性 → 该 key 的 vid 入死亡集。
- 复活（同 key 再写向量）→ 清死亡集条目，新向量进 overlay。
- 旧层残留向量靠 seq 归并压制，不需要改图结构。

### 4.5 确定性说明

- ANN 结果与顺序不保证逐次一致。这是算法本性。
- 单测通过固定种子与暴力对照断言召回下界，不断言固定输出。

## 5. 写路径集成

### 5.1 提交流程

1. 调用方提交 UpsertNode，props 含索引属性的向量。
2. 写前校验：`NormalizeVec` 失败（维度/类型/NaN/Inf）→ 该节点被索引跳过，记 warning。不报错，不阻塞。NaN/Inf 必须在此拦截——`json.Marshal` 遇 NaN/Inf 会令整帧编码失败。
3. Changeset 编码进 WAL 帧（JSON 含向量文本），fsync 节奏不变。
4. 微层持有向量引用。generation 发布时，overlay 追加该向量。
5. 调用方不得在提交后修改传入的向量 slice；归一层已做防御性拷贝，实时写路径同样拷贝。

### 5.2 folder 职责扩展（D10）

folder goroutine 增加独立向量车道（独立 signal + flight 对，与图层 fold 的 flightMu/flight 分离）：

1. overlay 向量数 ≥ 2048，或 fold 周期到达 → 打包成 L0 segment（离线建图，完成后发布）。
2. 层内第 2 个 segment 到位 → 合并晋升。死亡比 > 30% → 压实该段。
3. 失败处理：仅退避重试（沿用 backoff ladder）。**无 writer 同步兜底**——向量收敛是惰性的，失败只降低收敛速度，overlay + 既有 segment 保证查询正确性。
4. A6 契约扩展：向量车道占 folder 时间 ≤ 50%，图层 fold 保底。write stall 的 `signalMerge` 等待路径不接触向量 flight。

### 5.3 吞吐预算

- writer 关键路径只做：归一校验（约 2×dim 次浮点检查）+ JSON 编码 + WAL 追加 + overlay 数组追加。无图结构操作。
- 目标：向量写入 ≥ 20k 条/s（门 2b 验证；组提交节流 p99 5ms 上限不变）。

## 6. 读路径集成

### 6.1 挂载点

- `genView` 增加字段 `vec *vector.State`。generation 发布时原子挂载。
- **genView 构造点共 7 处**（merge 路径 2 处、vectra.go 内 5 处：Load 启动、fold、publish、Seal 等），每处显式继承 vec 状态，漏一处该路径丢向量。实现清单进 Phase 1 计划。
- `engine.View` 不感知向量（CSR 构建路径完全不变）。
- `Graph.QueryVector` 读取 `g.view.Load()`，走零锁路径。
- overlay 随 generation 发布的 COW 拷贝成本（2048×768×4B ≈ 6MB/代）在高频发布下不可忽略，Phase 1 用分块 overlay（追加块 + 发布时换尾块）削减，见第 12 节风险。

### 6.2 查询流程

```
QueryVector(name, vec, k, opts):
  1. 取当前 genView 的 vec.State。不存在该索引 → 报错。
  2. 校验查询向量维度 = 声明维度。
  3. 各 live segment 搜索（efSearch）→ 候选集。
  4. overlay 线性扫 → 候选集。
  5. 归并：同 vid 取最新 seq；死亡集过滤；label/prop 过滤（后过滤 + 3 倍过采样，终态方案）。
  6. 截断 top-k，返回 VectorHit 列表。
```

注：归并去重发生在各层搜索之后，旧向量占据候选名额造成的召回损失真实存在。缓解 = 各层搜索候选数取 `max(k, ef/4)` 参与归并 + 死亡压实控制残留量；残余损失计入 spike 的 recall 验收口径（分层形态下测量）。

### 6.3 延迟预算

- live segment ≤ 约 10 个，每搜索 O(ef·log n) 次距离计算。
- overlay 线性扫：2048 × 768 维 ≈ 1.6M 次乘加，Go 标量循环约 2–4ms。
- GC 注记：1M×768 ≈ 3GB 常驻，Go 标记辅助会进 p99。spike 基准实测并记录；缓解手段（向量 arena 化、GOGC 调优）Phase 1 评估。
- 预算合计：1M×768 场景 p99 < 10ms（门 2a 验收）。

## 7. 持久化与恢复

### 7.1 启动恢复

1. 读 store v4 checkpoint（含声明与已压实向量）。
2. 重放 WAL 尾部（向量在 Changeset 帧内，经归一层恢复）。
3. 此时全部 live 向量在内存，segment 森林未建。
4. 后台自底层向上重建 segment 森林。**重建完成前 QueryVector 走暴力扫：1M×768 单次约 0.5–1s，仅保证可用性**。底层先建先可用（部分层就绪即切换到"已建层索引 + 未建层暴力扫"混合模式）。
5. 全部就绪后切纯索引路径。

### 7.2 v2 索引文件（Phase 2）

- segment 森林序列化为独立文件（类似 Neo4j 的 .vex 思路），记录 generation 水位，不匹配则丢弃重建。目的是消除重启重建窗口。v1 不做。

## 8. 一致性语义

| 语义 | 保证 | 机制 |
|---|---|---|
| 持久性 | WAL fsync 后向量不丢 | 既有 Changeset 通道 |
| 可见性 | 提交即可查（近似候选含最新） | overlay 线性扫 |
| 收敛 | fold 后近似质量回升 | segment 层级演进 |
| 精度 | recall ≥ 验收线，非 100% | ANN 本性 |
| 顺序 | 不保证稳定 | 文档与 MCP 描述明示 |

## 9. 查询面

### 9.1 Go API（Phase 1）

```go
func (g *Graph) CreateVectorIndex(d VectorIndexDecl) error
func (g *Graph) DropVectorIndex(name string) error
func (g *Graph) VectorIndexes() []VectorIndexDecl

type QueryVectorOptions struct {
    K     int
    Ef    int     // 0 = 自适应默认（见 4.3）；显式值覆盖
    Label string  // 可选后过滤
}

func (g *Graph) QueryVector(index string, vec []float32, o QueryVectorOptions) ([]VectorHit, error)
```

### 9.2 Cypher（Phase 2）

- 计划语法：`CALL vectra.vector.search("idx", $v, 10) YIELD key, score`。
- 依赖既有 CALL 支撑或按需新增。Phase 2 设计另行细化。

### 9.3 MCP 工具（Phase 1）

- 新工具 `vector_search`。现有工具面 11 个，它成为第 12 个。
- 入参两种：`query_vector`（浮点数组）或 `similar_to`（节点 key，用其索引属性向量）。
- 限幅：k 默认 10，上限 100。结果带 score。
- 描述文案明示"近似结果"。

## 10. 测试与基准

### 10.1 单元测试

- 建图 / 搜索 / 死亡跳过 / 多层 seq 归并去重（同 key 取最新）/ 复活语义。
- 归一层：三种摄入类型、NaN/Inf、维度不符。
- 声明持久化往返（JSON）。
- 崩溃恢复：WAL 截断点扫描、checkpoint 后重放。
- 并发：`race_enabled` 构建下读写并发压测。

### 10.2 召回基准（brute-force 真值对照，Phase 0）

- 分布：均匀随机 + 聚类混合（聚类更接近 embedding 真貌）。
- 矩阵：N = 10k / 100k / 1M；dim = 384 / 768。
- 指标：recall@10、p50/p99、建图吞吐、内存放大（常驻 / 理论向量字节数）、recall–ef 曲线。
- **分层形态下测量**：测量值天然包含 seq 归并去重与死亡过滤的召回损失。

### 10.3 验收线（分门）

**门 2a（Phase 0，算法门）**：

| 指标 | 线 |
|---|---|
| recall@10（1M×768，聚类，自适应 ef） | ≥ 0.95 |
| 查询 p99（同场景，分层形态，含 overlay 满载 2048） | < 10ms |
| 内存放大 | ≤ 1.6× 理论向量字节 |

**门 2b（Phase 1，集成门）**：

| 指标 | 线 |
|---|---|
| 向量写入吞吐（writer 路径） | ≥ 20k 条/s |
| 写路径 p99 回归 | 不劣化（既有 5ms 节流上限不变） |
| 集成后 recall/p99 | 不劣化门 2a 结果超过 20% |

**常数 c 两步流程**：spike 扫 ef 得 recall–ef 曲线 → 拟合/选定 c → 用自适应 ef 复跑门 2a 验收。消除"验收用自适应 ef"与"c 由 spike 拟合"的循环表述。

### 10.4 基准落盘

- 报告写入 `docs/benchmarks/2026-10-19-vector-spike.md`（Phase 0 产出）与后续阶段报告。

## 11. 分阶段交付与门控

### Phase 0：spike（算法可保留标准）

- `internal/vector` 纯算法：距离/归一层、HNSW、分层 State、自适应 ef、基准 harness。
- 不接 WAL / View / writer。包内不 import 仓库其他包。
- 产出 = 门 2a 数据与 recall–ef 曲线。

### 门控判定

- 门 1：MCP 采用信号达到（阈值由产品侧定）。
- 门 2a：spike 算法验收线全达标。
- 门 2b：Phase 1 集成验收线达标。
- 门 1 + 门 2a 齐过 → Phase 1 启动。门 2b 不过 → 回 Phase 0 修。

### Phase 1：MVP 集成

- WAL Frame 扩展 / store v4 / genView 7 处挂载 / folder 向量车道 / Go API / MCP 工具。
- 基准复跑门 2b。

### Phase 2：完善

- Cypher CALL 语法、索引文件持久化、SIMD（可选）、向量二进制存储 section（可选）。

## 12. 风险与缓解

| 风险 | 缓解 |
|---|---|
| WAL 帧膨胀：768 维 JSON ≈ 7.2KB/向量（二进制 3KB 的 2.4 倍） | v1 接受（门 2b 实测吞吐）；Phase 2 二进制 section。CheckpointBytes 4MiB 在向量负载下将频繁触发 seal，Phase 1 按实测调高或按帧计数门控 |
| NaN/Inf 使整帧 `json.Marshal` 失败 | 写前 `NormalizeVec` 拦截（5.1），测试覆盖 |
| 旧版本兼容 | 不新增帧类型；IndexDecl JSON 加字段前向兼容；store v4 被 v3 二进制打开为硬失败（trailing-garbage 校验），发布说明明示 |
| overlay 代际 COW：2048×768 ≈ 6MB/代 | 分块 overlay：追加块 + 发布换尾块，Phase 1 落地 |
| GC 标记辅助进 p99（3GB 常驻） | spike 实测记录；Phase 1 评估向量 arena 化与 GOGC |
| 层内合并占 folder 时间 | 独立车道 + A6 扩展（≤50% 时间），write stall 路径不接触向量 flight |
| recall 不达标 | 参数扫描入 spike 范围；ef 可调面留给调用方 |
| spike 与正式实现重复劳动 | spike 按可保留标准写（算法核心同构），定型时复核而非重写 |
| server/ raft 通道膨胀 | Batch gob 对 []float32 保真，仅体积膨胀同 WAL；Phase 1 影响面评估 |

## 13. 已裁决事项（原开放问题，2026-10-19）

1. **pre-filter 不做**。后过滤 + 3 倍过采样为终态方案。低选择性 label 的查询由调用方放大 K 解决。依据：Agent 记忆主查询在单一池内做语义搜索，label 过滤是低频路径。
2. **不匹配宽容跳过 + warning**。归一层失败的节点被索引跳过并记 warning。不报错，不阻塞导入。多 chunk 需求引导拆子节点 + 边，进文档示例。
3. **efSearch 对数自适应**。`ef = max(64, c·log2(索引向量数))`。常数 c 由 spike 的 recall–ef 曲线按门 2a 验收点选定后锁定。硬下界 `ef ≥ k`。依据：先测量后设计。

## 14. 影响面

| 文件 / 包 | 变更 |
|---|---|
| `internal/vector/`（新，Phase 0） | 距离/归一层、HNSW、分层 State、自适应 ef、基准 |
| `internal/wal/wal.go` | IndexDecl 加字段；Frame 结构与重放透出 VectorIndexes |
| `vectra.go` | genView.vecState（7 处构造点继承）、公开 API、WAL 重放消费点 |
| `internal/store/store.go` | v4 格式（声明 + 向量 + 版本分发） |
| `internal/writer/` | folder 向量车道（独立 signal/flight、退避、A6 扩展） |
| `mcp/` | vector_search 工具（三语言，第 12 个工具） |
| `server/` | raft Batch 通道体积影响评估（Phase 1） |
| `internal/cypher/` | Phase 2 |
| `docs/` | README、benchmarks、多向量建模示例（拆子节点 + 边） |

## 15. 审计响应（2026-10-19，v2 → v3）

审计结论 APPROVE_WITH_CHANGES，六条必修的落地：

| 必修 | 落地 |
|---|---|
| 1. 编码通道更正 + 类型归一层 + NaN/Inf + 帧膨胀数字 | D4、3.2、3.3、5.1、12 节 |
| 2. 主键改 key→VID 稳定空间，Phase 0 前裁决 | D9、4.1（已裁决为 VID） |
| 3. 死亡集独立于 edge KillSet，复活语义定义 | D6、4.2、4.4 |
| 4. 重建触发重设计（分层合并），量化成本 | D1、4.2（×4 层容量、每层 ≤2、层内合并、摊销 5 次/向量；风暴路径删除） |
| 5. 单飞协议交互定义（re-check/stall/兜底/A6） | D10、5.2 |
| 6. 验收线拆门 | 0 节（门 2a/2b）、10.3、11 节 |

同步修正的次要项：MCP 工具计数（12）、归并召回损失写入 6.2、暴力扫耗时如实（7.1）、genView 构造点枚举（6.1）、overlay COW 风险（12）、store v4 降级硬失败（12）、server/ 影响面（14）、copyProps 浅共享约束（5.1 步 5）。

## 实现期偏差记录

（实现时逐条记录与本 spec 的偏差，模式沿用 typed-index spec。）

- Phase 0 (2026-10-19): dead set is a plain map[uint32]struct{} rather than a chunked bitmap (D6 allows implementation reuse; bitmap swap lands with Phase 1 integration).
- Phase 0 (2026-10-19): dead-vid hit counting (spec §4.3) deferred — no Phase 0 consumer.
- Phase 0 (2026-10-19): benchmark matrix is Cosine-only; L2/Dot covered by unit tests.
- Phase 0 (2026-10-19): memory口径 pinned to live HeapAlloc after explicit GC (spec's "常驻"); HeapInuse reported alongside for span-level visibility.
- Phase 0 (2026-10-19): HNSW is single-threaded for build AND search in Phase 0 — the epoch-visited array is mutable read-path state (data race under concurrent Search, -race verified). Phase 1 must localize visited state per reader before immutable snapshots publish concurrently (spec D1 zero-lock-read prerequisite). **Resolved 2026-10-20 (Phase 1 Task 1):** search state moved into a per-goroutine `Reader` (`State.NewReader`/`SearchReader`); 8-worker concurrent searches are `-race` clean. Read-during-write still awaits the immutable snapshot model (Task 2).
- Phase 0 (2026-10-19): query protocol is perturbed-indexed-vector (in-distribution, standard ANN). The original genClustered(n=50) queries were OOD (single far center; measured 0.928 vs 1.0000 recall at 10k@ef128). OOD curves are retained in the report as a stress upper bound.
- Phase 0 (2026-10-19): DefaultC locked at 2 from the in-distribution 100k sweep; the 64 floor dominates in practice.
- Phase 0 (2026-10-19): 500k/1M acceptance runs cancelled by the owner before completion (pivot to build-throughput optimization). 门 2a verdict is conditional on 100k-measured lines + extrapolation; re-measure after the optimization pass.
- Phase 0 (2026-10-19): build throughput decays superlinearly (~80 vec/s at 10k×768 → ~17 at 100k×768), 10-20x off hnswlib single-thread. Not a 门 2a line but an operational red line: sustained writes above the fold rate back up the overlay, and 1M restart rebuild is impractical. Dedicated optimization session commissioned (see ledger).
- Amendment (2026-10-19, owner-approved): in-tier merge fan-in raised from 2 to 4 (tier holds ≤3, 4th arrival merges all). Aligns §4.2's capacity column (2048·4^i) with the merge rule; pairwise merging produced half-capacity segments and double the tier count. Write amplification: 5.59x → 2.97x at 100k, 8.19x → 4.42x at 1M. Live-segment upper bound moves 10 → 15; post-build p99 re-measured (see 2026-10-19-vector-build-optimization.md).
- Amendment (2026-10-19, owner-approved then revised same day): default EfConstruction trialed at 100 (build −23%, recall-neutral at 10k) — but the 100k attribution run showed recall@ef32 dropping to 0.9940 (vs 1.0000 at efc=200; ef 64/128 unaffected). That breaks the optimization session's recall gate ("no point below the pre-optimization curve"), so the default reverts to 200. efc remains a caller knob; the driver exposes VECTRA_BUILD_EFC for sweeps.
