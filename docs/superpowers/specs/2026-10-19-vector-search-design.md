# 向量检索设计：内嵌纯 Go HNSW 异步二级索引

- 日期：2026-10-19
- 状态：Draft v2（2026-10-19 裁决三个开放问题，见第 13 节）
- 关联文档：
  - `2026-10-14-typed-index-cypher.md`（类型化索引与 Cypher 接入先例）
  - `2026-10-15-v07-delta-wal-aggregation.md`（微层 / WAL / fold 模型）
  - `2026-10-17-background-folder-design.md`（后台 folder 单飞契约）
  - 2026-10-19 产品分析（@ProductManager，本会话）：Agent 记忆为主线，向量为必需品

## 0. 背景与产品定位

产品结论（来自产品分析报告）：

- Vectra 定位为"图领域的 SQLite"。首要客户是 AI Agent 开发者。
- Agent 记忆的主查询是"语义相似的过往 + 图扩展上下文"。
- 当前索引只做精确匹配。没有语义召回，MCP 边车只能按 key 查实体。
- 因此向量检索是 Agent 路线的必需品，不是加分项。

产品化设两道门：

- **门 1（采用信号）**：MCP 边车有真实使用（下载量、issue、外部案例）。
- **门 2（技术基准）**：spike 基准过关（recall、延迟、内存）。

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
| D1 | 索引形态 = base HNSW + segments + overlay 三层 | 与微层/fold 同构。读侧全部不可变，零锁成立 |
| D2 | 纯 Go 自研，`internal/vector` 包 | 零依赖是既有哲学。核心约 1500 行 |
| D3 | 索引是派生数据，v1 启动重建，v2 索引文件 | 重建复用 WAL 真理源，v1 少一条持久化路径 |
| D4 | 向量走 Changeset 帧内属性；声明走 IndexDecl 帧扩展 | 不新增 WAL 帧类型。旧版本兼容风险最低 |
| D5 | writer 只追加 overlay 与 WAL，folder 负责建图 | 写路径 p99 不受索引影响 |
| D6 | 删除复用 KillSet，墓碑跳过 + 阈值重建 | 与 killset 工作合流 |
| D7 | KNN 读己之写由 overlay 线性扫保证 | 最新提交永远在 overlay 里 |
| D8 | 结果为近似，顺序不保证 | ANN 本性。Neo4j 同款语义，文档明示 |

## 3. 数据模型

### 3.1 向量索引声明

```go
// 与既有 typed index 声明并列。
type VectorIndexDecl struct {
    Name           string  // 索引名，库内唯一
    Prop           string  // 节点属性名，该属性存 []float32
    Dim            int     // 维度，建定时锁定；上限 4096
    Metric         string  // "cosine" | "l2" | "dot"
    M              int     // HNSW 每层最大邻居数，默认 16
    EfConstruction int     // 建图搜索宽度，默认 200
}
```

### 3.2 属性与查询类型

- 节点属性值新增支持 `[]float32`。
- 同一属性在不同节点上可以是普通值。索引只收留维度匹配的向量。
- 查询结果：

```go
type VectorHit struct {
    ID    NodeID
    Key   string
    Score float64 // cosine/l2/dot 原始度量值，越小或越大依度量而定
}
```

### 3.3 WAL 与 store 扩展

- 向量字节随 Changeset 帧走既有 gob 序列化。`[]float32` 为原生支持类型。
- `wal.IndexDecl` 结构体增加 `VectorIndexes []VectorIndexDecl` 字段。帧类型不变（仍为 frameIndexDecl）。旧代码解码新帧时忽略未知字段（gob 语义）。
- store 格式 v3 → v4：`store.Data` 增加同名字段，并落向量字节。v3 文件可读，读后无向量索引。

## 4. HNSW 引擎（internal/vector）

### 4.1 结构

- 多层邻接图。每节点层级由随机数决定，种子可注入，测试可复现。
- 节点主键为 `NodeID`（uint32），与既有 ratchet 编号兼容。
- 距离度量：
  - cosine：写入时归一化，比较用内积。
  - l2：平方欧氏。
  - dot：原始内积。

### 4.2 三层索引形态（D1，核心设计）

一个向量索引在任意时刻由三个不可变部分组成：

| 层 | 内容 | 构建 | 生命周期 |
|---|---|---|---|
| base | 全量稳定向量的 HNSW | folder 后台全量重建 | 构建完原子发布，只读 |
| segments | 若干小 HNSW，每个容量 ≤ 8192 | folder 把 overlay 打包成 segment | 构建完只读 |
| overlay | 未进任何索引的已提交向量 | writer 追加（只存数组，不建图） | fold 时清空 |

读写规则：

- 查询 = base 搜索 + 各 segment 搜索 + overlay 线性扫，三路归并 top-k。
- 同一 NodeID 出现在多层时，按 overlay > segment > base 优先级取最新。旧层结果丢弃。
- KillSet 中的 ID 在归并时过滤。
- segments 数量 ≥ 2，或 segments 向量总数 > base 的 5% 时，folder 触发全量重建 base。重建期间新旧并存，完成后原子替换。

这个形态与"sealed base + micro layers"逐点对应。读侧永远持有不可变三元组，零锁读成立。

### 4.3 插入与搜索

- 插入（仅发生在 folder 构建 segment / 重建 base 时）：贪心下降定位，宽度 EfConstruction 搜索候选，双向建链，修剪到 M。
- 搜索：参数 efSearch。默认自适应：`max(64, c·log2(base 节点数))`，常数 c 由 spike 的 recall–ef 曲线拟合后锁定。查询时可显式覆盖。硬下界 `ef ≥ k`。
- 墓碑节点在搜索展开时跳过，并计数。

### 4.4 删除

- 删除节点 / 覆盖向量属性 → 该 NodeID 在旧层的向量作废。
- 作废靠第 4.2 节的归并优先级与 KillSet 过滤实现，不需要改图结构。
- base 内死亡 ID 比例 > 30% 时，folder 顺带全量重建（与 4.2 的重建条件合并判定）。

### 4.5 确定性说明

- ANN 结果与顺序不保证逐次一致。这是算法本性。
- 单测通过固定种子与暴力对照断言召回下界，不断言固定输出。

## 5. 写路径集成

### 5.1 提交流程

1. 调用方提交 UpsertNode，props 含索引属性的 `[]float32`。
2. 校验：维度或类型不匹配 → 该节点被索引跳过，记 warning。不报错，不阻塞（第 13 节裁决）。
3. Changeset 编码进 WAL 帧（gob 含向量字节），fsync 节奏不变。
4. 微层持有向量引用。generation 发布时，overlay 追加该向量。

### 5.2 folder 职责扩展

folder 单飞 goroutine 在既有 fold 流程外，增加向量阶段：

1. overlay 向量数 ≥ 2048，或 fold 周期到达 → 把 overlay 构建成新 segment（离线建图，完成后发布）。
2. segments 触发 4.2 节条件 → 全量重建 base。
3. 向量阶段与图层 fold 共用单飞通道，不并发执行。

### 5.3 吞吐预算

- writer 关键路径只做：gob 编码 + WAL 追加 + overlay 数组追加。无图结构操作。
- 目标：向量写入 ≥ 20k 条/秒（ rides 既有组提交节流，p99 5ms 上限不变）。

## 6. 读路径集成

### 6.1 挂载点

- `genView` 增加字段 `vec *vector.State`（不可变三元组的持有者）。
- generation 发布时原子挂载。`engine.View` 不感知向量（CSR 构建路径完全不变）。
- `Graph.QueryVector` 读取 `g.view.Load()`，走零锁路径。

### 6.2 查询流程

```
QueryVector(name, vec, k, opts):
  1. 取当前 genView 的 vec.State。不存在该索引 → 报错。
  2. 校验查询向量维度 = 声明维度。
  3. base 搜索（efSearch）→ 候选 A。
  4. 各 segment 搜索 → 候选 B。
  5. overlay 线性扫 → 候选 C。
  6. 归并：同 NodeID 取最新层；KillSet 过滤；label/prop 过滤（后过滤 + 3 倍过采样，终态方案）。
  7. 截断 top-k，返回 VectorHit 列表。
```

### 6.3 延迟预算

- base + segments 搜索：O(ef · log N) 次距离计算。
- overlay 线性扫：2048 × 768 维 ≈ 1.6M 次乘加，Go 标量循环约 2–4ms。
- 预算合计：1M×768 场景 p99 < 10ms（验收线见第 10 节）。

## 7. 持久化与恢复

### 7.1 启动恢复

1. 读 store v4 checkpoint（含声明与已压实向量）。
2. 重放 WAL 尾部（向量在 Changeset 帧内）。
3. 此时全部 live 向量在内存，但 base/segments 未建。
4. 后台并行重建 base HNSW。重建完成前，QueryVector 走暴力扫（正确但慢）。
5. 重建完成，原子挂载，查询切到索引路径。

### 7.2 v2 索引文件（Phase 2）

- base HNSW 序列化为独立文件（类似 Neo4j 的 .vex 思路）。
- 文件记录 generation 水位。水位不匹配则丢弃重建。
- 目的：缩短大库重启时间。v1 不做。

## 8. 一致性语义

| 语义 | 保证 | 机制 |
|---|---|---|
| 持久性 | WAL fsync 后向量不丢 | 既有 Changeset 通道 |
| 可见性 | 提交即可查（近似候选含最新） | overlay 线性扫 |
| 收敛 | fold 后近似质量回升 | segment/base 演进 |
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

- 计划语法：`CALL vectra.vector.search("idx", $v, 10) YIELD id, score`。
- 依赖既有 CALL 支撑或按需新增。Phase 2 设计另行细化。

### 9.3 MCP 工具（Phase 1）

- 新工具 `vector_search`，成为第 11 个工具。
- 入参两种：`query_vector`（浮点数组）或 `similar_to`（节点 key，用其索引属性向量）。
- 限幅：k 默认 10，上限 100。结果带 score。
- 描述文案明示"近似结果"。

## 10. 测试与基准

### 10.1 单元测试

- 建图 / 搜索 / 墓碑跳过 / 三路归并去重（同 ID 取最新）。
- 维度不匹配跳过、度量锁定、声明持久化往返。
- 崩溃恢复：WAL 截断点扫描、checkpoint 后重放。
- 并发：`race_enabled` 构建下读写并发压测。

### 10.2 召回基准（brute-force 真值对照）

- 分布：均匀随机 + 聚类混合（聚类更接近 embedding 真貌）。
- 矩阵：N = 10k / 100k / 1M；dim = 384 / 768。
- 指标：recall@10、p50/p99、插入吞吐、内存放大（常驻 / 理论向量字节数）、recall–ef 曲线（拟合自适应常数 c）。

### 10.3 验收线

| 指标 | 线 |
|---|---|
| recall@10（1M×768，聚类，自适应 ef） | ≥ 0.95 |
| 查询 p99（同场景） | < 10ms |
| 向量写入吞吐（writer 路径） | ≥ 20k 条/s |
| 写路径 p99 回归 | 不劣化（既有 5ms 节流上限不变） |
| 内存放大 | ≤ 1.6× 理论向量字节 |

### 10.4 基准落盘

- 报告写入 `docs/benchmarks/2026-10-19-vector-spike.md`（Phase 0 产出）与后续阶段报告。

## 11. 分阶段交付与门控

### Phase 0：spike（throwaway 标注）

- `internal/vector` 核心算法 + 召回/延迟基准。
- 不接 WAL / View / writer。纯算法验证。
- 产出 = 第 10.3 节基准数据与 recall–ef 曲线，即门 2 判据。

### 门控判定

- 门 1：MCP 采用信号达到（具体阈值由产品侧定）。
- 门 2：spike 全部验收线达标。
- 两门齐过 → Phase 1 启动。只过门 2 → 设计资产保留，产品化等待。

### Phase 1：MVP 集成

- WAL / store v4 / genView 挂载 / folder 向量阶段 / Go API / MCP 工具。
- 基准复跑：集成后不劣化 spike 结果超过 20%。

### Phase 2：完善

- Cypher CALL 语法、索引文件持久化、SIMD（可选）。

## 12. 风险与缓解

| 风险 | 缓解 |
|---|---|
| base 重建期新旧并存，内存峰值 | 向量数组跨代共享，只放大图结构；重建走后台单飞 |
| overlay 线性扫拖慢 p99 | 阈值 2048 + folder 向量阶段独立触发 |
| WAL 帧膨胀（768 维 ≈ 3KB/向量） | 既有批次聚合吸收；WAL 滚动上限不变 |
| 旧版本打开新库 | 不新增帧类型；IndexDecl gob 扩展向后兼容；store v4 降级读取 |
| recall 不达标 | 参数扫描已入 spike 范围；ef 可调面留给调用方 |
| spike 与正式实现重复劳动 | spike 代码按可保留标准写（算法核心同构），定型时复核而非重写 |

## 13. 已裁决事项（原开放问题，2026-10-19）

1. **pre-filter 不做**。后过滤 + 3 倍过采样为终态方案。低选择性 label 的查询由调用方放大 K 解决。依据：Agent 记忆主查询在单一池内做语义搜索，label 过滤是低频路径。
2. **不匹配宽容跳过 + warning**。维度或类型不匹配的节点被索引跳过并记 warning。不报错，不阻塞导入。多 chunk 需求引导拆子节点 + 边，进文档示例。
3. **efSearch 对数自适应**。`ef = max(64, c·log2(base 节点数))`。常数 c 由 spike 的 recall–ef 曲线拟合后锁定。硬下界 `ef ≥ k`。依据：先测量后设计。

## 14. 影响面

| 文件 / 包 | 变更 |
|---|---|
| `internal/vector/`（新） | HNSW、State 三元组、归并 |
| `internal/wal/wal.go` | IndexDecl 结构扩展 |
| `internal/store/store.go` | v4 格式 |
| `vectra.go` | genView.vecState、公开 API |
| `internal/writer/` | folder 向量阶段 |
| `mcp/` | vector_search 工具（三语言） |
| `internal/cypher/` | Phase 2 |
| `docs/` | README、benchmarks、多向量建模示例（拆子节点 + 边） |

## 实现期偏差记录

（实现时逐条记录与本 spec 的偏差，模式沿用 typed-index spec。）
