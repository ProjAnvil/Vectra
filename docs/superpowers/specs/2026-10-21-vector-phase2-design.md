# Vectra 向量 Phase 2 设计增补（2026-10-21）

状态：待用户裁决范围后生效。本文按推荐范围撰写（P2-1/P2-2/P2-3 三项）；
Cypher CALL（spec §9.2）建议移 Phase 3 单独 brainstorm。SIMD 不做（用户裁决
2026-10-21，不再列入任何阶段）。

母 spec：`2026-10-19-vector-search-design.md`（v3 + 偏差记录）。本文只写增量
设计；语义冲突时以本文为准并回写母 spec 偏差记录。

## 0. 输入与动机（全部实测）

| 痛点 | 数据 | 来源 |
|---|---|---|
| WAL 帧膨胀 | 768 维 JSON ≈7.2KB/向量（二进制 3KB 的 2.4x） | spec §12 |
| 批边界延迟 | p50 ≈21ms = 512×7.2KB JSON 编码+fsync | 门 2b 线 1 |
| checkpoint 频率 | 4MiB/7.2KB ≈570 向量/seal；每次 seal 全量快照 | A.2 实测（见偏差记录更新） |
| 快照体积 | 向量经 node props 走 JSON 通道，快照同步膨胀 | internal/store 注释 |
| 重启重建 | 森林不落盘，重启后自底层重建；1M 级分钟窗口 | spec §7.1 |
| TS/Python 缺口 | 无 REST 向量端点；server 模式无法声明索引 | 门 2b 偏差与遗留 |

## 1. P2-1 WAL 二进制 section + store v5

### 1.1 目标

帧内 float32 数组离开 JSON 通道。向量帧 7.2KB → ≈3KB；批边界 p50 显著下
降；checkpoint 间隔（按向量数）同步拉长 2.4x。

### 1.2 WAL 帧格式 v2

- 新帧类型 `frameChangesetV2 = 4`（既有 1/2/3 不动）。
- payload 布局（帧内、既有 12B 帧头之后）：

  ```
  [4B magic "VVB2"][4B jsonLen][json 骨架][二进制向量区]
  ```

  json 骨架 = Changeset JSON，唯一差异：被索引的向量属性值替换为
  `{"$vec":[binOffset, dim]}` 引用；二进制区 = raw float32 LE 连续存放。
- 选型规则（**按声明，不按 Go 类型**——builder check 裁定）：Append 时
  对每个节点 upsert，若其某属性名命中已声明向量索引的 `Prop` 且值是数值
  数组（`[]float32` 或 JSON 解码后的 `[]any(float64)`），该值走二进制区。
  整个 changeset 一旦有一处命中即为 v2 帧；否则沿用 `frameChangeset`
  （纯图写帧零变化，非向量库完全不受影响）。
  - 依据：从快照加载或经 JSON 管道进入的向量是 `[]any(float64)`，按类型
    检测会让迁移库与导入管道全部留在 JSON 通道，收益落空。
  - 未声明属性上的数值数组按契约不是向量，保持 JSON。边属性不索引，保持
    JSON。
- `$vec` 引用解析只在已声明的向量属性上进行（重放时声明帧先于数据帧；
  store v5 把声明 section 前移到节点之前）。未声明属性上形如 `$vec` 的
  用户字面量原样保留——无保留字冲突。已声明属性上的 `$vec` 引用越界 =
  硬解码错误（视为损坏）。
- `NormalizeVec` 拦截位置不变（§5.1）；NaN/Inf 行为与 v1 一致。
- 新读者双向兼容：v1 帧照常解码（v0.11 可读一切存量库）。

### 1.3 兼容与迁移（本任务的关键决策）

事实：旧二进制把未知帧类型按断尾静默截断（`scanFrames` 对未知 type
break，与撕裂尾不可区分）。带内信号无法让旧代码报错，只能让它截断。

方案：迁移式升级 + store v5 绊线。

1. v0.11 `Open` 检测到旧格式库（store v4，或 WAL 含向量帧）→ 打开成功后
   立即执行一次 checkpoint（Seal）：写出 store v5 快照并 Reset WAL。
2. store v5 沿 v3→v4 先例：版本头前置，旧读者硬失败。从此降级到 v0.10 在
   快照加载处被明确拦截（而非静默截断 WAL）。
3. 残余窗口：迁移 checkpoint 失败（如磁盘错误）时库停留 v4+WAL 混链状
   态。此时降级会静默截断——发布说明明示"迁移未完成的库禁止降级"，
   v0.11 下次 Open 重试迁移。
4. 迁移是幂等的：检测条件只看快照版本与 WAL 内容，重入安全。
5. 全新库：v0.11 首次 `Open` 创建新库时立即写出一个空的 v5 快照 +
   manifest（成本≈0）。绊线从第一个字节就存在；否则"首次 checkpoint 前
   崩溃"的新库对 v0.10 仍是静默截断窗口（builder check 指出）。
6. server 模块的混合版本集群**不支持**（本文裁定，用户可推翻）：raft 快
   照是 store 编码字节，v0.11 leader 产出 v5 快照后 v0.10 follower 的
   `Restore` 硬失败（旧二进制报 "unsupported version"，文案不可改）。不做
   滚动升级保证；升级 runbook 落发布说明与 server README（全节点停机升
   级，或先升全部 follower 再升 leader）。raft 日志本身 gob add-only 兼
   容，不受影响。

### 1.4 store v5

- 头部版本 4 → 5（`"VCTR"|u16`）。旧读者硬失败（同 v4 先例）。
- section 顺序调整：声明 section 前移到节点之前（头 | 声明 | 节点 | 边 |
  indexKey | 二进制向量区 | CRC）。解码节点时已知声明，`$vec` 解析按
  §1.2 规则进行。
- 增量：已声明向量属性的数值数组改走二进制向量区（与 WAL v2 同一
  codec，props JSON 内留 `{"$vec":[off,dim]}` 引用；检测同样按声明不按
  类型，迁移库的首次 v5 checkpoint 即获收益）。快照体积与 checkpoint 耗
  时同步受益（A.2 实测快照随向量数线性膨胀的根因）。
- 非向量节点 props 编码不变；v4 及以下快照读取保留（迁移入口依赖）。

### 1.5 实现面

- `internal/wal`：帧类型 4、envelope codec（编码/解码/扫描三处）、
  `Replay` 解码分支。
- `internal/store`：v5 编码 + 版本分派。
- 根包：Append 选型、Open 迁移、golden-bytes 测试固定 v0.10 生成物。
- 不新增依赖；`go.mod` 零变化。

### 1.6 验收

- 单元：v1/v2 混链 round-trip；v0.10 库被 v0.11 打开并完成迁移；迁移后
  v0.10 二进制打开同库硬失败（golden bytes 固定）；崩溃在迁移中途
  （半写快照/未 Reset）后 v0.11 重开自愈。
- 基准（单独跑，全模块不并行）：
  - 门 2b 三线不劣化（吞吐 ≥20k/s、普通写 p50 比值 ≤110%、recall@10
    ≥0.8x 基线）；
  - 批边界 p50 21ms 基线对比（目标：编码项消除后 <10ms；留 fsync 项）；
  - A.2 checkpoint 复测：同 N 下 seal 次数降至 ~1/2.4，seal 耗时随快照
    二进制化进一步下降。

## 2. P2-2 v2 索引文件（森林序列化 + 水位）

### 2.1 目标

消除重启重建窗口（spec §7.2）。100k 重启加载替代秒级重建；1M 级把分钟窗
口压到加载耗时。失败兜底仍是现有重建路径。

### 2.2 文件与时机

- 每库一个 `vectors.vec`（与快照同目录，多索引 = 文件内多 section）。
- 写入时机：Seal 流水线内、快照写完后（temp + fsync + rename，同样的原
  子性纪律）。快照与森林文件在同一 checkpoint 批次产生。
- 内容：每个索引的 State 全量——配置（metric/dim/m/efc/seed）、
  `nextVID`、`vidOf`、`deadBits`、overlay entries（key/vid/vec）、分层
  森林（每 segment：tier、seq、HNSW 邻接逐层 + 入口点 + ids、keys 数
  组）。

### 2.3 水位与加载条件

- 文件头记录生成时的 checkpoint LSN 与声明集合。
- `Open` 在重放前裁定（全部满足才加载，否则丢弃走现有重建路径）：
  1. 文件完整（CRC 过）；
  2. 文件 LSN == manifest LSN；
  3. 声明集合与快照一致（名称/维度/metric/m/efc/seed 全同）。
- 加载后：跳过快照节点 props 的向量摄入（森林已含 ≤LSN 的全部向量）；
  WAL 尾部帧照常走实时写入的同一摄入路径（`State.Add`/删除）——尾部
  向量追加到 overlay，folder 车道照常打包。重建窗口从全量缩为尾部。
- **不**因"WAL 尾含向量帧"而丢弃（builder check 裁定）：4MiB checkpoint
  下向量库崩溃时几乎总有尾部向量帧，按帧丢弃会让任务动机落空。森林写
  于 Seal 批次、精确对应 ≤LSN 的 State（单写者、摄入同步），尾部重放
  即 live 路径的确定性重演，无需指纹。
- `DropVectorIndex`/重名声明冲突时直接丢弃对应 section 重建（声明以
  store/WAL 为准，森林文件是纯缓存）。

### 2.4 实现面

- `internal/vector`：`State.Encode/Decode`（新增，包内自洽——不引入仓
  库其他依赖，维持 spike 纪律）；HNSW 邻接序列化。
- 根包：Seal 集成、Open 加载分支（跳过快照向量摄入）、尾部重放走既有摄
  入。

### 2.5 验收

- 单元（可证伪口径，builder check 裁定）：
  - **身份 round-trip**：`Encode` 前的 State 与 `Decode` 后的 State，对
    同一查询集（acceptance harness 的扰动查询）产出**逐位相同**的 hits
    （同森林 + 确定性搜索）。这是加载路径的正确性线——加载结果 ==
    崩溃前状态，而非 == 重建路径（两者森林形态本就不同）。
  - **recall 不劣化**：加载后 recall@10（brute-force 真值）≥ 重建路径同
    数据的 recall@10。
  - 三类丢弃各一例：LSN 错位、CRC 坏、声明不一致 → 走重建且结果正确；
    半写文件（崩溃于 rename 前）→ 旧文件/无文件均正确处理。
  - 尾部重放：森林 LSN=X，WAL 尾含向量 upsert/删除 → 加载 + 尾部摄入后
    与"全量重建"的 live 向量集合逐 key 相同（集合等价，非结构等价）。
- 基准：100k 重启对比（重建 vs 加载）冷启动耗时；门 2b 不劣化；纯图帧
  尾部不影响加载。

## 3. P2-3 server REST 向量端点 + TS/Python parity + 声明通道

### 3.1 目标

server 部署获得向量查询与索引声明/删除能力；TS/Python MCP 获得向量工具；
embedded wrapper（`writer.Attach`）获得声明通道（修复门 2b 偏差记录的
遗留项）。

### 3.2 读路径前置修复：clone 挂载向量快照（builder check 发现）

事实：server 所有读端点经 `n.graph()` 走 published clone；`Graph.Clone()`
经 `graphFromEngine` 重建，**不带向量注册表**。按原设计 REST 向量查询会
返回"索引不存在"，`GET indexes` 永远为空。

裁定：`Clone()` 传播向量快照（只读挂载）。publish 时对每个 live 索引取
不可变 `Snapshot`（指针级：segment 共享、冻结 chunk 共享、只复制尾块与
deadBits），连同声明一起挂到 clone 的注册表；clone 的 `QueryVector` 读
这些快照头。无写者，无锁故事变化，符合 D1。一致性 = publish 节奏（与
其他读端点同级，文档明示）。向量数据本身已在 clone 的 node props 里
（`unionRecords` 带 props），`similar_to` 路径不受影响。

备选（否决）：读 `f.live` 需进 `f.mu`，改变 FSM 锁故事；pubState 另携
带快照头与挂到 clone 等价但多一条路径。

### 3.3 端点

- `POST /v1/vector/search`：入参 `{index, query_vector | similar_to, k,
  ef?, label?}`；出参 hits（key/score）。published clone 上执行。k 默认
  10、上限 100（对齐 MCP 面）。
- `POST /v1/vector/indexes`：声明；`DELETE /v1/vector/indexes/{name}`：删
  除。两者是写路径，经 raft。
- `GET /v1/vector/indexes`：列出（clone 上读）。

### 3.4 raft Batch 扩展（显式操作，不做全表对账）

- `Batch` 增字段 `VectorIndexOps []VectorIndexOp`，每项二选一：
  `Create *VectorIndexDecl` / `Drop string`。`nil` = 无声明变化。
- 不采用"全表对账"：WAL `IndexDecl` 帧的 nil-vs-empty 语义（空=全部删
  除）搬到每个普通 Batch 上会让遗漏字段等于删全部索引。显式操作无此
  陷阱，且天然支持删除。
- gob add-only 规则已立；`TestBatchGobLegacyRoundtrip` 扩展覆盖新字段的
  双向兼容。fsm 应用时按序调用 Graph 声明/删除 API。

### 3.5 wrapper 声明通道（embedded）

- `writer.Handle` 增 `DeclareVectorIndex(decl) error` 与
  `DropVectorIndex(name) error`。走与 `Submit` 同一请求通道（请求多一种
  kind），由 writer goroutine 在 batch 内调用 Graph 声明 API（写出
  IndexDecl 帧）。同通道保证与后续 Submit 的顺序：声明后提交的向量立即
  被摄入。
- 现状：Attach 后在应用 goroutine 调 `CreateVectorIndex` 在 debug 构建触
  `ErrNotWriter`、release 构建与 writer 竞态——这是遗留项的实质。

### 3.6 客户端

- TS/Python：四端点代理为工具 `vector_search`、`vector_index_create`、
  `vector_index_drop`、`vector_index_list`；工具描述对齐 Go 版
  `vector_search` 文案（"近似结果"明示）。三语言工具清单对照表更新进
  README。

### 3.7 验收

- 根包：`Clone()` 后 `QueryVector` 结果与源图逐位相同；clone 上
  `VectorIndexes()` 等于源图声明。
- server 集成测试：声明→写入→查询 round-trip（leader）、follower 最终一
  致收敛、删除后查询报未知索引、k 限幅与错误面（未知索引/维度不符/
  similar_to 无向量）、gob 旧字节 round-trip。
- writer：Attach 后经 Handle 声明 → Submit 向量 → 查询命中；`-race` 绿。
- TS/Python 单测；go test 三模块全绿。

## 4. 顺序、依赖与门

1. P2-1 先行（帧与快照格式是地基；后续所有基准在最终格式上复跑一次即
   可）。
2. P2-2 次之（依赖 P2-1 落定后的 Seal 批次结构与 store v5 声明前移）。
3. P2-3 可与 P2-2 并行：不碰 WAL/森林格式。但它不再是"三个端点"——含
   `Clone()` 向量挂载、raft Batch 字段、writer Handle 声明通道、四端点、
   两客户端四工具。中等体量。
4. 每任务完整回归门（用户方法清单）：`go test ./...` 三模块；
   vector/writer/root 向量与并发测试 `-race`；门 2b 三线；零依赖；多样
   性启发式与两段式 pack parity 测试。验收类（bench/acceptance）单独
   跑。
5. 发布：CHANGELOG + 三语言版本串 + annotated tag + gh release，从干净
   clone 验证 build+test。版本号按内容（P2-1/P2-2/P2-3 合并为 minor）。
   发布说明必须含：迁移行为、禁止降级条款、server 混合版本不支持 +
   runbook。

## 5. 非目标

- SIMD 汇编内核（用户裁决不做）。
- Cypher CALL 语法（Phase 3 单独 brainstorm；§9.2 维持草案）。
- 1M 内存放大实测（沿门 2a 遗留口径，不在本阶段扩）。
- WAL 帧级压缩（lz4/zstd 类）——二进制化后收益有限且引入依赖面。
- server 滚动升级保证（§1.3-6 裁定不支持）。

## 6. 本文裁定清单（builder check 后，用户可逐条推翻）

| # | 裁定 | 依据 |
|---|---|---|
| 1 | 向量检测按声明不按 Go 类型（WAL v2 与 store v5 同） | 加载/导入路径的向量是 `[]any(float64)`，按类型会让收益落空 |
| 2 | store v5 声明 section 前移到节点之前 | 解码节点时需知声明以解析 `$vec` |
| 3 | P2-2 不因 WAL 尾含向量帧而丢弃，尾部走实时摄入路径 | 4MiB checkpoint 下几乎总有尾部向量帧，按帧丢弃打穿动机 |
| 4 | P2-2 验收 = 身份 round-trip 逐位相同 + recall 不劣化 | "与重建等价"不可证伪（森林形态本就不同） |
| 5 | `Clone()` 传播向量快照（只读挂载） | 否则 server 向量端点读到空注册表；指针级成本；符合 D1 |
| 6 | raft 声明走显式 `VectorIndexOps`（Create/Drop），不做全表对账 | nil-vs-empty 陷阱；天然支持删除 |
| 7 | 增 `DELETE /v1/vector/indexes/{name}` 与客户端 `vector_index_drop` | 否则 server 部署的误声明不可逆 |
| 8 | writer `Handle` 增声明/删除方法，同通道有序 | 遗留项的实质是 Attach 后无合法声明入口 |
| 9 | 新库首次 Open 即写空 v5 快照 | 关闭"首次 checkpoint 前崩溃"的静默截断窗口 |
| 10 | server 混合版本集群不支持；`Restore` 报错指向 runbook | 滚动升级无法保证 leader 不在旧 follower 落后时产 v5 快照 |

## 7. 开放问题（交用户）

1. 范围裁决：推荐三项（P2-1/2/3），CALL 移 Phase 3。
2. store v5 是否顺带二进制化快照向量（本文按"是"撰写；若只要版本绊
   线，快照体积/耗时收益放弃，P2-1 缩小约三分之一；裁定 #1/#2 随之
   失效）。
3. A.2 结论若支持调默认 CheckpointBytes（或按帧计数门控），默认值变更
   属行为变化——patch 版本随 P2-1 一起发，或单独 patch（本文默认：随
   P2-1，因为二进制化本身改变 cadence，分开发会误导部署调参）。
4. 裁定 #10（server 滚动升级不支持）是否接受。若要求支持，P2-1 需增
   "leader 延迟产 v5 快照直到全员就绪"的协商机制，体量显著上升。
