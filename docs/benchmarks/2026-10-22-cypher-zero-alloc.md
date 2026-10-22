# Cypher 展开行路径零分配：hotfix/0.11.x 首单（2026-10-22）

- 分支：hotfix/0.11.x（基于 v0.11.0）
- 提交：ee99453（rowSink + executor scratch + nodePred 去装箱 + dedup 成对快路）、634fc33（gofmt）
- 背景：行路径零分配报告（2026-10-19，f50d0d0）遗留的明示缺口——"cypher 展开的
  directions 闭包仍走 Rows 每调用分配……无实测证据先行；若混合负载出长尾同法迁移"。
- 环境：Apple M5 Pro（18 核/64GB），安静窗口独占。

## 分配证据（testing.AllocsPerRun，1-hop MATCH，fan 32）

| 版本 | allocs/query | 说明 |
|---|---|---|
| v0.11.0（修复前） | **545** | directions 闭包每节点 4 分配（Rows + 三切片 zip）+ nodePred 伪属性装箱 + dedup 键拼接 |
| 修复后 | **244**（-55%） | 展开访问器稳态 **0 分配**（内部零分配测试精确钉死）；剩余 = 结果行 map/装箱（公共契约 2/行）+ 绑定 map 克隆等 |

分解模型：基础 53 + 6/行 × 33 行 ≈ 244（拟合实测）。要到 ≤120 需动结果契约
（[]map[string]any）或绑定 map 克隆手术——超出 hotfix 范围，记 v0.12 候选。

## 修复内容（镜像 f50d0d0 三件套）

1. `internal/cypher` 可选 `rowSink`（同 traverse 形态）：directions 闭包一次性断言，
   无 sink 的源走复制回退（等价测试钉死）。
2. per-executor scratch（rows 缓冲 + tg/eids/lbs 三切片）：入口截断、按需增长、
   返回切片有效至下一次调用。var-length BFS（reachableWithin）确认逐行内联消费，
   共享 scratch 安全（审查核verified）。
3. 根门面 `cypherSource.RowsInto` 委托 `engine.View.RowsInto`。
4. 顺手两件（审查均判语义等价）：nodePred 伪属性比较去装箱；≤64 行 dedup 走成对
   零分配快路（>64 行原 map 路径不变）。

## 并发尾延迟（16 worker × 300 query × 2 查询形态，VECTRA_CYHP=1，同窗口配对）

条件如实记录：背景为用户视频会议的稳态负载（load ~4.8，无突发进程）；配对同
窗口，两侧噪声相同。

| 查询 | 版本 | p50 | p99 | max |
|---|---|---|---|---|
| 1-hop | v0.11.0 | 2.11-2.33ms | 5.17-5.41ms | 7.2-7.4ms |
| 1-hop | 修复后 | 2.08-2.24ms | 5.13-5.44ms | 7.5-8.3ms |
| *1..3 | v0.11.0 | 2.20ms | 5.34ms | 8.1ms |
| *1..3 | 修复后 | 2.49ms | 5.62ms | 8.5ms |

**结论（如实）**：两轮配对的方向不一致，|Δ| 全部在运行方差（±10%）内——短
查询（每查询展开数百节点）下延迟收益不可测；每查询的解析、视图缓存、结果构
造主导耗时。本修复的可靠证据是分配侧：545→244 allocs/query（确定性）与展开
访问器稳态 0 分配（永久钉死）。按 f50d0d0 的行路径证据，分配率削半的尾部收
益出现在"持续长展开"机制区（原 BFS 证据：16 worker × 10s 持续满载）；本探
针的图形状（每查询 ~2.5K 节点可达）未进入该区。构造真正进入该区的 cypher 探
针需要 10 万级节点图 × 持续 10s 级运行，属后续测量项，不阻塞本修复。

## M-w16 回归检查（make bench，10s，背景负载同上）

| 指标 | 2026-10-19（安静机） | 本次（v0.11.0+fix，会议负载） |
|---|---|---|
| mixed-read p99 | 21.27/22.05ms | **15.38ms** |
| mixed-read p50 | 189.7/211.5µs | **171.0µs** |
| mixed-read QPS | 6,867/6,593 | **9,373** |
| mixed-write p99 | 9.53/4.98ms | **4.24ms** |

无回归；混合读三项均优于历史（含 Phase 2 写路径改进的叠加贡献，两项测量条
件不同仅供方向参考）。

## 复现

```
go test . -run TestCypherViewPathAllocs -count=1 -v          # 分配钉子（≤280）
VECTRA_CYHP=1 go test . -run TestCypherConcurrentViewTail -v # 尾探针
make bench SIZE=medium WORKERS=16 DURATION=10s BENCH_EXTRA='-warmup 1s'
```
