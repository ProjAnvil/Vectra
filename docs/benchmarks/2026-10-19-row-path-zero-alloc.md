# 行路径零分配：M-w16 mixed-read p99 减半

- 日期：2026-10-19
- 提交：`f50d0d0`（main 本地，未推送）
- 背景：v0.8 复测报告（2026-10-17）遗留项——「M-w16 读 p99 44ms：分配器竞争——16 个并行 BFS worker 的 row 路径（`View.Rows` 每调用新分配 + viewRow 三切片 zip）+ mheap span 锁」
- 环境：Apple M5 Pro（18 核/64GB），`make bench SIZE=medium WORKERS=16 DURATION=10s BENCH_EXTRA='-warmup 1s'`（seed 42，100k 节点/1M 边）

## 结论

| 指标（M-w16） | 修复前（当前 main 复测） | 修复后（两轮取样） | 变化 |
|---|---|---|---|
| mixed-read p99 | 42.27ms | **21.27 / 22.05ms** | **-50%** |
| mixed-read max | 165.04ms | 104.25ms | -37% |
| mixed-read QPS | 4,975 | 6,867 / 6,593 | **+32~38%** |
| mixed-read p50 | 175.3µs | 189.7 / 211.5µs | 略增（缓冲复位成本，量级无关） |
| mixed-write p99 | 10.95ms | 9.53 / 4.98ms | -13~-55% |
| neighbors p99（纯读） | 10.6µs | 9.3 / 11.3µs | 噪声内 |

纯读负载（bfs2/bfsall/path）两轮间自身波动 ±20-40%：这些负载在 Seal 后走 CSR 快路（`FastAdjacency`），不经过本次改动的代码路径，归因机器方差，非回归。

## 机制

分配链（每展开一个节点、每个 worker）：`View.Rows` 的 append 增长分配（2-3 次）+ `viewRow.Row` 的三切片 `make`（3 次）。16 个并行 worker 在混合负载（有 layer → 走 View 路径）下把它放大成 mheap span 锁竞争与调度器颠簸，表现为 mixed-read 的长尾。

修复三件套：

1. **`View.RowsInto(u, dir, buf)`**：追加进调用方缓冲，稳态零分配（`TestViewRowsIntoZeroAlloc` 钉死）；`Rows` 保留为 nil 缓冲包装，"freshly allocated" 契约不变。
2. **`viewRow` 自带 scratch**（行缓冲 + 三切片 zip 缓冲），经可选 `rowSink` 接口走 RowsInto（无该接口的源退回复制路径）；返回切片有效至下一次 `Row` 调用。
3. **`expandRowsParallel` 每 worker `fork()`**：scratch 按 goroutine 独占；顺带把单实现的 `rower` 接口换成 `*viewRow`，消掉接口跳转。

TDD：两个零分配测试先失败后通过（3 allocs/op）；等价测试在第一版实现里抓到真缺陷——缓冲复用未在入口截断，上次调用的残留行会漏进结果。

## 验证

- `go test ./...`（根模块）全绿；`bench` 包的 A6 单独跑通过（全模块并行跑时的 A6 失败是负载下时序门漂移，HEAD 与修复版同样表现，与本改动无关——沿用 v0.7 时期的「负载下跑验收门」教训）。
- `-race`：traverse/engine/根包集成（WAL 并发）全净。
- 未覆盖：cypher 展开的同构 zip（`internal/cypher/exec.go` 的 `directions` 闭包）仍走 `Rows` 每调用分配——engine 模式 bench 不含 cypher 负载，无实测证据先行；若后续 http/cypher 混合负载出长尾，同法迁移。

## 复现

```
make bench SIZE=medium WORKERS=16 DURATION=10s BENCH_EXTRA='-warmup 1s'
```

指标口径与 2026-10-17 复测一致；运行间方差见上，判定以两轮取样为准。
