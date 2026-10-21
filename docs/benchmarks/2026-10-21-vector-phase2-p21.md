# Vector Phase 2 P2-1 基准：二进制 WAL/store 通道（2026-10-21）

分支 phase2/vector。安静机器独占运行（quiet-watcher 21:06 触发），复现命令见各节。
对照基线：门 2b 报告（2026-10-20，v0.10.0 JSON 帧）与 wrapper cadence 文档。

## 门 2b 三线（全部 PASS）

| 线 | v0.10 基线 | P2-1 后 | 变化 |
|---|---|---|---|
| 向量写入吞吐（≥20k/s） | 23-26k/s | **68,778/s** | **2.7x** |
| 批边界 p50 | ~21ms | **7.0ms** | **3.0x**（512×3.07KB 编码+fsync） |
| 普通写 p50 比值（≤110%） | 101% | **102.5%**（4.906 vs 4.788ms） | 持平 ✓ |
| recall@10 | 1.0000 全档 | **1.0000 全档** | ✓ |
| 集成 p50 比值（同会话参照 ≤1.2x） | 0.82-0.87x | **0.86/0.86/0.88x**（ef 32/64/128） | ✓ |

复现：`VECTRA_GATE2B=1 go test ./bench/ -run TestVectorGate2B -v -timeout 4h`

## 帧与快照体积（768 维）

| 项 | JSON（v0.10） | 二进制（P2-1） | 比例 |
|---|---|---|---|
| WAL 帧 | 9.29KB/vec | **3.07KB/vec** | 3.0x |
| 全量快照 @8192 vecs | 74.1MB | **24.4MB** | 3.0x |

## wrapper 模式 checkpoint cadence（默认 4MiB 配置，同轮复测）

| 项 | JSON 帧 | 二进制帧 |
|---|---|---|
| seal 间隔 | ~547 向量 | **~1360 向量**（4MiB/3.07KB） |
| 8192 向量 seal 次数 | 15 | **5** |
| 摄入吞吐 | 2,835-2,919/s | **10,213/s** |
| seal 停顿 p50 / max | 155-166 / 276-286ms | 119.8 / 180.8ms |
| seal 时间占比 | 84% | 76% |

64MiB 配置（Phase 2 新默认，见 writer.go）：**44,849/s，0 次中途 seal**。
seal 的绝对耗时仍随 N 线性（全量快照本性）；间隔拉长 3x 摊薄总写放大 3x。

复现：`VECTRA_GATE_CP=1 go test ./bench/ -run TestVectorWrapperCheckpoint -v -count=1 -timeout 1h`

## 结论

1. 批边界 p50 从 21ms 降至 7.0ms：JSON 浮点编码项消失，剩余为 fsync 与组提交。
2. 吞吐 2.7x 超出 spec §1.1 预估（帧缩 3x，直写路径受益略低于帧比因 fsync 占比上升）。
3. 4MiB 默认下 cadence 改善但 seal 占比仍 76%——64MiB 默认（裁定 #3）配合后
   wrapper 向量摄入 44.8k/s，为直写模式的 65%，恢复可用区间。
4. 三线门全过，无回归。
