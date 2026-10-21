# wrapper 模式向量 checkpoint 频率实测（2026-10-21）

测量代码：`bench/vector_wrapper_checkpoint_test.go`（`VECTRA_GATE_CP=1`）。
协议：`writer.Attach` + `Submit`（组提交，每 changeset 256 节点×768 维），顺序
提交；seal 次数按 `g.WALSize()` 骤降计，seal 停顿归因于跨 seal 边界的下一个
Submit 排队等待（writer goroutine 在 afterBatch 内跑完整 Seal 流水线）。机器
安静窗口独占运行，两轮结果一致。

## 数据（8192 向量）

| 配置 | 摄入耗时 | 吞吐 | seal 次数 | 停顿 p50 / max | seal 占比 | 帧成本 | 末次快照 |
|---|---|---|---|---|---|---|---|
| 默认 4MiB | 2.81-2.89s | 2835-2919/s | **15**（每 ~547 向量） | 155-166ms / 276-286ms | **84%** | 9.29KB/vec | 74.1MB |
| 调高 64MiB | 0.69s | 11719-11912/s | 1 | 264-266ms | 38% | 9.29KB/vec | 74.1MB |

对照：直写模式（无 writer）同协议吞吐 ~26k/s（门 2b 线 1）。Close 的最终
Seal 另计 7.4-9.9s（全量快照 74.1MB @ 8192 向量，体积随 N 线性）。

## 结论

1. **默认 4MiB 在向量负载下不可用**：4MiB / 9.29KB ≈ 547 向量即触发一次
   全量快照 seal；seal 停顿随 N 增长（实测 p50 155ms → max 286ms 于 8192），
   84% 摄入时间耗在 seal，吞吐塌到直写模式的 1/9。写放大推得 O(N²)：15 次
   seal 的快照总量 ≈ Σ 增长 ≈ 560MB（磁盘只保留最新一份，由停顿时长与末次
   74.1MB 反推）。
2. **spec §12 "按实测调高或按帧计数门控" 裁定为：默认调高到 64MiB**。理由：
   4.1x 吞吐恢复；非向量 wrapper 部署 64MiB ≈ 数十万次普通写才 checkpoint 一
   次，恢复重放秒级可接受；层栈折叠由 MergeEdges 规则独立驱动，checkpoint
   频率不影响读路径栈深。
3. **与 Phase 2 二进制 section 一并交付**（P2-1）：帧 9.29KB → ~3.1KB 后间
   隔再拉长 ~3x（64MiB ≈ 2.1 万向量/seal）。两项都改变 cadence，分开发布会
   误导部署调参。发布前向量密集 wrapper 部署应显式配置 CheckpointBytes。
4. 帧实测 9.29KB/向量 > spec 估算 7.2KB（JSON 浮点尾数平均更长 + key/帧头）；
   二进制化收益按 3KB 计为 ~3x。

## 复现

```
VECTRA_GATE_CP=1 go test ./bench/ -run TestVectorWrapperCheckpoint -v -count=1 -timeout 1h
```

## 附录：二进制帧复测（2026-10-21 21:20，安静机器）

Phase-2 二进制 WAL section 落地后，同协议同机复测一轮。上方原始小节为
JSON 帧基线（9.29KB/vec）；本次唯一变量是帧编码：二进制帧把每向量帧成本
降到 3.07KB，4MiB 预算可容纳 ~1360 向量（基线 ~547），seal 密度降 ~2.5x，
但 8192 向量下 seal 仍占摄入墙钟 76%——结论与裁定不变：默认调高到
64MiB，已随 Phase-2 二进制 section 一并交付（`internal/writer` 默认值）。

| 配置 | 吞吐 | seal 次数（8192） | 停顿 p50 / max | seal 占比 | 帧成本 | 末次快照 |
|---|---|---|---|---|---|---|
| 默认 4MiB | 10213/s | 5（每 ~1360 向量） | 119.8ms / 180.8ms | 76% | 3.07KB/vec | 24.4MB |
| 调高 64MiB | 44849/s | 0（运行中） | — | — | 3.07KB/vec | — |

复现命令同上。
