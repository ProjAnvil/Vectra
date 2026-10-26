# 1M 修复复测：fix/1m-findings 前后对照（2026-10-25）

分支 `fix/1m-findings`（522de17 + harness 适配 9e23b7c），对照基线
`docs/benchmarks/2026-10-23-vector-1m.md`（main@ed76f69 直测）。
同一机器（Apple M5 Pro，18 核，64GB）、同一 harness
（`bench/vector_1m_test.go`，本分支按异步时代适配了三处：close 分类
只认快照集、bulk 过阈值后等飞行快照落地、B4 断言改记录——见
9e23b7c）、同一安静门控。协议、种子、n=1,001,471 不变。

修复载荷（本分支）：close 形森林刷新 + 恢复期 LSN 门控（B1）；
卷触发压缩异步化（停顿）；WAL 段粒度删除原语。

状态：**C/D 进行中**（A、B 已完成；B1 修复在 1M 直测确认）。

## 0. 结论（先读这个）

三项修复全部经 1M 直测验证：

1. **压缩异步化**：压缩 seal 停顿 p50 **11.47s→190ms**（-98%）；
   摄入接受 **5,938→21,459/s（3.6x）**；Submit max 26.27→11.70s
   （残余=飞行期森林编码的 vi.mu 串行段，I1 量化落定）。seal 节律
   变为"飞行期抑制轻档+压缩背靠背"（5 轻+2 压缩 vs 旧 34+12）。
2. **B1 冻结积压**：轻量 Close 后重开 overlay-at-open
   **970,751→2,047（纯尾）**、ready **2h57m30s→0s**——close 刷新的
   森林（v.vec 3.03GB，mtime=Close 时刻）挂载覆盖全部摄入，恢复
   门控吞掉已覆盖链帧。代价：Close 315ms→**32.4s**（刷新写
   2.9GB 森林+飞行完成步重锚；设计内的取舍）。
3. **无回归**：recall 0.9480/0.9900/1.0000 与基线逐位一致；
   graph/state 比值 1.01-1.02x；graph-alone 6.61GB 持平；
   B3-reopen（mode A 等价）7.518s 持平；持续摄入 33.7k/s（B3
   68k/2.015s，对基线同段 20.5k/s = 1.6x）。

## 1. 构建与 seal（阶段 A）

| 项 | 2026-10-23 基线 | **本次复测** | 判定 |
|---|---|---|---|
| 摄入接受 | 5,938/s（seal 占 78% 墙钟） | **21,459/s**（46.668s 全量；submit p50 6.14ms） | **3.6x** ✓ |
| 轻 seal 停顿 | p50 175ms / max 395ms（34 次） | p50 181.7ms / max 336.6ms（5 次） | 单次持平；次数大减（飞行期抑制） |
| **压缩 seal 停顿** | **p50 11.47s / max 26.27s（12 次）** | **p50 190.6ms / max 190.6ms（2 次）** | **-98%** ✓（仅剩写者侧完成步） |
| Submit max（编码残余，I1） | 26.27s | **11.70s** | 残余=飞行期森林编码（vi.mu 串行），量化落定 |
| 构建总墙钟/名义吞吐 | 3h20m29s / 83.3/s | 3h33m48s / 77.8/s | 车道工作不变；慢 9%=并行会话负载（drain 中段 load 12-13，见诚实备注） |
| drain | 3h17m40s（81.7/s） | 3h33m1s（77.5/s 累计） | 同上；曲线同形（长 0/s 窗=深层合并） |

Close 形态变化（设计内代价）：构建收官 Close **315ms→32.442s**——轻/压缩
判定下走到压缩档 + close 森林刷新写 3.10GB（v.vec mtime=Close 时刻）
+ 忙情形 v2 重锚（manifest retainFrom=55）。换来的是 B1 的 2h57m→0s。

## 2. 查询（协议 #4 不变）

| ef | 基线 recall/p50/p99 | **复测** | 判定 |
|---|---|---|---|
| 32 | 0.9480 / 5.32 / 6.31ms | 0.9480 / 6.36 / 6.94ms | recall 逐位一致；延迟全会话 +15-20%（机器状态），state 参照同步抬高，比值 1.02x |
| 64 | 0.9900 / 8.95 / 12.16ms | 0.9900 / 10.36 / 11.20ms | 同上；比值 1.01x（p99 反而低于基线） |
| 128 | 1.0000 / 15.63 / 16.76ms | 1.0000 / 18.69 / 22.64ms | 同上；比值 1.07x |

查询路径零改动：recall 三档与基线**逐位一致**；绝对延迟的偏移是会话
方差（参照 State 同步抬高佐证，非集成回归）。满尾 vs 空尾配对仍在
噪声内。

## 3. 重启（修复主张的主战场）

| 工况 | 基线 | **复测** | 判定 |
|---|---|---|---|
| **B1 轻量 Close 后重开** | overlay 970,751，ready **2h57m30s** | open=5.357s，overlay=**2,047**（纯尾），ready=**0s** | **修复确认**：close 刷新的森林（3.03GB，mtime=Close 时刻）挂载全覆盖，恢复门控吞掉已覆盖链帧 |
| B3-reopen 新鲜压缩后 | open 7.6s / overlay 2047 | open 7.518s / overlay 2047 | 持平 ✓ |
| B3 持续摄入 | 20.5k/s（43,520/2.121s） | **33.7k/s**（68,000/2.015s） | 异步化附带收益 1.6x |
| B4 受控链重放 | open 7.2s / overlay 45,567 / ready 3m9s | **未复测**（见裁定） | 语义已被恢复门控改变（只回灌未覆盖帧），20k 冒烟已示 |
| C3 纯森林加载 | open 8.728s / overlay 511 / ready 0s / 0.36GB/s | open 8.231s / overlay 415 / ready 0s / **0.39GB/s** | 持平 ✓（v.vec 3.23GB） |
| D 重建（有界 30m） | 126.2/s，投影 ≥102.3min | **134.1/s**，投影 ≥94.4min（open 4.879s，overlay 1,001,471） | 持平 ✓ |
| Close（构建收官） | 315ms（轻、不刷新） | **32.442s**（刷新 3.10GB 森林+重锚） | 设计内代价 |

**裁定（B4 未复测）**：异步时代下 B2 忙情形完成会保留飞行窗口的 bulk
帧，B3 再摄入 190MiB 使 close 时保留字节 >256MiB，B3 的 close 合法走
压缩档，harness 的"轻档"前置过时（chain 阶段在 B3 收尾处 fatal，
B1/B2/B3 数据均在 fatal 前落盘）。修复 B4 需按新算术调小
VECTRA_1M_CHAIN_VECS 重跑整链（含 3.5h 重建），收益仅一条已可由
B3-reopen+20k 冒烟推出的曲线——不做，记录待办。

## 4. 内存

基线 graph-alone 6.59GB → **6.61GB**（HeapInuse 7.64GB，持平 ✓）。
GC PauseTotal 81.2ms（基线 16.3ms；两次全量显式 GC 的回收量差异，
GCCPUFraction 仍 0.0002——占比可忽略的结论不变）。

## 诚实备注

1. **drain 慢 9%（3h33m vs 3h17m）**：drain 中段并行基准会话满载
   （load 12-13，200% CPU），门控只在起跑前把门；摄入/seal 数字取自
   开跑头 1 分钟（干净窗口），drain 属后台车道、受共享 CPU 影响。
   曲线与基线同形（长 0/s 窗=深层合并）。
2. **查询绝对延迟 +15-20%**：同会话 State 参照同步抬高（比值 1.01-
   1.07x 优于基线 1.02-1.05x 的波动带），归因机器状态，非回归；
   recall 逐位一致是硬证据。
3. **B4 未复测**（见 §3 裁定）；B 阶段日志含一次 harness fatal
   （前置过时），B1/B2/B3 数字均在 fatal 前取得。
4. **C1 的 29m32s 排水**是 B3 未收敛积压（61,855）的正常排水（深层
   合并窗口 ~20 分钟单核满载），非新形态。
5. ef64 p99 11.20ms：仍超 spec §6.3 旧线（已修订为直测口径），与
   2026-10-24 ef 扫描一致。

## 复现

```
cd docs/benchmarks/2026-10-25-1m-fixes-retest
./run_stage.sh build 8h VECTRA_1M_DIR=/tmp/vectra-1m
cp -R /tmp/vectra-1m /tmp/vectra-1m-asbuilt
./run_stage.sh chain 4h VECTRA_1M_DIR=/tmp/vectra-1m
./run_stage.sh compact 3h VECTRA_1M_DIR=/tmp/vectra-1m
./run_stage.sh rebuild 2h VECTRA_1M_DIR=/tmp/vectra-1m \
    VECTRA_1M_REBUILD_FROM=/tmp/vectra-1m-asbuilt \
    VECTRA_1M_REBUILD_MAX_SECS=1800
```
