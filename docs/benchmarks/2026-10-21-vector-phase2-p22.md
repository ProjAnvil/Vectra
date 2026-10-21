# Vector Phase 2 P2-2 基准：森林持久化重启对比（2026-10-21）

分支 phase2/vector。安静机器独占运行（quiet-watcher 21:20 触发）。
测量代码：`bench/vector_restart_test.go`（VECTRA_RESTART=1，VECTRA_RESTART_N=100000）。

## 口径

restart-to-ready：`Open` + 车道排空至 overlay < 2048（与构建收敛同一判据）。
Open 本身分不开两条路径——`State.Add` 是 O(1) overlay 追加，重建成本在 Open
返回后的后台车道排空（open-only: load 455ms vs rebuild 429ms，见测试日志）。
路径区分由 overlay-at-open 断言保证：load=1696（合法尾部驻留）vs
rebuild=100000（全部向量待重建），失败即 fail loudly。

## 数据（100k×768，1000 簇，768 维）

| 路径 | restart-to-ready | overlay at open | 证据 |
|---|---|---|---|
| 加载 `<base>.vec` | **455ms** | 1696 | v.vec 309.4MB（CRC+水位+声明三重门通过） |
| 删除 v.vec（重建） | **7m18.65s** | 100000 | folder 车道重打全部 HNSW（对照：原始构建 7m19.7s） |
| **加速** | **964x** | | |

构建基线 7m19.7s（batched 512/批摄入 + 车道排空），两条重启路径各自在独立
副本上计时，查询正确性断言（自查询 top-1 == 自身 key）两路均过。

## 结论

1. spec §7.2 的目标（消除重启重建窗口）在 100k 达成：分钟级窗口压到亚秒。
   按 folder 车道 ~230 vec/s 的重建速率外推，1M 级重建窗口为小时级，加载路
   径价值更大（读 3GB 文件量级，预计 ~10s 级，未实测）。
2. v.vec 体积 = 向量数据（300MB）+ 森林结构（9.4MB）：加载带宽 ~680MB/s，
   反序列化不构成新瓶颈。
3. 门 2b 三线同轮复测全过（见 2026-10-21-vector-phase2-p21.md），森林持久
   化无回归。

## 复现

```
VECTRA_RESTART=1 VECTRA_RESTART_N=100000 go test ./bench/ -run TestVectorRestartLoadVsRebuild -v -count=1 -timeout 75m
```
