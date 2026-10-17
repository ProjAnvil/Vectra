# 后台折叠器设计(direct 模式写侧减负)

Date: 2026-10-17 · 依据: `docs/benchmarks/2026-10-17-perf-fixes-retest.md` 残余项 1 + 同行架构调研(LevelDB/RocksDB/Pebble/Badger)
范围: direct 模式的层栈折叠从写者路径移到后台 goroutine。零锁内核不变量(A8)不变;公共 API 不变。

## 背景

per-generation 视图槽位修复(本轮第一步)后,M-w8 混合负载: 读 p50 238µs / p99 16ms(两条验收线达成),写 132 批/s(供速 200),写 p99 60ms / max 330ms。写者 CPU 仅 1.4%——节流来自**尾部事件**: F-merge(O(F))+ 其后的全量视图重建 + 快照拷贝,落在写者上。

同行共识(调研): 写者只做 O(1) 发布;折叠/压实在后台线程;用不可变输入 + 原子指针安装;背压用 slowdown/stop 两级 stall。

## 设计

### 职责划分

| 角色 | 做什么 | 不做什么 |
|---|---|---|
| 写者 goroutine | 追加层、预构建追加(O(新增))、发布、**并入**折叠结果、体量 Seal(forceMerge,罕见) | 任何 FoldLayers / 快照拷贝 / 全量视图重建 |
| 折叠器 goroutine | 读取已发布代(不可变),计算 FoldLayers(T+tail)与可选 F-merge,**从零构建**折叠前缀的视图状态(含 F 边界快照),放入交接槽 | 发布视图;触碰写者私有状态 |

发布权**只归写者**(与 Pebble 双方 CAS 不同): 折叠器纯计算 + 原子交接槽,写者在下次发布时并入。无 CAS 循环、无丢失更新、无共享可变状态——仅原子量与一个 channel。

### 数据流

1. 写者发布后检查压力: `len(layers) ≥ cap && tailEdges ≥ 64 && !inFlight` → 置 inFlight,非阻塞唤醒折叠器(惰性启动 goroutine)。
2. 折叠器: `v := g.view.Load()`;按现有分段规则(F/T/tail、T ≥ max(512, F/2) 并入 F)计算 `folded []*Layer`(1-2 层);对 `[base, folded]` 从零构建 `viewBuild`(含 splitF 处快照);写入 `g.pendingFold.Store(&foldResult{src: v.layers, folded, ext, splitF, splitT})`;清 inFlight。
3. 写者下次发布(publishLayer): `r := g.pendingFold.Swap(nil)`;若 `layerPrefixMatch(cur.layers, r.src, len(r.src))`(写者只追加,前缀仍有效;Seal/rebase 会打破)→ `newLayers = r.folded + cur.layers[len(r.src):]`,`ext = r.ext` 并追加尾部层(O(尾部)),splits 取结果值;否则丢弃结果。
4. 体量 Seal 不变: Batch 边界 `total ≥ max(64K, base)` → forceMerge(写者私有镜像重建,必须留在写者;摊销 ≤ 每 base 记录一次)。

### 并发安全(零锁)

- 共享状态: `g.view`(atomic)、`pendingFold`(atomic.Pointer)、`inFlight`(atomic.Bool)、唤醒 channel(cap 1,非阻塞发送)、`folderRunning`(atomic.Bool)。
- 折叠器只读不可变层与 `baseKeys`(合并时整体替换,折叠器持旧指针仍安全);其产出的 Layer/viewBuild 在交接前为折叠器私有,交接后为写者私有(发布即转移所有权)。
- 写者的 splitF/splitT 仍为写者私有普通字段(折叠器把 splits 放在结果里)。
- 预构建的 rewind 路径删除: 折叠结果自带完整 ext,写者只需追加尾部。

### 生命周期

- 惰性启动: 首次压力信号时 `folderRunning.CompareAndSwap(false,true)` → `go g.foldLoop()`。
- 空闲退出: 500ms 无信号则 `Store(false)` 后再非阻塞检查一次 channel(消除"决定退出后信号到达"的丢失),有则重启继续。
- Close: 置 closed;折叠器循环检查后退出。无需等待——折叠器不触碰写者状态。
- wrapper 模式(writerAttached)整体跳过(wrapper 自有合并器)。

### 背压(简化版 LevelDB slowdown)

折叠在飞且 `len(layers) ≥ 4 × cap` 时,写者每次发布 `time.Sleep(1ms)`;不实现 stop 级。混合负载(50 边/批,200/s)下折叠远快于积累,预期不触发;作为长跑安全阀记录。

### 不做

- 折叠器 CAS 发布(双写者协议): 复杂度高且无必要——交接模式已把全部重活移出写者。
- 引用计数回收层: Go GC 已处理不可变层的回收;无需 refcount。

## 验收线(M-w8, writerate 200)

| 线 | 槽位修复后 | 目标 |
|---|---|---|
| 写达成速率 | 132 批/s | ≥ 190 批/s(供速 200) |
| 写 p99 | 60ms | ≤ 10ms |
| 写 max | 330ms | ≤ 50ms(体量 Seal 除外,10s 内不触发) |
| 读 p50 / p99 | 238µs / 16ms | 不回退 |
| 门 A2-A7 + race | PASS | PASS |

## 测试

1. 差分等价: 折叠器开/关的同一操作流在所有可观察面一致(复用现有差分桩,折叠异步不改可见状态,随时可比;Seal 后 ID 容忍)。
2. 交接正确性: 前缀匹配并入;Seal 打破前缀 → 丢弃;并入后 ext 追加尾部与全量重建等价(复用白盒等价测试)。
3. 生命周期: 惰性启动/空闲退出/Close 后不残留 goroutine(goleak 风格断言: runtime.NumGoroutine 回落)。
4. race 全套 + 门 A2-A7 + 混合复测画像。
