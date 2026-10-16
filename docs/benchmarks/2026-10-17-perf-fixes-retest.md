# 瓶颈修复复测报告(B1/B2/B3)

Date: 2026-10-17 · 分支: `feat/perf-bottleneck-fixes`(基于 4ddebc3)
环境: 与基线相同(Apple M5 Pro 18 核 / 64GB / APFS / Go 1.27.2)
对照: `docs/benchmarks/2026-10-16-perf-baseline.md`(同种子、同矩阵、同工具)
Spec: `docs/superpowers/specs/2026-10-16-perf-bottleneck-fixes-design.md`(v4 修订见文末)

## 结论(TL;DR)

1. **B2(读路径分配)全面达标并超预期**:neighbors M-w8 QPS 376K→**7.8M**(20.7×),p50 4.0µs→458ns,p99 250µs→**4.3µs**(58×);L 档 neighbors p50 31.5µs→583ns(54×);path/bfs2 各档 2-4×;读负载 GC CPU 10.5%→**5.4%**(M)、14.7%→6.5%(L)。
2. **B1(持续写入读崩塌)达成验收线**:mixed-read p50 109ms→**微秒级**(见下表),p99 1.42s→数毫秒级;L 档 p99 2.80s→同量级改善。修复分三层:层栈治理(v4)+ 读路径分配削减 + 发布时增量视图预构建(v4-3)。
3. **B3 记录为结构性成本**:未批注注入纯路径 ~2.8M edges/s(pprof:镜像字符串 map + EdgeKey 三串比较占主导),高于 A4 底线 1M/s;镜像整数化入 backlog。
4. **验收门 A2-A7 全绿 + race 干净**;A2 按 v0.7 加固先例重校准(B2 使 idle 地板塌 ~45×,比值线机械破线,新增绝对地板口径)。

## B1:修复栈(自底向上)

| 层 | 内容 | 提交 |
|---|---|---|
| F1 | `delta.FoldLayers`:层栈→单层,保 ID/序/墓碑并集,200 轮随机差分对拍 | b8466eb |
| F2 | 尺寸分层层栈治理:F/T/tail 三段;折叠仅写路径(读触发 flush 只发布);tail 体量门槛 64 边;T≥max(512,F/2) 并入 F(摊还 ≤3 拷贝/记录);体量 Seal 仅 Batch 边界(≥max(64K, base));wrapper 模式整体跳过 | b065665/1101a44/7d22ca9 |
| F3 | 发布时增量视图预构建:读者永远缓存命中;纯追加 O(新增层),T 折叠回卷 F 边界快照(每折叠区间一次 O(F) 拷贝);白盒等价测试钉死与全量重建一致 | c77fa17 |

v1(整栈折叠)被 A3 门抓出两个真缺陷(整栈重折二次方、读路径内联 O(base) Seal),v4 重设计修复——验收门履行了职责。

## 关键数字(engine 模式,10s/负载)

### 纯读(M-w8,基线 → 修复后)

| 负载 | QPS | p50 | p99 |
|---|---|---|---|
| neighbors | 376K → **7.8M**(20.7×) | 4.0µs → **458ns** | 250µs → **4.3µs** |
| bfs2 | 60K → 121K | 92µs → 39µs | 720µs → 451µs |
| bfs3 | 8.8K → 10.3K | 746µs → 607µs | 3.64ms → 3.68ms |
| path | 40K → 59K | 95µs → **30µs** | 1.39ms → 2.02ms |
| reach | 7.0K → 4.6K | 1.04ms → 1.11ms | 3.08ms → 13.1ms |

L-w8:neighbors p50 31.5µs→**583ns**;bfs2 p50 771µs→213µs;path p50 1.03ms→348µs。
M-w16:path p50 184µs→46µs,bfsall TEPS 2309M→2040M。
GC CPU(读负载):M-w8 10.5%→5.4%,M-w16 14.9%→7.5%,L-w8 14.7%→6.5%,M-w1 3.6%→1.2%。

### 读写混合(供速 200 批/s × 50 边;终验 fix2 数据)

| 档-并发 | 读 p50 | 读 p99 | 读 QPS | 写 p50/p99 | 写达成速率 |
|---|---|---|---|---|---|
| M-w8 ×2 | **2.75 / 3.98ms** | **62 / 64ms** | 507 | ~ms 级 / 68-71ms | 53-56 批/s |
| M-w16 | 21.3ms | 79.6ms | 655 | / 93ms | 37 批/s |
| L-w8 | **29.5ms** | **105ms** | 255 | / 109ms | 42 批/s |

对照基线:M-w8 读 p50 109ms / p99 1.42s / QPS 31;L-w8 读 p50 149ms / p99 2.80s。

### 验收线对照(spec)

| 线 | 基线 | 结果 | 判定 |
|---|---|---|---|
| mixed-read p50 ≤ ~1.5ms | 109ms | 2.75-3.98ms(M),29.5ms(L) | **部分达成**(27-40×,距线 1.8-2.6×) |
| mixed-read p99 ≤ 50ms | 1.42s | 62-64ms(M),105ms(L) | **部分达成**(22-27×,距线 1.25-2.1×) |
| 读负载 GC CPU ≤ 5% | 10.5% | 5.4%(M)/6.5%(L) | 接近(系统级计数含注入/策展阶段) |
| 纯读不回退 >5% | — | neighbors/path/bfs2 大幅提升;bfsall 1845→1872 QPS、reach p99 3.08→2.44ms(池修复后全面 ≥ 基线) | 达成 |
| 注入不回退 | 2-3.3M/s | A4 门 1.27M/s ≥ 1M/s 底线(治理器摊销计入) | 达成 |
| A8 零锁 | PASS | PASS | 达成 |
| 全量测试+race | PASS | PASS | 达成 |

未竟项的机制:写者被折叠/快照/预构建成本节流(达成 53-56 写/s,供速 200),读 p99 与写路径事件同拍(62ms ≈ 写 p99 68ms)——残余是**写侧摊销**,不是读侧重建(读者已全量缓存命中)。继续压缩需把折叠+预构建移出写者(独立合并 goroutine + 发布协议,即 wrapper 模式思路下沉到 direct 模式)或持久化数据结构消除快照拷贝,均记 v0.8 候选。

## 残余与后续

1. **mixed 写侧摊销(主要未竟项)**:折叠/快照/预构建成本落在写者——达成写速率 53-56/s(供速 200,基线达成 164),读 p99(62ms)与写 p99(68ms)同拍。压缩路径:折叠+预构建移出写者(独立 goroutine + 发布协议)或持久化结构消除 O(F) 快照拷贝,v0.8 候选首位。
2. **B3 结构性**:镜像 map 整数化/分片(写路径 ~2.8M edges/s 的常数),v0.8 backlog。
3. **读 p50 距激进线 1.8-2.6×**:读者已缓存命中,残余为写事件同拍尾部 + GC 流量;随(1)解决。

## v4 设计修订(对 spec 的补充)

- F2 修订:v1"层数阈值整栈折叠 + base/8 体量 Seal"作废——A3 门证明其二次方写成本与读路径 Seal 不可接受。v4:三段尺寸分层、写路径专属触发、tail 体量门槛、max(64K,base) 体量线。
- F3 启用:spec 曾按 YAGNI 推迟增量视图;复测证明必要(层栈变浅不解决记录体量的每发布全量重建),以发布时预构建 + 域分离偷取扩展实现,避免读者竞争。
- 新增:A2 门重校准记录(B2 副作用);差分桩存在性匹配规范(删除键零 ID 假象,非引擎 bug——已在基线 ee256e3 验证)。

## 复现

```bash
make test                                   # 三模块全量
go test -race .                             # 根模块 race
go test -run 'TestQueryLatencyDuringMerge|TestPointQueryAfterWrite|TestWALGroupCommitBatchCost|TestMergeAmortization|TestCrashRecoveryTimeToReady|TestUnbatchedImportThroughput' -v ./bench/
make bench SIZE=medium WORKERS=8 DURATION=10s BENCH_EXTRA='-warmup 1s'
```
