package server

import (
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"

	"vectra"
)

// defaultSmallGraphLimit：live 图规模低于此值时发布不做节流（每 batch
// 立即 Seal 发布）；之上按 SealInterval 节流。依赖图场景绝大多数
// 远低于该值。
const defaultSmallGraphLimit = 100_000

// ApplyResult 是 FSM.Apply 的返回值（经 ApplyFuture.Response 传回）。
type ApplyResult struct {
	NodesApplied int
	EdgesApplied int
	EdgesSkipped int // 引用不存在节点的边被确定性跳过（计数进指标）
}

// Metrics 是 server 的原子计数器（expvar 在 api.go 挂载）。
type Metrics struct {
	AppliedBatches atomic.Int64
	AppliedNodes   atomic.Int64
	AppliedEdges   atomic.Int64
	SkippedEdges   atomic.Int64
	Seals          atomic.Int64
	Restores       atomic.Int64
	LeaderChanges  atomic.Int64
}

// fsm 实现 raft.FSM：live 可写图 + 热替换发布器。
//
//   - Apply：确定性回放 batch 到 live（RLock 内短临界区）；
//   - 发布器：克隆 live（RLock 下 O(V+E) 拷贝）→ 锁外 Seal →
//     atomic.Pointer 交换 → 查询永不中断、免锁并发读；
//   - Snapshot/Persist：克隆 live 直接序列化（无需 Seal）；
//   - Restore：字节流重建 live 并立即首发布。
type fsm struct {
	mu        sync.RWMutex
	live      *vectra.Graph
	dirty     bool
	version   int64 // 单调递增，发布器用它判断克隆后是否有新写入
	published atomic.Pointer[vectra.Graph]

	sealInterval    time.Duration
	smallGraphLimit int

	lastSealNanos atomic.Int64

	ready     chan struct{}
	readyOnce sync.Once
	stop      chan struct{}
	stopOnce  sync.Once
	wake      chan struct{}

	metrics *Metrics
}

func newFSM(sealInterval time.Duration) *fsm {
	f := &fsm{
		live:            vectra.New(),
		sealInterval:    sealInterval,
		smallGraphLimit: defaultSmallGraphLimit,
		ready:           make(chan struct{}),
		stop:            make(chan struct{}),
		wake:            make(chan struct{}, 1),
		metrics:         &Metrics{},
	}
	// 节流间隔从进程启动起算，避免零值纪元（Since=59 年）触发即时发布
	f.lastSealNanos.Store(time.Now().UnixNano())
	go f.publishLoop()
	return f
}

// Apply 是 raft 提交后的确定性回放入口。返回 ApplyResult 或 error。
func (f *fsm) Apply(l *raft.Log) interface{} {
	b, err := decodeBatch(l.Data)
	if err != nil {
		return err
	}
	res := ApplyResult{}
	f.mu.Lock()
	for _, n := range b.Nodes {
		if _, err := f.live.AddNode(n.Key, n.Label, n.Props); err == nil {
			res.NodesApplied++
		}
	}
	for _, e := range b.Edges {
		if _, err := f.live.AddEdge(e.SrcKey, e.DstKey, e.Label, e.Props); err == nil {
			res.EdgesApplied++
		} else if errors.Is(err, vectra.ErrNodeNotFound) {
			res.EdgesSkipped++ // 权威校验：回放端跳过无效项，不分叉
		}
	}
	f.dirty = true
	f.version++
	small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
	f.mu.Unlock()

	f.metrics.AppliedBatches.Add(1)
	f.metrics.AppliedNodes.Add(int64(res.NodesApplied))
	f.metrics.AppliedEdges.Add(int64(res.EdgesApplied))
	f.metrics.SkippedEdges.Add(int64(res.EdgesSkipped))
	if small {
		f.wakePublisher()
	}
	return res
}

// Published 返回最近一次密封发布的图（不可变，免锁并发读）。
// 首次发布前返回 nil。
func (f *fsm) Published() *vectra.Graph {
	return f.published.Load()
}

// Ready 在首次发布完成后关闭（重启恢复的读就绪门）。
func (f *fsm) Ready() <-chan struct{} { return f.ready }

func (f *fsm) wakePublisher() {
	select {
	case f.wake <- struct{}{}:
	default:
	}
}

// publishInterval 决定本次发布等待：小图立即（wake 已触发则不等），
// 大图按 SealInterval 节流。
func (f *fsm) publishLoop() {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-f.stop:
			return
		case <-f.wake:
			f.publishOnce()
		case <-ticker.C:
			// 大图按 SealInterval 节流；小图（< smallGraphLimit）
			// 每 tick 检查即可（Apply 已即时 wake）。
			f.mu.RLock()
			dirty := f.dirty
			small := f.live.NodeCount()+f.live.EdgeCount() < f.smallGraphLimit
			f.mu.RUnlock()
			if dirty && (small || time.Since(time.Unix(0, f.lastSealNanos.Load())) >= f.sealInterval) {
				f.publishOnce()
			}
		}
	}
}

// sealNow 立即发布一次（测试与大图周期到期共用）。
func (f *fsm) sealNow() { f.publishOnce() }

func (f *fsm) publishOnce() {
	f.mu.RLock()
	if f.live == nil {
		f.mu.RUnlock()
		return
	}
	clone := f.live.Clone()
	version := f.version
	f.mu.RUnlock()

	if err := clone.Seal(); err != nil {
		return
	}
	f.published.Store(clone)
	f.metrics.Seals.Add(1)
	f.lastSealNanos.Store(time.Now().UnixNano())

	f.mu.Lock()
	if f.version == version { // 克隆之后没有新写入才清 dirty
		f.dirty = false
	}
	f.mu.Unlock()

	f.readyOnce.Do(func() { close(f.ready) })
}

// Snapshot 实现 raft.FSM：返回 live 的克隆视图（快照在锁外序列化）。
func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return &graphSnapshot{clone: f.live.Clone()}, nil
}

type graphSnapshot struct {
	clone *vectra.Graph
}

// Persist 把克隆的 live 图序列化写入 sink（未密封态可直接导出，
// 无需先 Seal——省一次大图密封）。
func (s *graphSnapshot) Persist(sink raft.SnapshotSink) error {
	data, err := s.clone.ExportSnapshot()
	if err != nil {
		return err
	}
	if _, err := sink.Write(data); err != nil {
		return err
	}
	return sink.Close()
}

func (s *graphSnapshot) Release() {}

// Restore 从快照字节重建 live 并立即首发布（就绪门由此打开）。
func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return err
	}
	g, err := vectra.ImportSnapshot(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.live = g
	f.dirty = true
	f.version++
	f.mu.Unlock()
	f.metrics.Restores.Add(1)
	f.sealNow()
	return nil
}

// Close 停止发布器（幂等）。
func (f *fsm) Close() {
	f.stopOnce.Do(func() { close(f.stop) })
}
