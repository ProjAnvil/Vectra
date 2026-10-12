// Package server 是 Vectra 的可选服务模块：hashicorp/raft 复制 +
// HTTP JSON API + 热替换发布。核心引擎模块保持零第三方依赖；
// 本模块的依赖树只影响选择服务形态的用户（README 有完整声明）。
//
// 一致性语义：写线性一致（raft 提交即返回）；读有界过期
// （本地 published 快照，新鲜度 ≤ SealInterval + 发布耗时）。
// 防脑裂约束：Join 非空的节点绝不 bootstrap；两个空 Join 的新目录
// 各自引导属于运维错误（与 etcd initial-cluster 同类）。
package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"

	"vectra"
)

// DefaultSealInterval 是大图发布的默认节流间隔；小于 smallGraphLimit
// 的图每 batch 立即发布。
const DefaultSealInterval = 2 * time.Second

var (
	ErrNotLeader   = errors.New("server: not leader")
	ErrTimeout     = errors.New("server: apply timeout")
	ErrDirMismatch = errors.New("server: data dir belongs to a different node")
)

// Config 组装一个服务节点。LogStore/StableStore/SnapshotStore/Transport
// 为 nil 时使用生产默认（BoltDB + FileSnapshotStore + TCP）；
// 注入 Inmem 系列可做进程内集群测试。RaftConfig 为 nil 时用默认参数。
type Config struct {
	NodeID        string
	HTTPAddr      string // 对外 HTTP 监听（:0 可用）
	RaftAddr      string // raft 对等地址
	DataDir       string // 持久化目录（raft.db / snapshots / meta.json）
	Join          string // seed 节点的 HTTP 地址；空 = 单节点引导
	SealInterval  time.Duration
	LogStore      raft.LogStore
	StableStore   raft.StableStore
	SnapshotStore raft.SnapshotStore
	Transport     raft.Transport
	RaftConfig    *raft.Config
}

type nodeMeta struct {
	NodeID   string `json:"nodeID"`
	HTTPAddr string `json:"httpAddr"`
	RaftAddr string `json:"raftAddr"`
}

// Node 是一个 Vectra 服务节点：raft 复制 + FSM 发布 + HTTP API。
type Node struct {
	cfg Config
	fsm *fsm
	r   *raft.Raft

	bolt  *raftboltdb.BoltStore // 生产模式持有；测试模式为 nil
	trans raft.Transport

	regMu    sync.Mutex
	registry map[string]string // raftAddr -> httpAddr（尽力而为的提示表）

	closeOnce sync.Once
	closed    chan struct{}

	leaderObs *raft.Observer
	leaderChg chan raft.Observation

	queryCount atomic.Int64 // api.go 使用
}

// New 打开（或创建）一个节点：恢复 raft 状态 → 组装 FSM →
// 按需引导/加入。Join 为空且无既有状态时单节点引导。
func New(cfg Config) (*Node, error) {
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("server: NodeID required")
	}
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("server: DataDir required")
	}
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, err
	}
	if cfg.SealInterval == 0 {
		cfg.SealInterval = DefaultSealInterval
	}

	meta, err := loadOrCreateMeta(cfg)
	if err != nil {
		return nil, err
	}

	n := &Node{
		cfg:      cfg,
		registry: map[string]string{cfg.RaftAddr: meta.HTTPAddr},
		closed:   make(chan struct{}),
	}

	// 持久化与传输
	if cfg.LogStore == nil || cfg.StableStore == nil {
		bolt, err := raftboltdb.NewBoltStore(filepath.Join(cfg.DataDir, "raft.db"))
		if err != nil {
			return nil, err
		}
		n.bolt = bolt
		cfg.LogStore, cfg.StableStore = bolt, bolt
	}
	if cfg.SnapshotStore == nil {
		cfg.SnapshotStore, err = raft.NewFileSnapshotStore(cfg.DataDir, 2, os.Stderr)
		if err != nil {
			n.Close()
			return nil, err
		}
	}
	if cfg.Transport == nil {
		addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
		if err != nil {
			n.Close()
			return nil, err
		}
		cfg.Transport, err = raft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
		if err != nil {
			n.Close()
			return nil, err
		}
	}
	n.trans = cfg.Transport

	// FSM（发布器随之启动）
	n.fsm = newFSM(cfg.SealInterval)

	// 引导决策：有既有状态 → 不动；Join 空 → 单节点引导；Join 非空 → 等 seed 收编
	hasState, err := raft.HasExistingState(cfg.LogStore, cfg.StableStore, cfg.SnapshotStore)
	if err != nil {
		n.Close()
		return nil, err
	}
	if !hasState && cfg.Join == "" {
		conf := n.raftConfig()
		servers := []raft.Server{{
			ID:       raft.ServerID(cfg.NodeID),
			Address:  raft.ServerAddress(cfg.RaftAddr),
			Suffrage: raft.Voter,
		}}
		if err := raft.BootstrapCluster(conf, cfg.LogStore, cfg.StableStore,
			cfg.SnapshotStore, cfg.Transport, raft.Configuration{Servers: servers}); err != nil {
			n.Close()
			return nil, err
		}
	}

	raftCfg := n.raftConfig()
	r, err := raft.NewRaft(raftCfg, n.fsm, cfg.LogStore, cfg.StableStore, cfg.SnapshotStore, cfg.Transport)
	if err != nil {
		n.Close()
		return nil, err
	}
	n.r = r

	// leader 变更观察（指标）
	n.leaderChg = make(chan raft.Observation, 16)
	n.leaderObs = raft.NewObserver(n.leaderChg, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	r.RegisterObserver(n.leaderObs)
	go n.watchLeader()

	// 加入既有集群（注册表随 join 响应扩散）
	if cfg.Join != "" {
		if err := n.Join(cfg.Join); err != nil {
			// 不致命：节点保持候选状态，运维可重试 join
			fmt.Fprintf(os.Stderr, "vectra/server: join %s 失败: %v\n", cfg.Join, err)
		}
	}
	return n, nil
}

func (n *Node) raftConfig() *raft.Config {
	if n.cfg.RaftConfig != nil {
		return n.cfg.RaftConfig
	}
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(n.cfg.NodeID)
	return c
}

func loadOrCreateMeta(cfg Config) (nodeMeta, error) {
	path := filepath.Join(cfg.DataDir, "meta.json")
	var m nodeMeta
	if data, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(data, &m); err != nil {
			return m, fmt.Errorf("server: 坏 meta.json: %w", err)
		}
		if m.NodeID != cfg.NodeID {
			return m, fmt.Errorf("%w: dir=%s node=%s cfg=%s", ErrDirMismatch, cfg.DataDir, m.NodeID, cfg.NodeID)
		}
	} else {
		m = nodeMeta{NodeID: cfg.NodeID}
	}
	// 地址允许漂移（重启换端口），ID 不允许
	m.HTTPAddr, m.RaftAddr = cfg.HTTPAddr, cfg.RaftAddr
	data, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return m, err
	}
	return m, nil
}

// Join 向 seed 节点登记自己（seed 侧执行 AddVoter 并回传注册表）。
func (n *Node) Join(seedHTTP string) error {
	body := map[string]string{
		"nodeID":   n.cfg.NodeID,
		"raftAddr": n.cfg.RaftAddr,
		"httpAddr": announceAddr(n.cfg.HTTPAddr),
	}
	data, _ := json.Marshal(body)
	resp, err := httpPost(seedHTTP+"/v1/join", data, 15*time.Second)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		return fmt.Errorf("join: %s: %s", resp.Status, raw)
	}
	var out struct {
		Registry map[string]string `json:"registry"`
	}
	_ = json.Unmarshal(raw, &out)
	n.mergeRegistry(out.Registry)
	return nil
}

// announceAddr 把 :0 之类的占位换成可广播形式（尽力而为）。
func announceAddr(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	if host == "" || host == "0.0.0.0" {
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

// Apply 把变更集经 raft 复制后提交到 FSM。非 leader 返回 ErrNotLeader。
func (n *Node) Apply(ctx context.Context, b Batch) (ApplyResult, error) {
	data, err := encodeBatch(b)
	if err != nil {
		return ApplyResult{}, err
	}
	if n.r.State() != raft.Leader {
		return ApplyResult{}, ErrNotLeader
	}
	timeout := 10 * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if t := time.Until(dl); t < timeout {
			timeout = t
		}
	}
	fut := n.r.Apply(data, timeout)
	if err := fut.Error(); err != nil {
		if errors.Is(err, raft.ErrNotLeader) {
			return ApplyResult{}, ErrNotLeader
		}
		if errors.Is(err, raft.ErrLeadershipLost) {
			return ApplyResult{}, ErrNotLeader
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, raft.ErrRaftShutdown) {
			return ApplyResult{}, fmt.Errorf("%w: %v", ErrTimeout, err)
		}
		return ApplyResult{}, err
	}
	switch v := fut.Response().(type) {
	case ApplyResult:
		return v, nil
	case error:
		return ApplyResult{}, v
	default:
		return ApplyResult{}, nil
	}
}

// IsLeader 报告当前节点是否为 leader。
func (n *Node) IsLeader() bool { return n.r != nil && n.r.State() == raft.Leader }

// LeaderHint 返回已知 leader 的 HTTP 地址（尽力而为，可能为空）。
func (n *Node) LeaderHint() string {
	if n.r == nil {
		return ""
	}
	addr := string(n.r.Leader())
	if addr == "" {
		return ""
	}
	n.regMu.Lock()
	defer n.regMu.Unlock()
	return n.registry[addr]
}

func (n *Node) mergeRegistry(m map[string]string) {
	if len(m) == 0 {
		return
	}
	n.regMu.Lock()
	defer n.regMu.Unlock()
	for k, v := range m {
		n.registry[k] = v
	}
}

// Published 返回本地最近密封发布的图（nil = 尚未发布）。
func (n *Node) Published() *vectra.Graph { return n.fsm.Published() }

// Ready 在首次发布完成后可读（重启恢复的读就绪门）。
func (n *Node) Ready() <-chan struct{} { return n.fsm.Ready() }

// Close 幂等停机：停发布器 → raft.Shutdown → 关持久化。
func (n *Node) Close() {
	n.closeOnce.Do(func() {
		close(n.closed)
		n.fsm.Close()
		if n.r != nil {
			f := n.r.Shutdown()
			if f.Error() != nil {
				// 已在停机路径上，忽略二次关闭错误
				_ = f
			}
		}
		if n.bolt != nil {
			_ = n.bolt.Close()
		}
		if closer, ok := n.trans.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	})
}

func (n *Node) watchLeader() {
	for {
		select {
		case <-n.closed:
			return
		case <-n.leaderChg:
			if n.IsLeader() {
				n.fsm.metrics.LeaderChanges.Add(1)
			}
		}
	}
}
