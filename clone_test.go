package vectra_test

import (
	"bytes"
	"errors"
	"os"
	"slices"
	"testing"

	"github.com/projanvil/vectra"
)

func TestClonePreSealIndependentEvolution(t *testing.T) {
	g := mustBuild(t) // 4 节点 4 边，未 Seal
	c := g.Clone()
	if _, err := c.AddNode("extra", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddEdge("db", "auth", "CALLS", nil); err != nil {
		t.Fatal(err)
	}
	if g.NodeCount() != 4 || g.EdgeCount() != 4 {
		t.Errorf("原图被克隆的写入污染: %d/%d, want 4/4", g.NodeCount(), g.EdgeCount())
	}
	if c.NodeCount() != 5 || c.EdgeCount() != 5 {
		t.Errorf("克隆图写入失败: %d/%d, want 5/5", c.NodeCount(), c.EdgeCount())
	}
	if c.Sealed() {
		t.Error("克隆应为未密封态")
	}
}

func TestCloneSealedGraphIsMutableAgain(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	r1, _ := g.Reachable("gateway")
	c := g.Clone()
	if c.Sealed() {
		t.Fatal("密封图的克隆应回到未密封态（可继续写入）")
	}
	if _, err := c.AddNode("kafka", "Bus", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.AddEdge("db", "kafka", "EVENT", nil); err != nil {
		t.Fatal(err)
	}
	if err := c.Seal(); err != nil {
		t.Fatal(err)
	}
	// 原图不受影响
	r2, _ := g.Reachable("gateway")
	if !slices.Equal(r1, r2) {
		t.Error("克隆的写入影响了原图")
	}
	// 克隆查询语义完整
	r3, _ := c.Reachable("gateway")
	if !slices.Contains(r3, "kafka") {
		t.Errorf("克隆图可达集缺少新增节点: %v", r3)
	}
	st := c.Stats()
	if st.Nodes != 5 || st.Edges != 5 || st.SCCs != 5 {
		t.Errorf("克隆 stats = %+v", st)
	}
}

func TestWithNodeFilter(t *testing.T) {
	g := buildDAG(t) // gateway->auth/ledger->db->kafka
	// 只遍历 key!="db" 的节点：db(kafka 的唯一入边) 被过滤 → kafka 不可达
	res, err := g.BFS([]vectra.NodeID{ids(t, g, "gateway")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return n.Key != "db" }))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reached(ids(t, g, "db")[0]) {
		t.Error("db(Storage) 应被谓词过滤")
	}
	if !res.Reached(ids(t, g, "auth")[0]) {
		t.Error("auth(Svc) 应可达")
	}
	// 起点恒入选（即使不满足谓词）
	res2, err := g.BFS([]vectra.NodeID{ids(t, g, "db")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return false }))
	if err != nil {
		t.Fatal(err)
	}
	if res2.Count() != 1 {
		t.Errorf("全拒绝谓词下起点仍应入选: %d", res2.Count())
	}
	// 与并行路径组合
	res3, err := g.BFS([]vectra.NodeID{ids(t, g, "gateway")[0]},
		vectra.WithNodeFilter(func(n vectra.Node) bool { return n.Key != "db" }),
		vectra.WithParallelThreshold(0))
	if err != nil {
		t.Fatal(err)
	}
	if res3.Count() != res.Count() {
		t.Errorf("谓词在并行路径结果不一致: %d vs %d", res3.Count(), res.Count())
	}
}

func TestQueryWhere(t *testing.T) {
	g := buildDAG(t)
	res, err := g.Query().From("gateway").Out().Where(func(n vectra.Node) bool {
		return n.Key != "db"
	}).Run()
	if err != nil {
		t.Fatal(err)
	}
	if res.Reached(ids(t, g, "db")[0]) {
		t.Error("Where 应过滤 Storage 节点")
	}
	if res.Count() != 3 { // gateway, auth, ledger
		t.Errorf("Count = %d, want 3", res.Count())
	}
}

func TestIndexPropertyLookup(t *testing.T) {
	g := vectra.New()
	_, _ = g.AddNode("a", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("b", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("c", "Svc", map[string]any{"env": "dev"})
	_, _ = g.AddNode("d", "Svc", map[string]any{"env": "prod"})
	_, _ = g.AddNode("e", "Svc", map[string]any{"tier": float64(3)}) // 非字符串不入索引
	g.IndexProperty("env")
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	// 未注册的键
	if _, ok := g.LookupByProperty("tier", "3"); ok {
		t.Error("未注册键应返回 ok=false")
	}
	// Seal 前注册的键命中（升序）
	nodes, ok := g.LookupByProperty("env", "prod")
	if !ok || len(nodes) != 3 {
		t.Fatalf("env=prod 命中 %d 个, ok=%v, want 3/true", len(nodes), ok)
	}
	want := []string{"a", "b", "d"}
	for i, n := range nodes {
		if n.Key != want[i] {
			t.Errorf("命中[%d] = %s, want %s", i, n.Key, want[i])
		}
		if n.Props["env"] != "prod" {
			t.Errorf("命中[%d] props 错误", i)
		}
	}
	// 命中空集（注册过但无匹配）
	if nodes, ok := g.LookupByProperty("env", "staging"); !ok || len(nodes) != 0 {
		t.Errorf("无匹配值应返回空集+ok=true, got %d/%v", len(nodes), ok)
	}
	// Seal 后注册是空操作
	g.IndexProperty("team")
	if _, ok := g.LookupByProperty("team", "x"); ok {
		t.Error("Seal 后注册不应生效")
	}
	// 未 Seal 查询
	g2 := vectra.New()
	_, _ = g2.AddNode("x", "S", map[string]any{"env": "prod"})
	if _, ok := g2.LookupByProperty("env", "prod"); ok {
		t.Error("未 Seal 的 LookupByProperty 应返回 ok=false")
	}
}

func TestExportImportSnapshotUnsealed(t *testing.T) {
	g := mustBuild(t) // 未 Seal
	data, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	g2, err := vectra.ImportSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if g2.Sealed() {
		t.Error("ImportSnapshot 应返回未密封图（可继续写）")
	}
	if _, err := g2.AddNode("extra", "Svc", nil); err != nil {
		t.Fatal(err)
	}
	if g2.NodeCount() != 5 {
		t.Errorf("导入后写入失败: %d, want 5", g2.NodeCount())
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	st1, st2 := g.Stats(), g2.Stats()
	if st1.Nodes != 4 || st2.Nodes != 5 || st2.Edges != 4 {
		t.Errorf("stats: %+v / %+v", st1, st2)
	}
}

func TestExportSealedMatchesSave(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	exp, err := g.ExportSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	var saved bytes.Buffer
	if err := g.Save(&saved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exp, saved.Bytes()) {
		t.Error("密封态 ExportSnapshot 应与 Save 字节一致")
	}
	g2, err := vectra.ImportSnapshot(exp)
	if err != nil {
		t.Fatal(err)
	}
	if err := g2.Seal(); err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Errorf("导入密封 stats 不一致: %+v vs %+v", g2.Stats(), g.Stats())
	}
}

// ---- 快照格式 v2：CRC 完整性 ----

func TestSnapshotCRCv2Roundtrip(t *testing.T) {
	g := buildCyclic(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// 版本号 = 2
	if data[4] != 2 || data[5] != 0 {
		t.Errorf("版本字节 = %d/%d, want 2/0", data[4], data[5])
	}
	g2, err := vectra.Load(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	if g2.Stats() != g.Stats() {
		t.Error("CRC 快照往返 stats 不一致")
	}
}

func TestSnapshotCRCRejectsCorruption(t *testing.T) {
	g := buildCyclic(t)
	var buf bytes.Buffer
	if err := g.Save(&buf); err != nil {
		t.Fatal(err)
	}
	data := buf.Bytes()
	// 翻转 body 中间一个字节
	corrupt := slices.Clone(data)
	corrupt[len(corrupt)/2] ^= 0xFF
	if _, err := vectra.Load(bytes.NewReader(corrupt)); err == nil {
		t.Error("body 损坏应被 CRC 拒绝")
	}
	// 翻转 CRC 本身
	corrupt2 := slices.Clone(data)
	corrupt2[len(corrupt2)-1] ^= 0x01
	if _, err := vectra.Load(bytes.NewReader(corrupt2)); err == nil {
		t.Error("CRC 损坏应被拒绝")
	}
	// 截断（CRC 之前）
	if _, err := vectra.Load(bytes.NewReader(data[:len(data)-2])); err == nil {
		t.Error("截断文件应被拒绝")
	}
}

func TestSnapshotV1StillReadable(t *testing.T) {
	// 手工构造 v1 快照（无 CRC 尾）：用 store 包格式规范拼一个最小图
	v1 := bytes.NewBuffer(nil)
	v1.WriteString("VCTR")
	v1.Write([]byte{1, 0})       // version 1
	v1.Write([]byte{1, 0, 0, 0}) // 1 node
	v1.Write([]byte{0, 0, 0, 0}) // 0 edges
	v1.Write([]byte{1, 0, 0, 0}) // key len 1
	v1.WriteByte('a')            // key
	v1.Write([]byte{1, 0, 0, 0}) // label len 1
	v1.WriteByte('S')            // label
	v1.Write([]byte{0, 0, 0, 0}) // props len 0
	g, err := vectra.Load(bytes.NewReader(v1.Bytes()))
	if err != nil {
		t.Fatalf("v1 快照应可读: %v", err)
	}
	n, ok := g.NodeByKey("a")
	if !ok || n.Label != "S" {
		t.Errorf("v1 内容错误: %+v", n)
	}
	if err := g.Seal(); err != nil { // Load 已自动 Seal，幂等
		t.Error(err)
	}
}

func TestSaveFileAtomicNoTmpLeftover(t *testing.T) {
	g := mustBuild(t)
	if err := g.Seal(); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/atomic.snap"
	if err := g.SaveFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := vectra.LoadFile(path); err != nil {
		t.Fatal(err)
	}
	if _, err := vectra.LoadFile(t.TempDir() + "/missing.snap"); !errors.Is(err, os.ErrNotExist) {
		if err == nil {
			t.Error("缺失文件应报错")
		}
	}
}
