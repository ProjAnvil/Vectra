package store_test

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/projanvil/vectra/internal/store"
)

func TestEncodeDecodeRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{
		{Key: "gw", Label: "Service", Props: map[string]any{"team": "core", "qps": float64(1200)}},
		{Key: "db", Label: "Storage"},
	}
	edges := []store.Edge{
		{SrcKey: "gw", DstKey: "db", Label: "QUERY", Props: map[string]any{"mode": "read"}},
		{SrcKey: "db", DstKey: "gw", Label: "EVENT"},
	}
	if err := store.Encode(&buf, nodes, edges); err != nil {
		t.Fatal(err)
	}
	gotNodes, gotEdges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(gotNodes) != 2 || len(gotEdges) != 2 {
		t.Fatalf("counts = %d/%d, want 2/2", len(gotNodes), len(gotEdges))
	}
	if gotNodes[0].Key != "gw" || gotNodes[0].Label != "Service" || gotNodes[0].Props["team"] != "core" || gotNodes[0].Props["qps"] != float64(1200) {
		t.Errorf("node0 = %+v", gotNodes[0])
	}
	if gotNodes[1].Props != nil {
		t.Errorf("node1 props = %v, want nil", gotNodes[1].Props)
	}
	if gotEdges[0].SrcKey != "gw" || gotEdges[0].DstKey != "db" || gotEdges[0].Label != "QUERY" || gotEdges[0].Props["mode"] != "read" {
		t.Errorf("edge0 = %+v", gotEdges[0])
	}
	if gotEdges[1].Props != nil {
		t.Errorf("edge1 props = %v, want nil", gotEdges[1].Props)
	}
}

func TestDecodeRejectsBadMagic(t *testing.T) {
	if _, _, err := store.Decode(bytes.NewReader([]byte("NOPE"))); err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestDecodeRejectsTruncated(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	if _, _, err := store.Decode(bytes.NewReader(buf.Bytes()[:5])); err == nil {
		t.Fatal("expected error for truncated input")
	}
}

func TestUnicodeKeysRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	nodes := []store.Node{{Key: "服务-总账", Label: "Svc"}}
	if err := store.Encode(&buf, nodes, nil); err != nil {
		t.Fatal(err)
	}
	got, _, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Key != "服务-总账" {
		t.Errorf("key = %q", got[0].Key)
	}
}

func TestEmptyGraphRoundtrip(t *testing.T) {
	var buf bytes.Buffer
	if err := store.Encode(&buf, nil, nil); err != nil {
		t.Fatal(err)
	}
	nodes, edges, err := store.Decode(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 0 || len(edges) != 0 {
		t.Errorf("counts = %d/%d, want 0/0", len(nodes), len(edges))
	}
}

func TestDecodeRejectsUnsupportedVersion(t *testing.T) {
	data := []byte("VCTR\x09\x00" + "\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("v9 应被拒绝")
	}
}

func TestDecodeRejectsTrailingGarbage(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	data := append([]byte{}, buf.Bytes()...)
	data = append(data, 0xFF, 0xFF)
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("v1 尾部垃圾应被拒绝")
	}
}

func TestDecodeRejectsHugeLengthField(t *testing.T) {
	// v1 文件，key 长度字段伪造为 4GB——必须被边界检查快速拒绝
	data := []byte("VCTR\x01\x00")
	data = append(data, 1, 0, 0, 0)             // 1 node
	data = append(data, 0, 0, 0, 0)             // 0 edges
	data = append(data, 0xFF, 0xFF, 0xFF, 0xFF) // key len = 4294967295
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("超长长度字段应被拒绝")
	}
}

func TestDecodeRejectsV2WithoutCRC(t *testing.T) {
	var buf bytes.Buffer
	_ = store.Encode(&buf, []store.Node{{Key: "a", Label: "L"}}, nil)
	data := buf.Bytes()[:len(buf.Bytes())-3] // 掐掉 CRC 的一部分
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("不完整 CRC 应被拒绝")
	}
}

func TestDecodeRejectsBadPropsJSON(t *testing.T) {
	// 手工 v1：props 长度 5 但内容非法 JSON
	data := []byte("VCTR\x01\x00")
	data = append(data, 1, 0, 0, 0) // 1 node
	data = append(data, 0, 0, 0, 0) // 0 edges
	data = append(data, 1, 0, 0, 0)
	data = append(data, 'a') // key "a"
	data = append(data, 1, 0, 0, 0)
	data = append(data, 'L')                     // label "L"
	data = append(data, 5, 0, 0, 0)              // props len 5
	data = append(data, '{', 'b', 'a', 'd', '!') // 非法 JSON
	if _, _, err := store.Decode(bytes.NewReader(data)); err == nil {
		t.Fatal("坏 props JSON 应被拒绝")
	}
}

type failWriter struct{}

func (failWriter) Write(p []byte) (int, error) { return 0, errors.New("disk full") }

func TestEncodePropagatesWriteErrors(t *testing.T) {
	if err := store.Encode(failWriter{}, []store.Node{{Key: "a", Label: "L"}}, nil); err == nil {
		t.Fatal("写失败应传播错误")
	}
}

// failAfterWriter 在第 n 次写入后开始失败——遍历 n 覆盖 Encode 的全部错误分支。
type failAfterWriter struct{ n int }

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.n--
	if w.n < 0 {
		return 0, errors.New("disk full")
	}
	return len(p), nil
}

func TestEncodeFlushFailurePropagates(t *testing.T) {
	// bufio 聚合中间写入，失败统一在 Flush 点冒出——一次注入即可覆盖
	w := &failAfterWriter{n: 0}
	big := strings.Repeat("k", 64<<10)
	err := store.Encode(w, []store.Node{{Key: big, Label: "L"}}, nil)
	if err == nil {
		t.Fatal("Flush 失败未传播")
	}
}

func TestEncodeRejectsUnmarshalableProps(t *testing.T) {
	// func 值无法 JSON 序列化 → writeProps 的 Marshal 错误分支
	var buf bytes.Buffer
	err := store.Encode(&buf, []store.Node{{Key: "a", Label: "L", Props: map[string]any{"f": func() {}}}}, nil)
	if err == nil {
		t.Fatal("不可序列化 props 应报错")
	}
}
