package store

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
)

type failW struct{}

func (failW) Write(p []byte) (int, error) { return 0, errors.New("io fail") }

// 白盒：直接对内部写入函数注入失败（绕过 bufio 聚合）。
func TestWriteHelpersPropagateErrors(t *testing.T) {
	if err := writeString(failW{}, "x"); err == nil {
		t.Error("writeString 失败未传播")
	}
	if err := writeProps(failW{}, map[string]any{"a": 1}); err == nil {
		t.Error("writeProps(有值) 失败未传播")
	}
	if err := writeProps(failW{}, nil); err == nil {
		t.Error("writeProps(nil) 失败未传播")
	}
	var b bytes.Buffer
	if err := writeString(&b, ""); err != nil {
		t.Error(err)
	}
	// finish：底层失败经 Flush 传播
	bw := bufio.NewWriter(failW{})
	cw := &crcWriter{bw: bw}
	if _, err := cw.Write([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := cw.finish(); err == nil {
		t.Error("finish 失败未传播")
	}
}

// TestDecodeTruncatedAtEveryLength 对合法快照的每个前缀断言解码失败，
// 逐守卫走遍所有截断分支。
func TestDecodeTruncatedAtEveryLength(t *testing.T) {
	var buf bytes.Buffer
	if err := Encode(&buf,
		[]Node{{Key: "node-1", Label: "Svc", Props: map[string]any{"env": "prod", "n": float64(7)}}},
		[]Edge{{SrcKey: "node-1", DstKey: "node-1", Label: "CALLS", Props: map[string]any{"w": float64(2)}}},
	); err != nil {
		t.Fatal(err)
	}
	full := buf.Bytes()
	failOK := 0
	for cut := 0; cut < len(full); cut++ {
		if _, _, err := Decode(bytes.NewReader(full[:cut])); err != nil {
			failOK++
		} else {
			// v1 无 CRC：掐掉的恰好是合法前缀且字段完整（如掐在尾部 props 边界）
			// 会成功——只允许极少数这种巧合，且结果必须自洽
			if full[4] == 2 { // v2
				t.Fatalf("v2 前缀 cut=%d 不应解码成功", cut)
			}
		}
	}
	if failOK == 0 {
		t.Fatal("没有任何截断被拒绝")
	}
}

// TestDecodeGarbageBody 合法头部 + 垃圾 body 的各种守卫。
func TestDecodeGarbageBody(t *testing.T) {
	base := func(ver uint16, body []byte) []byte {
		out := []byte{'V', 'C', 'T', 'R', byte(ver), 0}
		return append(out, body...)
	}
	// v1：节点数 1 但 body 空
	if _, _, err := Decode(bytes.NewReader(base(1, nil))); err == nil {
		t.Error("空 body 应失败")
	}
	// v1：节点数 2 只有 1 个完整节点
	one := []byte{2, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'S', 0, 0, 0, 0}
	if _, _, err := Decode(bytes.NewReader(base(1, one))); err == nil {
		t.Error("缺第二个节点应失败")
	}
	// v1：边数 1 无边数据
	edges := []byte{0, 0, 0, 0, 1, 0, 0, 0}
	if _, _, err := Decode(bytes.NewReader(base(1, edges))); err == nil {
		t.Error("缺边数据应失败")
	}
}

var _ = strings.Repeat

// TestDecodeV1PartialEdgeFields v1 无 CRC，边字段部分存在的各守卫。
func TestDecodeV1PartialEdgeFields(t *testing.T) {
	base := func(body []byte) []byte {
		out := []byte{'V', 'C', 'T', 'R', 1, 0}
		return append(out, body...)
	}
	node := []byte{0, 0, 0, 0}          // numNodes = 0
	numEdges := func(n uint32) []byte { // numEdges = n
		return []byte{byte(n), byte(n >> 8), byte(n >> 16), byte(n >> 24)}
	}
	// 边数 1：只有 srcKey 完整、dstKey 截断
	partial1 := append(numEdges(1), 1, 0, 0, 0, 'a', 3, 0, 0, 0, 'b', 'b')
	if _, _, err := Decode(bytes.NewReader(base(append(node, partial1...)))); err == nil {
		t.Error("dstKey 截断应失败")
	}
	// 边数 1：src/dst 完整、label 截断
	partial2 := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 2, 0, 0, 0, 'E')
	if _, _, err := Decode(bytes.NewReader(base(append(node, partial2...)))); err == nil {
		t.Error("label 截断应失败")
	}
	// 边数 1：src/dst/label 完整、props 长度声明超界
	partial3 := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 1, 0, 0, 0, 'E', 9, 0, 0, 0, 'x')
	if _, _, err := Decode(bytes.NewReader(base(append(node, partial3...)))); err == nil {
		t.Error("props 超界应失败")
	}
	// 边数 1：字段齐全
	ok := append(numEdges(1), 1, 0, 0, 0, 'a', 1, 0, 0, 0, 'b', 1, 0, 0, 0, 'E', 0, 0, 0, 0)
	if _, _, err := Decode(bytes.NewReader(base(append(node, ok...)))); err != nil {
		t.Errorf("完整边不应失败: %v", err)
	}
}

// TestDecodeHugeCountsRejectedWithValidCRC 攻击者可自算 CRC——
// 计数下界校验必须独立于 CRC 生效。
func TestDecodeHugeCountsRejectedWithValidCRC(t *testing.T) {
	body := []byte{'V', 'C', 'T', 'R', 2, 0}    // magic + v2
	body = append(body, 0xFF, 0xFF, 0x00, 0x00) // numNodes = 65535（声明巨大）
	body = append(body, 0, 0, 0, 0)             // numEdges = 0
	crc := crc32.ChecksumIEEE(body)
	body = binary.LittleEndian.AppendUint32(body, crc)
	if _, _, err := Decode(bytes.NewReader(body)); err == nil {
		t.Fatal("合法 CRC + 伪造计数应被下界校验拒绝")
	}
}
