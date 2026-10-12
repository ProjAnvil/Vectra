// Package store 实现 Vectra 单文件二进制快照。小图全内存假设下磁盘
// 只做崩溃恢复：整图重写、加载后重建 CSR/SCC，无需增量 WAL。
// 格式 v2（小端）："VCTR" | u16 版本(2) | u32 节点数 | u32 边数 |
// 节点{ key 字符串, label 字符串, props JSON }* | 边{ srcKey, dstKey, label, props }*
// | u32 CRC32(IEEE，覆盖此前全部字节)。
// 字符串 = u32 长度 + 字节；props = u32 JSON 长度 + JSON（0 = nil）。
// v1（无 CRC 尾）仍可读，按头部版本号分派。
package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"io"
)

const (
	magic         = "VCTR"
	versionV2     = 2
	versionLegacy = 1 // 无 CRC 尾，仅读兼容
	headerSize    = 6 // magic(4) + version(2)，计数字段从这之后开始
)

// Node / Edge 用字符串 key 而非内部 NodeID：快照与 ID 分配顺序解耦，
// 加载时重建。props 序列化为 JSON（数字经往返变 float64，v1 已知限制）。
type Node struct {
	Key   string
	Label string
	Props map[string]any
}

type Edge struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

// crcWriter 聚合写入字节流的 CRC（尾插，覆盖此前全部字节）。
type crcWriter struct {
	bw  *bufio.Writer
	crc uint32
}

func (c *crcWriter) Write(p []byte) (int, error) {
	n, err := c.bw.Write(p)
	if n > 0 {
		c.crc = crc32.Update(c.crc, crc32.IEEETable, p[:n])
	}
	return n, err
}

func (c *crcWriter) finish() error {
	var tail [4]byte
	binary.LittleEndian.PutUint32(tail[:], c.crc)
	if _, err := c.bw.Write(tail[:]); err != nil {
		return err
	}
	return c.bw.Flush()
}

func putU32(w io.Writer, v uint32) error {
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], v)
	_, err := w.Write(b[:])
	return err
}

func writeString(w io.Writer, s string) error {
	if err := putU32(w, uint32(len(s))); err != nil {
		return err
	}
	_, err := io.WriteString(w, s)
	return err
}

func writeProps(w io.Writer, props map[string]any) error {
	if props == nil {
		return putU32(w, 0)
	}
	b, err := json.Marshal(props)
	if err != nil {
		return err
	}
	if err := putU32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// Encode 把整图以 v2 格式写入 w（尾部 CRC32）。
func Encode(w io.Writer, nodes []Node, edges []Edge) error {
	cw := &crcWriter{bw: bufio.NewWriter(w)}
	var head [4]byte
	copy(head[:], magic)
	if _, err := cw.Write(head[:]); err != nil {
		return err
	}
	if err := binary.Write(cw, binary.LittleEndian, uint16(versionV2)); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(nodes))); err != nil {
		return err
	}
	if err := putU32(cw, uint32(len(edges))); err != nil {
		return err
	}
	for _, n := range nodes {
		if err := writeString(cw, n.Key); err != nil {
			return err
		}
		if err := writeString(cw, n.Label); err != nil {
			return err
		}
		if err := writeProps(cw, n.Props); err != nil {
			return err
		}
	}
	for _, e := range edges {
		if err := writeString(cw, e.SrcKey); err != nil {
			return err
		}
		if err := writeString(cw, e.DstKey); err != nil {
			return err
		}
		if err := writeString(cw, e.Label); err != nil {
			return err
		}
		if err := writeProps(cw, e.Props); err != nil {
			return err
		}
	}
	return cw.finish()
}

// reader 直接消费字节切片：一切长度字段都对着剩余字节做边界检查，
// 坏输入不可能触发超长分配。
type reader struct {
	b []byte
	i int
}

var errTruncated = errors.New("store: truncated input")

func (rd *reader) take(n int) ([]byte, error) {
	if n < 0 || rd.i+n > len(rd.b) {
		return nil, errTruncated
	}
	p := rd.b[rd.i : rd.i+n]
	rd.i += n
	return p, nil
}

func (rd *reader) u16() (uint16, error) {
	p, err := rd.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(p), nil
}

func (rd *reader) u32() (uint32, error) {
	p, err := rd.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(p), nil
}

func (rd *reader) str() (string, error) {
	n, err := rd.u32()
	if err != nil {
		return "", err
	}
	p, err := rd.take(int(n))
	if err != nil {
		return "", err
	}
	return string(p), nil
}

func (rd *reader) props() (map[string]any, error) {
	n, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	p, err := rd.take(int(n))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Decode 从快照恢复节点与边数据。先整体读入，v2 校验 CRC，v1 兼容。
func Decode(r io.Reader) ([]Node, []Edge, error) {
	raw, err := io.ReadAll(r)
	if err != nil {
		return nil, nil, err
	}
	if len(raw) < headerSize+4 {
		return nil, nil, errTruncated
	}
	if string(raw[:4]) != magic {
		return nil, nil, errors.New("store: bad magic")
	}
	ver := binary.LittleEndian.Uint16(raw[4:6])
	body := raw
	switch ver {
	case versionV2:
		body = raw[:len(raw)-4]
		want := binary.LittleEndian.Uint32(raw[len(raw)-4:])
		if got := crc32.ChecksumIEEE(body); got != want {
			return nil, nil, errors.New("store: CRC mismatch")
		}
	case versionLegacy:
		// v1 无 CRC 尾
	default:
		return nil, nil, errors.New("store: unsupported version")
	}

	rd := &reader{b: body[headerSize:]} // 头部已预解析
	nn, err := rd.u32()
	if err != nil {
		return nil, nil, err
	}
	ne, err := rd.u32()
	if err != nil {
		return nil, nil, err
	}
	nodes := make([]Node, nn)
	for i := range nodes {
		if nodes[i].Key, err = rd.str(); err != nil {
			return nil, nil, err
		}
		if nodes[i].Label, err = rd.str(); err != nil {
			return nil, nil, err
		}
		if nodes[i].Props, err = rd.props(); err != nil {
			return nil, nil, err
		}
	}
	edges := make([]Edge, ne)
	for i := range edges {
		if edges[i].SrcKey, err = rd.str(); err != nil {
			return nil, nil, err
		}
		if edges[i].DstKey, err = rd.str(); err != nil {
			return nil, nil, err
		}
		if edges[i].Label, err = rd.str(); err != nil {
			return nil, nil, err
		}
		if edges[i].Props, err = rd.props(); err != nil {
			return nil, nil, err
		}
	}
	if rd.i != len(rd.b) {
		return nil, nil, errors.New("store: trailing garbage")
	}
	return nodes, edges, nil
}
