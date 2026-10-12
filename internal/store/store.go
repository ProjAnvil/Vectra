// Package store 实现 Vectra 单文件二进制快照。小图全内存假设下磁盘
// 只做崩溃恢复：整图重写、加载后重建 CSR/SCC，无需增量 WAL。
// 格式（小端）："VCTR" | u16 版本 | u32 节点数 | u32 边数 |
// 节点{ key 字符串, label 字符串, props JSON }* | 边{ srcKey, dstKey, label, props }*
// 字符串 = u32 长度 + 字节；props = u32 JSON 长度 + JSON（0 = nil）。
package store

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

const (
	magic   = "VCTR"
	version = 1
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

func writeString(w *bufio.Writer, s string) error {
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], uint32(len(s)))
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	_, err := w.WriteString(s)
	return err
}

func writeProps(w *bufio.Writer, props map[string]any) error {
	var lenBuf [4]byte
	n := uint32(0)
	var payload []byte
	if props != nil {
		b, err := json.Marshal(props)
		if err != nil {
			return err
		}
		payload = b
		n = uint32(len(b))
	}
	binary.LittleEndian.PutUint32(lenBuf[:], n)
	if _, err := w.Write(lenBuf[:]); err != nil {
		return err
	}
	if payload != nil {
		if _, err := w.Write(payload); err != nil {
			return err
		}
	}
	return nil
}

// Encode 把整图写入 w。
func Encode(w io.Writer, nodes []Node, edges []Edge) error {
	bw := bufio.NewWriter(w)
	var header [4]byte
	copy(header[:], magic)
	if _, err := bw.Write(header[:]); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint16(version)); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(nodes))); err != nil {
		return err
	}
	if err := binary.Write(bw, binary.LittleEndian, uint32(len(edges))); err != nil {
		return err
	}
	for _, n := range nodes {
		if err := writeString(bw, n.Key); err != nil {
			return err
		}
		if err := writeString(bw, n.Label); err != nil {
			return err
		}
		if err := writeProps(bw, n.Props); err != nil {
			return err
		}
	}
	for _, e := range edges {
		if err := writeString(bw, e.SrcKey); err != nil {
			return err
		}
		if err := writeString(bw, e.DstKey); err != nil {
			return err
		}
		if err := writeString(bw, e.Label); err != nil {
			return err
		}
		if err := writeProps(bw, e.Props); err != nil {
			return err
		}
	}
	return bw.Flush()
}

type reader struct {
	r *bufio.Reader
}

func (rd *reader) u16() (uint16, error) {
	var b [2]byte
	if _, err := io.ReadFull(rd.r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(b[:]), nil
}

func (rd *reader) u32() (uint32, error) {
	var b [4]byte
	if _, err := io.ReadFull(rd.r, b[:]); err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(b[:]), nil
}

func (rd *reader) str() (string, error) {
	n, err := rd.u32()
	if err != nil {
		return "", err
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(rd.r, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func (rd *reader) props() (map[string]any, error) {
	n, err := rd.u32()
	if err != nil {
		return nil, err
	}
	if n == 0 {
		return nil, nil
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(rd.r, b); err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return nil, err
	}
	return m, nil
}

// Decode 从快照恢复节点与边数据。
func Decode(r io.Reader) ([]Node, []Edge, error) {
	rd := &reader{r: bufio.NewReader(r)}
	var m [4]byte
	if _, err := io.ReadFull(rd.r, m[:]); err != nil {
		return nil, nil, err
	}
	if string(m[:]) != magic {
		return nil, nil, errors.New("store: bad magic")
	}
	ver, err := rd.u16()
	if err != nil {
		return nil, nil, err
	}
	if ver != version {
		return nil, nil, errors.New("store: unsupported version")
	}
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
	return nodes, edges, nil
}
