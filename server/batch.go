package server

import (
	"bytes"
	"encoding/gob"
)

// batchVersion 是变更集格式版本。演进规则：只加字段（gob 兼容）。
const batchVersion = 1

// NodeUpsert / EdgeUpsert 是变更集的原子项。v2 语义只有 upsert：
// 节点 key 已存在则更新属性；边追加（多边允许）。删除不在 v2 范围。
type NodeUpsert struct {
	Key   string
	Label string
	Props map[string]any
}

type EdgeUpsert struct {
	SrcKey string
	DstKey string
	Label  string
	Props  map[string]any
}

// Batch 是一条 raft 日志承载的变更集。
type Batch struct {
	Version int
	Nodes   []NodeUpsert
	Edges   []EdgeUpsert
}

func encodeBatch(b Batch) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(b); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeBatch(data []byte) (Batch, error) {
	var b Batch
	if err := gob.NewDecoder(bytes.NewReader(data)).Decode(&b); err != nil {
		return Batch{}, err
	}
	return b, nil
}
