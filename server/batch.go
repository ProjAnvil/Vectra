package server

import (
	"bytes"
	"encoding/gob"
)

// batchVersion is the changeset format version. Evolution rule: add fields only (gob compatible).
const batchVersion = 1

// NodeUpsert / EdgeUpsert are the atomic items of a changeset. v2 semantics are upsert-only:
// an existing node key updates its props; edges append (multi-edges allowed). Deletes are out of scope for v2.
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

// Batch is a changeset carried by one raft log.
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
