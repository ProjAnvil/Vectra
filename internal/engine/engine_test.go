package engine

import (
	"encoding/json"
	"testing"
)

func sealIdx(t *testing.T, keys []string, props []map[string]any) *Snapshot {
	t.Helper()
	nodes := make([]NodeRec, len(props))
	for i, p := range props {
		nodes[i] = NodeRec{Key: string(rune('a' + i)), Label: "L", Props: p}
	}
	s, err := Seal(nodes, nil, []string{"L"}, keys, nil) // []string{"L"} = 边 label 注册表（LabelID 的查询对象）
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func fp(v float64) *float64 { return &v }

func TestTypedIndexBucketsAndLookups(t *testing.T) {
	s := sealIdx(t, []string{"type", "weight", "on"},
		[]map[string]any{
			{"type": "controller", "weight": 3, "on": true},
			{"type": "service", "weight": 7.5, "on": false},
			{"type": "controller", "weight": json.Number("5")}, // 无 on 字段 → bool 桶跳过
		})
	if ids, ok := s.LookupIndex("type", "controller"); !ok || len(ids) != 2 || ids[0] != 0 || ids[1] != 2 {
		t.Fatalf("string eq: %v %v", ids, ok)
	}
	if ids, ok := s.LookupNumber("weight", 7.5); !ok || len(ids) != 1 || ids[0] != 1 {
		t.Fatalf("number eq: %v %v", ids, ok)
	}
	if ids, _ := s.LookupNumber("weight", 3); len(ids) != 1 || ids[0] != 0 { // int 3 → float64 3 命中
		t.Fatalf("int normalization: %v", ids)
	}
	if ids, _ := s.LookupNumberRange("weight", fp(3), fp(5)); len(ids) != 2 { // [3,5] → a 和 json.Number("5")
		t.Fatalf("range [3,5]: %v", ids)
	}
	if ids, _ := s.LookupNumberRange("weight", nil, fp(4)); len(ids) != 1 { // (-∞,4] → a
		t.Fatalf("range hi only: %v", ids)
	}
	if ids, _ := s.LookupNumberRange("weight", fp(6), nil); len(ids) != 1 { // [6,∞) → b
		t.Fatalf("range lo only: %v", ids)
	}
	if ids, ok := s.LookupBool("on", true); !ok || len(ids) != 1 || ids[0] != 0 {
		t.Fatalf("bool: %v %v", ids, ok)
	}
	if _, ok := s.LookupIndex("absent", "x"); ok {
		t.Fatal("unindexed key must return ok=false")
	}
	if ids, _ := s.LookupIndex("type", "nope"); len(ids) != 0 { // 注册但无命中 → 空集 + ok=true
		t.Fatalf("empty hit: %v", ids)
	}
	if _, ok := s.LabelID("L"); !ok {
		t.Fatal("LabelID hit")
	}
	if _, ok := s.LabelID("X"); ok {
		t.Fatal("LabelID miss")
	}
}

func TestTypedIndexSkipsNonIndexableValues(t *testing.T) {
	s := sealIdx(t, []string{"misc"},
		[]map[string]any{
			{"misc": []any{1, 2}},
			{"misc": map[string]any{"k": "v"}},
			{}, // nil 值
		})
	// key 已注册（ok=true）但三桶皆空：所有查询返回空集而非 ok=false
	if ids, ok := s.LookupIndex("misc", "x"); !ok || len(ids) != 0 {
		t.Fatalf("string bucket empty: %v %v", ids, ok)
	}
	if ids, ok := s.LookupNumber("misc", 1); !ok || len(ids) != 0 {
		t.Fatalf("number bucket empty: %v %v", ids, ok)
	}
	if ids, ok := s.LookupBool("misc", true); !ok || len(ids) != 0 {
		t.Fatalf("bool bucket empty: %v %v", ids, ok)
	}
}
