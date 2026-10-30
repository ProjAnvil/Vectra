// Command go-embedded walks one "project knowledge graph" scenario through
// the embedded engine, mirroring the root README quickstart: batch-upsert
// Person/Project nodes (one project carries a CJK name) and
// WORKS_ON edges, declare a FULLTEXT index and query the CJK name, run a
// Cypher write with RETURN (CREATE and SET) plus a MATCH read, then declare
// a vector index, write one 4-dim vector through batch props, and answer a
// top-1 vector search.
//
// From the repo root:
//
//	go run ./examples/go-embedded
package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/projanvil/vectra"
)

func main() {
	dataDir := flag.String("data", "example-data", "directory for the example graph (removed on exit when this run created it)")
	flag.Parse()

	// The example owns its data dir: create it when missing, remove it on
	// exit only when this run created it (a pre-existing dir, e.g. an
	// explicit -data, is kept).
	created := false
	if _, err := os.Stat(*dataDir); err != nil {
		if err := os.MkdirAll(*dataDir, 0o755); err != nil {
			log.Fatalf("create data dir: %v", err)
		}
		created = true
	}
	defer func() {
		if created {
			_ = os.RemoveAll(*dataDir)
		}
	}()

	// A durable WAL session: writes log to example-data/knowledge.vg.wal
	// and Close checkpoints into the snapshot. AddNode and UpsertEdge are
	// idempotent upserts, and the vector block below drops and re-creates
	// its index, so reruns against a kept -data dir complete cleanly too.
	g, err := vectra.Open(filepath.Join(*dataDir, "knowledge.vg"), vectra.Options{WALMode: vectra.ModeInterval})
	if err != nil {
		log.Fatalf("open graph: %v", err)
	}
	defer g.Close()

	// Declare the full-text index before the writes: the Seal below builds
	// the bigram buckets (pending declarations answer via full scan).
	g.IndexFullText("name")

	// [1] Batch upsert: one micro-layer for the whole import.
	err = g.Batch(func(g *vectra.Graph) {
		g.AddNode("ada", "Person", map[string]any{"name": "Ada"})
		g.AddNode("linus", "Person", map[string]any{"name": "Linus"})
		g.AddNode("vectra", "Project", map[string]any{"name": "Vectra engine"})
		g.AddNode("graphdb", "Project", map[string]any{"name": "图数据库"})
		g.UpsertEdge("ada", "vectra", "WORKS_ON", map[string]any{"since": 2025})
		g.UpsertEdge("ada", "graphdb", "WORKS_ON", map[string]any{"since": 2026})
		g.UpsertEdge("linus", "graphdb", "WORKS_ON", map[string]any{"since": 2026})
	})
	if err != nil {
		log.Fatalf("batch upsert: %v", err)
	}
	if err := g.Seal(); err != nil {
		log.Fatalf("seal: %v", err)
	}
	fmt.Println("[1] batch upsert: 4 nodes, 3 WORKS_ON edges (sealed)")

	// [2] Cypher write with RETURN: CREATE a person plus her edge, anchored
	// on a MATCHed project. Summary carries the mutation counters.
	res, err := g.Cypher(`MATCH (pr:Project {key:'graphdb'})
CREATE (p:Person {key:'zoe', name:'Zoe'})-[:WORKS_ON {since: 2026}]->(pr)
RETURN p.name AS person, pr.name AS project`)
	if err != nil {
		log.Fatalf("cypher CREATE: %v", err)
	}
	fmt.Printf("[2] cypher CREATE: createdNodes=%d createdEdges=%d, %s works on %s\n",
		res.Summary.CreatedNodes, res.Summary.CreatedEdges,
		res.Rows[0]["person"], res.Rows[0]["project"])

	// [3] Cypher SET with RETURN (a null RHS would delete the property).
	res, err = g.Cypher(`MATCH (p:Person {key:'zoe'}) SET p.role = 'engineer' RETURN p.key, p.role`)
	if err != nil {
		log.Fatalf("cypher SET: %v", err)
	}
	fmt.Printf("[3] cypher SET: propertiesSet=%d, %s is now an %s\n",
		res.Summary.PropertiesSet, res.Rows[0]["p.key"], res.Rows[0]["p.role"])

	// [4] Cypher read: who works on what.
	res, err = g.Cypher(`MATCH (p:Person)-[:WORKS_ON]->(pr:Project)
RETURN p.name AS person, pr.name AS project ORDER BY p.name`)
	if err != nil {
		log.Fatalf("cypher read: %v", err)
	}
	fmt.Println("[4] who works on what:")
	for _, row := range res.Rows {
		fmt.Printf("    %s -> %s\n", row["person"], row["project"])
	}

	// [5] Full-text lookup over the declared key: case-insensitive
	// substring, rune-pair bigrams under the hood — the CJK name hits.
	fts, ok := g.LookupByContains("name", "图数")
	if !ok {
		log.Fatal("full-text index on name not declared")
	}
	fmt.Printf("[5] full-text 图数: %d hit(s)\n", len(fts))
	for _, n := range fts {
		fmt.Printf("    %s = %s\n", n.Key, n.Props["name"])
	}

	// [6] Vector search: declare a 4-dim cosine index, write one vector
	// through batch node props (the Cypher subset has no list literals),
	// then query top-1. Dropping a previous declaration first keeps a
	// kept -data dir rerunnable (a duplicate name errors).
	if err := g.DropVectorIndex("person-emb"); err != nil && !errors.Is(err, vectra.ErrVectorIndexNotFound) {
		log.Fatalf("drop vector index: %v", err)
	}
	if err := g.CreateVectorIndex(vectra.VectorIndexDecl{
		Name: "person-emb", Prop: "emb", Dim: 4, Metric: "cosine",
	}); err != nil {
		log.Fatalf("create vector index: %v", err)
	}
	if err := g.Batch(func(g *vectra.Graph) {
		g.AddNode("ada", "Person", map[string]any{"name": "Ada", "emb": []float32{0.10, 0.90, 0.20, 0.40}})
	}); err != nil {
		log.Fatalf("write vector: %v", err)
	}
	hits, err := g.QueryVector("person-emb", []float32{0.12, 0.88, 0.25, 0.35}, vectra.QueryVectorOptions{K: 1})
	if err != nil {
		log.Fatalf("query vector: %v", err)
	}
	fmt.Printf("[6] vector top-1: %s (score %.4f)\n", hits[0].Key, hits[0].Score)
}
