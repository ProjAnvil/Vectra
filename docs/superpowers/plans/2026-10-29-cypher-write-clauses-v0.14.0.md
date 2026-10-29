# Cypher Write Clauses + Index DDL (v0.14.0) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Cypher subset a write language — CREATE / SET / DELETE / DETACH DELETE / MERGE (+ ON CREATE SET / ON MATCH SET) and `CREATE [FULLTEXT] INDEX` DDL — executed against the live Graph with Neo4j-compatible surface semantics and Vectra-native identity semantics, routed over raft in server mode.

**Architecture:** The v0.13 clause pipeline (clause-list AST, stage-loop executor) gains new clause types and a `Writer` interface (key/triple-addressed, implemented by the root facade over the live Graph). Write statements are: a read prefix (MATCH/WITH/UNWIND, unchanged machinery, pinned generation) → one or more write clauses applied per row through `Writer` → an optional RETURN projected after rebinding entity variables through keys against the post-write generation. No read clause may follow a write clause in v1. Server `POST /v1/cypher` classifies the statement; writes replicate through the existing raft command path (deterministic FSM re-execution — generated keys come from the node-ID ratchet, never from time/random).

**Tech Stack:** Go stdlib only (zero new dependencies, all three modules); no store-format change (stays v7); no WAL change (writes are the existing primitive calls).

**Spec:** docs/research/2026-10-26-gap-fill-feasibility.md §3 (write clauses: API mapping, MERGE atomicity ruling, visibility trap, error contract) and §0 (E5-E9); docs/research/2026-10-29-lightweight-adoption-assessment.md §4 (P0a ruling).

## Global Constraints

- Zero new third-party dependencies; Go stdlib only (root module must keep an empty require list).
- Read-path allocation pins must not move: the existing cypher alloc tests (244/177 on the 1-hop shapes) and ns budgets stay green, bit-identical.
- Store format unchanged (VCTR v7); WAL frames come from the existing primitive write calls only — no new frame types.
- Determinism across raft replay: any value a write derives (generated keys, iteration order) must be a pure function of state + statement sequence. No time, no randomness, no Go map-iteration order driving observable writes.
- Single-writer discipline: `Cypher` with a write statement inherits exactly the calling convention of `AddNode`/`UpsertEdge` (writer goroutine in direct mode; raft FSM apply in server mode). Document, do not add a new enforcement mechanism.
- Error contract: first error stops the statement; already-applied writes remain (the Batch contract, README "no rollback"). Errors are positioned `*cypher.Error` where the parser/validator produces them.
- English comments/commit messages; comments state constraints, not narration.
- All suites green at every commit: `go test ./...` root; `go test ./...` in server and mcp; the race markers that gate CI-style runs stay green (`go test -race ./...` at root must pass; A6-style benches are out of scope here).

## Review Focus

1. **Determinism across replicas** — the same write statement applied to two graphs with identical starting states must produce identical generated keys and identical end state (FSM re-execution). Test in Task 3 (and the ratchet test in Task 2).
2. **First-error-stops, partial-application contract** — a multi-row `UNWIND ... CREATE` whose second row errors must leave row 1 applied and return the error. Test in Task 3.
3. **Read-path zero regression** — existing alloc pins and the whole read suite stay green after every task; run the alloc tests explicitly in Tasks 1 and 9.
4. **Read-after-write visibility in RETURN** — `SET n.p = 1 RETURN n.p` must show 1, `CREATE (a:P {key:'x'}) RETURN a.label` must show P, `DELETE` followed by `RETURN n` must render nil cells (not stale entities). Tests in Tasks 3–5.
5. **DELETE edge parallel-edge divergence** — `DELETE e` deletes every parallel edge on the (src,dst,label) triple (E6: single-parallel delete is not expressible); the test pins this documented behavior so it cannot change silently. Test in Task 5.
6. **Write inside `g.Batch`** — the write executor must not query inside Batch (pre-batch generation trap, gap-fill §3); the executor reads via the pinned pre-write source and writes via primitives. Test in Task 3 documents the boundary; the rule is structural (no `g.Batch` call anywhere in the write path).

---

### Task 1: AST types, parser arms, and validation for write clauses

**Files:**
- Modify: `internal/cypher/ast.go` (new clause types after `UnwindClause`)
- Modify: `internal/cypher/parser.go` (parseQuery head + new arms; validate pass extension)
- Test: `internal/cypher/parser_write_test.go` (new)

**Interfaces:**
- Produces (later tasks parse and switch on these):

```go
// CreateClause is CREATE followed by comma-separated path patterns.
// Endpoint patterns with a Var already in scope REBIND to that node
// (they must be bare variables: no label, no props); fresh endpoints
// are created. Edge patterns must be directed, fixed-length.
type CreateClause struct{ Patterns []*PathPattern }

// SetItem is one "var.prop = expr" assignment.
type SetItem struct {
    Var, Prop string
    Value     Expr
}

// SetClause is SET followed by comma-separated items. Applied per row,
// left to right (later items see earlier ones, matching Neo4j).
type SetClause struct{ Items []*SetItem }

// DeleteClause is [DETACH] DELETE var[, var...]. Node vars require DETACH
// when the node has incident edges (runtime check); edge vars delete the
// whole (src,dst,label) triple (documented divergence, see Review Focus 5).
type DeleteClause struct {
    Vars    []string
    Detach  bool
}

// MergeClause is MERGE with exactly one pattern (single node, or one
// directed edge whose endpoints are bound-or-fresh) plus optional
// ON CREATE SET / ON MATCH SET trailers.
type MergeClause struct {
    Pattern    *PathPattern
    OnCreate   []*SetItem // nil when absent
    OnMatch    []*SetItem // nil when absent
}

// IndexClause is the DDL statement CREATE [FULLTEXT] INDEX ON (n.prop).
// Label-scoped forms are rejected at parse time (indexes are global per
// property key in Vectra).
type IndexClause struct {
    Prop     string
    FullText bool
}
```

- Grammar (parseQuery): a statement may now start with `CREATE`, `MERGE`, or `SET`/`DELETE` (error: no scope to act on) in addition to MATCH/OPTIONAL MATCH; `CREATE [FULLTEXT] INDEX` is detected by two-token lookahead (`INDEX` after `CREATE [FULLTEXT]`) and parses as an `IndexClause` — a standalone statement (no other clauses allowed with it). RETURN remains terminal and is now OPTIONAL when the statement contains at least one write clause; a pure-read statement still requires RETURN (existing error unchanged).
- Validation rules (extend the existing validate pass in parser.go — locate the function that walks `q.Clauses` and owns scope tracking for WITH; it grew the scope-chain in v0.13):
  - After the first write clause, only write clauses and (final) RETURN may appear — any MATCH/WITH/UNWIND after a write clause is a positioned error "read clauses after a write clause are not supported in this version".
  - SET/DELETE items must reference variables bound in scope AND must be entity bindings (a scalar WITH alias is a positioned error "SET/DELETE requires a node or edge variable").
  - CREATE edge patterns: directed only (`--` and `<--` are positioned errors), fixed hop only (`*` is an error); endpoint reuse: an endpoint with a Var that is in scope must be a bare `(v)` (label or props on a rebind is an error "cannot re-specify label/props when rebinding v").
  - MERGE pattern: exactly one pattern, one or three nodes (single node, or edge with both endpoints), directed edge only, same rebind rule; node MERGE requires a `key` property in the map — literal string or expression evaluating to a string at runtime — (positioned error otherwise: "MERGE node requires a {key: ...} property — node identity in Vectra is the logical key"); edge MERGE endpoints must be resolvable (bound vars or fresh keyed/generated nodes).
  - Prop VALUES are expressions in write patterns: MATCH patterns keep literal maps (index anchoring depends on literals), but CREATE/MERGE pattern props parse each value via the expression parser (`parseOr`) into a new `Exprs map[string]Expr` field on `NodePattern` — `UNWIND ['a','b'] AS k CREATE (n:P {key:k})` is the bulk-import shape and MUST work. `NodePattern.Props` (literals) is only populated for MATCH patterns; the two maps are mutually exclusive by construction. A literal in a write pattern lands in `Exprs` as a literal expression node.
  - CREATE node pattern: if the props contain `key` and it is statically a literal, it must be a string literal (positioned error otherwise); a non-literal `key` expression passes validation and is checked at runtime (evaluated value must be a string — the Task 3 partial-application test rides exactly this). `key` is extracted as the logical key and is NOT stored as a regular property (symmetric with the read anchor's `key` pseudo-prop).
  - IndexClause: the pattern is exactly `(ident.ident)`; a label (`(n:Label.prop)` shape or `(n:Label)`) is a positioned error "indexes are global per property key; label-scoped indexes are not supported".

- [ ] **Step 1: Write the failing parser tests** — `TestParseCreateNode`, `TestParseCreateEdge`, `TestParseCreateRebind`, `TestParseCreateExprProps` (expression-valued props land in `NodePattern.Exprs` as expression nodes; MATCH patterns keep literal `Props`), `TestParseSetItems`, `TestParseDeleteDetached`, `TestParseMergeWithOnClauses`, `TestParseIndexDDL`, `TestParseWriteNoReturnOptional`, plus the error table `TestParseWriteErrors` (each rule above: one asserted error substring + position). Assertions compare against hand-built ASTs for the happy paths (the existing parser tests' style).
- [ ] **Step 2: Run** `go test ./internal/cypher/ -run TestParseCreate -v` **— expect FAIL** (unknown clause).
- [ ] **Step 3: Implement** the AST types (with `isClause()` markers), the parser arms (`parseCreateClause`, `parseSetItems` (shared with MERGE trailers), `parseDeleteClause`, `parseMergeClause`, `parseIndexClause`), the parseQuery head relax + INDEX lookahead, and the validate rules. Follow the existing arm style: keywords matched via `p.kw(...)` case-insensitively, positioned errors via `p.errf`, comma loops mirroring `parseMatchClause`.
- [ ] **Step 4: Run** `go test ./internal/cypher/ -v` **— expect PASS** including every pre-existing parser test (no grammar regression) and the root alloc gate `go test . -run TestCypherAlloc -v` staying green.
- [ ] **Step 5: Commit** — `feat(cypher): parse + validate CREATE/SET/DELETE/MERGE clauses and CREATE INDEX DDL`

### Task 2: Writer interface and the root facade adapter

**Files:**
- Create: `internal/cypher/write.go` (Writer interface + WriteSummary + Classify/IsWrite — executor wiring comes in Task 3)
- Create: `cypher_write.go` (root package: `writeGraph` implementing Writer over `*Graph`, `CypherIsWrite`, `CypherWriteSummary` alias)
- Test: `cypher_write_test.go` (root; adapter semantics against a real Graph)

**Interfaces:**
- Produces:

```go
// internal/cypher/write.go
// WriteSummary is the statement's mutation counters (the Neo4j counters
// analogue). Result carries it when the statement wrote.
type WriteSummary struct {
    CreatedNodes, CreatedEdges int
    DeletedNodes, DeletedEdges int
    PropertiesSet, IndexesCreated int
}

// Writer is the mutation surface the write clauses drive. Every method is
// key- or triple-addressed: the executor resolves bound IDs to keys once,
// against the pinned pre-write source, before the first write call. All
// calls arrive sequentially from one goroutine (the writer goroutine in
// direct mode, the FSM apply goroutine in server mode).
type Writer interface {
    // NewNodeKey returns a fresh unique node key for CREATE patterns
    // without an explicit key: "_c<id>" drawn from the node-ID ratchet.
    // Deterministic: replaying the same statement sequence on identical
    // state yields identical keys (required for raft FSM re-execution).
    NewNodeKey() string
    NodeIDByKey(key string) (uint32, bool)
    // CreateNode applies AddNode upsert semantics; created=false when the
    // key already existed (props replaced, per AddNode).
    CreateNode(key, label string, props map[string]any) (id uint32, created bool, err error)
    // SetNodeProps merges props; a nil value deletes the key; returns the
    // number of property keys written (sets + deletes).
    SetNodeProps(key string, props map[string]any) (int, error)
    NodeHasEdges(key string) bool
    DeleteNodeCascade(key string) error
    // DeleteEdgeTriple deletes every parallel edge on the triple (E6
    // divergence, pinned by test).
    DeleteEdgeTriple(srcKey, dstKey, label string) (int, error)
    UpsertEdgeTriple(srcKey, dstKey, label string, props map[string]any) (created bool, err error)
    // CreateEdge adds a NEW parallel edge (CREATE semantics — duplicates
    // allowed; contrast UpsertEdgeTriple's fold). Fails when an endpoint
    // is missing (statement error, first-error-stops).
    CreateEdge(srcKey, dstKey, label string, props map[string]any) (id uint32, err error)
    // SetEdgeProps writes props onto every live parallel edge of the
    // triple; returns the number of edges touched.
    SetEdgeProps(srcKey, dstKey, label string, props map[string]any) (int, error)
    // EdgeIDByTriple returns the first live parallel edge (used to rebind
    // RETURN'd edge variables after writes).
    EdgeIDByTriple(srcKey, dstKey, label string) (uint32, bool)
    IndexProperty(key string) error
    IndexFullText(key string) error
}

// IsWrite reports whether the statement mutates (write clauses or DDL).
// The parse error is returned unchanged.
func IsWrite(q string) (bool, error)
```

- Root aliases: `type CypherWriteSummary = cypher.WriteSummary`; `func CypherIsWrite(query string) (bool, error)` (server routing uses this).
- Root adapter rules (the load-bearing ones):
  - `NewNodeKey` consumes the same node-ID ratchet AddNode draws from (locate `nextNode` in vectra.go); the ID is allowed to skip — ratchets only grow. Never time/UUID (Global Constraints determinism).
  - `SetNodeProps` = read current props OUTSIDE any Batch (from the live generation), overlay/delete, write back with AddNode (gap-fill E8: no per-property primitive; whole-record rewrite is the mechanism).
  - `SetEdgeProps` iterates the triple's live edges (EdgesOf scan) with UpsertEdge per parallel edge.
  - `IndexProperty`/`IndexFullText` wrap the chainable root APIs; error only if called on a closed graph (parity with existing behavior).
- [ ] **Step 1: Write failing adapter tests** in `cypher_write_test.go`: `TestWriterCreateNodeUpsert` (created flags for new/existing key), `TestWriterNewNodeKeyDeterministic` (two fresh graphs, same state, same sequence → same keys; keys never collide with user keys across 10k draws), `TestWriterSetNodePropsMergeAndDelete` (merge, nil-deletes, counter), `TestWriterEdgeTripleOps` (upsert created flag, CreateEdge makes parallels, triple delete of parallels, SetEdgeProps touches each parallel, EdgeIDByTriple), `TestWriterIndexDecls` (IndexProperty/IndexFullText pending until Seal, idempotent), `TestWriterWALReopen` (write via Writer under WALMode Always, Close, Open → state and keys identical).
- [ ] **Step 2: Run** `go test . -run TestWriter -v` **— expect FAIL** (no write.go / cypher_write.go).
- [ ] **Step 3: Implement** `internal/cypher/write.go` (interface + summary + IsWrite walking parsed clauses) and root `cypher_write.go` (writeGraph struct wrapping `*Graph`). IsWrite returns (false, err) on parse errors — classification never invents errors.
- [ ] **Step 4: Run** `go test . -run TestWriter -v` and `go test ./internal/cypher/` **— expect PASS**.
- [ ] **Step 5: Commit** — `feat(cypher): Writer interface + root facade adapter (key/triple-addressed, deterministic keygen)`

### Task 3: Write executor — CREATE stage, RunWrite entry, Summary plumbing

**Files:**
- Modify: `internal/cypher/write.go` (RunWrite, createStage, rebinding)
- Modify: `internal/cypher/exec.go` (Result gains `Summary *WriteSummary`; runStatement gains the write-clause arms — they are methods on executor, gated on `ex.w != nil`)
- Modify: `internal/cypher/ast.go` or `exec.go` (wherever Result lives: add the field)
- Modify: `cypher.go` (root `Cypher` classifies; write statements route to `cypher.RunWrite` with the writeGraph; `CypherResult` gains `Summary *CypherWriteSummary`; retype unchanged)
- Test: `cypher_write_api_test.go` (root, end-to-end through `g.Cypher`)

**Interfaces:**
- Consumes: Task 1 AST + validate; Task 2 Writer/writeGraph/aliases.
- Produces:

```go
// RunWrite executes a write statement. The read prefix runs over src
// (pinned generation, existing stage machinery); write stages drive w
// per input row, left to right; a trailing RETURN projects rows whose
// entity variables have been REBOUND through keys via w against the
// post-write state (Review Focus 4). The single implicit empty row
// anchors a bare CREATE/MERGE statement (same seeding matchRows uses).
func RunWrite(src Source, w Writer, q string) (*Result, error)
```

- Executor struct: add `w Writer` (nil on read paths) and a per-statement `refs map[string]entRef` sidecar (`entRef` = `nodeRef{key}` | `edgeRef{src,dst,label}`) captured lazily when a write stage or the final RETURN first needs a bound entity (resolve via the PINNED pre-write source, then never again).
- createStage semantics (per input row, per pattern, nodes then edges, patterns left to right):
  - Pattern props evaluate per row via `ex.eval` over the `NodePattern.Exprs` map (Task 1); a `key` expression whose value is not a string is a positioned-style runtime error ("key property must be a string").
  - Fresh node: key = evaluated `key` (extracted from the map), else `w.NewNodeKey()`; `w.CreateNode`; bind var; count CreatedNodes.
  - Rebind endpoint `(v)`: resolve var → NodeID → `src.NodeInfo` → key (record the nodeRef); v must be a node binding (else positioned-style error).
  - Edge: resolve both endpoint keys (rebind or create-as-needed), `w.CreateEdge` (new parallel — CREATE semantics, not fold); a failed endpoint write errors the statement (first-error-stops). Count CreatedEdges.
  - New bindings flow to RETURN (a node created in stage N is renderable in RETURN).
- RETURN rebinding: for each row, remap `row.nodes[v]`/`row.edges[v]` through refs via `w.NodeIDByKey`/`w.EdgeIDByTriple` against the post-write state, then run the existing `project`. Deleted entities resolve to (0,false) → the var is dropped from the row map and itemValue's unbound arm renders null (existing null-safety). Deleted-node RETURN cell = nil (pinned in Review Focus 4 test).
- `Result.Summary` populated by RunWrite; root copies to `CypherResult.Summary`.
- Root `Cypher`: branch on `cypher.IsWrite(query)` (writes only — read statements keep their single-parse path; a write statement parses twice, once for classification and once inside RunWrite, which is acceptable and keeps the read path untouched); write path flushes pend AFTER writes complete (before rebinding reads), mirroring the read path's flush-then-load discipline.
- [ ] **Step 1: Write failing end-to-end tests**: `TestCypherCreateKeyedNode` (`CREATE (n:Service {key:'gateway', env:'prod'}) RETURN n.key, n.env` → row `gateway/prod`, Summary.CreatedNodes=1, prop map has NO "key" entry), `TestCypherCreateGeneratedKey` (no key prop → `_c`-prefixed key, deterministic across two graphs), `TestCypherCreateEdgeAndEndpoints`, `TestCypherCreateRebindEndpoint` (`MATCH (a {key:'x'}) CREATE (a)-[:CALLS {w:1}]->(b:Service {key:'y'})`), `TestCypherCreateParallelEdge` (two CREATEs same triple → 2 edges, AddEdge not upsert), `TestCypherWriteNoReturn` (`CREATE (n:P {key:'a'})` → empty Columns/Rows, Summary set), `TestCypherUnwindCreateBulk` (`UNWIND ['a','b','c'] AS k CREATE (n:P {key:k}) RETURN count(n)` → 3, bulk-import shape — expression-valued props), `TestCypherWriteFirstErrorStops` (`UNWIND ['a', 2] AS k CREATE (n:P {key:k})` → row 1's node 'a' EXISTS, error "key property must be a string" returned — Review Focus 2), `TestCypherWriteWALRoundTrip` (Cypher CREATE under WAL → Close → Open → node present, keys identical), `TestCypherWriteReadPathUnchanged` (existing read query results identical before/after a Cypher write — three-source visibility).
- [ ] **Step 2: Run** `go test . -run TestCypherCreate -v; go test . -run TestCypherWrite -v; go test . -run TestCypherUnwind -v` **— expect FAIL** ("expected MATCH").
- [ ] **Step 3: Implement** per the interfaces above (RunWrite + createStage + executor wiring + root routing).
- [ ] **Step 4: Run** the full suites: `go test .`, `go test ./internal/...`, alloc gate `go test . -run TestCypherAlloc` **— expect PASS, pins unchanged**.
- [ ] **Step 5: Commit** — `feat(cypher): CREATE execution — RunWrite, per-row create stage, summary, WAL-visible writes`

### Task 4: SET stage

**Files:**
- Modify: `internal/cypher/write.go` (setStage)
- Test: `cypher_write_api_test.go` (extend)

**Interfaces:**
- Consumes: Task 3 executor wiring + refs; `ex.eval(expr, row)` (project.go:506) for RHS values.
- setStage semantics (per row, items left to right):
  - Node var: resolve key via refs; RHS evaluated against the CURRENT row (so `SET n.x = 1, n.y = n.x` works — items see earlier items through the rebound row's project path: evaluate via ex.eval on the pre-write source for reads of OTHER entities, and for `n.x` reads of just-set values, overlay the staged values in a per-row scratch map consulted before falling through to eval — implement as a small `pending map[string]map[string]any` keyed by var). RHS null → delete the prop. `PropertiesSet` counts sets + deletes across rows.
  - Edge var: resolve triple via refs; SetEdgeProps (all parallels — divergence documented at the SET level too).
  - Unbound var (OPTIONAL MATCH miss): error "SET requires a bound node or edge variable; <var> is null" (Neo4j parity).
- [ ] **Step 1: Failing tests**: `TestCypherSetNodeProp` (`MATCH (a {key:'x'}) SET a.env = 'staging', a.weight = 3 RETURN a.env, a.weight`), `TestCypherSetNullDeletes` (SET a.gone = null → prop absent, counter counts it), `TestCypherSetFromOtherVar` (`MATCH (a {key:'x'}), (b {key:'y'}) SET a.env = b.env`), `TestCypherSetSeesEarlierItem` (`SET a.x = 1, a.y = a.x`), `TestCypherSetEdgeProp` (edge triple, parallel edges both updated), `TestCypherSetUnboundErrors` (OPTIONAL MATCH miss → error substring "bound").
- [ ] **Step 2: Run** — expect FAIL ("expected RETURN" / unknown clause behavior).
- [ ] **Step 3: Implement** setStage + pending-overlay.
- [ ] **Step 4: Run** `go test .` + `go test ./internal/cypher/` **— PASS**.
- [ ] **Step 5: Commit** — `feat(cypher): SET stage — per-property writes, null-deletes, edge triple form`

### Task 5: DELETE / DETACH DELETE stage

**Files:**
- Modify: `internal/cypher/write.go` (deleteStage)
- Test: `cypher_write_api_test.go` (extend)

**Interfaces:**
- deleteStage semantics (per row, vars left to right):
  - Node var: without DETACH — `w.NodeHasEdges(key)` true → error "cannot delete node <key>: it still has edges (use DETACH DELETE)" (first-error-stops); with DETACH or no edges — `w.DeleteNodeCascade`, DeletedNodes++ per row. Repeated deletion across rows (same node bound in several rows): second delete is a no-op (node gone; NodeHasEdges false, cascade of missing node = no-op) — DeletedNodes counts actual deletions.
  - Edge var: resolve triple; `w.DeleteEdgeTriple`, DeletedEdges += deleted parallels (Review Focus 5 pin: two parallels → DeletedEdges=2 from one `DELETE e`).
  - Unbound var: error (same shape as SET).
- [ ] **Step 1: Failing tests**: `TestCypherDeleteDetached` (node + 2 edges gone), `TestCypherDeleteNodeWithEdgesErrors` (substring "use DETACH DELETE", node survives), `TestCypherDeleteEdgeTripleParallel` (Review Focus 5), `TestCypherDeleteThenReturn` (`MATCH (a {key:'x'}) DELETE a RETURN a.key` → nil cell, Review Focus 4), `TestCypherDeleteUnboundErrors`.
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement** deleteStage.
- [ ] **Step 4: Run** `go test .` **— PASS**.
- [ ] **Step 5: Commit** — `feat(cypher): DELETE / DETACH DELETE — node guard, cascade, triple edge delete`

### Task 6: MERGE + ON CREATE SET / ON MATCH SET

**Files:**
- Modify: `internal/cypher/write.go` (mergeStage)
- Test: `cypher_write_api_test.go` (extend)

**Interfaces:**
- mergeStage semantics (per row, ONE pattern):
  - Node MERGE (validator guarantees a `key` prop): key evaluated per row (Task 1 Exprs); `NodeIDByKey(key)` hit → bound, `created=false`, apply OnMatch items; miss → CreateNode with the pattern's evaluated props (key extracted), `created=true`, apply OnCreate items. Multi-match cannot occur (key identity). Other props in the pattern are creation-time only — on match they are NOT updated (documented divergence: match-by-key, not by full prop map).
  - Edge MERGE: resolve endpoints (rebind or fresh-node rules — fresh endpoints use the same create path as CREATE, and Neo4j's "MERGE creates specified pattern when the full pattern is absent" means endpoint creation only happens when the edge does not exist: check `EdgeIDByTriple` FIRST; if the edge exists, endpoints are not created even if fresh — validator only requires resolvability, runtime decides); hit → UpsertEdgeTriple(created=false)+OnMatch; miss → create endpoints as needed, CreateEdge, OnCreate. Label required in MERGE edge patterns (validator).
  - ON items use the SetItem machinery (Task 4) against the just-bound/created var.
- [ ] **Step 1: Failing tests**: `TestCypherMergeNodeCreateThenMatch` (same statement twice: first Summary.CreatedNodes=1 + ON CREATE fires; second created=0 + ON MATCH fires — assert prop outcomes), `TestCypherMergeNodeMatchByKeyOnly` (existing node with different other props: MERGE matches, props untouched), `TestCypherMergeEdgeFoldOrCreate`, `TestCypherMergeEdgeExistingSkipsEndpointCreation` (edge exists, pattern's fresh-node endpoint label would-be-created → node count unchanged), `TestCypherMergeDeterminismReplicas` (Review Focus 1, full-statement version).
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement** mergeStage.
- [ ] **Step 4: Run** `go test .` **— PASS**.
- [ ] **Step 5: Commit** — `feat(cypher): MERGE — key-identity node merge, triple edge merge, ON CREATE/ON MATCH SET`

### Task 7: CREATE INDEX / CREATE FULLTEXT INDEX DDL

**Files:**
- Modify: `internal/cypher/write.go` (ddlStage / RunWrite arm for IndexClause)
- Test: `cypher_write_api_test.go` (extend)

**Interfaces:**
- Semantics: standalone statement; calls `w.IndexProperty(prop)` / `w.IndexFullText(prop)`; Summary.IndexesCreated=1; declarations are pending-until-Seal (existing lifecycle) — the test asserts pending-fullscan then post-Seal typed lookup behavior through Cypher (`CREATE INDEX ON (n.env)` then `MATCH (a {env:'prod'}) ...` still correct pre-Seal via compensation).
- [ ] **Step 1: Failing tests**: `TestCypherCreateIndexDDL`, `TestCypherCreateFullTextDDL` (bigram `CONTAINS` pushdown engages post-Seal on the declared key — reuse the contains-pushdown test's shape), `TestCypherIndexDDLRejectsLabel`, `TestCypherIndexDDLPendingThenSeal`.
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement** (small: one arm + two Writer calls).
- [ ] **Step 4: Run** `go test .` **— PASS**.
- [ ] **Step 5: Commit** — `feat(cypher): CREATE INDEX / CREATE FULLTEXT INDEX DDL`

### Task 8: Server raft routing + MCP passthrough

**Files:**
- Modify: `server/api.go` (handleCypher branches on `vectra.CypherIsWrite`)
- Modify: `server/node.go` + `server/fsm.go` (new raft command carrying the statement text; FSM applies via `g.Cypher`, returns the summary — follow the Batch command end to end: encode → `n.Apply`-style submit → FSM decode case → apply → response marshaling; follower forward + leader-check parity with handleBatch)
- Modify: `mcp/server.go` (Go MCP `cypher` tool result includes summary counters when present)
- Test: `server/cypher_write_test.go` (new)

**Interfaces:**
- Consumes: `vectra.CypherIsWrite`, `CypherResult.Summary` (Task 3), the existing Batch command path (`n.Apply(r.Context(), Batch{...})`, api.go:209-239, as the template).
- Response: read statements — unchanged JSON. Write statements — `{"summary": {"createdNodes":..,"createdEdges":..,"deletedNodes":..,"deletedEdges":..,"propertiesSet":..,"indexesCreated":..}}` alongside empty `columns`/`rows`.
- FSM determinism note: the FSM re-executes the statement on every replica; determinism is guaranteed by the ratchet keygen + key-addressed writes (Global Constraints; pinned by TestCypherMergeDeterminismReplicas / TestWriterNewNodeKeyDeterministic).
- [ ] **Step 1: Failing tests**: `TestCypherWriteRaftSingle` (POST /v1/cypher CREATE on a 1-node cluster → 200 + summary; node queryable), `TestCypherWriteFollowerForward` (POST to follower forwards/applies on leader — mirror the existing batch forward test), `TestCypherReadUnchangedOnServer` (read statement response shape identical to pre-branch), `TestMCPCypherSummary` (Go MCP tool result carries counters).
- [ ] **Step 2: Run** `go test ./...` in server/ and mcp/ **— expect FAIL**.
- [ ] **Step 3: Implement** the command, FSM case, handler branch, MCP field.
- [ ] **Step 4: Run** `go test ./...` in server/ and mcp/, plus `go test .` at root **— PASS**.
- [ ] **Step 5: Commit** — `feat(server,mcp): route Cypher write statements through raft; surface write summaries`

### Task 9: Documentation, CHANGELOG, verification sweep

**Files:**
- Modify: `README.md` + `README.zh-CN.md` (Cypher grammar section: write clauses, identity/divergence rules — key prop, match-by-key MERGE, parallel-edge triple DELETE/SET, no-read-after-write, first-error-stops, DDL global-key scope; quickstart gains a write example)
- Modify: `CHANGELOG.md` (Unreleased → 0.14.0 entry, Keep-a-Changelog style, English)
- Test: verification runs only

**Interfaces:** none (docs).

- [ ] **Step 1: Update README grammar tables + write-clause section (EN first, then zh-CN mirror; the zh-CN README missed a section last release — keep them section-aligned).**
- [ ] **Step 2: CHANGELOG 0.14.0 entry summarizing Added + the documented divergences.**
- [ ] **Step 3: Verification sweep**: `go test ./...` (root), `go test ./...` (server), `go test ./...` (mcp), `go vet ./...` (all three), `go test -race .` (root package), alloc gates (`go test . -run TestCypherAlloc -v` — pins unchanged), gofmt clean (`make fmt` idempotent). Record the read-path bench smoke (`go test . -run TestCypherBench -bench '1Hop|With' -benchtime=1s -count=1`) numbers in the CHANGELOG-adjacent bench note if any ns/allocs moved (they must not).
- [ ] **Step 4: Commit** — `docs(cypher): write-clause grammar + divergences; CHANGELOG 0.14.0`

---

## Post-plan arcs (explicitly out of scope here)

- `SET n = map` / `SET n += map` (needs map-literal expressions), `SET n:Label`, `REMOVE`.
- Read clauses after write clauses within one statement (WITH/MATCH post-write).
- `$param` placeholders, arithmetic/string-concat expressions (gap-fill E2 — separate "query language catch-up" arc).
- Single-parallel-edge delete (E6) and per-property engine primitives (E8).
- Statement-scoped WAL frame grouping (a big UNWIND CREATE currently WAL-frames per primitive write; group via Changeset buffering if the bench shows it matters).
- Python/TS REST SDK (P0b — next arc; wraps the final surface this arc produces).
- Unique constraints (gap-fill ⑥), TLS, backup endpoints (P2).
