# P2 (part 2): Unique Constraints Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `(label, prop)` string-value uniqueness constraints per the audited spec: declaration with Neo4j-style existing-data validation, entry-point enforcement on the two root write methods, WAL/store-v8 persistence, Cypher DDL through all four routing layers, and the server wire faces.

**Architecture:** A persistent writer-private registry (`constraint -> value -> owning key`) maintained incrementally at the two node-mutating root entries (`AddNode` — which transitively covers the `SetNodeProps` facade — and `DeleteNode`), with conservative stale-marked rebuilds on panic/repair. Declarations ride `IndexDecl` WAL frames (full-list, replace semantics) and a new store v8 section; Cypher DDL threads through prefilter, parser gate, both classification lists, and exec; the server FSM batch lane gains first-error-stops and a 400 `UNIQUE_VIOLATION` wire face.

**Tech Stack:** Go 1.27, std-lib only; no new dependencies. Existing patterns: `VectorIndexes` WAL-field precedent, v6→v7 store versioning, `applyVecDecls` replace-replay, `indexStage` DDL exec, vector shape-validation abort in the FSM.

**Spec:** `docs/design/2026-10-30-unique-constraints.md` (v2, post fact-check audit). **The spec is the binding authority — read it before your task; every ruling below cites its section.** All file:line references in the spec were audit-verified against main@70d2ddf.

## Global Constraints

- English only for comments, docs, commit messages.
- Zero new dependencies; root module stays zero-dep; std-lib only.
- Work on branch `feat/p2-unique-constraints` from `main@70d2ddf`. No push. ff-merge to local `main` at arc end.
- With NO constraints declared, every existing behavior and alloc gate is byte-identical (`TestCypher.*Allocs`, `exec_alloc_test` — the zero-constraint fast path must be a `len == 0` early-out).
- Store files stamped v8 must be refused by v7 readers (version-byte tripwire); v7 files decode with zero constraints.
- Wire face ruling (spec §2.6): violations map to 400 `UNIQUE_VIOLATION` on `/v1/cypher` and `/v1/batch` — never wrapped in `*cypher.Error` (would flip to 400 PARSE), never left to `writeApplyErr` (would land 500).
- Port 8080 occupied on this machine — free ports in all server tests.
- CHANGELOG: bullets go into the existing `## [0.16.0] - Unreleased` section (Added + a Changed entry for the /v1/batch error-surface change + the store-v8 downgrade tripwire note).

## Review Focus

1. **Zero-constraint parity** — no declarations → all existing suites and alloc gates byte-identical. Pinned by running the full suite + alloc gates in every task's verification, plus a dedicated test that a fresh graph's write path never consults a nil registry (spec §3.1 fast path).
2. **Zero-side-effect rejection** — a rejected AddNode leaves WAL frames, pendOps, the ID ratchet, the registry, and vector intake untouched (spec §2.3). Test inspects all five.
3. **Pre-state asymmetry** — the SetNodeProps facade reads published state; the AddNode-side release reads builder-first (spec §2.3/§8). Pinned by the A→B→A two-keys-one-statement test (spec §6).
4. **Replay replace semantics** — a dropped constraint must not resurrect on WAL replay; nil field = skip (spec §3.4). Round-trip test declares, drops, writes, closes, reopens.
5. **Server determinism + no silent skip** — a violating batch on two replicas aborts identically with 400 UNIQUE_VIOLATION; nothing silently missing from a 200 (spec §2.4/§4.3). Two-node cluster test.

---

### Task 1: Types, errors, WAL frame, store v8 — round-trip only

**Files:**
- Modify: `internal/wal/wal.go` (IndexDecl field + UniqueConstraint type)
- Test: `internal/wal/wal_test.go` (extend)
- Modify: `internal/store/store.go` (Data field, v8 stamp + section)
- Test: `internal/store/store_v8_test.go` (new)
- Modify root-package error types file (alongside existing sentinels, e.g. vectra.go errors block): `UniqueConstraint` root type, `UniqueViolationError`, `ErrUniqueViolation`, `ErrConstraintExists`, `ErrConstraintNotFound`

**Interfaces:**
- Produces (all later tasks):

```go
// root package
type UniqueConstraint struct{ Label, Prop string }
type UniqueViolationError struct{ Label, Prop, Value, ExistingKey, AttemptedKey string }
func (e *UniqueViolationError) Error() string // "unique constraint violation: (Person.email) value \"a@x\" already owned by key \"n1\""
var ErrUniqueViolation = errors.New(...)      // errors.Is target; UniqueViolationError.Unwrap() -> it
var ErrConstraintExists   = errors.New(...)
var ErrConstraintNotFound = errors.New(...)

// internal/wal
type UniqueConstraint struct {
    Label string `json:"label"`
    Prop  string `json:"prop"`
}
// IndexDecl gains (no omitempty; null = none, empty = drop-all):
//   UniqueConstraints []UniqueConstraint `json:"uniqueConstraints"`
```

- store v8 rules (spec §4.2): `versionV8 = 8`; Encode section appends after ftsKeys; decode gates on `ver >= versionV8`; `VersionV8` exported like `VersionV7`.

- [ ] **Step 1: Failing tests** — wal_test.go: extend the IndexDecl round-trip family (mirror `TestIndexDeclFullTextKeysRoundtrip`): declare+drop full-list round-trip, null-vs-empty preservation on decode, old-style frame (no field) decodes nil. store_v8_test.go: v8 round-trip with constraints; v7 file (forged, `store_v7_test.go` pattern) decodes zero constraints; forged v8 file rejected by version byte (the v6 tripwire forging pattern).
- [ ] **Step 2: Run, verify FAIL** (`go test ./internal/wal/ ./internal/store/`).
- [ ] **Step 3: Implement** per Interfaces; wire nothing else (no engine consumption yet).
- [ ] **Step 4: Verify PASS** + root package compiles (`go build ./...`).
- [ ] **Step 5: Commit** — `feat(wal,store): unique-constraint declarations — IndexDecl field and store v8 section`

### Task 2: Engine core — registry, declaration API, two-entry enforcement

**Files:**
- Modify: `vectra.go` — registry fields on Graph (spec §3.1), `DeclareUnique`/`DropUnique`/`UniqueConstraints` (spec §2.1/§2.2 incl. mid-Batch rejection per the vector-decl precedent), `opUnique` pendOp + recordUniqueDecl + flush full-list coalescing (spec §4.1), enforcement + release/register at `AddNode` (:1175) and `DeleteNode` (:1328) entries (spec §2.3/§3.2), pre-state fetch helper (builder-then-published), stale fields (§3.1)
- Test: `unique_test.go` (new, root package)

**Interfaces:**
- Consumes: Task 1 types/errors; the spec's audit-verified facts (two root entries; SetNodeProps facade coverage).
- Produces:

```go
func (g *Graph) DeclareUnique(label, prop string) error
func (g *Graph) DropUnique(label, prop string) error
func (g *Graph) UniqueConstraints() []UniqueConstraint // sorted copy
// internal (used by Task 3): registry rebuild entrypoint + stale flag handling
```

- [ ] **Step 1: Failing tests** — `unique_test.go`, per spec §6 engine rows: declare-fails-on-duplicates (listing ≤10, deterministic sort); declare-then-violate AddNode (zero-side-effect inspection: WAL frame count, pendOps len, nextNode ratchet, registry, vecIntake); violate via SetNodeProps facade (nil-delete frees value); override releases old; delete frees (same-batch reclaim); revive re-checks; non-string invisible; DropUnique stops enforcement; ErrConstraintExists/NotFound; mid-Batch declare/drop rejected; Go-Batch partial application (pre-ops durable, violating op no-op, post-ops skipped); A→B→A two-keys test (pre-state asymmetry). Run both WAL-on and WAL-off variants where the path differs.
- [ ] **Step 2: Run, verify FAIL**.
- [ ] **Step 3: Implement**. Declaration validation scan = published ∪ builder-visible live state (legal: never mid-Batch). Enforcement before `recordNode` AND before the ratchet. Fast path: `len(uniqueIdx) == 0` early-out (zero cost, zero allocs).
- [ ] **Step 4: Verify PASS** — `go test . -run Unique -v`; then full root suite + `go test -run 'TestCypher.*Allocs' -count=1 .` + `exec_alloc_test` byte-identical (Review Focus 1).
- [ ] **Step 5: Commit** — `feat(engine): unique-constraint registry, declaration API, write-entry enforcement`

### Task 3: Recovery & carriers — replay, decode funnel, Clone, stale rebuilds

**Files:**
- Modify: `vectra.go` — WAL replay branch (REPLACE semantics, spec §3.4, beside applyVecDecls :1821-1825); `loadGraphForest` post-construction registration (spec §3.5); `saveView` Data assembly (:2815-2835); `Clone()` carry (spec §3.5); Batch panic defer + `RepairAfterRebase` stale marks (spec §3.3)
- Test: `unique_replay_test.go` (new), extends `unique_test.go` for stale/panic rows

**Interfaces:**
- Consumes: Task 2 registry internals (rebuild entrypoint, stale flag).
- Produces: full persistence parity; `verifyUniqueInvariant()` test-only helper (export_test.go) comparing registry to ground truth (used here and by Task 6's stress).

- [ ] **Step 1: Failing tests** — restart round-trip (declare, write, close, Open, violation rejected + `UniqueConstraints()` restored); replay REPLACE (declare + drop + write + reopen → constraint gone, not resurrected; nil-field old frames skip); Clone (constrained clone rejects violations); store-v8 end-to-end (Save/LoadFile via saveView); stale rebuilds: Batch-panic recovery then next write correct; RepairAfterRebase then next write correct; fold-during-writes `verifyUniqueInvariant` equality (drive folds with autoCompactLayers options).
- [ ] **Step 2: Run, verify FAIL**.
- [ ] **Step 3: Implement** per spec §3.3-§3.5 (rebuild source: published ∪ builder-visible; skip non-strings; validation skipped on trust paths).
- [ ] **Step 4: Verify PASS** — full root suite green (restart/clone/migrate families) + race on the new tests.
- [ ] **Step 5: Commit** — `feat(engine): unique-constraint persistence — replay, decode funnel, clone, self-healing rebuilds`

### Task 4: Cypher DDL — four routing layers + exec + summary

**Files:**
- Modify: `cypher.go` — `writeClauseKeywords` gains `"constraint"` (spec §5.2 layer 1)
- Modify: `internal/cypher/ast.go` (`ConstraintClause{Label, Prop string; Drop bool}`), `parser.go` (entry gate `DROP`, clause-switch case, CREATE-vs-CONSTRAINT lookahead, standalone rule, `isWriteClause` entry), `write.go` (`IsWrite` case, Writer interface `DeclareUnique`/`DropUnique`, `constraintStage` beside indexStage, `constraintsCreated` counter on the summary struct), lexer if DROP keyword missing (audit: no DROP keyword today — verify)
- Modify: `cypher_write.go` — `(*writeGraph)` implements the two new Writer methods (delegate to Graph, outside Batch — statements never run inside Batch so this holds)
- Test: `internal/cypher/parser_write_test.go` (extend), `cypher_write_api_test.go` or new `cypher_constraint_test.go` (root), exec/summary tests

**Interfaces:**
- Consumes: Task 2 Graph API.
- Produces: `CypherWriteSummary.ConstraintsCreated int` (json `constraintsCreated`; 0 or 1 — spec §5.2 divergence note); `ConstraintClause` AST node; the four layers green end-to-end.

- [ ] **Step 1: Failing tests** — parser: standalone rule error text family; REQUIRE-form parse (create + drop); DROP reaches exec through ALL FOUR layers (the root `g.Cypher("DROP CONSTRAINT ...")` end-to-end test is the layer-1 prefilter pin — without the keyword it dies as "expected MATCH, got DROP"); IsWrite + isWriteClause classification; exec: create/drop over `g.Cypher`, summary counter 1/0, duplicate create → statement error carrying the UniqueViolation (errors.Is through the Cypher layer — NOT rewrapped as positioned `*cypher.Error`, spec §2.6); CREATE CONSTRAINT with existing duplicates → statement error with the listing.
- [ ] **Step 2: Run, verify FAIL**.
- [ ] **Step 3: Implement** per spec §5.2; error passthrough discipline: writer-method errors propagate raw out of RunWrite (existing behavior — do not wrap).
- [ ] **Step 4: Verify PASS** — root suite + cypher package suite + alloc gates byte-identical (zero-constraint graphs).
- [ ] **Step 5: Commit** — `feat(cypher): CREATE/DROP CONSTRAINT ... REQUIRE ... IS UNIQUE — parse, route, exec`

### Task 5: Server — FSM abort, wire faces, determinism, backup parity

**Files:**
- Modify: `server/fsm.go` — node lane first-error-stops (spec §2.4, vector shape-validation abort precedent :154-158); applyCypher's Writer surface already delegates (no change beyond what Task 4 added)
- Modify: `server/api.go` — `errors.As(*vectra.UniqueViolationError)` branch → 400 `UNIQUE_VIOLATION` in `applyCypherWrite` and `handleBatch` (spec §2.6); `cypherSummaryOut.ConstraintsCreated` json `constraintsCreated`
- Test: `server/constraint_test.go` (new), `server/fsm_test.go` (extend for the abort)

**Interfaces:**
- Consumes: Tasks 2-4 engine surface.
- Produces: server wire contract per spec §2.4/§2.6.

- [ ] **Step 1: Failing tests** — FSM: violating batch entry fails the Apply (no silent skip; NodesApplied excludes it); determinism: two-node cluster (existing cluster harness), violating write via follower → both replicas' FSMs reject identically, error reaches client as 400 UNIQUE_VIOLATION with the violation fields in the message (NOT 400 PARSE, NOT 500); `/v1/batch` same face; `/v1/cypher` CREATE CONSTRAINT over HTTP → 200 with `constraintsCreated:1`, violation → 400 UNIQUE_VIOLATION; /v1/txn lane aborts the same way; backup-restore parity: declare + write on node A, `GET /v1/backup`, restore to new dir, constraint active on restored node (spec §6 last row).
- [ ] **Step 2: Run, verify FAIL**.
- [ ] **Step 3: Implement** per spec §4.3 (FSM has no WAL — statement re-execution is the server persistence; nothing else to wire).
- [ ] **Step 4: Verify PASS** — full server module suite.
- [ ] **Step 5: Commit** — `feat(server): constraint enforcement — FSM abort, 400 UNIQUE_VIOLATION face, replica determinism`

### Task 6: Docs + full-branch verification

**Files:**
- Modify: `README.md`, `README.zh-CN.md` (Cypher section: constraint syntax + semantics: string-only, duplicate-on-declare, per-surface partial application, mid-Batch rejection, the store-v8 downgrade note pointer), `sdk/python/README.md`, `sdk/ts/README.md` (short DDL example via `cypher(...)` + violation error shape), MCP operational note in root READMEs (spec §5.4)
- Modify: `CHANGELOG.md` — under `## [0.16.0] - Unreleased`: Added (constraints feature bullets), Changed (`/v1/batch` & `/v1/txn` now fail on constraint violations — previously impossible-to-fail lane; behavior change flag), the store-v8 "cannot downgrade past 0.16.0" tripwire note
- Verification only otherwise; fixes fold back

- [ ] **Step 1: Docs** per above; every syntax string greps true against parser tests.
- [ ] **Step 2: Full verification** — `make test vet`; root race `-count=1 -timeout 40m`; alloc gates + `exec_alloc_test` byte-identical; declaration scan timing at 100k nodes recorded (informational, fixture generator exists — `vectra_fixture_gen_test.go`); `git log --oneline main..HEAD` = planned commits.
- [ ] **Step 3: Commit** — `docs(readme): unique-constraint semantics and examples; changelog 0.16.0`

---

## Execution Notes

- Final review: whole-branch code-reviewer with the spec attached, one fix wave, ff-merge `feat/p2-unique-constraints` → local `main` (no push).
- Bench wall-clock gates under load are a known flaky class — isolate and rerun before judging.
- The stress/`verifyUniqueInvariant` rows in Task 3 and the determinism rows in Task 5 are the arc's highest-value tests; if the fix wave must prioritize, it prioritizes those.
