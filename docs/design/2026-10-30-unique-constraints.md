# Unique Constraints — Design Spec

Status: v2 (post fact-check audit) · Date: 2026-10-30 · Arc: P2 item 3 (final piece)
Source authority: `docs/research/2026-10-26-gap-fill-feasibility.md` §5 (ruling: spec-level design required; Neo4j community-edition granularity; single-writer makes write-time checks lock-free).
Baseline: main@70d2ddf. Audit: 2026-10-30 fact-check pass verified every code claim below (v1 findings folded in: two root methods not three; FSM batch lane abort ruling; two Cypher routing layers added).

## 1. Goal & Non-Goals

Add property-value uniqueness constraints over `(label, property)` pairs, enforced on every write path (Go API, Batch, Cypher writes, WAL replay, server raft apply) with Neo4j-community semantics: declaring validates existing data (fails atomically, naming duplicates); afterwards any write that would make two live nodes share a `(label, prop)` string value fails at the write-method entry with zero side effects for that operation; partial-application contract documented per surface.

Non-goals (v1): existence/type constraints (Neo4j enterprise), numeric/bool uniqueness (string only, §2.5), constraint names (`(label, prop)` IS the identity), node-key uniqueness (keys already unique), per-role DDL permissions, hot disable, **wrapper-mode (`internal/writer` Handle) surface — Handle has no Cypher lane and no declaration op in v1; wrapper-mode sessions cannot declare constraints** (revisit if asked for).

## 2. Semantic Contract

### 2.1 Declarations

- Go API: `DeclareUnique(label, prop string) error`, `DropUnique(label, prop string) error`, `UniqueConstraints() []UniqueConstraint` (sorted, copy). `type UniqueConstraint struct{ Label, Prop string }` (root package + `internal/wal` mirror).
- Cypher DDL: `CREATE CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE` and `DROP CONSTRAINT FOR (n:Label) REQUIRE n.prop IS UNIQUE` (REQUIRE-form only on drop; no name form — documented divergence).
- Duplicate declare → `ErrConstraintExists`; drop of unknown → `ErrConstraintNotFound`. Both pre-side-effect.
- **Mid-Batch ruling: `DeclareUnique`/`DropUnique` reject inside `g.Batch` (vector-decl precedent — `CreateVectorIndex` rejects mid-batch at vectra_vector.go:233; typed `IndexProperty` is the opposite precedent and is NOT followed). Rationale: the declaration mutates the registry mid-window and scans full state; Cypher statements never run inside Batch, so the DDL path is unaffected; the FSM/wrapper declaration lanes already apply decl ops outside the Batch closure (fsm.go:169-190 pattern).**

### 2.2 Declaration-time validation (Neo4j rule)

`DeclareUnique` scans full live state (published snapshot ∪ open-window builder records — this union is legal because the call is never mid-Batch, §2.1) for `(label, prop)`; any duplicated string value → declaration fails with `ErrUniqueViolation` listing up to 10 duplicate value/key pairs, sorted by value then key (deterministic across raft replicas). Constraint NOT registered.

### 2.3 Write-time enforcement — exactly TWO root entry points

The engine has exactly two node-mutating root methods (audit-verified; `materializeEndpoint`'s lift at vectra.go:960 copies props verbatim and is invisible to uniqueness; edge paths never touch node props; folds operate in key space only):

- `AddNode(key, label, props)` (vectra.go:1175) — covers: fresh adds, overrides (props replaced), builder revive, AND `SetNodeProps` transitively: the Cypher facade `(*writeGraph).SetNodeProps` (cypher_write.go:77) reads `NodeByKey` from the published generation, merges (nil deletes a key), and funnels into ONE `AddNode` call (:94). Release-then-register reads the OLD (label, props) from builder-then-mirror — note the asymmetry: the facade reads published state (statements never run inside Batch), while the AddNode-side release must read builder-first (`g.nodes` is reconciled only at publish, reconcileMirrors :1097-1128).
- `DeleteNode(key)` (vectra.go:1328) — releases every constraint value the node owns.

Checks run at method ENTRY, before `recordNode` AND before the `nextNode` ratchet advances (:1191-1193): a rejected AddNode leaves WAL frames, pendOps, registry, ratchet, and vector intake untouched (zero side effects, pinned by test).

Violation error: `type UniqueViolationError struct{ Label, Prop, Value, ExistingKey, AttemptedKey string }`, sentinel `ErrUniqueViolation` via `errors.Is`.

Value lifecycle: override releases old then registers new (pre-state fetch per above); delete frees immediately — a value freed by delete is reclaimable in the same batch (no tombstone hold; pinned by test). Revive re-checks via its AddNode entry.

### 2.4 Partial application — PER SURFACE (audit correction)

- Go `Batch`/`applyChangeset` (vectra.go:1524) and wrapper replay (writer.go:497-525): first-error-stops — writes before the violation are durable/published, the violating op has zero side effects, ops after it do not execute. Existing behavior, unchanged.
- **Server `/v1/batch` and `/v1/txn` (fsm.Apply): TODAY fsm.go:200-204 silently skips AddNode errors (`if err == nil { NodesApplied++ }`, no else) — a violation would return 200 with the node missing. RULING: change the FSM node lane to first-error-stops (fail the whole Apply with the violation error), following the vector shape-validation abort precedent (fsm.go:154-158) — log-content-derived aborts are the established determinism-safe pattern. Same for the edge lanes' error handling ONLY if needed to surface the violation (edge ops cannot violate node constraints in v1, so edge lanes stay as-is). This is a small, deliberate behavior change: an entry that previously could not fail from AddNode now can; the batch wire contract gains an error case (§2.6).**

### 2.5 Value domain ruling

Only `string` prop values participate; non-string values are invisible (never error, never conflict). Rationale: schema-less mixed-type props; typed planes are string-first. Documented v1 divergence from Neo4j (which errors on wrong type).

### 2.6 Wire error face (audit correction)

A constraint violation is a CLIENT error. Ruling: on both `/v1/cypher` (applyCypherWrite) and `/v1/batch` (handleBatch), an error that is/contains `*vectra.UniqueViolationError` maps to **400 with code `UNIQUE_VIOLATION`** (message from the error), decided by an `errors.As` branch in front of the generic apply-error mapping (`writeApplyErr` would otherwise land it on 500 INTERNAL). Do NOT wrap the violation in `*cypher.Error` — that flips /v1/cypher to 400 PARSE (api.go:599-603), which is the wrong code and hides the payload. Neo4j answers violations 4xx; we match with 400.

## 3. State Model

### 3.1 The registry

Writer-private, on `*Graph`, spanning the open window AND persisting across publishes:

```go
// constraint -> value -> owning key (uniqueness = at most one owner).
// nil = no constraints (fast path: zero cost on every write — len check only).
// stale = rebuild before next use (see 3.3).
uniqueIdx  map[UniqueConstraint]map[string]string
uniqueStale bool
```

Key-based (not ID-based): folds/Seal/CaptureMerge renumber IDs, never keys/labels/props (audit-verified, merge.go:110-124) — the registry is fold-immune. The `bNodeIDs` per-publish-clear + lazy-lift pattern (publishLayer :1040-1041, cutLayer carry :1157-1159) is NOT reusable: uniqueness needs the full value view and O(V) per batch would break the write-throughput red line. Ruling stands: persistent registry, incremental maintenance, self-healing rebuilds.

### 3.2 Incremental maintenance points (exhaustive)

1. `DeclareUnique` (outside Batch): full build of that constraint's value map from published ∪ builder-visible live state; register.
2. `AddNode` entry (after the check passes): release the key's previously-owned value (if any, reading pre-state builder-then-published), register the new value (when label matches and the prop is a string). Covers override, revive, and the SetNodeProps facade.
3. `DeleteNode` entry: release the node's owned values (pre-state fetch as above).
4. `DropUnique`: delete the constraint's map.

### 3.3 Stale-marked rebuilds (self-healing)

Mark `uniqueStale = true` (rebuild all maps from published ∪ builder-visible state on next use) at: Batch panic recovery (the deferred builder drop, vectra.go:1594-1603); `RepairAfterRebase` and the mirror-rebuild sites; nothing else. Normal publish/fold/seal paths do NOT mark (incremental + key-stable = correct across layer changes; this claim is itself a tested invariant via `verifyUniqueInvariant`, a test-only helper comparing registry to ground truth). Rebuild skips non-string values.

### 3.4 Initial-state construction & replay REPLACE semantics

- `New`/`LoadFile`/`Open`: seed from decoded `store.Data.UniqueConstraints` (§4.2), full build, validation SKIPPED (data was valid when persisted).
- WAL replay: the `IndexDecl` handler branch (vectra.go:1810-1827) gains a constraints branch with **REPLACE semantics exactly like `applyVecDecls` (:1821-1825): `nil` field = skip (pre-feature frames); non-nil INCLUDING empty = replace the whole set** — merge-style replay would resurrect dropped constraints. Trust-path (no re-scan, no validation): replaying original frame order reconstructs the registry exactly.

### 3.5 Known easy-to-miss carriers (both have bitten before)

- `Clone()` (vectra.go:3207-3236): must carry constraint declarations and registry state (today it carries the vector registry via `setVecRegistry` — the Phase-2 bug class). Pinned by test: clone a constrained graph, violate on the clone, expect rejection.
- Decode-side wiring: vector decls do NOT ride `graphFromEngine`; they register post-construction in `loadGraphForest` (vectra.go:2948-2960, `applyVecDecls`). Constraints follow the same discipline: post-construction registration in `loadGraphForest` beside `applyVecDecls`. `loadGraphForest` is the decode funnel for Load/LoadFile/ImportSnapshot/ModeOff-Open — hence also raft `Restore` (fsm.go:511): the backup-restore parity test depends on this chain. Encode side: `saveView` (:2815-2835) is the single `store.Data` assembly point (SaveFile/Save/ExportSnapshot/checkpoint/async-compact all flow through it) — the sole Encode wiring site.

## 4. Persistence

### 4.1 WAL (forward-compatible, VectorIndexes precedent)

`wal.IndexDecl` gains `UniqueConstraints []UniqueConstraint` — `json:"uniqueConstraints"`, **no omitempty** (null = frame carries none; explicit empty = "drop all" survives the wire — the audit-hardened Phase-1 rule, honored by recordVecDecls building non-nil slices). New pendOp kind `opUnique`; flush coalesces into the IndexDecl run carrying the FULL declaration list (last-in-run wins, flushPending :1462-1483 pattern). `internal/wal.UniqueConstraint{Label, Prop string}` mirrors the root type.

### 4.2 Store v8

`store.Data` gains `UniqueConstraints []UniqueConstraint`; `Encode` (via saveView) stamps **v8** = v7 layout + trailing unique-constraint section. Open rules mirror v6→v7 exactly: decode gates the section on `ver >= versionV8` (older files → nil field, zero constraints, recompact-eligible to v8); any newer version fails on the version byte (store.go:459-460 tripwire). Downgrade line: v8 stores refuse to open on v7 binaries — CHANGELOG carries the "cannot downgrade past 0.16.0" note (v6 tripwire precedent).

### 4.3 Server/raft

No new raft command: `CREATE CONSTRAINT` is a Cypher write statement, replicated and re-executed per replica (fsm.applyCypher → f.live.Cypher → writeGraph → root methods; determinism: constraint state is log-derived, so the violation surfaces at the same op everywhere). The FSM's live graph has NO WAL attached (fsm.go:198) — constraint IndexDecl frames never exist server-side; server persistence rides raft-log re-execution + raft snapshots (Snapshot→ExportSnapshot flows through saveView; Restore→ImportSnapshot through loadGraphForest). FSM batch lane behavior change per §2.4; wire face per §2.6.

## 5. Surfaces

### 5.1 Go (root module)

`DeclareUnique`/`DropUnique`/`UniqueConstraints` + `UniqueViolationError`/`ErrUniqueViolation`/`ErrConstraintExists`/`ErrConstraintNotFound`, godoc'd; examples in example_test.go.

### 5.2 Cypher — FOUR routing/parser layers (audit correction; missing any one breaks DROP or misroutes)

1. Root-package prefilter: `writeClauseKeywords` in `maybeWriteQuery` (cypher.go:120-135) — add `"constraint"` so `DROP CONSTRAINT` routes to the write path (CREATE CONSTRAINT already matches "create"; the keyword is added for the DROP form). Zero-alloc (containsFoldASCII).
2. Parser entry gate: the statement-initial keyword whitelist (parser.go:89-93) gains `DROP`; the clause switch gains the ConstraintClause case; a CREATE-vs-CONSTRAINT lookahead beside the existing CREATE INDEX lookahead (:116-121, :503 area). Standalone-statement rule: "CREATE CONSTRAINT is a standalone statement (no other clauses may accompany it)" — same wording family as parser.go:138.
3. BOTH classification lists: `IsWrite` (write.go:74) and the parser's `isWriteClause` (:154-160, drives RETURN-optional logic) gain ConstraintClause.
4. Exec: `Writer` interface gains `DeclareUnique(label, prop string) error` + `DropUnique(label, prop string) error`; `constraintStage` beside `indexStage` (write.go:669-681). Summary: `constraintsCreated` on `CypherWriteSummary` — 0 or 1 only (a duplicate declare is an ERROR per §2.1, unlike `indexesCreated` which counts idempotent re-declarations — documented divergence, do not "harmonize").

### 5.3 Server wire

`cypherSummaryOut` gains `constraintsCreated` (additive). `/v1/batch` & `/v1/txn`: FSM abort per §2.4 + 400 UNIQUE_VIOLATION face per §2.6 (errors.As branch ahead of writeApplyErr in handleBatch and applyCypherWrite). `/v1/txn` optimistic validation untouched.

### 5.4 SDKs / CLI / MCP

SDKs and CLI: no code changes (DDL rides `cypher(...)`). READMEs (root en+zh, both SDKs): syntax, partial-application note per surface, string-only ruling, duplicate-on-declare behavior. MCP operational note: MCP-injected graphs may fail `DeclareUnique` at declaration time on legacy duplicate data (declare before injecting, or dedupe first).

## 6. Test Matrix (minimum, all pinned)

Engine: declare-fails-on-duplicates (listing, determinism); declare-then-violate on AddNode and via the SetNodeProps facade (incl. nil-delete freeing the value); override releases old value; delete frees value (same-batch reclaim); revive re-checks; non-string invisible; DropUnique stops enforcement; ErrConstraintExists/NotFound; mid-Batch declare/drop rejected; zero-side-effect rejection (ratchet/pendOps/registry/vecIntake untouched); partial application per surface (Go Batch stop-set; server FSM abort). State model: fold-during-writes registry equality (`verifyUniqueInvariant`); Batch-panic → stale rebuild; RepairAfterRebase → stale rebuild; SET constrained prop twice in one statement-batch (value A→B→A across two keys — the pre-state fetch asymmetry test). Persistence: WAL round-trip (declare, write, close, reopen, violation rejected); replay REPLACE semantics (drop survives replay, nil-frame skip); store v8 round-trip; v7 file opens with zero constraints; forged v8 refused by v7 reader. Cypher: parse (standalone rule, REQUIRE form), FOUR-layer routing (DROP CONSTRAINT reaches exec — the prefilter test), exec create/drop, summary counter, IsWrite + isWriteClause. Server: CREATE CONSTRAINT over HTTP; violation face 400 UNIQUE_VIOLATION on /v1/cypher AND /v1/batch (not 400 PARSE, not 500); two-node determinism (both replicas reject identically); FSM abort (no silent-skip regression test — violating batch returns error, prior ops' partial application documented); backup-restore round-trip carries constraints (P2 backup arc integration). Performance: root `TestCypher.*Allocs` + `exec_alloc_test` byte-identical with zero constraints; write-path bench (M1 small, engine mode) within noise of baseline, numbers recorded; declaration scan cost at 100k nodes recorded (informational).

## 7. Milestones (each independently testable)

1. Types + WAL frame + store v8 + decode funnel wiring (saveView/loadGraphForest/Clone) — round-trip tests only, no behavior change.
2. Registry + enforcement at the two root entries + lifecycle tests (in-memory, WAL off).
3. Persistence wiring (replay REPLACE branch, Open seeding, restart parity) + stale/rebuild paths (panic, repair) + verifyUniqueInvariant.
4. Go public API + errors + examples.
5. Cypher DDL: four routing/parser layers + exec + summary; FSM batch-lane abort; server wire faces (400 UNIQUE_VIOLATION), determinism, backup-restore parity.
6. Docs (root READMEs en+zh, SDK READMEs, CHANGELOG 0.16.0 Added bullet — same unreleased block as TLS/backup; include the store-v8 downgrade tripwire note) + full verification.

## 8. Risks (explicit)

- New writer-private state adjacent to the most invariant-dense code (publish/rebase/repair). Mitigations: conservative stale-marking, verifyUniqueInvariant in stress tests, panic path drops to rebuild.
- The FSM batch-lane change (§2.4) alters `/v1/batch`'s error surface for the first time — must be flagged in CHANGELOG as behavior change, and the abort must be determinism-safe (log-content-derived, vector precedent).
- The SetNodeProps pre-state asymmetry (facade reads published; release reads builder-first) — pinned by the A→B→A test; wrong source = silent value leak.
- Declaration scan O(V) once per constraint (same class as a fulltext build at Seal) — documented, measured.
- store v8 downgrade tripwire needs the CHANGELOG note; goget-smoke unaffected.
