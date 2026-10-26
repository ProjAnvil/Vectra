# v0.13.0 Clause Pipeline (WITH / UNWIND / OPTIONAL MATCH) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship v0.13.0 with a clause-list Cypher pipeline: WITH (incl. aggregation, WHERE/ORDER BY/SKIP/LIMIT trailer, DISTINCT, entity pass-through), UNWIND, and OPTIONAL MATCH — plus the SKIP keyword (closes gap E1) — with zero performance regression on existing query shapes.

**Architecture:** Big-bang migration to `Query{Clauses []Clause}` (interface sum type) with `runStatement` becoming a stage loop over materialized `[]row`; no legacy fast path (zero `Query`-literal construction sites exist outside the parser, so the degenerate `[Match, Return]` case is just a 2-iteration loop). Scalars ride a third COW map `row.vals` (nil = zero cost); WITH projection constructs fresh rows, seeding `nodes[name] = NodeVal.ID` for entity items so MATCH-after-WITH reuses the existing shared-var join unchanged. OPTIONAL MATCH runs the whole MATCH clause per base row with no cross-row dedup. Neo4j-compatible scope-chain validation at parse time only — no runtime scope structures.

**Tech Stack:** Go 1.27.2, std-lib only, no new dependencies, no store/WAL format change (store stays v7). MCP/REST need no surface change (queries pass verbatim); only descriptions/docs.

**Spec:** `docs/research/2026-10-26-gap-fill-feasibility.md` §4 + the 2026-10-26 v0.13.0 audit verdicts (structure audit, perf-baseline audit, architect C1-C12) embedded per-task below.

## Global Constraints

- Go 1.27.2; `make test` / `make vet` / `make fmt` across `. server mcp`; zero new dependencies; modern Go (type switch without closures in hot loops, `for i := range n`, no redundant loop-var copies).
- **Performance red lines (owner's standing directive — loosening a threshold is a gate failure, not a negotiation):**
  - `TestCypherViewPathAllocs` (root, non-race): named-end 1-hop ≤ 260 allocs/query, anonymous ≤ 200 — unchanged numbers.
  - `TestRowCOWAllocShape`: anonymous expand ≤ 12, named ≤ 80 — unchanged.
  - `TestDirectionsZeroAllocSteadyState`: exactly 0 — unchanged.
  - A/B vs main@1ec9508 on T1's characterization benchmarks: ns/op medians (`-count=10`) within **+5%**; **allocs/op must be identical** on the two 1-hop shapes.
  - New-stage baselines (T4-T6) land green-with-headroom in their own task, never retrofitted.
  - Race suite green throughout (`go test -race ./...`); alloc tests self-skip under race — keep that discipline.
- **Scope-creep guard — must NOT land in v0.13:** no write clauses (v0.14; the loop treats unknown clause kinds as unreachable), no general expression machinery (arithmetic, string concat, `range()`, function calls, `IS [NOT] NULL`, `=~`), no `$param`, no `WITH *`, no subqueries/CALL, no `RETURN DISTINCT` (WITH DISTINCT only), no multi-statements, no executor model change (tree-walking + materialized `[]row` stays).
- Error-string compat: the pure-MATCH validation errors keep today's exact strings (REST 400 PARSE contract + parser_test substring assertions).
- Neo4j-divergence documentation duty: UNWIND fan-out capped by maxRows (same `result too large: more than %d intermediate rows...` error); OPTIONAL MATCH has no Neo4j-style global dedup.
- Commit style: English conventional, one commit per task unless stated. Branch: `feat/v0.13.0-with-pipeline` (create from main@1ec9508 before Task 1). No pushes, no merge to main — owner decides.

## Review Focus

1. **dedupRows identity change regresses legacy dedup** — `dedupKey`/`sameRow` (exec.go:192-203, 375-390) gain vals awareness; any leak into the vals-nil path changes existing query results. → T3: strictly `if r.vals != nil`-gated + golden tests for both paths.
2. **COW third-map aliasing** — `vals` is the first new mutation surface since the COW discipline was sealed; an aliasing bug shows up as wrong results under fan-out, not crashes. → T3 extends `exec_cow_test.go` goldens to vals-carrying rows BEFORE any WITH/UNWIND stage exists.
3. **Alloc regression on degenerate shapes** — per-stage closures, runtime scope maps, or per-row vals allocations break ≤260/≤200. → T1 anchors the A/B; T3 is the first hard gate; thresholds never loosened.
4. **Scope validation behavior change on legacy queries** — a validate rewrite can silently change accept/reject on today's grammar. → T2 extends the table-driven error cases BEFORE executor work; pure-MATCH strings byte-identical.
5. **UNWIND/OPTIONAL semantic cliffs** — cross-row dedup eating OPTIONAL-MATCH rows; maxRows hit where Neo4j succeeds; cartesian-with-null wrong. → T5/T6 dedicated tests + docs; per-row merge without cross-row dedup.

---

### Task 1: Characterization benchmarks (pre-refactor anchor)

**Files:**
- Create: `bench/cypher_bench_test.go`
- Test: same file (benchmarks; plus a smoke test asserting the queries parse)

**Interfaces:**
- Produces: `BenchmarkCypher1Hop/named`, `BenchmarkCypher1Hop/anonymous`, `BenchmarkCypherVarLength`, `BenchmarkCypherUndirected` over a deterministic layered fixture; each `b.ReportAllocs()`, `b.Loop()`. The A/B anchor table recorded in the task report (ns/op + allocs/op medians of `-count=10` on the pre-refactor branch state).

**Facts:** No Cypher benchmark exists anywhere (verified: grep `Benchmark` + cypher in bench/ is empty). Fixture shape: mirror root `cypherLayeredGraph` (cypher_alloc_test.go — 50-node chain + fan-32, unsealed so the RunView path runs); construct it locally in bench/ (root test fixtures are unexported). Query set: (1) `MATCH (a {key:'0'})-[:CALLS]->(b) RETURN b.key` (named), (2) `MATCH (a {key:'0'})-[:CALLS]->() RETURN a.key` (anonymous), (3) `MATCH (a {key:'0'})-[:CALLS*1..3]->(b) RETURN b.key`, (4) `MATCH (a {key:'0'})-[c:CALLS]-(b) RETURN b.key`. Per-iteration `g.Cypher(q)` on one graph built in setup; note `defer g.Close()`.

- [ ] **Step 1: Write the benchmarks + a parse-smoke test** (all four queries must succeed on current engine).
- [ ] **Step 2: Run `go test ./bench/ -bench 'BenchmarkCypher1Hop|BenchmarkCypherVarLength|BenchmarkCypherUndirected' -benchtime=1s -count=10 -run '^$'`** (alone, quiet machine); record the medians table (ns/op + allocs/op per shape) **as a comment block at the top of `bench/cypher_bench_test.go` (committed — the T3 and T7 gates reference it as the pre-refactor baseline; a task-scoped subagent cannot see an uncommitted report)**.
- [ ] **Step 3: Verify alloc cross-check**: allocs/op of shapes (1)/(2) must be ≤ the TestCypherViewPathAllocs pins' neighborhood (they measure a smaller graph; just record, no assert beyond sanity).
- [ ] **Step 4: `make test` green; commit** — `bench(cypher): characterization benchmarks for the executor path (pre-refactor anchor)`

### Task 2: Clause-list AST + parser loop + scope-chain validation + SKIP

**Files:**
- Modify: `internal/cypher/ast.go` (Query → `Clauses []Clause`; `Clause` interface sum with `isClause()` marker; `MatchClause` unchanged + later `Optional bool`; new `WithClause`, `UnwindClause` placeholders ONLY if parseable — see step 3; `ReturnClause` gains `Skip int` (Skip<0 = none))
- Modify: `internal/cypher/parser.go` (parseQuery → keyword-dispatch clause loop; parseOperand unchanged in this task; validate → scope-chain)
- Test: `internal/cypher/parser_test.go` (extend table-driven cases), new `internal/cypher/scope_test.go`
- Modify (single line): `internal/cypher/exec_cow_test.go:217` (`ast.Match.Patterns[0]` → via temporary accessor)

**Interfaces:**
- Produces: `type Clause interface{ isClause() }; type Query struct{ Clauses []Clause }`; temporary accessors `func (q *Query) firstMatch() *MatchClause` and `func (q *Query) finalReturn() *ReturnClause` used by the UNTOUCHED executor (exec.go/project.go read these instead of `q.Match`/`q.Return` — mechanical rename only). Parser emits exactly `[Match, Return]` for legacy queries in this task. **SKIP**: `SKIP n` parsed into `ReturnClause.Skip` after ORDER BY / before LIMIT — and wired through projection (`project`/`projectAgg` slice `rows[skip : skip+limit]` semantics with both bounds) since the machinery already exists; closes gap E1. Scope rules (validate, parse-time only): (1) MATCH adds pattern vars (node/edge kinds; reuse = join, allowed); (2) WITH starts a new scope — only projected names survive; bare-variable items pass under their own name; non-bare items REQUIRE `AS` → error `expression in WITH must be aliased (use AS)`; (3) UNWIND alias mandatory → declares scalar; (4) WHERE-after-WITH allowed, referencing projected names (parse: the WHERE grammar's operand extension for bare identifiers lands in T4 — in T2 the scope checker accepts it syntactically); (5) ORDER BY/SKIP/LIMIT after WITH reference projected names only; (6) RETURN with undeclared var → error `variable %q is not defined in this scope (WITH starts a new scope; only projected variables survive)` — BUT pure-MATCH queries keep the existing strings (`variable %q is not declared in the pattern` etc., parser.go:711-788) byte-identical; (7) scalar var used as pattern var → positioned error; (8) property access on a scalar var (`k.foo` where k is scalar) → error `variable %q is a scalar; property access requires a node or edge` (validate-time kind check; the null-instead-of-error path remains only for unbound OPTIONAL vars).

**Facts:** parseQuery's fixed sequence is parser.go:75-167 (`expectKw("MATCH")` :76, `expectKw("RETURN")` :101); reusable pieces: parseReturnItem :395-414, parseAlias :418-429, parseAggItem :440-487, parseOrderKey :501-526, LIMIT :151-162. `STARTS WITH`/`ENDS WITH` already consume WITH inside parseCmp (:588-607) — clause-boundary `kw("WITH")` check is only reached at boundary positions, no conflict (verified: WITH-item expressions always begin with TIdent, so no item expression can begin with STARTS). Validate reads only `q.Match` (:690-703). AST construction sites: production code has zero outside the parser, but **parser_test.go has 15 `q.Match`/`q.Return` assertions (:13-:94) that stop compiling when the fields go** — rewrite them to the `Clauses[]` shape directly (NOT via accessors, so T3's accessor deletion doesn't break them again). exec_cow_test.go:217 is the only other AST-referencing test. Sum-type idiom exists (`Expr interface{ pos() }` ast.go:80).

**Grammar cardinality (pinned):** RETURN is terminal — any clause keyword after RETURN is an unexpected-token error (`MATCH (a) RETURN a MATCH (b)` → error). MATCH may REPEAT (`MATCH (a) MATCH (b) RETURN ...` is legal; disjoint vars = cartesian, shared vars = join — the natural stage-loop semantics, matches Neo4j; document as new capability). `RETURN *` is valid ONLY for the degenerate single-MATCH-no-WITH query: validate rejects it otherwise with `RETURN * is not supported after WITH/UNWIND or multiple MATCH clauses` (starItems reads only the first MATCH today; scope-aware star enumeration is deferred with `WITH *`).

- [ ] **Step 1: Write failing tests**
  - scope_test.go: **construct the clause list directly** (`Query{Clauses: []Clause{...}}` in-package) and call `validate()` for the WITH/UNWIND scope rules (alias-required; survival set; UNWIND alias; ORDER-after-WITH name checks; RETURN-after-WITH undeclared; scalar-as-pattern-var; PropRef-on-scalar-var error), each with exact error substring — the parse-path variants of these land in T4/T5 (T2's parser does not accept those tokens yet).
  - parser_test.go: rewrite the 15 existing `q.Match`/`q.Return` assertions to `Clauses[]` form; SKIP happy path (`ORDER BY k DESC SKIP 2 LIMIT 3`) + SKIP validation (negative/non-integer); grammar cardinality pins: `MATCH (a) MATCH (b) RETURN a` ACCEPTS (cartesian; pins repeated-MATCH parsing), `MATCH (a) RETURN a MATCH (b)` errors (RETURN terminal); encountering WITH/UNWIND/OPTIONAL at clause position yields the standard unexpected-token error (these three pins get REPLACED by T4/T5/T6 when their arms land — each of those tasks owns its keyword's pin replacement); keyword-as-alias current behavior pinned (`WITH a AS match` alias lexes as ident — divergence noted, documented in T7).
  - Error-string family selection: the scope validator tracks whether scope was reset (WITH/UNWIND seen) — pure-MATCH errors keep today's strings byte-identical; both families pinned by tests.
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** — AST restructure; parser clause loop (MATCH arm = existing comma-pattern + WHERE; RETURN arm = items + ORDER BY + SKIP + LIMIT); validate scope-chain (two passes: build scope per clause, check references); executor/project mechanical rename to accessors; wire Skip through project/projectAgg (respecting `Limit<0` = no limit; `Skip<0` = no skip; skip beyond length → empty). PLACEHOLDER types for WithClause/UnwindClause are NOT added — YAGNI; each lands with its parser arm in T4/T5.
- [ ] **Step 4: Run** — new tests PASS; **full `make test` green** (legacy behavior identical); error-string diff check: run existing parser_test + server api PARSE tests untouched.
- [ ] **Step 5: Commit** — `feat(cypher): clause-list AST, scope-chain validation, SKIP keyword`

### Task 3: row.vals third COW arm + stage loop (first hard alloc gate)

**Files:**
- Modify: `internal/cypher/exec.go` (row struct + ownVals + clone third arm; dedupKey/sameRow vals extension; runStatement → stage loop over q.Clauses; matchRows base param; delete temporary accessors)
- Modify: `internal/cypher/project.go` (project/projectAgg read via final return clause — already accessor-based from T2; `itemValue`/`scalarOf` gain vals lookup)
- Modify (accessor/reference cleanup): any test file still referencing the T2 accessors or removed fields (parser_test.go should already be in `Clauses[]` form from T2 — verify; exec_cow_test.go:217 path)
- Test: `internal/cypher/exec_cow_test.go` (vals goldens), `internal/cypher/exec_test.go` (stage-loop degenerate behavior + repeated-MATCH cartesian/join)

**Interfaces:**
- Produces: `row{pos; nodes; edges; vals map[string]any; nodesShared/edgesShared/valsShared bool}`; `ownVals()` mirroring ownNodes; `clone()` shares vals only `if r.vals != nil`; `dedupKey`/`sameRow` include vals (via `encodeVal` value identity) **only when vals != nil** — legacy path bit-identical. `runStatement` = `for _, c := range q.Clauses { switch c := c.(type) { case *MatchClause: rows = ex.matchRows(c, rows) + optional WHERE filter (extract the inline filter :107-115 into a helper shared with future WHERE-after-WITH); case *ReturnClause: return ex.project(c, rows) } }` — no closures, no runtime scope. **`matchRows` gains a base-rows parameter** (`matchRows(m *MatchClause, base []row)`, seeding `rows := base` instead of `[]row{}` — required for repeated MATCH in one statement and for MATCH-after-WITH; the first call passes `nil`). pos invariant comment on row: "pos is authoritative only on rows produced by runPath/expand; **stage-constructed rows (WITH/UNWIND/OPTIONAL merges) normalize pos to 0** so DISTINCT/dedup consumers never see a stale frontier".
- Consumes: T2 accessors (deleted here).

**Facts:** COW helpers exec.go:148-187; invariant comment :119-126 must be extended; dedupKey :192-203 (`p=%d` + sorted bindings), sameRow :375-390; maxRows sites :325/:430/:439; mutation-site discipline: WITH constructs fresh rows (T4), UNWIND clones+ownVals (T5) — none in T3. `itemValue` :338-354 and `scalarOf` :376-402 gain a vals branch (read-only). Alloc fences that must be green AT THIS COMMIT: TestRowCOWAllocShape ≤12/≤80, TestCypherViewPathAllocs ≤260/≤200, TestDirectionsZeroAllocSteadyState = 0.

- [ ] **Step 1: Write failing tests** — exec_cow_test vals goldens: sibling isolation through ownVals; nil-vals clone adds no allocation (`AllocsPerRun` on clone with vals==nil must equal the two-map shape); dedupKey distinguishes rows differing only in vals; sameRow true for equal vals; dedupRows keeps both. exec_test: degenerate `[Match, Return]` produces byte-identical results to a recorded golden of the same queries (reuse existing expected outputs).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** the third arm, identity extension, stage loop, accessor deletion.
- [ ] **Step 4: HARD GATE — run alone, quiet machine**: `go test . -run 'TestCypherViewPathAllocs' -count=1 -v` and `go test ./internal/cypher -run 'TestRowCOWAllocShape|TestDirectionsZeroAllocSteadyState' -count=1 -v` and re-run T1's benchmarks: **ns/op within +5% of T1 baseline AND allocs/op identical**. Any breach: stop and fix before T4.
- [ ] **Step 5: `make test` + `go test -race ./...` green; commit** — `feat(cypher): row scalar bindings (third COW arm) and stage-loop executor`

### Task 4: WITH (projection, trailer, WHERE-after-WITH, DISTINCT, entity re-seeding)

**Files:**
- Modify: `internal/cypher/ast.go` (`WithClause` — reuse ReturnItem/OrderKey + `Skip`/`Limit` + `Distinct bool` + `Where Expr`)
- Modify: `internal/cypher/parser.go` (WITH arm in the clause loop reusing parseReturnItem/parseAlias/parseOrderKey + ORDER BY/SKIP/LIMIT + optional WHERE; parseOperand extension: bare identifier allowed as operand — resolves to scalar var at eval, node/edge vars rejected at validate)
- Modify: `internal/cypher/exec.go` (stage loop gains `case *WithClause: rows, err = ex.withStage(c, rows)`)
- Modify: `internal/cypher/project.go` (withStage: non-aggregating = 1:1 fresh rows; aggregating = projectAgg refactored to yield `[]row` instead of terminating — shared trailer func for ORDER BY/SKIP/LIMIT; entity items seed `nodes[name]=NodeVal.ID`/`edges[name]=EdgeVal.ID`; scalars into vals)
- Test: `internal/cypher/with_test.go`, `internal/cypher/exec_alloc_test.go` or root `cypher_alloc_test.go` (new baselines)

**Interfaces:**
- Produces: WITH semantics pinned — (a) non-aggregating: one output row per input, items evaluated per row, bare vars pass entities (re-seeded into nodes/edges of the fresh row AND absent from vals), `var.prop AS k` → vals[k] scalar, `count(*)`-style aggregations divert to (b); (b) aggregating: group keys = non-agg items, `projectAgg` logic reused with termination changed to emit rows (keys+agg results into vals; entity key items re-seed nodes/edges), maxGroups + empty-input-one-row rules carried over; (c) trailer: ORDER BY (projected names only) + SKIP + LIMIT shared with RETURN via one tail function; (d) `WHERE` after WITH filters rows (three-valued eval; bare-ident operands read vals); (e) `WITH DISTINCT` dedups output rows via the T3-extended dedupKey (vals-aware); (f) scope: only projected names survive (validated in T2 already; executor assumes it). Node pass-through keeps node identity for subsequent MATCH joins via re-seeding. New alloc baselines (record measured + ~20-30% headroom, house style of cypher_alloc_test.go:82-92): `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a, b.key AS k RETURN k` (WITH passthrough), `WITH a, count(*) AS c` aggregating shape.

**Facts:** itemValue emits NodeVal{ID,...} for bare node vars (project.go:340-343) — the re-seed source; runPath's join branch (exec.go:405-428) consumes `b.nodes[first.Var]` unchanged; pos overwritten at :413/:421 before any expand read; encodeVal covers every scalar kind (project.go:263-293, NodeVal=3/EdgeVal=4 by ID — group keys on entity keys are ID-identity, acceptable); parseOperand today: parser.go:626-658 (literal/paren/var.prop only); the WHERE filter extraction happened in T3.

- [ ] **Step 1: Write failing tests** — with_test.go: passthrough + scalar alias + mixed; WHERE-after-WITH (incl. bare-ident operand, null semantics: NOT null stays null); ORDER BY/SKIP/LIMIT after WITH (trailer order: sort → skip → limit); WITH DISTINCT incl. **the multi-hop discriminator `MATCH (a {key:'0'})-[:CALLS]->(b)-[:CALLS]->(c) WITH DISTINCT b RETURN b`** (rows identical in bindings but differing in stale frontier pos MUST collapse to one — pins pos normalization at stage construction) and entity+scalar mixed identity; aggregating WITH grouping (multi-key, collect, empty-input-one-row); entity re-seed: `MATCH (a {key:'0'})-[:CALLS]->(b) WITH a, b.key AS k MATCH (a)-[:CALLS]->(c) RETURN c.key` (join through WITH — consumes T3's `matchRows(c, rows)` threading); scope survival (vars not projected are gone → error on reference); parser rejects non-aliased expression items; **non-aggregating WITH preserves input row order absent ORDER BY**; **`RETURN *` after WITH/multi-MATCH rejected** with the T2-pinned error; replace T2's unexpected-token pin for the WITH keyword with real parse-path scope tests (alias-required, survival set, via query strings).
- [ ] **Step 2: Run → FAIL.**
- [ ] **Step 3: Implement** parser arm, withStage, projectAgg-to-rows refactor + shared trailer.
- [ ] **Step 4: Gate** — new baselines recorded with headroom; existing fences + T1 A/B re-check (within +5%, allocs identical); `make test` + race green.
- [ ] **Step 5: Commit** — `feat(cypher): WITH clause - projection, aggregation, trailer, DISTINCT, entity pass-through`

### Task 5: UNWIND

**Files:**
- Modify: `internal/cypher/ast.go` (`UnwindClause{Expr Expr; Alias string}`)
- Modify: `internal/cypher/parser.go` (UNWIND arm: `UNWIND <expr> AS ident` — expr ∈ {list literal (parseListLit in operand position, UNWIND-only), bare var, var.prop})
- Modify: `internal/cypher/exec.go` (stage: per input row, evaluate expr; null or empty list → row dropped; non-list non-null → positioned error `UNWIND requires a list, got %T`; one flattening level; clone row + ownVals + bind alias; output capped by maxRows with the standard error)
- Test: `internal/cypher/unwind_test.go` + baseline

**Interfaces:**
- Produces: `UNWIND [1,2,3] AS x`, `UNWIND ks AS k` (vals list from collect), `UNWIND n.tags AS t` (props list). Elements bind as scalars into vals (an element that is itself a list binds as a list value — one level only). Killer idiom test: `MATCH (n) WITH collect(n.key) AS ks UNWIND ks AS k RETURN k`.

- [ ] **Step 1: Failing tests** — literal/var/prop sources; null/empty drop; one-level flatten (nested lists bind as lists); error on non-list; alias mandatory (parser error); fan-out × maxRows error; DISTINCT-after-UNWIND via WITH DISTINCT (**incl. the stale-pos discriminator: same UNWIND element reached from rows with different frontier pos must collapse**); alloc baseline for a fan-32 UNWIND shape (record + headroom); replace T2's unexpected-token pin for the UNWIND keyword.
- [ ] **Step 2: FAIL → Step 3: Implement → Step 4: Gate** (fences + A/B + race) **→ Step 5: Commit** — `feat(cypher): UNWIND clause`

### Task 6: OPTIONAL MATCH

**Files:**
- Modify: `internal/cypher/ast.go` (`MatchClause.Optional bool`)
- Modify: `internal/cypher/parser.go` (`OPTIONAL MATCH` arm — two keywords; scope adds its vars as nullable; validation: optional vars referenced after the clause may be null — no new errors)
- Modify: `internal/cypher/exec.go` (stage: for each incoming row b, run the clause's patterns via matchRows machinery over base `[]row{b}` (plus its WHERE); empty result → append b unchanged; else append all sub-rows. **No cross-row dedup on the merge.** Cumulative maxRows check on merged output.)
- Test: `internal/cypher/optional_test.go` + baseline

**Interfaces:**
- Produces: Neo4j semantics — optionality applies to the whole MATCH keyword incl. comma patterns (all-or-nothing per row); WHERE inside the optional pattern that filters to empty still keeps the base row; unbound pattern vars read as null (itemValue/scalarOf already null-safe — verify WITH interaction: an optional null var projected through WITH yields null scalar, and MUST NOT seed nodes (nil NodeVal → skip re-seed)). Guard: scalar var as pattern var stays a validate error (T2 rule).

- [ ] **Step 1: Failing tests** — matched/unmatched rows mixed in one query; comma patterns all-or-nothing; WHERE-filters-to-empty keeps row; null reads through RETURN and through WITH; cartesian-with-null; no-dedup assertion (duplicate base rows preserved); maxRows cumulative; baseline shape; replace T2's unexpected-token pin for the OPTIONAL keyword.
- [ ] **Step 2: FAIL → Step 3: Implement → Step 4: Gate** → **Step 5: Commit** — `feat(cypher): OPTIONAL MATCH`

### Task 7: Docs, CHANGELOG, release gate, A/B verdict

**Files:**
- Modify: `README.md` + `README.zh-CN.md` (grammar line ~:121 gains WITH/UNWIND/OPTIONAL MATCH/SKIP; "Cypher subset" section: scope rules summary, UNWIND maxRows divergence, `WITH *`/`range()`/`RETURN DISTINCT` deferrals; both languages parity)
- Modify: `mcp/server.go` cypher tool description (grammar enumeration ~:317/:433) + `mcp/README.md`
- Modify: `CHANGELOG.md` (`[0.13.0]` — Added: clause pipeline, WITH/UNWIND/OPTIONAL MATCH, SKIP, WITH DISTINCT; Performance: A/B numbers; the documented limitations)
- Modify: version strings per release convention (mcp/server.go Version, pyproject.toml, package.json, package-lock.json — the cd43731/f3584f2 pattern), CHANGELOG date
- Test: none new; verification = the gate run

- [ ] **Step 1: Docs + CHANGELOG + version strings.** Divergence/limitation notes to include: UNWIND fan-out capped by maxRows; OPTIONAL MATCH has no global dedup; keyword-as-alias accepted (`WITH a AS match`); repeated MATCH now legal (new capability); `RETURN *` restricted to the degenerate single-MATCH query; `WITH *`/`range()`/`RETURN DISTINCT`/expression machinery deferred.
- [ ] **Step 2: Full A/B vs main@1ec9508 via the two-worktree protocol** (docs/benchmarks/2026-10-15-delta-wal.md:30: medians of 3 × 2s, back-to-back, same machine): create `git worktree add /tmp/vectra-ab-main 1ec9508`, copy `bench/cypher_bench_test.go` into it (untracked — the file did not exist at 1ec9508), run `go test ./bench -bench 'BenchmarkCypher' -benchtime=2s -count=3 -run '^$'` in BOTH trees back-to-back; ALSO `go test . -run TestCypherViewPathAllocs -count=1 -v` on the branch (allocs must equal the T1-recorded values). **Red lines: medians within +5%, 1-hop allocs/op identical.** Clean up the worktree after. Record the verdict table.
- [ ] **Step 3: Full verification** — `make test` && `make vet` && `make fmt` && `go test -race ./...` green; `git log --oneline` one commit per task.
- [ ] **Step 4: Commit** — `docs: v0.13.0 clause pipeline documentation, changelog, version strings`

---

## Post-plan notes for the reviewer

- T2/T3 split is deliberate: parser/validation behavior changes are bisectable separately from executor/COW changes (risk #4 vs #1/#2); the repo's history shows alloc regressions are caught by shape-pinned tests that need the intermediate commit.
- The A/B worktree protocol (T7 Step 2) cherry-picks ONLY the T1 benchmark file into the main worktree — no code drift between sides.
- MCP/REST need zero surface change; only description strings and docs (verified: server/api.go:540 and mcp/server.go:845 pass query verbatim).
- v0.14 (write clauses) builds on this clause list; the stage loop must treat unknown clause kinds as unreachable, not error — the parser cannot emit them.
