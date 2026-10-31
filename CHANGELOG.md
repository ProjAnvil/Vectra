# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.2] - 2026-11-01

Remediation of the 2026-11-01 Go code audit (40 findings; report in
docs/research/2026-11-01-go-code-audit.md). Behavior changes are listed
under Changed; everything else is internal quality with allocation gates
byte-identical throughout.

### Fixed

- **MCP `inject_batch` no longer silently drops failed node writes.**
  A failing node write (e.g. a unique-constraint violation) or a delete
  of a missing key now aborts the injection with an error carrying the
  underlying cause and disclosing that earlier items were applied —
  aligning the node-write lane with `/v1/batch`'s first-error-stops
  semantics (node deletes abort here too, deliberately stricter than the
  server lane's skip-counted deletes). Edge-endpoint misses keep their
  documented skip-counting behavior.
- `meta.json` (node identity) is written atomically (temp + fsync +
  rename + dir fsync), matching every other persisted artifact; a crash
  mid-write can no longer corrupt it.
- json.Number prop values now match on the PENDING lookup path
  (LookupByNumber/LookupByNumberRange and Cypher numeric comparisons
  over unsealed layers) exactly as they already did after Seal — a
  pre-merge/post-merge parity gap, now pinned by tests.
- The rows-path BFS family in internal/traverse (fork/EnsureDepth/
  filledNoDepth/expandRowsSerial/expandRowsParallel and the visited
  pool) — a live production path — was never exercised by tests; now
  covered (package coverage 22.7% → 46.8%), along with the previously
  untested boolean typed-index anchor (`{on:true}` prop-map lookups).

### Changed

- `vectra.go` (4,100 lines) split along its natural seams into
  same-package feature files (`vectra_unique.go`, `vectra_recovery.go`,
  `vectra_snapshot_io.go`, `vectra_mirror.go`) — a pure file move with
  zero code-line deltas (verified by line-multiset proof) and identical
  allocation gates.
- The atomic-file-write ritual (temp/write/fsync/rename/dir-fsync) is
  single-sourced in `internal/atomicfile` (five former copies); temp
  files are now unique per writer (`os.CreateTemp`), and persisted
  artifacts carry 0600 pre-umask (stricter than the previous 0644/0666).
- The tiered-merge algorithm in internal/vector exists once (shared
  core with thin synchronous and folder-lane drivers, one compaction
  threshold); the vector `valsEq` row-dedup comparison is
  allocation-free (typed switch mirroring the encoder's tag structure;
  3 → 0 allocs per pair, pinned).
- `raceEnabled` build-tag pairs consolidated into `internal/racemode`;
  the five former copies are aliases. Cypher parser/lexer cleanups
  (hoisted write-clause guard, keyed error literals, honest lexer
  cur/adv naming, deduplicated key-expression/var-prop/position
  helpers) with allocation gates byte-identical (parse path 48 allocs
  pinned before and after).
- Modern-Go sweep on non-hot paths: `errors.AsType`, builtin `min/max`,
  `slices.SortFunc`+`cmp.Compare`, `slices.Clone`, `sync.OnceFunc`,
  `strings.CutPrefix/CutSuffix`; the server's join HTTP path shares one
  client with a per-request deadline; MCP errors carry a consistent
  `mcp:` prefix; `ErrSealed`/`ErrNotSealed` are marked Deprecated (no
  longer returned anywhere).
- New cold-path sentinels (`ErrInBatch`, vector-index validation
  errors) make ad-hoc rejections programmatically classifiable via
  `errors.Is`.

### Unchanged

- Wire formats (WAL frames, store layout, vecbin) byte-identical;
  allocation gates byte-identical (244/177/422/302/350/400 root, 48
  parse, 0 vector-dedup); no public API removals.

## [0.14.1] - 2026-11-01

Internal quality follow-ups to the unique-constraints feature; no API or
wire changes.

### Fixed

- `VerifyUniqueInvariantForTest` (test-only helper) no longer derives its
  expected constraint set from the registry it verifies — the key-set blind
  spot that hid a wrongly-dropped or wrongly-kept constraint is closed: the
  helper takes the expected constraints from the caller (`want ...`,
  zero args = expect none) and reports extra/missing by name.
- `scripts/goget-smoke.sh` now version-bumps from whatever the submodules
  currently require (previously it only rewrote the `v0.0.0` placeholder,
  so it could not run against a tree that had already shipped a release).

### Changed

- The unique-constraint violation scan on the write path is
  allocation-free (single pass tracking the order-minimal violated
  constraint, replacing a per-write candidate slice build + sort; reporting
  semantics and cross-replica determinism unchanged). A constrained
  `AddNode` now allocates exactly as much as an unconstrained one.

## [0.14.0] - 2026-10-31

The release where the write path grows into a language and the project ships
as a product. Cypher gains CREATE / SET / DELETE / DETACH DELETE / MERGE (with
ON CREATE SET / ON MATCH SET) and `CREATE [FULLTEXT] INDEX` DDL — read
statements keep the v0.13 pipeline byte-for-byte — and property-value
**unique constraints** (Neo4j-community semantics) enforce on every write
path. The server gains transport security and disaster recovery: HTTPS with
optional client-cert mTLS, X.509 mutual TLS for raft node-to-node traffic,
and a leader-served `GET /v1/backup` streaming a restore-ready tar.gz of the
raft snapshot. The first official Python (`vectra-graph`) and TypeScript
(`@projanvil/vectra`) SDKs cover the full REST surface, and the repo gains
`vectra-cli` (a Cypher REPL), a Docker image, CI over all three modules and
both SDKs, runnable examples, and a release guide with a tag-strategy smoke
test. The one deliberate change to an existing endpoint's error surface —
`/v1/batch` and `/v1/txn` failing on constraint violations — is flagged
under Changed below; with no TLS flags set the TLS planes are exactly the
previous plain-HTTP, plain-TCP ones.

### Added

- Unique constraints over `(label, prop)` pairs, enforced on every
  write path (Go API, `Batch`, Cypher writes, WAL replay, server raft
  apply). Cypher DDL — standalone statements, REQUIRE form only, no
  name form (the pair is the identity):

      CREATE CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE
      DROP CONSTRAINT FOR (n:Person) REQUIRE n.email IS UNIQUE

  The Go API mirrors them (`DeclareUnique` / `DropUnique` /
  `UniqueConstraints`; a duplicate declare fails with
  `ErrConstraintExists`, an unknown drop with `ErrConstraintNotFound`,
  and both are rejected inside `g.Batch`). Only `string` prop values
  participate — non-string values are invisible (a deliberate
  divergence from Neo4j, which errors on the wrong type). After
  declaration, a write that would give two live nodes of the label the
  same string value fails at the write-method entry with
  `*UniqueViolationError` (zero side effects for that operation);
  deleting a node frees its value immediately, and an override
  releases the old value first. Write summaries gain a
  `constraintsCreated` counter (0 or 1; a duplicate declare is an
  ERROR, unlike idempotent index re-declarations).
- Declaring validates existing data (the Neo4j community rule):
  `DeclareUnique` / `CREATE CONSTRAINT` scans full live state and
  fails atomically when the pair already holds a duplicated string
  value — the error lists up to ten duplicate value/key pairs (sorted,
  identical on every raft replica) and the constraint is not
  registered. The scan is O(live nodes) once per declaration.
- Constraint-DDL failures over `POST /v1/cypher` are client errors
  with dedicated wire faces (the VECTOR_INDEX_* precedent): a
  duplicate `CREATE CONSTRAINT` answers 409 `CONSTRAINT_EXISTS`, an
  unknown `DROP CONSTRAINT` 404 `CONSTRAINT_NOT_FOUND`, and a
  declaration scan over legacy duplicate data 400 `UNIQUE_VIOLATION`
  naming the `label.prop` pair — all three landed on 500 `INTERNAL`
  before this fix.
- Constraint declarations persist: WAL `IndexDecl` frames carry the
  full declaration list with replace-on-replay semantics (dropped
  constraints stay dropped across recovery), store snapshots stamp
  **v8** (v7 layout + trailing unique-constraint section; see the
  downgrade tripwire under Changed), and raft backup/restore
  (`GET /v1/backup`) round-trips them.

- HTTPS on the HTTP API (`-http-cert` + `-http-key`; either flag
  alone is a startup error) with optional client-certificate mTLS
  (`-http-client-ca` — clients without a cert from that CA fail the
  handshake; a client CA without the cert/key pair is a startup
  error too). Std-lib only (`crypto/tls`); unreadable or malformed
  cert/key/CA files abort startup naming the file — never a silent
  plaintext fallback.
- X.509 mutual TLS on the raft plane (`-node-cert` + `-node-key` +
  `-node-ca-cert`, all-or-nothing: a partial set aborts startup
  naming the missing flags) — every peer presents a leaf signed by
  the shared CA and verifies every other peer against it (TLS 1.2
  minimum). Rotation on both planes is restart-to-rotate (no hot
  reload). Client trust for a private CA rides the platform:
  `SSL_CERT_FILE` for the Go clients (`vectra-cli`, `vectra-mcp`),
  `NODE_EXTRA_CA_CERTS` for `sdk/ts` and the TS MCP flavor,
  `SSL_CERT_FILE`/`SSL_CERT_DIR` for Python; a joiner whose seed
  runs HTTPS trusts the seed's CA via `SSL_CERT_FILE` set before it
  starts.
- `GET /v1/backup` (leader-only; followers answer 503 `NOT_LEADER`
  with the `X-Vectra-Leader` hint) streams a chunked, never-in-memory
  tar.gz of the node's materialized raft snapshot — exactly
  `meta.json` + `snapshots/<id>/meta.json` + `snapshots/<id>/state.bin`.
  Restore per the README runbook: untar into an empty dir and boot
  `vectra-server -id <same-ID> -data <dir>`; a restored backup must
  never run concurrently with the original cluster (same node ID,
  empty raft log = fresh term) — disaster recovery / new clusters
  only, not fan-out.


- `vectra-cli` (`server/cmd/vectra-cli`) — a thin client of the
  server's `POST /v1/cypher` endpoint: a readline-style REPL with
  persistent history (`-history`, default `$HOME/.vectra_history`;
  rune-wise editing and arrow-key recall via `golang.org/x/term`, the
  one dependency this adds to the `server` module) and a one-shot
  `-c '<statement>'` mode. Statements end with `;` and may span
  multiple lines; `:help` lists the commands, and `:q` / `:quit` /
  `:exit` / `quit` / `exit` leave (recognized only on an empty
  buffer). Reads render an aligned table plus a `(N rows)` line;
  Cypher writes render the mutation summary (`Summary: created N
  nodes, ...`; a table too when the statement carried `RETURN`).
  Connection: `-url` (env `VECTRA_URL`, default
  `http://127.0.0.1:8080`), `-token` (env `VECTRA_TOKEN`, the bearer
  token for `-auth-token-file` servers), `-timeout` per request
  (default 30s). Piped stdin executes without prompts.
- `Dockerfile` (repo root) — multi-stage (`golang:1.27-alpine` build
  → `FROM scratch` runtime) single-node vectra-server image: non-root
  (uid 65534), `EXPOSE 8080 9000`, `VOLUME /data` created in the
  image already owned by the runtime user so the mounted volume
  inherits writable ownership (copy-up), default CMD
  `-id n1 -data /data -http :8080 -raft :9000` with the binary as
  ENTRYPOINT. Liveness is `GET /v1/health` from outside — no
  HEALTHCHECK directive, since the scratch runtime has no shell to
  run a probe with.
- GitHub Actions CI (`.github/workflows/ci.yml`) — a `go` job (vet +
  test for all three modules, the root module under `-race`, the
  Cypher allocation gate), `sdk-python` and `sdk-ts` jobs for both
  SDK suites, and an `examples` job running the three examples end
  to end.
- `examples/` — one scenario (a small project knowledge graph) in
  three languages: Go against the embedded engine (a durable WAL
  session in a scratch data dir; rerunnable against a kept `-data`
  dir) and the Python (`vectra-graph`) / TypeScript
  (`@projanvil/vectra`) SDK examples against a running server. Each
  variant batch-upserts Person/Project nodes and WORKS_ON edges,
  declares a FULLTEXT index and queries a CJK name through it, runs
  a Cypher write with `RETURN` plus a `MATCH ... RETURN` read, and
  answers a top-1 vector search.
- English benchmarks index (`docs/benchmarks/README.md`) — headline
  table and per-report index with the hardware each report ran on;
  every number copied verbatim from the linked report.
- `docs/RELEASING.md` — the three-module release guide (one
  version-prep commit carrying the root `vX.Y.Z`, `server/vX.Y.Z`,
  and `mcp/vX.Y.Z` annotated tags) plus `scripts/goget-smoke.sh`,
  which verifies the tag strategy by releasing into a throwaway
  local module mirror and `go get`ing all three modules from it.

### Changed

- **`/v1/batch` and `/v1/txn` now fail on a unique-constraint
  violation — a lane that previously could not fail on a node write.**
  The FSM's node lane used to silently skip a failing node upsert (the
  apply counter only incremented `if err == nil`); with constraints,
  that silent skip would answer 200 with the node missing. The apply
  now aborts on the first violation — deterministically on every
  replica (constraint state is log-derived; the vector
  shape-validation abort precedent) — and the request answers
  **400 `UNIQUE_VIOLATION`** with the violation payload (constraint,
  contended value, both keys) as the message. `/v1/cypher` gives a
  violating write statement the same 400 face (never 400 `PARSE`,
  never 500). Partial application is unchanged `Batch` semantics:
  entries before the violating one are applied, the violating entry
  has zero side effects, entries after it do not execute — no
  rollback.
- Store snapshots stamp **v8** (v7 layout + a trailing
  unique-constraint section; a v7 file is v8 minus that section).
  Pre-v8 readers — every v0.13.x binary — hard-fail on the version
  byte: libraries checkpointed by 0.14.0 cannot be downgraded to
  v0.13.x or earlier (same discipline as v5/v6/v7). v7-and-older
  files load unchanged (zero constraints, recompact-eligible to v8).
- `make build` now also emits `bin/vectra-cli` (previously
  vectra-server, vectra-bench, and vectra-mcp).
- New Make targets: `make race` (root module under the race
  detector), `make allocs` (the root Cypher allocation gate, single
  run), and `make goget-smoke [VERSION=x.y.z]` (the tag-strategy
  smoke above).

- Cypher write clauses. A statement is an optional read prefix (MATCH / WITH /
  UNWIND) feeding per-row write clauses left to right, plus an optional
  trailing RETURN over the post-write state (a deleted binding renders null);
  no read clause may follow a write clause in this version. Write-pattern
  prop values are expressions (`UNWIND [...] AS k CREATE (n:P {key:k})` is
  the bulk-import shape); SET items write one property each with `null`
  deleting the key; a plain DELETE of a node that still has edges fails
  before any write lands; MERGE trailers run per arm.
- Write results carry a summary: `createdNodes / createdEdges / deletedNodes /
  deletedEdges / propertiesSet / indexesCreated` (`CypherResult.Summary`;
  read statements leave it nil).
- Documented write identity & divergences: `key` in a write pattern's props
  is the node's logical key (extracted as identity, never stored as a
  property); a keyless CREATE draws a deterministic engine-generated `_c<id>`
  key (the `_c` prefix is reserved); CREATE on an existing key is an AddNode
  upsert (label and props replaced). MERGE matches a node by key only (the
  pattern's other props are creation-time) and an edge by its
  (src, dst, label) triple — an existing edge binds the whole pattern and
  suppresses endpoint creation. SET/DELETE on an edge variable addresses the
  whole triple (every parallel written by a SET, all removed by a DELETE;
  `DETACH DELETE a, r` no-ops r when a's cascade already killed it); the
  lowest-ID parallel rebind means a RETURNed edge scalar on a multigraph may
  render a different parallel's untouched props. Errors are
  first-error-stops with applied-writes-remain (the Batch contract — no
  rollback).
- `CREATE INDEX ON (n.prop)` / `CREATE FULLTEXT INDEX ON (n.prop)`: standalone
  DDL declaring an index GLOBAL per property key (label-scoped spellings are
  rejected), pending until the next Seal/merge, idempotent, counting one
  `indexesCreated` per applied statement.
- Server: `POST /v1/cypher` classifies each statement — reads stay on the
  local published snapshot with the exact pre-v0.14 response shape; writes
  replicate through raft exactly like `/v1/batch` (the leader's FSM
  re-executes the statement deterministically; follower/apply errors share
  the batch write faces) and return `{columns, rows, summary}` — never the
  read path's `count`. MCP: the `cypher` tool is annotated not-read-only;
  the Go server executes writes in-process and surfaces the summary
  counters (the Python/TS proxies pass them through).
- **Known limitation — Cypher writes are invisible to OCC validation.**
  Cypher write statements do not advance the server's per-key transaction
  version tables (the write summary carries counters, not touched keys), so
  an OCC/SI/SER `/v1/txn` batch cannot conflict-detect on keys written via
  Cypher. Until engine-level touched-key tracking lands, do not mix
  interactive `/v1/txn` traffic and Cypher writes on the same keys — keep
  them on disjoint key sets or route the contested writes through `/v1/txn`'s
  node/edge items.
- Python SDK `vectra-graph` (sdk/python) — synchronous, stdlib-only client
  (`urllib` transport; the wheel declares zero runtime dependencies,
  Python 3.9+, PyPI-ready pending the owner's publish). Covers the full
  REST surface: reads/traversals, batch, Cypher reads and writes (summary
  counters per statement), OCC `txn` with typed conflicts on both 409
  faces (`TxnConflictError` exposes the non-envelope report's conflicts/
  conflictTotal/hint), read-your-writes `await`, vector search + index
  CRUD, and Cypher index DDL helpers. E2E suite boots a real vectra-server
  (skips without a go toolchain; a serverless stub pins transport edges).
- TypeScript SDK `@projanvil/vectra` (sdk/ts) — zero-dependency client
  (native `fetch`, Node >= 18, strict-mode types with generated `.d.ts`,
  npm-ready pending the owner's publish) covering the same surface: union-
  typed Cypher results discriminated by `isCypherWrite`, `TxnConflictError`
  for the txn 409 report, `lastSnapshot` tracking `X-Vectra-Snapshot`
  (error responses included), vector search + index CRUD, index DDL. E2E
  suite is node:test against a real vectra-server. Both SDK packages
  version at 0.14.0, aligned with the server.

## [0.13.1] - 2026-10-29

### Fixed

- `go test -race ./...` is green end to end again. Two race-suite hygiene
  items, both pre-existing since the v0.12/v0.13 arcs: the vector package's
  600s budget died on `TestStateNoPhantomRetention` (8192x768 HNSW
  construction runs ~30x under race instrumentation — 32s to 15m+; the test
  is single-goroutine, so -race adds no coverage, and the Pack/merge paths
  stay race-covered by the tier-cascade and death-compaction tests — it now
  skips under race via the `raceEnabled` build-tag pair, mirroring the root
  package's alloc-bound precedent), and the A7 recovery gate asserted its
  3s line under -race, where instrumentation pushes a healthy 100K recovery
  to ~2.9-3.0s (under `raceBuild` the line now logs with a 30s pathology
  ceiling; the functional assertions and the non-race 3s contract are
  unchanged).
- Post-v0.13 cleanup bundle: gofmt on 4 pre-existing files, the stale
  "snapshot v6" error string and comment (writes are v7), `snapshotIsV6`
  renamed to `snapshotAtLeastV6`, a duplicated fragment in the 0.11.4
  changelog entry, and the missing zh-CN README vector-search section.

### Added

- Cypher `CONTAINS` anchor pushdown over full-text indexes: a top-level
  `WHERE n.key CONTAINS 'literal'` conjunct now narrows the anchor scan
  through the bigram buckets when `key` is FTS-declared. Semantics are
  byte-identical — the prefilter is a case-insensitive superset and the
  per-row case-sensitive `CONTAINS` re-check still decides — so queries on
  declared keys stop full-scanning while undeclared keys scan exactly as
  before (OR/NOT branches and non-literal needles are simply not lifted).
  The existing alloc pins are untouched (244/177 on the 1-hop shapes;
  hint extraction allocates only when a hint is found).
- `vectra-bench -auth-token-file` (http mode): enables bearer auth on the
  bench's own in-process node and attaches the first token to every client
  request, so the auth middleware's overhead shows up in workload numbers.

## [0.13.0] - 2026-10-26

The Cypher clause pipeline: queries are no longer one MATCH - they are a
clause chain. WITH scopes and projects, UNWIND fans lists out, OPTIONAL
MATCH keeps unmatched rows, MATCH repeats (cartesian disjoint variables,
join shared ones), and RETURN/WITH trailers gain SKIP. The parser emits a
clause-list AST validated through a scope chain and executed by a
stage-loop row executor with copy-on-write bindings; the first four
clauses parse and execute with zero clause-slice allocations. Release
gate (two-worktree A/B vs `1ec9508`, back-to-back, medians of 3 x 2s,
two rounds): every legacy shape within +5% ns/op with allocs/op
byte-identical - 1-hop named +4.3% (244 = 244 allocs), 1-hop anonymous
+4.4% (177 = 177), var-length +3.0% (272 = 272), undirected +2.7%
(312 = 312); worst single round +4.8%.

### Added

- `WITH [DISTINCT] items [ORDER BY ...] [SKIP n] [LIMIT n] [WHERE expr]`:
  projects into a NEW scope - only projected names survive. Bare
  variables pass their entity binding through under their own name or
  alias; `var.prop` and aggregation items require `AS` and project
  scalars. `DISTINCT` dedups the projected rows; the trailer
  (ORDER BY/SKIP/LIMIT) orders and windows them; a trailing `WHERE`
  filters AFTER the projection (references projected names only).
- `UNWIND expr AS alias`: fans each row's list value to one row per
  element - the expression may be a list literal, a bare variable naming
  a list-valued scalar (typically a `collect()` alias), or `var.prop`.
  Null/empty lists drop the row (strict stage semantics); the alias
  binds a scalar and the current scope survives.
- `OPTIONAL MATCH`: a row whose whole clause (every comma pattern plus
  the clause's WHERE) matches nothing is kept unchanged, the clause's
  pattern variables reading null through RETURN/WITH/UNWIND. Optionality
  is all-or-nothing across the comma patterns; the merge does not dedup
  across base rows; a leading OPTIONAL MATCH anchors from one implicit
  empty row.
- Repeated `MATCH` clauses: disjoint variables cartesian-product, shared
  variables join; stages are strict - a stage whose input is zero rows
  yields zero rows and never re-anchors. `SKIP n` also lands in the
  RETURN trailer (after ORDER BY, before LIMIT).
- MCP `cypher` tool description and READMEs (en/zh-CN) document the
  clause grammar, the scope rules, and the divergences (below).

### Fixed

- A pattern variable first declared under `OPTIONAL MATCH` is null on
  the rows the optional expansion missed; using it as a pattern variable
  in a later NON-optional MATCH silently cartesian-anchored those rows
  (Neo4j yields zero). Validation now tracks a nullable-var set: fresh
  OPTIONAL declarations are nullable (pre-bound join variables are not),
  a bare-variable WITH item carries nullability through (var.prop,
  aggregations, and WHERE filters do not clear or carry it), and the
  offending later MATCH is a positioned error suggesting the two
  restructures - fold the pattern into the OPTIONAL MATCH or mark the
  later MATCH optional (`IS [NOT] NULL` is not in the grammar). Caught
  in the T6 review before release.

### Performance

- A/B vs `1ec9508` (two-worktree protocol, `bench/cypher_bench_test.go`
  copied into the main worktree, quiet solo runs, 3 x 2s, two
  back-to-back rounds each side, six-sample medians):

  | Shape | main ns/op | v0.13.0 ns/op | delta | main allocs | v0.13.0 allocs |
  |---|---|---|---|---|---|
  | 1-hop named | 13,350 | 13,927 | +4.3% | 244 | 244 |
  | 1-hop anonymous | 10,619 | 11,085 | +4.4% | 177 | 177 |
  | var-length | 14,788 | 15,238 | +3.0% | 272 | 272 |
  | undirected | 14,808 | 15,213 | +2.7% | 312 | 312 |

  Verdict: PASS - all shapes within the +5% ns/op line (worst single
  round +4.8%, 1-hop named), allocs/op identical on every shape and
  equal to the T1 pre-refactor anchor; `TestCypherViewPathAllocs` pins
  244/177 on the branch. B/op is +736 on every shape at the same alloc
  count (clause-pipeline parse structs), which the identical allocs/op
  line covers. New-shape allocation gates: WITH pass-through 422 <= 530,
  WITH aggregate 302 <= 380, UNWIND 350 <= 440, OPTIONAL null-extension
  400 <= 500 allocs/query; queries without an OPTIONAL MATCH keep their
  exact pre-pipeline allocation footprint (the nullable-var map is
  lazy).
- Zero-alloc clause storage: the clause list is inline for the first
  four clauses and spills to a heap slice only at the fifth - realistic
  MATCH/WITH/UNWIND pipelines parse with zero clause-slice allocations.

## [0.12.0] - 2026-10-26

Auth and full-text search: the HTTP surface gains optional bearer-token
auth, and the engine gains opt-in full-text (substring) property indexes -
case-insensitive bigram buckets built at Seal time, persisted through WAL
frames and store v7. Release gate (`BenchmarkSealFTS100K`, 100K nodes/1M
edges, one key, ~32-rune mixed CJK/Latin values): the with-FTS clone+seal
cycle at 1.685x the without-FTS line, +162 MB/op, 0.339 s/op absolute
(lines: <=2x, <=250 MB, <=1.5 s).

### Added

- HTTP bearer-token auth (`-auth-token-file`): tokens load once at
  startup, one per line, multiple lines all accepted (rotation = append
  the new token to every node, roll restarts, remove the old); blank
  lines and CRLF tolerated, internal whitespace / unreadable / empty
  file aborts startup (fail-fast - auth never degrades into a silently
  open database). Every route requires `Authorization: Bearer <token>`
  (SHA-256 digests, constant-time compare, no early exit) except exactly
  `GET /v1/health`, the load-balancer probe. Join requests carry the
  joining node's token - one trust domain, every node shares the token
  file. The raft channel stays plaintext: HTTP auth never protects
  inter-node raft TCP; bind `-raft` to a private interface.
- Full-text property search: `Graph.IndexFullText(key)` declares a
  string property key for substring lookup, `Graph.LookupByContains(key,
  substr)` answers case-insensitively from seal-built bigram gram
  buckets (a value of m runes posts m-1 deduped two-rune grams - mixed
  CJK/Latin works naturally; every hit re-verifies with a real Contains,
  so the prefilter never returns false positives). The declaration
  contract mirrors IndexProperty (pending until the next merge, pending
  keys answer via scan; idempotent; WAL-logged) and persists in store v7
  snapshots; a key may be both typed- and full-text-indexed. Undeclared
  keys and empty needles answer (nil, false); Cypher `CONTAINS` is
  unchanged (case-sensitive scan). Memory: ~24-30 B per (gram, id) - 1M
  nodes x 100-char values lands around 3-5 GB; strictly opt-in.
- MCP: `index_fulltext` / `lookup_contains` tools (Go server, 14 tools)
  and `VECTRA_TOKEN` env in the Python/TS proxies (sent as the bearer
  token when the server runs auth).
- `BenchmarkSealFTS100K` (bench/clone_test.go): the FTS seal release
  gate - identical 100K-node datasets with and without one FTS
  declaration; the delta isolates the seal-time gram-bucket build.

### Changed

- Store snapshots stamp **v7** (the FTSKey section appended after the
  float32 section; a v6 file is v7 minus that section). Pre-v7 readers -
  every v0.11.x binary - hard-fail on the version byte: libraries
  checkpointed by v0.12.0 cannot be downgraded past v0.11.x (same
  discipline as v5/v6). v6-and-older files load unchanged; legacy v1-v5
  bases keep migrating in place on first open (`.bak` rollback artifact
  retained), and a v6 base upgrades to v7 at its next compact - light
  checkpoints over a v6 base stay legal (tier-gate fix below).

### Fixed

- The light-checkpoint legality probe compared the base's store version
  exactly (`== v6`), so a v7 base would have been misjudged as needing
  the full-snapshot compact tier; the probe now accepts v6-or-newer
  (`>=`), keeping light checkpoints O(1) across store-version bumps
  [d49d11e].

### Performance

- The FTS seal build is allocation-lean: interned gram strings, flat
  extraction with presized postings, counting-sort ordering - the seal
  gate moved from 5.12x / +653 MB (first round, failed) to 1.685x /
  +162 MB / 0.339 s (pass) [8c415ec].

## [0.11.4] - 2026-10-26

The 1M arc: direct measurement, three structural fixes, and the retest
that verifies them at scale. Ingest accept 3.6x, compact-tier stalls
-98%, and the worst-case restart (2h57m30s) eliminated. Measurement
records: `docs/benchmarks/2026-10-23-vector-1m.md`,
`docs/benchmarks/2026-10-25-1m-fixes-retest.md`; design:
`docs/superpowers/specs/2026-10-24-1m-fixes-design.md`.

### Fixed

- Light Close no longer drops converged pack work. The forest cache
  only advanced at compacts, so a wrapper session that converged its
  vector lane and closed light left the on-disk forest at the last
  compact's state - reopen restored its frozen overlay wholesale and
  re-paid the pack (1M direct measure: 970,751 entries, 2h57m30s).
  Close-shaped checkpoints (`SealForClose`, `Graph.Close`'s light
  branch) now refresh the cache when packs drifted since its last
  write; an idle no-drift close stays O(1) [5b90f6b].
- Recovery no longer re-packs forest-covered frames. The WAL tail
  replay re-intook chain frames the mounted forest already carried -
  vid dedup kept it correct but the copies re-packed (the bench-scale
  repro reopened with overlay=7616 despite a fresh forest). The
  mounted image's watermark now gates the vector intake during replay
  (store frames still apply - props are not in the forest) [5b90f6b].

### Performance

- Volume-triggered compaction goes async. The compact tier's
  snapshot+forest bytes (11.5-26.3s inside the writer's afterBatch at
  1M, 12 occurrences) now run on a background flight over the pinned
  immutable generation; the manifest and WAL mutations complete on the
  writer - quiescent flights Reset exactly as before, busy flights
  (frames appended during the flight) re-anchor v2-style and drop only
  fully-covered segments, so fsynced frames past the pin are never
  deleted. One flight at a time; light seals are suppressed during a
  flight (growth bounded by flight duration x ingest rate, with a
  backpressure valve at CheckpointBytes*4+1GiB) [42eef42, 7efea7b]. The forest
  ENCODE still serializes with submits under the vector index mutex -
  measured at 1M: submit max 11.70s during flights (vs the 11.5-26.3s
  stall every submit used to pay; retest:
  docs/benchmarks/2026-10-25-1m-fixes-retest.md - ingest accept
  5,938 -> 21,459/s, compact-tier submit stall p50 11.47s -> 190ms,
  B1 reopen ready 2h57m30s -> 0s, recall unchanged).

### Changed

- Spec 6.3's "1M x 768 p99 < 10ms" budget line is revised to the
  direct measurement: ef64 (the production adaptive ef) measures
  12.16ms at 1M. The latency-first knee is ef=48 (recall@10 0.968,
  p99 ~8-10ms); closing the 2-3x gap to hnswlib-class engines (SIMD
  dot product, level-0 layout) is a separate optimization arc.

## [0.11.2] - 2026-10-22

Checkpoint goes O(1): the retained-WAL incremental chain replaces the
full snapshot on every seal, wrapper-mode vector ingestion and Close
stop paying the O(N) snapshot tax, and Close no longer blocks on the
vector drain. Full data: `docs/benchmarks/2026-10-22-v0112-checkpoint.md`.

### Performance

- Interruptible vector drain at Close: `drainVectors` now checks quit
  at chunk boundaries, so shutdown waits at most one in-flight chunk
  (vector convergence stays lazy per spec D10 - a reopen resumes it in
  the background). Wrapper Close 39.2s -> 3.0s on the slow reference
  box [9c8c8fb].
- Cypher copy-on-write row bindings: `row.clone()` shares the parent's
  binding maps and copies exactly once, lazily, at the first write
  (anonymous expansion 243 -> 177 allocs/query; join-branch clones are
  free) [4c7ddd3].

### Added

- O(1) light checkpoints - the retained-WAL incremental chain. A
  checkpoint now rotates the WAL (`wal.Rotate`) and rewrites the
  manifest (v2 `RetainFrom` names the first retained segment) instead
  of writing a snapshot; recovery replays base + retained chain + live
  tail. Compaction (the full-snapshot tier) fires at 256MiB of
  retained bytes, bounding recovery replay volume. Wrapper
  default-config vector ingestion 10.2k/s -> **27.4k/s**, seal stall
  p50 119.8ms -> **10.8ms** (a WAL-segment rotation, not a snapshot),
  Close 7.4-9.8s -> **~1s**; gate-2b lines and the 100k restart
  parity (498ms load vs 7m39.1s rebuild, 922.9x) held.

### Changed

- Store snapshots stamp **v6** (version-only bump; layout identical)
  and the WAL manifest is v2. v0.11.1 and older binaries hard-fail on
  v6 bases - libraries checkpointed by v0.11.2 cannot be downgraded
  (same discipline as v5). v4/v5 libraries migrate in place on first
  open (the pre-migration watermark snapshot is retained as a
  `<watermark>-v<version>.bak` artifact); fresh libraries write a v6
  tripwire from byte one. Downgrade-before-migration is safe; after,
  not.

## [0.11.1] - 2026-10-22

### Performance

- Cypher view-path expansion is allocation-free: the last gap from the
  row-path zero-alloc work (2026-10-19) — the cypher `directions` zip —
  now reuses per-executor scratch via the same rowSink pattern (1-hop
  MATCH at fan 32: 545 → 244 allocs/query, -55%; the expansion accessor
  itself holds a steady-state zero-allocation pin). Two semantics-neutral
  companions landed with it: pseudo-property predicate comparison without
  boxing, and a pairwise fast path for row deduplication under 64 rows.
  Concurrent-cypher tail and M-w16 mixed numbers:
  docs/benchmarks/2026-10-22-cypher-zero-alloc.md.

## [0.11.0] - 2026-10-21

Vector Phase 2: vectors ride a binary WAL/store channel, the HNSW
forest persists across restarts, and the vector surface reaches REST
and MCP. 768-dim frames shrink to a third of their JSON size, vector
write throughput goes 26k/s -> 68.8k/s, and a 100k-vector restart
loads in 455ms where it used to rebuild for 7m19s. Reference docs:
`docs/benchmarks/2026-10-21-vector-phase2-p21.md`,
`docs/benchmarks/2026-10-21-vector-phase2-p22.md`,
`docs/benchmarks/2026-10-21-wrapper-checkpoint-cadence.md`.

### Added

- Binary vector channel: WAL frame type 4 (`VVB2` self-describing
  envelope) and store v5 snapshots (declarations-first, binary float
  section) - 768-dim frames shrink 9.29KB -> 3.07KB, snapshots
  74.1MB -> 24.4MB @8192 vectors; vector write throughput 26k/s ->
  68.8k/s, batch-boundary p50 21ms -> 7.0ms.
- Forest persistence (`<db>.vec`, watermark-gated): 100k-vector
  restart 7m19s rebuild -> 455ms load (964x); WAL tail replays onto
  the loaded forest; mismatched/corrupt caches rebuild transparently.
- Vector REST endpoints: POST /v1/vector/search (query_vector |
  similar_to, k≤1000), POST/GET /v1/vector/indexes, DELETE
  /v1/vector/indexes/{name}; raft-replicated explicit index ops;
  TS/Python MCP clients gain vector_search, vector_index_create/drop/
  list.
- `writer.Handle.DeclareVectorIndex` / `DropVectorIndex` (embedded
  wrapper declaration lane).
- `Graph.Clone()` mounts read-only vector snapshots (server clones
  serve vector queries).

### Changed

- Migration-on-open: legacy (v4) libraries checkpoint to v5
  immediately on first open; fresh libraries write a v5 tripwire from
  byte one. v0.10 binaries hard-fail on v5 snapshots - migrated
  libraries cannot be downgraded (release notes: finish migration
  before any downgrade attempt; a failed migration leaves the library
  v4-readable and retried on next open). The last pre-migration
  watermark snapshot is retained as a `<watermark>-v<version>.bak`
  rollback artifact (e.g. `v-2.db-v4.bak`; copied before the migration
  seal; recovery never reads it, later seals never delete it); the
  migration open transiently costs ~3x snapshot size on disk (old +
  new + copy).
- Default CheckpointBytes 4MiB -> 64MiB (measured cadence:
  `docs/benchmarks/2026-10-21-wrapper-checkpoint-cadence.md`).
- /v1/batch and /v1/txn now accept JSON numeric arrays as vector props
  (previously failed at raft encoding).
- Vector search k cap raised 100 -> 1000 (default 10 unchanged)
  across REST and MCP surfaces, per industry comparison.

### Unsupported

- Mixed-version raft clusters: v0.11 leaders produce v5 raft snapshots
  that v0.10 followers hard-fail to restore. Upgrade all nodes
  together (stop-the-world, or all followers before the leader).

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.11.2...HEAD
[0.11.2]: https://github.com/projanvil/vectra/compare/v0.11.1...v0.11.2
[0.11.1]: https://github.com/projanvil/vectra/compare/v0.11.0...v0.11.1
[0.11.0]: https://github.com/projanvil/vectra/compare/v0.10.0...v0.11.0

## [0.10.0] - 2026-10-20

Vector Phase 1: the index becomes a first-class embedded subsystem.
Declarations persist (WAL + store v4), writes intake through the skip-
and-warn rules, queries run zero-lock over immutable snapshots, and
background folder lanes pack without ever blocking the writer. Gate 2b:
write throughput 23-26k/s against the 20k line, plain-write p50 within
101% of an unindexed graph, integrated recall@10 = 1.0000 at ef
32/64/128. Full data: `docs/benchmarks/2026-10-20-vector-phase1-gate2b.md`.

### Added

- `Graph.CreateVectorIndex` / `DropVectorIndex` / `VectorIndexes` /
  `QueryVector` (+ `QueryVectorOptions`, `VectorHit`,
  `Graph.VectorForNode`, `Options.OnVecSkip`): declaration API and
  zero-lock top-k with adaptive ef and the 3x-oversample label
  post-filter (spec §9.1/§13-1). Mid-batch declarations are rejected
  (their replay semantics would diverge); documentation on the methods.
- WAL `IndexDecl.VectorIndexes`: full-list declaration frames, drop =
  absence; `null` vs empty-array keeps keys-only frames byte-identical
  (old binaries replay them untouched).
- Store format v4: vector declaration section after indexKeys. v3/v2/v1
  files load unchanged; **a v4 file opened by a pre-v0.10 binary hard-
  fails on the version byte** - plan upgrades before downgrades.
- Folder lanes (spec D10): direct mode packs inside the fold loop;
  wrapper mode gets an independent vecSig on the merger goroutine that
  the write-stall path never observes. Draining is two-phase (short
  lock to capture, lock-free HNSW build, short lock to commit) - the
  writer's intake never waits on graph building (spec D5; the
  single-lock first cut measured 601/s, the two-phase cut 26k/s).
- `internal/vector`: per-goroutine search Reader (concurrent-search
  prerequisite), immutable Snapshot model (chunked overlay, chunked
  dead bitmap, key arrays off the read path), recovery drain with the
  overlay as the exact fallback.
- MCP tool #12 `vector_search` (query_vector or similar_to, k ≤ 100,
  approximation note on every response).

### Changed

- Recovery rebuilds vector forests bottom-up after Load/Open (spec
  §7.1); until fully packed, the overlay's linear scan serves queries
  exactly.

### Fixed

- None beyond the above; the audit trail (five code-reviewer audits,
  every must-fix landed with a regression test) is in the commit log.

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.10.0...HEAD
[0.10.0]: https://github.com/projanvil/vectra/compare/v0.9.0...v0.10.0

## [0.9.0] - 2026-10-20

A vector-index and throughput release: the embedded pure-Go HNSW lands
as Phase 0 internals, its build path gets 7.8x faster after a
profile-driven optimization pass, and the traversal row path becomes
allocation-free. 100K x 768 single-threaded builds drop from 1h40m to
12m54.6s with recall@10 = 1.0000 at ef 32/64/128 and query p99 still
28-31% better than the pre-optimization baseline; the 1M verification
builds in 3h38m with gate-2a lines passing directly (recall 0.9960,
p99 7.78ms at the production ef=64). M-w16 mixed-read p99 42.27 ->
21.27/22.05ms, QPS +32-38%. Full data:
`docs/benchmarks/2026-10-19-vector-build-optimization.md` and
`docs/benchmarks/2026-10-19-row-path-zero-alloc.md`.

### Added

- `internal/vector`: embedded pure-Go HNSW (diversity-heuristic
  linking, epoch-stamped visited set), tiered segment State (overlay +
  size-tiered forest + dead set), and the JSON-safe `NormalizeVec`
  intake layer — the Phase 0 spike of the vector-search design. Not
  yet wired into the public API (Phase 1); zero new dependencies, no
  cgo.
- `engine.View.RowsInto`: buffer-reuse form of `View.Rows` — steady
  state allocates nothing, keeping concurrent BFS workers off the
  allocator. `View.Rows` keeps its fresh-allocation contract.
- Tier-merge fan-in raised from 2 to 4 (tier holds <=3 segments; the
  4th arrival merges all four — spec 4.2 amended to match its capacity
  column): write amplification 5.59x -> 2.97x at 100K, 8.19x -> 4.42x
  at 1M. Live-segment upper bound moves 10 -> 15.
- Build-rate gate driver `TestBuildThroughput` (`VECTRA_BUILD_BENCH=1`)
  and kernel micro-bench `BenchmarkDistance`.

### Changed

- Distance kernels unrolled with eight independent accumulators
  (768-dim cosine 577 -> 92 ns, 6.3x); L2 no longer pays the wasted
  dot-product loop. Build-path allocation churn cut 2.77GB -> 507MB
  per 10K build (hand-rolled heaps, per-HNSW scratch, no double
  normalize on the rebuild path).
- Traversal row path is allocation-free: `viewRow` carries
  per-goroutine scratch (parallel expansion forks one per worker) and
  the single-implementation `rower` interface becomes `*viewRow`.
- `pruneTo` sorts its <=33-element candidate lists with a typed
  insertion sort (primitive 4.5x; the reflect-swapper allocation is
  gone).

### Fixed

- The v0.8.0 documented residual "M-w16 read p99 ~44ms from allocator
  contention on the row path": 16 parallel BFS workers allocated 4-6
  slices per expanded node (`View.Rows` append churn plus the
  three-slice zip); with `RowsInto` and per-worker scratch the
  mixed-read p99 halves (42.27 -> 21.27/22.05ms across two samples)
  and QPS rises 32-38%.

[Unreleased]: https://github.com/projanvil/vectra/compare/v0.9.0...HEAD
[0.9.0]: https://github.com/projanvil/vectra/compare/v0.8.0...v0.9.0

## [0.8.0] - 2026-10-19

A performance-focused release: the read path no longer collapses under
sustained writes, point reads are ~20x faster, and GC pressure is halved.
Benchmarks against the 2026-10-16 baseline (M tier, 100K nodes / 1M edges,
8 workers, 200 x 50-edge write batches/s): mixed-read p50 109ms -> ~0.2ms,
p99 1.42s -> ~15ms; writes hold the offered rate with p99 ~5ms;
unbatched import 1.56M edges/s. Full data and methodology:
`docs/benchmarks/2026-10-16-perf-baseline.md` and
`docs/benchmarks/2026-10-17-perf-fixes-retest.md`.

### Added

- `delta.FoldLayers`: semantics-preserving layer-stack compaction primitive
  (winner records keep their IDs, tombstone sets union, differential-tested
  against a random oracle).
- Direct-mode layer-stack governor: `Graph.SetAutoCompact` /
  `Graph.SetAutoSealEdges`. Folds run on a lazily started background
  goroutine (LevelDB/RocksDB/Pebble model adapted to the single-writer
  core: pure computation plus an atomic hand-over, publication stays with
  the writer; no locks added to the core — A8 unchanged).
- Size-tiered fold scheduling: F/T/tail runs, half-size merge rule
  (amortized constant record copies), tail volume gate, layer-count cap,
  and volume-proportional seals at Batch boundaries.
- Per-generation view slots: the writer fills a generation's three-source
  View before publishing it, so readers of a fresh generation always hit.
- Incremental view prebuilding: pure appends extend the previous build
  state at O(new records); layers above 4096 edges (bulk-import windows)
  skip prebuilding — import-shaped workloads pay nothing.
- `engine.KillSet` and `engine.NewViewWithKills`: copy-on-write chunked
  kill-set bitmaps; per-publish view assembly aggregates only new kills.
- vectra-bench: mixed read/write workload with a paced single writer
  (`-writerate`/`-writebatch`), bfsall TEPS (GAP/Graph500 normalization),
  phase-boundary memory/GC sampling, snapshot-restore cold-start phase,
  and a `-profile` pprof attribution mode.

### Changed

- `NeighborsOf` dedups small rows by linear scan and allocates a local
  bitmap for hub-sized rows (was: one whole-graph `[]bool` per call).
- `ShortestPath` pools its parent arrays (`parent[v] = u+1`, zeroed
  acquire replaces the fill loop) and reuses frontier scratch buffers
  (CSR-hot-path allocations 44 -> 14 measured).
- BFS results materialize their depth array lazily on first
  `Depth`/`Reached` use; `Layers`/`Count` consumers allocate nothing
  ID-space-sized.
- Acceptance gate A2 recalibrated: the allocation fixes collapsed the
  idle p99 floor ~45x, mechanically breaking the <=2x ratio line; it now
  also accepts the recorded pre-fix idle floors.

### Performance

- Mixed read/write (M tier): read p50 109ms -> ~0.2ms (~530x), read p99
  1.42s -> ~15ms (~95x), read QPS 31 -> ~4800; write p99 68ms -> ~5ms.
- Point reads: `NeighborsOf` QPS 376K -> 7.8M (p99 250µs -> 4.3µs); at the
  L tier (1M/10M) p50 31.5µs -> ~0.5µs.
- Traversals: bfsall p99 13.2ms -> 6.6ms, bfs3 p99 3.64ms -> 1.94ms,
  reach p99 3.08ms -> 1.68ms (M tier).
- GC CPU during read workloads halved (10.5% -> 5.4% at M, 14.7% -> 6.5%
  at L); cumulative read-path allocations dropped from 1.24TB to a
  fraction over the same runs.
- Unbatched import: 1.56M edges/s (A4 floor 1.0M).

### Fixed

- Read collapse under sustained writes: per-publish view assembly used to
  re-aggregate the whole unmerged stack (and a single-slot cache let
  concurrent readers force full rebuilds on every publish); the
  layer-stack governor plus per-generation slots plus incremental
  prebuilding remove the collapse at any stack depth or volume.
- Writer throttling: folds, snapshot copies, and kill-set re-aggregation
  no longer run on the writer goroutine (write rate held at the offered
  ceiling; p99 60ms -> ~5ms).
- Known residuals (documented in the retest report, not fixed here):
  M-w16 read p99 ~44ms from allocator contention on the row path; mirror
  string-map constant in the write path (B3).

[0.8.0]: https://github.com/projanvil/vectra/compare/v0.7.1...v0.8.0

## [0.7.1] - 2026-10-16

Hotfix: review fixes (WAL rollover fsync, BFS empty-layer guard, bKilled
counting, MCP snapshot openErr) and WAL IndexDecl frames (frame type 3).

[0.7.1]: https://github.com/projanvil/vectra/compare/v0.7.0...v0.7.1

## [0.7.0] - 2026-10-15

Continuous writes: immutable micro-layer generations, three-source
traversal views, single-writer wrapper with write-stall, embedded WAL with
watermark checkpoints, Cypher aggregation.

[0.7.0]: https://github.com/projanvil/vectra/compare/v0.6.0...v0.7.0

## [0.6.0] - 2026-10-13

OLTP transactions: group commit, SI/SER transaction guards, RYW await.

[0.6.0]: https://github.com/projanvil/vectra/compare/v0.5.0...v0.6.0
