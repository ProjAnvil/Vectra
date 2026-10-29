# Python + TypeScript REST SDKs (P0b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** First-class, zero-dependency REST client SDKs for Python (PyPI `vectra-graph`) and TypeScript/JavaScript (npm `@projanvil/vectra`), covering the complete v0.14.0 server surface — including Cypher writes with summaries, OCC transactions with typed conflict errors, await, vector CRUD, and index DDL — so non-Go developers can use Vectra without hand-writing HTTP.

**Architecture:** Two new self-contained trees `sdk/python/` and `sdk/ts/` wrapping the existing REST API (server/api.go routes, unchanged). Python: sync client on stdlib `urllib.request`, typed errors, `unittest` E2E against a self-provisioned `vectra-server` process. TS: promise client on native `fetch` (Node 18+), `node:test` E2E with the same harness pattern. No Go code changes.

**Tech Stack:** Python 3.9+ stdlib only (pyproject/setuptools packaging); TypeScript strict ESM (tsc, zero runtime deps, `node:test`).

**Spec:** docs/research/2026-10-29-lightweight-adoption-assessment.md §3.2 (P0b ruling: "Python/TS 正式 REST SDK（补 txn/await/删除/索引面，发 PyPI/npm）") + §4 P0b; wire truth = server/api.go + server/batch.go on this branch.

## Global Constraints

- **Zero runtime dependencies in both SDKs.** Python imports stdlib only; the built wheel installs nothing else. `sdk/ts/package.json` has `"dependencies": {}` and uses native `fetch` (engines: node >= 18).
- **No Go/module changes**: server, mcp, root untouched except README/CHANGELOG (Task 4). All three Go modules' suites stay green.
- **Wire fidelity is the correctness bar**: JSON keys byte-match the server (see Interfaces); E2E tests run against a real `vectra-server` binary — no mocks.
- **Process hygiene**: the E2E harness must never leak server processes or fixed ports — allocate a free port before spawn, kill in teardown (including on test failure), and skip the suite with a clear message when `go` or the server binary is unavailable (`VECTRA_SDK_TEST_BIN` overrides the build step).
- English code/comments/tests and SDK READMEs; the root README gains a bilingual SDK section (Task 4).
- SDK versions track the engine: **0.14.0**.
- Publishing (PyPI/npm upload) is the owner's action; this arc ships publish-ready packaging + a documented release command.

## Wire Contract (verified against server/api.go @ cef540b — the SDKs' source of truth)

- Base: `Authorization: Bearer <token>` optional; errors are always `{"error": {"code": "<CODE>", "message": "<msg>"}}` with an HTTP status.
- `GET /v1/health` → `{"status":"ok",...}`; `GET /v1/stats` → `{"nodes","edges","sccs","cyclicNodes",...}` (+ leader/term fields).
- `GET /v1/nodes/{key}` → node (`key`,`label`,`props`) or 404 `NODE_NOT_FOUND`.
- `GET /v1/neighbors?key=&dir=&label=` → neighbors list (shape per handler).
- `POST /v1/query` `{"from":[...],"labels":[...],"direction":"out|in","maxDepth":n}` → traversal result.
- `GET /v1/path?from=&to=` → `{"found":bool,"nodes":[...]}`-shaped result; `GET /v1/reachable?from=` → set; `GET /v1/cycles` → `{"cycles":[{"nodes":[...]}]}`.
- `POST /v1/cypher` `{"query": q}` — READ → `{"columns","rows","count"}`; WRITE → `{"columns","rows","summary":{"createdNodes","createdEdges","deletedNodes","deletedEdges","propertiesSet","indexesCreated"}}` (never `count`).
- `POST /v1/batch` `{"nodes":[NodeUpsert],"edges":[EdgeUpsert],"edgeDeletes":[EdgeDelete],"nodeDeletes":[key]}` → applied counts. **NodeUpsert/EdgeUpsert/EdgeDelete field names: read server/batch.go (the TxnGuard block is at :52; upsert types are adjacent) and mirror exactly.**
- `POST /v1/txn` — batch body + `"txn": TxnGuard` where `TxnGuard = {"snapshot": uint64, "ser": bool, "readNodes": [key], "readEdges": [EdgeRef]}`; conflict → HTTP 409 with a conflict code (find the exact code in server/node.go / api.go error mapping and type it as `VectraConflictError`).
- `GET /v1/await?seq=&timeoutMs=` → `{"publishedSeq","reached"}` + `X-Vectra-Snapshot` header.
- `POST /v1/vector/search` `{"index","query_vector"| "similar_to","k","ef","label"}` → `{"count","hits":[{"key","score",...}]}`.
- `GET/POST /v1/vector/indexes`, `DELETE /v1/vector/indexes/{name}` — create body `{"name","prop","dim","metric","m","efConstruction"}` (metric cosine|l2|dot; duplicate → 409 `VECTOR_INDEX_EXISTS`).
- Index DDL (typed/FTS) over Cypher: `CREATE INDEX ON (n.prop)` / `CREATE FULLTEXT INDEX ON (n.prop)` — the SDK's `index_property` / `index_full_text` helpers are thin Cypher writers (declarations pending until the next Seal — document).

## Review Focus

1. **Error envelope fidelity** — every non-2xx response raises the typed error carrying the server's code/message/status (test: invalid Cypher → `PARSE`; unknown node → 404 `NODE_NOT_FOUND`; duplicate vector index → 409 `VECTOR_INDEX_EXISTS`; authed server + no token → 401 typed).
2. **Wire-shape fidelity** — request/response keys byte-match the contract above (E2E asserts exact server-produced payloads, not SDK re-serializations).
3. **cypher read vs write response difference** — read has `count` and no `summary`; write has `summary` and no `count`; both SDKs' types/methods reflect this (test both paths).
4. **Process hygiene** — harness teardown kills the server on success AND failure; two suites can run concurrently without port clashes (free-port allocation); `VECTRA_SDK_TEST_BIN` short-circuits the go build.
5. **Dep hygiene** — building/installing the Python package pulls zero deps (verify by installing the built wheel into a fresh venv with `--no-deps`-style check or inspecting metadata); `npm ls` in sdk/ts shows zero dependencies.
6. **Auth** — token client against `-auth-token-file` server round-trips a write and a read; wrong/absent token surfaces 401 typed.

---

### Task 1: Python SDK core — transport, errors, reads, batch, cypher, await

**Files:**
- Create: `sdk/python/pyproject.toml`, `sdk/python/README.md` (stub, finished Task 2), `sdk/python/src/vectra_graph/__init__.py`, `sdk/python/src/vectra_graph/client.py`, `sdk/python/src/vectra_graph/errors.py`, `sdk/python/tests/harness.py`, `sdk/python/tests/test_core.py`

**Interfaces:**
- Produces (Task 2/3 consume the same shapes):

```python
# errors.py
class VectraError(Exception):  # .status: int, .code: str, .message: str
class VectraNotFoundError(VectraError)      # 404 / NODE_NOT_FOUND
class VectraConflictError(VectraError)      # 409 (txn conflict, VECTOR_INDEX_EXISTS)
class VectraAuthError(VectraError)          # 401/403

# client.py — sync, stdlib-only (urllib.request; timeout on every call)
class Client:
    def __init__(self, base_url: str | None = None, token: str | None = None,
                 timeout: float = 30.0)  # env defaults VECTRA_URL / VECTRA_TOKEN (.strip()'d)
    # reads
    def health(self) -> dict
    def stats(self) -> dict
    def node(self, key: str) -> dict                    # 404 -> VectraNotFoundError
    def neighbors(self, key: str, direction: str = "out", label: str = "") -> dict
    def bfs(self, from_: list[str], max_depth: int = 0, labels: list[str] | None = None,
            direction: str = "out") -> dict             # POST /v1/query
    def path(self, from_: str, to: str) -> dict
    def reachable(self, from_: str) -> dict
    def cycles(self) -> dict
    # cypher — READ; returns {"columns": [...], "rows": [...], "count": n}
    def cypher(self, query: str) -> dict
    # writes
    def batch(self, nodes: list[dict] | None = None, edges: list[dict] | None = None,
              edge_deletes: list[dict] | None = None, node_deletes: list[str] | None = None) -> dict
    # read-your-writes: returns (published_seq, reached); snapshot header available
    def await_seq(self, seq: int, timeout_ms: int = 5000) -> dict
```

- Harness (`tests/harness.py`): `build_server() -> str` (go build to a tempdir; honor `VECTRA_SDK_TEST_BIN`), `class ServerFixture` (context manager: pick a free port, temp `-data` dir, spawn, poll `/v1/health` until ready with a deadline, terminate/kill + tempdir cleanup in `__exit__` even on exception; all tests share it via `setUpModule`). If `go` is missing → `raise unittest.SkipTest("...")`.

- [ ] **Step 1: Write failing E2E tests** (`test_core.py`): `TestHealthStats`, `TestNodeLifecycle` (batch upsert → node() → node_deletes → 404 typed), `TestTraversals` (neighbors/bfs/path/reachable/cycles over a small dependency graph), `TestCypherRead` (MATCH...RETURN asserts columns/rows/count), `TestCypherWriteSummary` (CREATE...RETURN via `cypher()` → summary keys, no count key), `TestErrorEnvelope` (bad query → `PARSE` code + status 400; unknown node 404 typed), `TestAwait` (write → await_seq(seq) reached). All against `ServerFixture`; RED first (no client).
- [ ] **Step 2: Run** `cd sdk/python && python3 -m unittest discover -s tests -v` **— expect FAIL (import error)**.
- [ ] **Step 3: Implement** errors.py + client.py (urllib transport: one `_request` helper — method, path, params/body, timeout, auth header, JSON encode/decode, envelope → typed errors) + harness.py + pyproject skeleton.
- [ ] **Step 4: Run** the suite **— expect PASS, output clean**; also `python3 -m py_compile` all files; go suites untouched.
- [ ] **Step 5: Commit** — `feat(sdk): python core client — transport, typed errors, reads, batch, cypher, await`

### Task 2: Python SDK advanced — txn, vector CRUD/search, index DDL, packaging

**Files:**
- Modify: `sdk/python/src/vectra_graph/client.py` (+`__init__.py` exports), `sdk/python/README.md`
- Create: `sdk/python/tests/test_txn.py`, `sdk/python/tests/test_vector.py`

**Interfaces:**
- Extends Client (wire types dict-first; TypedDict optional but keep zero-boilerplate):

```python
    def txn(self, nodes=None, edges=None, edge_deletes=None, node_deletes=None,
            snapshot: int = 0, ser: bool = False,
            read_nodes: list[str] | None = None, read_edges: list[dict] | None = None) -> dict
    # 409 conflict -> VectraConflictError (pin the server's exact conflict code from the handler)
    def vector_search(self, index: str, *, query_vector: list[float] | None = None,
                      similar_to: str = "", k: int = 10, ef: int = 0, label: str = "") -> dict
    def vector_create_index(self, name: str, prop: str, dim: int,
                            metric: str = "cosine", m: int = 0, ef_construction: int = 0) -> dict
    def vector_list_indexes(self) -> list[dict]
    def vector_drop_index(self, name: str) -> None
    def index_property(self, prop: str) -> dict      # Cypher CREATE INDEX; summary.indexesCreated == 1
    def index_full_text(self, prop: str) -> dict     # Cypher CREATE FULLTEXT INDEX
```

- Packaging: `pyproject.toml` complete (name `vectra-graph`, version 0.14.0, requires-python >=3.9, no dependencies, setuptools backend, README as long-description); README quickstart + API table + DDL-pending-until-Seal note + env conventions.
- [ ] **Step 1: Failing tests**: `test_txn.py` — `TestTxnCommit` (guard snapshot from stats/await, upsert+delete commit, counts), `TestTxnConflict` (two clients, stale snapshot + readNodes → `VectraConflictError` with the server's code), `TestTxnSER`; `test_vector.py` — `TestVectorLifecycle` (create → list → search by query_vector AND similar_to → drop → 404), `TestVectorErrors` (duplicate → 409 typed; bad metric → 400 `VALIDATION`), `TestIndexDDL` (index_property → summary; post-Seal typed lookup works).
- [ ] **Step 2: Run** — expect FAIL.
- [ ] **Step 3: Implement** the methods + packaging + README.
- [ ] **Step 4: Run** the full python suite; **dep hygiene check**: `python3 -m pip install --quiet --force-reinstall dist/*.whl` into a fresh `python3 -m venv` and import from it (wheel has zero requires); `python3 -m build` if available else `pip wheel . -w dist --no-deps`.
- [ ] **Step 5: Commit** — `feat(sdk): python txn/vector/index-DDL + zero-dep packaging (vectra-graph 0.14.0)`

### Task 3: TypeScript SDK — full surface, typed, node:test E2E

**Files:**
- Create: `sdk/ts/package.json`, `sdk/ts/tsconfig.json`, `sdk/ts/README.md`, `sdk/ts/src/index.ts` (or split `client.ts`/`errors.ts` — implementer's call, keep one clear responsibility per file), `sdk/ts/tests/harness.ts`, `sdk/ts/tests/core.test.ts`, `sdk/ts/tests/txn-vector.test.ts`

**Interfaces:**
- Mirrors the Python surface exactly (names camelCase; `bfs` → `query`? NO — keep `bfs`/`from` mapping identical to Python for cross-SDK familiarity: `health, stats, node, neighbors, bfs, path, reachable, cycles, cypher, batch, awaitSeq, txn, vectorSearch, vectorCreateIndex, vectorListIndexes, vectorDropIndex, indexProperty, indexFullText`).
- `class VectraError extends Error { status: number; code: string; message: string }` + `VectraNotFoundError`, `VectraConflictError`, `VectraAuthError`.
- Typed results: `CypherReadResult {columns: string[]; rows: Record<string, unknown>[]; count: number}` vs `CypherWriteResult {columns: string[]; rows: Record<string, unknown>[]; summary: WriteSummary}` (Review Focus 3); `WriteSummary` with the six counters.
- Constructor: `new VectraClient({baseUrl?, token?, timeoutMs?, fetch?})` — env defaults `VECTRA_URL`/`VECTRA_TOKEN`; the `fetch` override enables tests/fetch-mocks without breaking zero-dep (default = global fetch).
- Harness (tests/harness.ts): same contract as Python — build (honor `VECTRA_SDK_TEST_BIN`), free port, spawn, health-poll deadline, kill on `after()` regardless of failures, `t.skip` when go missing.
- [ ] **Step 1: Failing E2E tests** mirroring Task 1+2's cases (core incl. cypher read/write-summary difference + error envelope; txn incl. typed conflict; vector lifecycle + 409/400; index DDL; auth token round-trip if Task 1/2 didn't already pin it — one of the suites must pin Review Focus 6).
- [ ] **Step 2: Run** `cd sdk/ts && npm install --no-save && npx tsc --noEmit && npm test` **— expect FAIL (no src)**.
- [ ] **Step 3: Implement** client + errors + harness + package.json (`@projanvil/vectra` 0.14.0, `"type": "module"`, engines node>=18, zero deps, `prepare`/build script = `tsc`; exports types+ESM) + tsconfig (strict).
- [ ] **Step 4: Run** the suite **— PASS**; `npm ls --all` shows zero dependencies; `npx tsc --noEmit` clean.
- [ ] **Step 5: Commit** — `feat(sdk): typescript client @projanvil/vectra — full surface, typed results, zero deps`

### Task 4: Cross-SDK docs, root README, CHANGELOG, verification sweep

**Files:**
- Modify: `README.md` + `README.zh-CN.md` (SDK section: install, 5-line quickstart per language, link into sdk/), `CHANGELOG.md` (0.14.0 Added bullets)
- Verify only: both SDK suites, Go suites

- [ ] **Step 1: README SDK section (EN then zh-CN, section-aligned)**: `pip install vectra-graph` / `npm install @projanvil/vectra` quickstarts (connect → cypher write+summary → read back → vector search), pointer that publishing is pending/owner action if not yet on the registries.
- [ ] **Step 2: CHANGELOG 0.14.0 Added** — both SDKs, zero-dep note, version alignment.
- [ ] **Step 3: Verification sweep** (sequential, record in report): `cd sdk/python && python3 -m unittest discover -s tests`; `cd sdk/ts && npm test`; root `go test ./...`; `cd server && go test ./...`; `cd mcp && go test ./...`; wheel zero-dep re-verify; `npm ls` zero-dep; gofmt/`npx tsc` clean. Bench wall-clock gates are load-flaky (known) — rerun in isolation before judging.
- [ ] **Step 4: Commit** — `docs(sdk): root README SDK section, CHANGELOG 0.14.0 SDK bullets`

---

## Post-plan (out of scope)

- Publishing to PyPI/npm (owner action; command documented in sdk READMEs).
- CI workflows, Dockerfile, examples/ directory, English bench index (P1 release-engineering arc).
- vectra-cli Cypher REPL (P1).
- Async Python client (httpx) variant; Retries/backoff policy knobs.
- Pyodide/browser build of the TS SDK.
