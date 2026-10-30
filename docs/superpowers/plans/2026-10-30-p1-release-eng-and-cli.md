# P1: Release Engineering + vectra-cli Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the P1 arc from the adoption assessment §4: a release-engineering package (Dockerfile, GitHub Actions CI, English bench summary, examples/, `go get` tag-strategy smoke) and `vectra-cli`, a minimal Cypher REPL over the vectra-server HTTP API.

**Architecture:** vectra-cli is a pure client in the `server` Go module (`server/cmd/vectra-cli`) — no server-side changes; it talks to `POST /v1/cypher` only. Release engineering is all new files plus README/CHANGELOG/Makefile edits. The `go get` smoke proves the multi-module tag strategy (root `vX.Y.Z` + `server/vX.Y.Z` + `mcp/vX.Y.Z`) against a local bare-mirror, including the version-prep commit (submodule `require github.com/projanvil/vectra v0.0.0` must become a real version at release time, because a dependency's own `replace ../` is ignored outside the main module).

**Tech Stack:** Go 1.27 (three modules: `.`, `server`, `mcp`), `golang.org/x/term` (new, server module only) for the CLI line editor, Docker multi-stage (golang:1.27-alpine → scratch), GitHub Actions (setup-go/setup-python/setup-node), Python stdlib `unittest` + `node --test` for the SDK suites.

**Spec:** `docs/research/2026-10-29-lightweight-adoption-assessment.md` §4 item 3 (P1: 发布工程包 + vectra-cli REPL).

## Global Constraints

- English only for commits, comments, docs, release notes. Exception: CJK sample *data* inside examples (CJK FTS is a deliberate showcase).
- Root module `github.com/projanvil/vectra` stays zero-dependency. New runtime deps land only in `server/go.mod` (`golang.org/x/term`). No other new deps anywhere.
- Work on branch `feat/p1-release-eng-cli` from `main@595f83c`. Do **not** push. At arc end, ff-merge into `main` locally.
- This machine's port 8080 is occupied by a daemon — every local server boot uses a free port.
- CI YAML takes effect only after push (not done in this arc): every CI command must be mirrored locally, verbatim, before merge.
- No changes to server API semantics or wire shapes; the CLI is a client only. Alloc gates (`TestCypher.*Allocs`) and root race suite must stay green.
- CHANGELOG gets a new `## [0.15.0] - Unreleased` section on top of the existing `[0.14.0] - Unreleased`.
- Vectors are never written via Cypher (no list literals in the subset) — examples write vectors via `/v1/batch` props.
- `sdk/ts` has no `package-lock.json` — use `npm install`, never `npm ci`.

## Wire Facts (pinned, from `server/api.go` — do not re-derive)

- `POST /v1/cypher` body: `{"query": "..."}`.
- READ response: `{"columns": [...], "rows": [{...}, ...], "count": N}`.
- WRITE response: same `columns`/`rows` (from RETURN, empty if absent) plus `"summary": {"createdNodes":n,"createdEdges":n,"deletedNodes":n,"deletedEdges":n,"propertiesSet":n,"indexesCreated":n}` and **no** `count` key.
- Error response: `{"error": {"code": "...", "message": "..."}}` (400 PARSE for Cypher errors; 401 without valid bearer when auth enabled).
- Auth: `Authorization: Bearer <token>` header; attach only when token non-empty.

## Review Focus

1. **Server error faces must not kill the REPL.** A 400 PARSE / 401 / connection-refused prints `error: <code>: <message>` and the loop continues; `-c` mode exits 1. — pinned by `client_test.go` (httptest 400/401 shapes) and `repl_test.go` (scripted source continues after error).
2. **Write responses render both table and summary.** A write with RETURN prints the table then the summary line; a write without RETURN prints only the summary. — pinned by `repl_test.go` against a real in-process node.
3. **JSON number formatting:** `float64(3)` renders `3`, not `3.0`; `nil` renders `null`; nested maps/slices render as compact JSON. — pinned by `render_test.go` table test.
4. **Bearer attached only when non-empty** (auth-off deployments keep today's wire behavior). — pinned by `client_test.go` header assertions.
5. **Multi-line statement accumulation:** a statement submits only when the accumulated buffer ends with `;`; the trailing `;` is stripped before sending; commands (`:q` etc.) are recognized only on an empty buffer. — pinned by `repl_test.go`.

---

### Task 1: vectra-cli — HTTP client + result rendering

**Files:**
- Create: `server/cmd/vectra-cli/client.go`
- Create: `server/cmd/vectra-cli/render.go`
- Test: `server/cmd/vectra-cli/client_test.go`
- Test: `server/cmd/vectra-cli/render_test.go`
- Modify: `server/go.mod` + `server/go.sum` (add `golang.org/x/term`, resolved by `go get golang.org/x/term@latest` — needed this early so Task 2 compiles)

**Interfaces:**
- Consumes: the wire facts above.
- Produces (Task 2 depends on all of these):

```go
// client.go
type cypherSummary struct { // exact JSON tags as pinned above
    CreatedNodes   int `json:"createdNodes"`
    CreatedEdges   int `json:"createdEdges"`
    DeletedNodes   int `json:"deletedNodes"`
    DeletedEdges   int `json:"deletedEdges"`
    PropertiesSet  int `json:"propertiesSet"`
    IndexesCreated int `json:"indexesCreated"`
}
type cypherResponse struct {
    Columns []string         `json:"columns"`
    Rows    []map[string]any `json:"rows"`
    Count   int              `json:"count"`
    Summary *cypherSummary   `json:"summary"`
}
// apiError.Error() returns "<Code>: <Message>".
type apiError struct{ Code, Message string }

// newCypherClient parses baseURL (scheme required), stores bearer ("" = no header).
func newCypherClient(baseURL, bearer string, timeout time.Duration) (*cypherClient, error)
// cypher POSTs {"query": query}; non-2xx decodes the error envelope and
// returns *apiError; ctx bounds the request.
func (c *cypherClient) cypher(ctx context.Context, query string) (*cypherResponse, error)

// render.go
// formatValue: nil→"null"; string→as-is (unquoted); bool→"true"/"false";
// float64→integral ? strconv.FormatInt(int64(f),10) : strconv.FormatFloat(f,'g',-1,64);
// anything else (map/slice)→compact json.Marshal output.
func formatValue(v any) string
// renderTable writes an aligned table: header row = columns, cells padded to
// the widest cell of the column (two trailing spaces), rows in order;
// newline after each row. No truncation.
func renderTable(w io.Writer, columns []string, rows []map[string]any) error
// renderSummary writes one line: "Summary: " + non-zero counters joined by
// ", " in the order [created X nodes, created X edges, deleted X nodes,
// deleted X edges, set X properties, created X indexes]; all zero →
// "Summary: no changes".
func renderSummary(w io.Writer, s *cypherSummary)
```

- [ ] **Step 1: Write failing tests** — `client_test.go` with `httptest.Server` fakes: (a) 200 read shape decodes columns/rows/count; (b) 200 write shape decodes summary and leaves Count zero; (c) 400 body `{"error":{"code":"PARSE","message":"bad"}}` returns `*apiError` with `Error()=="PARSE: bad"`; (d) bearer header present iff token non-empty (assert via capturing handler); (e) `newCypherClient` rejects a URL without scheme. `render_test.go` table tests: number formatting (3 / 3.5 / -0.25), null, bool, nested map `[1,2]`-style JSON, table alignment with mixed widths, summary line each counter case + "no changes".
- [ ] **Step 2: Run tests, verify they fail** — `cd server && go test ./cmd/vectra-cli/` → FAIL (no non-test files).
- [ ] **Step 3: Implement `client.go` and `render.go`** per the Interfaces block. `cypher` uses one `*http.Client` with the constructor timeout; request body is `json.Marshal(map[string]string{"query": query})`.
- [ ] **Step 4: Run tests, verify pass** — `cd server && go test ./cmd/vectra-cli/ -v` → PASS.
- [ ] **Step 5: `go get golang.org/x/term@latest` in `server/`** (dependency for Task 2; `go mod tidy` after Task 2 will finalize).
- [ ] **Step 6: Commit** — `git add server/cmd/vectra-cli server/go.mod server/go.sum && git commit -m "feat(cli): vectra-cli cypher client and result rendering"`

### Task 2: vectra-cli — REPL, terminal line editor, main

**Files:**
- Create: `server/cmd/vectra-cli/repl.go`
- Create: `server/cmd/vectra-cli/term.go`
- Create: `server/cmd/vectra-cli/main.go`
- Test: `server/cmd/vectra-cli/repl_test.go`

**Interfaces:**
- Consumes: `cypherClient`, `cypherResponse`, `cypherSummary`, `renderTable`, `renderSummary` from Task 1.
- Produces: the `vectra-cli` binary.

```go
// repl.go
type lineSource interface { // Next returns (line, ok=false on EOF, err)
    Next() (string, bool, error)
}
// runREPL: primary prompt "> ", continuation prompt "... ". Lines accumulate
// (trimmed) until the buffer ends with ';' → strip ALL trailing ';' and
// whitespace, send via client. Commands recognized only on an empty buffer,
// after TrimSpace, case-insensitive: ":q", ":quit", ":exit", "quit", "exit"
// → return 0; ":help" → print usage lines and continue. Empty submit → no-op.
// After each statement: read → renderTable + "(N rows)" line (N=len(rows));
// write → renderTable (only if len(columns)>0) + renderSummary. On error
// print "error: <code>: <message>" and continue. Returns 0 on EOF/exit,
// 1 only on source error.
func runREPL(ctx context.Context, c *cypherClient, src lineSource, out io.Writer) int

// term.go
type history struct{ /* entries []string, path string, max int */ }
func loadHistory(path string, max int) *history // missing file → empty, no error
func (h *history) add(line string)              // skip empty and dup-of-last; cap at max (drop oldest)
func (h *history) save() error                  // best-effort; no-op when path == ""
// newTermSource puts f into raw mode via x/term (IsTerminal gate) and returns
// a lineSource with line editing + history. Editor handles: printable runes,
// Backspace/Delete, Left/Right/Home/End arrows (ESC [ C/D/H/F and ESC [ 1~/4~),
// Up/Down (ESC [ A/B) history nav, Ctrl+U clear line, Ctrl+C discard line,
// Ctrl+D EOF on empty line, Enter (\r or \n) submit. Redraw = "\r\x1b[K" +
// prompt + buffer, cursor repositioned with ESC [ n D using rune counts
// (documented limitation: wide CJK runes skew repositioning). On Close/EOF
// restore cooked mode.
type termSource struct{ /* ... */ }
func newTermSource(f *os.File, prompt, contPrompt string, h *history) (*termSource, error)
func (t *termSource) Next() (string, bool, error)
func (t *termSource) Close() error

// main.go flags (defaults: env override then hard default):
//   -url      env VECTRA_URL      default "http://127.0.0.1:8080"
//   -token    env VECTRA_TOKEN    default "" (no header)
//   -c        one-shot query: execute, render, exit 0/1 (no REPL)
//   -timeout  default 30s (client timeout + per-request ctx)
//   -history  default $HOME/.vectra_history; "" disables file persistence
// stdin non-tty → bufio line source (each line = one statement), so pipes and
// -c work everywhere; tty → termSource. Exit codes: 0 ok, 1 execution error
// (-c mode) or startup error, 2 usage.
```

- [ ] **Step 1: Write failing tests** — `repl_test.go` with (a) a scripted `lineSource` (type `sliceSource` over `[]string`) driving `runREPL` against an `httptest.Server` fake: multi-line statement submits only at `;`; read renders table + `(N rows)`; write renders summary; error response prints `error: PARSE: ...` then the next statement still executes; `:q` returns 0; (b) an in-process real-node e2e: bootstrap a `server.Node` the same way existing `server` package tests do (grep `server/*_test.go` for the pattern; reuse it) on a free `127.0.0.1` port, run `runREPL` with `CREATE (n:P {key:'a'}) RETURN n` then `MATCH (n:P) RETURN n.key`, assert output contains the summary counters and the value; also add a `history` unit test (add/skip-dup/cap).
- [ ] **Step 2: Run tests, verify they fail** — `cd server && go test ./cmd/vectra-cli/` → FAIL.
- [ ] **Step 3: Implement `repl.go`, `term.go`, `main.go`** per the Interfaces block.
- [ ] **Step 4: Run tests, verify pass** — `cd server && go test ./cmd/vectra-cli/ -v` → PASS.
- [ ] **Step 5: Non-tty smoke** — `cd server && go build -o ../bin/vectra-cli ./cmd/vectra-cli`, boot a real server (in a scratch dir, free port, `bin/vectra-server -id t -data <tmp> -http 127.0.0.1:<free> -raft 127.0.0.1:<free2>`), then `printf 'CREATE (n:P {key:"x"}) RETURN n.key;\nMATCH (n:P) RETURN n.key;\n:q\n' | ./bin/vectra-cli -url http://127.0.0.1:<free>` shows table, `(1 rows)`, summary. Kill server, clean scratch dir.
- [ ] **Step 6: Best-effort pty smoke** — same via `printf '...' | script -q /dev/null ./bin/vectra-cli -url ...`; if `script` on this machine is flaky, note it and move on (gate is Steps 4–5).
- [ ] **Step 7: `go vet` + `go mod tidy`** — `cd server && go vet ./... && go mod tidy && go build ./...` clean.
- [ ] **Step 8: Commit** — `git commit -am "feat(cli): REPL with line editing, history, one-shot mode"`

### Task 3: examples/ — Go embedded, Python, TypeScript

**Files:**
- Create: `examples/README.md`
- Create: `examples/go-embedded/main.go` (inside root module — no new go.mod)
- Create: `examples/python/main.py`
- Create: `examples/typescript/package.json`, `examples/typescript/main.mjs`

**Interfaces:**
- Consumes: root engine API exactly as the README "Quickstart (embedded)" and "Vector search (v0.9, Phase 1)" sections spell it (read them; do not invent API names); Python SDK surface per `sdk/python/README.md`; TS SDK per `sdk/ts/README.md`.
- Produces: three runnable examples; CI (Task 4) runs them verbatim as scripted here.

One scenario, three languages ("project knowledge graph"): batch-upsert ~4 `Person`/`Project` nodes (one with a CJK `name`, e.g. 图数据库) and `WORKS_ON` edges; create a FULLTEXT index on `name`; run a Cypher write with RETURN (`CREATE`/`SET`) and a read (`MATCH ... RETURN`); run a fulltext query hitting the CJK name; declare a vector index, write one 4-dim vector **via batch props** (never Cypher), `QueryVector` top-1. Print labeled results.

- [ ] **Step 1: `examples/go-embedded/main.go`** — flag `-data` default `example-data` (created under the current dir, removed on exit if created); uses `vectra` root-module APIs mirrored from README quickstart; CJK sample name included; `defer` close. Verify: `go run ./examples/go-embedded` (from repo root) runs clean and prints all labeled sections; rerun works (fresh dir each run).
- [ ] **Step 2: `examples/python/main.py`** — stdlib-only run: `PYTHONPATH` instructions in `examples/README.md` (`PYTHONPATH=../../sdk/python/src`), env `VECTRA_URL` default `http://127.0.0.1:18080`, optional `VECTRA_TOKEN`; uses `vectra_graph` SDK (import surface per `sdk/python/README.md` quickstart). Verify against a real server on a free port: `VECTRA_URL=http://127.0.0.1:<free> PYTHONPATH=../../sdk/python/src python3 main.py` runs clean.
- [ ] **Step 3: `examples/typescript/`** — `package.json` (`"type":"module"`, `"@projanvil/vectra": "file:../../sdk/ts"`, engines node>=18, no other deps); `main.mjs` mirrors the scenario (env `VECTRA_URL`/`VECTRA_TOKEN`). Because a `file:` dep links the SDK dir, `dist/` must exist: `examples/README.md` documents `(cd ../../sdk/ts && npm install)` first. Verify: build SDK, `npm install` in the example, `VECTRA_URL=http://127.0.0.1:<free> node main.mjs` runs clean.
- [ ] **Step 4: `examples/README.md`** — per-example prerequisites + exact commands (free-port note: examples show `-http 127.0.0.1:18080`; call out that any free port works), one "run the server" block (make build or Docker) reused by both SDK examples; note that once `vectra-graph`/`@projanvil/vectra` are published the `PYTHONPATH`/`file:` scaffolding collapses to `pip install vectra-graph` / `npm i @projanvil/vectra`.
- [ ] **Step 5: Verify root module still clean** — `go vet ./... && go test ./examples/... ./... -count=1` from root passes (examples compile as part of the root module; no test files needed).
- [ ] **Step 6: Commit** — `git add examples && git commit -m "docs(examples): runnable Go/Python/TypeScript examples"`

### Task 4: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: test names (root `TestCypher.*Allocs` gates), SDK test commands, examples run commands (Task 3).
- Produces: `ci` workflow; every command below must be mirrored locally in Task 9 before merge.

Pinned workflow: `name: ci`; `on: push: branches: [main]` + `pull_request:`; `concurrency: group: ci-${{ github.ref }}, cancel-in-progress: true`; `permissions: contents: read`. Jobs:

1. **`go`** (ubuntu-latest, timeout 30m): `actions/setup-go@v5` with `go-version: '1.27'`; then
   - `go vet ./...` and `go test ./...` for each of `.`, `server`, `mcp` (cd per module, four commands total);
   - root race: `go test -race ./...`;
   - alloc gate: `go test -run 'TestCypher.*Allocs' -count=1 .` (root module).
2. **`sdk-python`** (ubuntu-latest): setup-go 1.27 + `actions/setup-python@v5` python 3.11; `cd sdk/python && python3 -m unittest discover -s tests -v` (harness builds the server binary itself).
3. **`sdk-ts`** (ubuntu-latest): setup-go 1.27 + `actions/setup-node@v4` node 20; `cd sdk/ts && npm install && npm test`.
4. **`examples`** (ubuntu-latest): setup-go 1.27 + python 3.11 + node 20; steps: build `bin/vectra-server`; pick a free port `PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("",0)); print(s.getsockname()[1]); s.close()')`; boot server on `127.0.0.1:$PORT` into `$RUNNER_TEMP/data` (single-node, no join); `go run ./examples/go-embedded`; `cd examples/python && PYTHONPATH=../../sdk/python/src VECTRA_URL=http://127.0.0.1:$PORT python3 main.py`; `(cd sdk/ts && npm install && npm run build)` then `(cd examples/typescript && npm install && VECTRA_URL=http://127.0.0.1:$PORT node main.mjs)`; kill server in an `if: always()` step.

- [ ] **Step 1: Write `ci.yml`** exactly per the pinned spec above (anchors `@v4`/`@v5`, no matrix).
- [ ] **Step 2: Validate YAML locally** — `ruby -ryaml -e 'YAML.load_file(".github/workflows/ci.yml"); puts "yaml ok"'` (actionlint is not installed; treat the pinned spec as the source of truth). Also verify anchors/keys by eye against this task.
- [ ] **Step 3: Note the push dependency** — add a one-line comment at the top of `ci.yml`: workflow activates only once the branch is pushed.
- [ ] **Step 4: Commit** — `git commit -am "ci: go modules test/vet, root race, cypher alloc gate, sdk suites, examples"`

### Task 5: Dockerfile + .dockerignore

**Files:**
- Create: `Dockerfile`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: `server/cmd/vectra-server` build (module has `replace ../` → build context must be repo root).
- Produces: image recipe documented in READMEs (Task 8) and RELEASING (Task 7).

Pinned Dockerfile:

- Stage 1 `FROM golang:1.27-alpine AS build`, `WORKDIR /src`; copy `go.mod`, `go.work`, `server/go.mod`, `mcp/go.mod`; `cd server && go mod download` (third-party only; replace keeps the engine local); `COPY . .`; `RUN cd server && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/vectra-server ./cmd/vectra-server`.
- Stage 2 `FROM scratch`; `COPY --from=build /out/vectra-server /vectra-server`; `VOLUME /data`; `EXPOSE 8080 9000`; `USER 65534:65534`; `ENTRYPOINT ["/vectra-server"]`; `CMD ["-id","n1","-data","/data","-http",":8080","-raft",":9000"]`. Top comment: no HEALTHCHECK because scratch has no shell; liveness = `GET /v1/health`.

`.dockerignore`: `.git`, `benchdata/`, `bin/`, `node_modules/`, `dist/`, `dist-test/`, `__pycache__/`, `*.test`, `.worktrees/`, `docs/`.

- [ ] **Step 1: Write both files** per the pinned spec.
- [ ] **Step 2: Verify the build path from a clean tree** — `tmp=$(mktemp -d) && git archive HEAD | tar -x -C $tmp` then `cd $tmp/server && CGO_ENABLED=0 go build ./cmd/vectra-server` and `CGO_ENABLED=0 GOOS=linux go build ./cmd/vectra-server` (cross-compile for the scratch image). Both must succeed; rm -rf the tmp dir.
- [ ] **Step 3: Attempt `docker build`** — the local Docker daemon (OrbStack) is currently down. Try `docker build -t vectra-server:smoke .` once; if the daemon is unreachable, record that as a known limitation in the commit message body and move on (do not start the user's VM).
- [ ] **Step 4: Commit** — `git commit -am "build(docker): multi-stage scratch image for vectra-server"`

### Task 6: English benchmarks index

**Files:**
- Create: `docs/benchmarks/README.md`

**Interfaces:**
- Consumes: every report in `docs/benchmarks/*.md` (see directory listing) + `DESIGN.md`.
- Produces: an English index page that root README (Task 8) links as the entry point.

- [ ] **Step 1: Extract headline numbers** — read each report's summary/conclusion; copy numbers verbatim (no reinvention). Group: OLTP/perf (stress, oltp-txn, ryw-await, delta-wal, perf-baseline, perf-fixes-retest), zero-alloc row (row-path-zero-alloc, cypher-zero-alloc), vector arc (vector-spike, build-optimization, phase1-gate2b, phase2-p21/p22, vector-1m, 1m-fixes-retest), ops (wrapper-checkpoint-cadence, v0112-checkpoint, v0112-scope).
- [ ] **Step 2: Write `docs/benchmarks/README.md`** — English: one-paragraph method note (M1 Pro, power-law graphs, pointers to DESIGN.md), a headline table (workload / report / headline result), then a one-line English takeaway + link per report, chronological. Mark Chinese-language originals as such.
- [ ] **Step 3: Commit** — `git commit -am "docs(bench): English benchmarks index and headline summary"`

### Task 7: go get tag smoke + RELEASING guide + Makefile targets

**Files:**
- Create: `scripts/goget-smoke.sh` (executable)
- Create: `docs/RELEASING.md`
- Modify: `Makefile` (build target gains vectra-cli; new `race`, `allocs`, `goget-smoke` targets)

**Interfaces:**
- Consumes: the tag-strategy facts in this task (they are the deliverable being verified).
- Produces: `make goget-smoke VERSION=0.15.0` exit 0; RELEASING.md referenced by Task 8 READMEs.

Why the smoke matters (verified design): a dependency module's own `replace ../` is **ignored** outside the main module, so a user `go get github.com/projanvil/vectra/server@v0.15.0` resolves its engine requirement `github.com/projanvil/vectra v0.0.0` — which has no tag and fails, unless the release bumps that require line to the real version. The smoke simulates the full release flow locally.

`scripts/goget-smoke.sh VERSION` (default: minor-bump of the top CHANGELOG version):

1. `root=$(git rev-parse --show-toplevel)`; `ver="${1:-<derived>}"`; `tmp=$(mktemp -d)` with cleanup trap.
2. Non-bare mirror clone: `git clone "$root" "$tmp/work"`, then in `$tmp/work`: sed `github.com/projanvil/vectra v0.0.0` → `v$ver` in `server/go.mod` and `mcp/go.mod` (this is exactly the version-prep commit RELEASING.md prescribes); commit; tag `v$ver`, `server/v$ver`, `mcp/v$ver` (annotated, same commit).
3. Bare mirror: `git clone --bare "$tmp/work" "$tmp/mirror.git"`.
4. Scratch consumer: `mkdir $tmp/smoke && cd && go mod init smoke.local/smoke`; `main.go` with one real root API call (open a temp Vectra, close it) plus blank imports of `github.com/projanvil/vectra/server` and `.../mcp`.
5. Env: `GOPRIVATE=github.com/projanvil/*`; `GIT_CONFIG_COUNT=1`, `GIT_CONFIG_KEY_0=url."<tmp>/mirror.git".insteadOf`, `GIT_CONFIG_VALUE_0=https://github.com/projanvil/vectra` (third-party deps still flow via the default proxy).
6. `go get github.com/projanvil/vectra@v$ver` then `.../server@v$ver` then `.../mcp@v$ver`; `go build ./...`. Print PASS/FAIL per step; exit non-zero on first failure.

- [ ] **Step 1: Write the script** per the pinned design; `chmod +x`.
- [ ] **Step 2: Run it** — `VERSION=0.15.0 scripts/goget-smoke.sh` → all steps PASS. If a step fails, the failure text is the finding: fix the script or document the strategy gap in RELEASING.md; do not weaken the assertion.
- [ ] **Step 3: Write `docs/RELEASING.md`** (English): the tag trio + version-prep commit as the release flow (numbered checklist: CHANGELOG date → version-prep commit → annotated tags → push → `gh release create`, noting gh needs the full target SHA → Docker build/publish command → SDK publish pointers); a "go get compatibility" section explaining the replace/main-module rule the smoke proves; how to run the smoke.
- [ ] **Step 4: Makefile** — `build` also emits `bin/vectra-cli`; new targets `race` (`go test -race ./...`, root), `allocs` (`go test -run 'TestCypher.*Allocs' -count=1 .`, root), `goget-smoke` (`VERSION ?= 0.15.0`; `bash scripts/goget-smoke.sh $(VERSION)`); help lines updated. `make build race allocs` all pass locally.
- [ ] **Step 5: Commit** — `git commit -am "build(release): go get tag smoke, RELEASING guide, make targets"`

### Task 8: READMEs (en + zh-CN) + CHANGELOG + repository layout

**Files:**
- Modify: `README.md`
- Modify: `README.zh-CN.md` (parity with the English changes)
- Modify: `CHANGELOG.md`
- Modify: `Makefile` only if Task 7 missed a help line

**Interfaces:**
- Consumes: everything built in Tasks 1–7 (link targets must exist).
- Produces: final user-facing docs.

- [ ] **Step 1: README.md** — add: CI badge at top (`actions/workflows/ci.yml/badge.svg`); "vectra-cli" section after Server mode (build via `make build`, flags table, example session incl. auth token note, `-c` one-shot); Docker subsection inside Server mode (`docker build -t vectra-server .` + `docker run -p 8080:8080 -v vectra-data:/data vectra-server`, health via `/v1/health`); "Examples" section linking `examples/`; "Concurrency discipline (SQLite-style)" section: embedded = one process owns the data dir, all goroutines share one `*Vectra` handle; readers are lock-free, writes are single-writer — never open the same data dir from two processes; multi-process or remote access is what server mode is for; cross-link the v0.7 micro-layer section; Benchmarks section gains "English summary: [docs/benchmarks/README.md](...)"; Repository layout adds `examples/`, `scripts/`, `.github/workflows/`, `docs/RELEASING.md`.
- [ ] **Step 2: README.zh-CN.md** — translated parity of every Step 1 addition (technical terms and flags stay English).
- [ ] **Step 3: CHANGELOG.md** — new top section `## [0.15.0] - Unreleased` with Added (vectra-cli REPL; Docker image; CI workflow; examples; English bench index; RELEASING + goget-smoke) and Changed (make build emits vectra-cli; new race/allocs/goget-smoke targets) bullets.
- [ ] **Step 4: Link check** — every relative link touched or added in both READMEs resolves (`ls` each target).
- [ ] **Step 5: Commit** — `git commit -am "docs(readme): cli, docker, examples, concurrency discipline; changelog 0.15.0"`

### Task 9: Full-branch verification (local CI mirror)

**Files:** none (verification only; fixes fold back into the owning task's commit)

- [ ] **Step 1: Mirror every CI command verbatim** — for each of `.`, `server`, `mcp`: `go vet ./...` + `go test ./...`; root `go test -race ./...`; root `go test -run 'TestCypher.*Allocs' -count=1 .`; `cd sdk/python && python3 -m unittest discover -s tests -v`; `cd sdk/ts && npm install && npm test`; the whole `examples` job (build server, free port, run all three examples).
- [ ] **Step 2: Arc-specific checks** — `make build race allocs`; `VERSION=0.15.0 scripts/goget-smoke.sh`; Dockerfile clean-tree build (Task 5 Step 2) re-run on the final HEAD; `git log --oneline main..feat/p1-release-eng-cli` shows only planned commits.
- [ ] **Step 3: Record results** — any failure loops back to the owning task with a fix commit; all-green is the merge precondition.

---

## Execution Notes

- Final review: code-reviewer subagent over the whole branch, then a fix wave, then ff-merge `feat/p1-release-eng-cli` → `main` locally (no push), per the established repo process.
- Bench wall-clock gates (e.g. A4) under machine load are a known flaky class — isolate and rerun before judging; they are out of scope here anyway (no bench reruns in this arc).
