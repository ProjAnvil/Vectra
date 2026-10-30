# Examples

One scenario — a small "project knowledge graph" — in three languages:
embedded Go, the Python SDK (`vectra-graph`), and the TypeScript SDK
(`@projanvil/vectra`). Every example batch-upserts `Person`/`Project`
nodes (one project carries a CJK name) and `WORKS_ON` edges,
declares a `FULLTEXT` index on `name` and queries the CJK name, runs a
Cypher write with `RETURN` (`CREATE` and `SET`) plus a `MATCH ... RETURN`
read, then declares a vector index, writes one 4-dim vector **through
batch node props** (the Cypher subset has no list literals — vectors
never ride Cypher), and answers a top-1 vector search.

| Example | Runs against | Entry point |
| --- | --- | --- |
| [`go-embedded/`](go-embedded/) | the embedded engine (no server) | `examples/go-embedded/main.go` |
| [`python/`](python/) | a Vectra server via REST | `examples/python/main.py` |
| [`typescript/`](typescript/) | a Vectra server via REST | `examples/typescript/main.mjs` |

## Go (embedded engine)

No server, nothing to install — the example opens a durable WAL session
in a scratch data dir (default `example-data` under the current dir,
removed on exit when this run created it; override with `-data`):

```bash
go run ./examples/go-embedded           # from the repo root
go run ./examples/go-embedded -data /tmp/my-example-data
```

## Run the server (both SDK examples)

The Python and TypeScript examples talk to a running server. Build the
binary and boot a single node — the examples default to
`http://127.0.0.1:18080`, but any free port works (export `VECTRA_URL` to
match it; if the server enables `-auth-token-file`, export `VECTRA_TOKEN`
too):

```bash
make build        # or: cd server && go build -o ../bin/vectra-server ./cmd/vectra-server

DATA=$(mktemp -d /tmp/vectra-example.XXXXXX)
./bin/vectra-server -id n1 -data "$DATA" -http 127.0.0.1:18080 -raft 127.0.0.1:19080
```

Stop it with Ctrl-C when done; `rm -rf "$DATA"` cleans the scratch dir.

Each boot above starts from an empty graph, so every example run sees a
clean slate. Rerunning an example against an already-populated server
also works — batch upserts and `SET` are idempotent and the vector block
drops and re-creates its index — but note that the Cypher `CREATE`
appends a parallel `WORKS_ON` edge on rerun (CREATE semantics), so
restart the server for byte-identical output.

## Python (vectra-graph)

Prerequisite: Python 3.9+ and nothing else — the SDK is stdlib-only and
not yet on PyPI, so it is imported straight from the checkout via
`PYTHONPATH`:

```bash
cd examples/python
VECTRA_URL=http://127.0.0.1:18080 PYTHONPATH=../../sdk/python/src python3 main.py
```

## TypeScript (@projanvil/vectra)

Prerequisite: Node >= 18. The SDK is not yet on npm, so the example
depends on it via npm's `file:` protocol, which links the `sdk/ts`
directory — build the SDK's `dist/` once first (the `prepare` script in
`sdk/ts` runs the build on `npm install`):

```bash
(cd ../../sdk/ts && npm install)   # once: installs devDependencies and builds dist/

cd examples/typescript
npm install                        # links @projanvil/vectra from file:../../sdk/ts
VECTRA_URL=http://127.0.0.1:18080 node main.mjs
```

## Once the SDKs are published

`vectra-graph` and `@projanvil/vectra` are versioned with the server at
0.14.0 but not yet on PyPI/npm. Once they are published, the
`PYTHONPATH` / `file:` scaffolding above collapses to:

```bash
pip install vectra-graph           # then: python3 main.py (no PYTHONPATH)
npm install @projanvil/vectra      # then: node main.mjs (no file: dependency)
```
