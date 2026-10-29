# vectra-graph — Python SDK for Vectra

Python client for the Vectra REST graph API. Standard library only — the
wheel installs **zero runtime dependencies** (`urllib` transport, JSON,
typing). Python 3.9+.

## Quick start

```python
from vectra_graph import Client

client = Client(base_url="http://127.0.0.1:8080")  # or set VECTRA_URL / VECTRA_TOKEN

client.batch(nodes=[{"key": "gateway", "label": "Service", "props": {"env": "prod"}}])
aw = client.await_seq(client.stats()["publishedSeq"] + 1)
for row in client.cypher("MATCH (n:Service) RETURN n.key")["rows"]:
    print(row["n.key"])
```

Optimistic transaction (read at a snapshot, commit under validation):

```python
client.node("gateway")                     # read -> X-Vectra-Snapshot recorded
res = client.txn(
    nodes=[{"key": "gateway", "label": "Service", "props": {"env": "staging"}}],
    snapshot=client.last_snapshot,         # raft index the reads were based on
    ser=True, read_nodes=["gateway"],      # serialize against the read set
)
```

Vector search:

```python
client.vector_create_index("emb", prop="embedding", dim=3)
client.batch(nodes=[{"key": "a", "label": "Doc", "props": {"embedding": [1.0, 0.0, 0.0]}}])
hits = client.vector_search("emb", similar_to="a", k=5)["hits"]
```

## Installation

```
python3 -m pip install vectra-graph
```

## Environment conventions

| Variable | Meaning | Default |
| --- | --- | --- |
| `VECTRA_URL` | server base URL (whitespace-stripped, trailing `/` dropped) | `http://127.0.0.1:8080` |
| `VECTRA_TOKEN` | bearer token (whitespace-stripped — copy-pasted tokens often carry a trailing newline) | empty (auth off) |
| `VECTRA_SDK_TEST_BIN` | tests: prebuilt `vectra-server` binary path, skips the `go build` | unset |

Explicit `Client(base_url=..., token=...)` arguments always win over the
environment. The `Authorization: Bearer` header is attached only when the
token is non-empty, matching auth-off servers.

## API

| Method | Wire | Notes |
| --- | --- | --- |
| `health()` | `GET /v1/health` | liveness + leader/term; 503 until the first raft publish |
| `stats()` | `GET /v1/stats` | counters, `publishedSeq` (the guard snapshot token) |
| `node(key)` | `GET /v1/nodes/{key}` | typed 404 `NODE_NOT_FOUND` |
| `neighbors(key, direction="out", label="")` | `GET /v1/neighbors` | adjacent nodes |
| `bfs(from_, max_depth=0, labels=None, direction="out")` | `POST /v1/query` | breadth-first traversal |
| `path(from_, to)` | `GET /v1/path` | shortest path |
| `reachable(from_)` | `GET /v1/reachable` | reachable key set |
| `cycles()` | `GET /v1/cycles` | elementary cycles |
| `cypher(query)` | `POST /v1/cypher` | reads: `columns/rows/count`; writes: `columns/rows/summary` |
| `batch(nodes, edges, edge_deletes, node_deletes)` | `POST /v1/batch` | auto-commit changeset, fixed apply order |
| `txn(nodes, edges, edge_deletes, node_deletes, snapshot=0, ser=False, read_nodes=None, read_edges=None)` | `POST /v1/txn` | OCC commit; raises `TxnConflictError` on 409 |
| `await_seq(seq, timeout_ms=5000)` | `GET /v1/await` | read-your-writes long poll; sets `last_snapshot` |
| `vector_search(index, *, query_vector=None, similar_to="", k=10, ef=0, label="")` | `POST /v1/vector/search` | exactly one of `query_vector` / `similar_to` (an empty vector **is** the "neither" case, mirroring the server); `similar_to` to a missing node → 400 `NODE_NOT_FOUND` |
| `vector_create_index(name, prop, dim, metric="cosine", m=0, ef_construction=0)` | `POST /v1/vector/indexes` | metric `cosine`/`l2`/`dot`; duplicate → 409 `VECTOR_INDEX_EXISTS` |
| `vector_list_indexes()` | `GET /v1/vector/indexes` | declared indexes from the published clone |
| `vector_drop_index(name)` | `DELETE /v1/vector/indexes/{name}` | typed 404 `VECTOR_INDEX_NOT_FOUND` |
| `index_property(prop)` | Cypher `CREATE INDEX ON (n.prop)` | typed index; `summary.indexesCreated == 1` |
| `index_full_text(prop)` | Cypher `CREATE FULLTEXT INDEX ON (n.prop)` | full-text index |

Every method takes the shared per-request `timeout` (constructor, default
30 s; `await_seq` raises its client-side timeout to cover the server wait).

## Errors

All non-2xx responses raise typed exceptions carrying `status`, `code`
(the server's envelope code), and `message`:

- `VectraError` — anything else (400 `VALIDATION`, 400 `PARSE`, 500 `INTERNAL`, ...)
- `VectraNotFoundError` — 404 (`NODE_NOT_FOUND`, `VECTOR_INDEX_NOT_FOUND`)
- `VectraAuthError` — 401/403 (`UNAUTHORIZED`; static bearer auth via the
  server's `-auth-token-file`)
- `VectraConflictError` — 409 (`VECTOR_INDEX_EXISTS`, ...)
- `TxnConflictError(VectraConflictError)` — the **txn 409**, the one
  documented non-envelope response (see below)

Transport-level failures (DNS refused, connection reset, timeout) surface
as raw `urllib.error.URLError` (`HTTPError` subclasses are converted to the
typed exceptions above) — by design, keeping the SDK dependency-free.

## Transactions (OCC)

`txn()` wraps a batch in a guard: `snapshot` is the raft log index the
caller's reads were based on — get it from `stats()["publishedSeq"]` or
`Client.last_snapshot` (refreshed by every read and await — and by error
responses from read endpoints too, e.g. a 404 read still updates it: the
server sets the header before writing the error body).

Validation covers **the batch's own write set always**: writing a key that
any raft entry newer than `snapshot` touched rejects the whole commit
(nothing partial applies). With `ser=True` the read set is validated the
same way (`read_nodes` are keys, `read_edges` are `{"src", "dst", "label"}`
dicts) — serialization against concurrent rewrites of what you read.
`snapshot=0` means "read at epoch 0" and conflicts with any prior write to
the same keys; fresh never-written keys commit fine at 0.

A rejected commit is **409 without the error envelope** — the body is the
conflict report `{"committed": false, "conflicts": [...], "conflictTotal":
n, "hint": "..."}`. The SDK raises it as `TxnConflictError` with
`code="TXN_CONFLICT"` (synthesized — the server sends no envelope code
here) plus `conflicts`, `conflict_total`, `hint`, and `committed=False`
attributes. Every other 409 (e.g. `VECTOR_INDEX_EXISTS`) keeps the
envelope face and the server's own code. Retry pattern:

```python
from vectra_graph import TxnConflictError

for _ in range(5):
    try:
        client.txn(nodes=..., snapshot=client.stats()["publishedSeq"])
        break
    except TxnConflictError as e:
        ...  # backoff; e.conflicts lists the hot keys
```

## Vector indexes

Declarations (`vector_create_index` / `vector_drop_index`) are raft writes;
`vector_list_indexes` and searches serve from the published clone, so await
`publishedSeq` after declaring when you need read-your-declaration. Nodes
carry vectors as plain numeric arrays on the indexed prop
(`{"embedding": [1.0, 0.0, 0.0]}`). `vector_search` takes exactly one of
`query_vector` (dim must match the declaration) or `similar_to` (a node
key); hits are `{"key", "score"}` with smaller scores better. Two
deliberate faces to know: `similar_to` pointing at a node that does not
exist answers **400 `NODE_NOT_FOUND`** (a bad body reference, not the 404
resource face), and an empty `query_vector` is treated as absent — the
server's own rule is `(len == 0) == (similar_to == "")` — so the client
raises the "exactly one" `ValueError` instead of sending a request.

## Index DDL and the Seal note

`index_property` / `index_full_text` send Cypher
`CREATE [FULLTEXT] INDEX ON (n.prop)` — a write statement whose response
summary reports `indexesCreated`. Declarations are **global per property
key** (label-scoped spellings are rejected) and stay **pending until the
next Seal/publish**: lookups on a pending key answer via a compensation
scan and switch to the index once the declaration merges. Re-declaring is
idempotent (each applied statement still counts one `indexesCreated`).
The prop identifier is deferred to the server parser — there is no
client-side identifier check (a regex would wrongly reject forms the
engine accepts, e.g. backtick-quoted keys): an empty prop round-trips as
`CREATE INDEX ON (n.)` and answers the server's 400 `PARSE`.

## Tests

End-to-end tests boot real `vectra-server` processes (skipped with a
message when the `go` toolchain is absent; `VECTRA_SDK_TEST_BIN` points at
a prebuilt binary):

```
cd sdk/python && python3 -m unittest discover -s tests -v
```

## Publishing

```
python3 -m pip install --upgrade build twine
python3 -m build                 # sdist + wheel (or: python3 -m pip wheel . -w dist --no-deps)
python3 -m twine upload dist/*
```
