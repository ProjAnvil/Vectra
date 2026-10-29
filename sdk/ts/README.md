# @projanvil/vectra

Zero-dependency TypeScript client for the [Vectra](https://github.com/ProjAnvil/Vectra)
REST API (`/v1/*`): graph reads, traversals, Cypher, batch/txn writes,
vector indexes, and index DDL. Requires Node >= 18 (native `fetch`); the
published package has **no runtime dependencies** and ships
`strict`-mode TypeScript with generated `.d.ts`.

```bash
npm install @projanvil/vectra
```

## Quickstart

```js
import { VectraClient, TxnConflictError } from "@projanvil/vectra";

const vectra = new VectraClient({ baseUrl: "http://127.0.0.1:8080" });

// Auto-commit write, then read-your-writes.
await vectra.batch({
  nodes: [
    { key: "api", label: "Service", props: { env: "prod" } },
    { key: "auth", label: "Service" },
  ],
  edges: [{ srcKey: "api", dstKey: "auth", label: "CALLS" }],
});
const s = await vectra.stats();
await vectra.awaitSeq(s.publishedSeq + 1);

// Traversal.
const walk = await vectra.bfs(["api"], { maxDepth: 2 });

// Optimistic transaction: refresh the snapshot on conflict and retry.
try {
  await vectra.txn({
    nodes: [{ key: "api", label: "Service", props: { env: "staging" } }],
    snapshot: vectra.lastSnapshot,
  });
} catch (err) {
  if (err instanceof TxnConflictError) {
    await vectra.txn({
      nodes: [{ key: "api", label: "Service", props: { env: "staging" } }],
      snapshot: (await vectra.stats()).publishedSeq,
    });
  }
}
```

## Configuration

```js
new VectraClient({
  baseUrl: "http://127.0.0.1:8080", // default: $VECTRA_URL, then http://127.0.0.1:8080
  token: "secret",                  // default: $VECTRA_TOKEN ("" = no Authorization header)
  timeoutMs: 30_000,                // per-request timeout
  fetch: myFetch,                   // fetch override (tests / fetch-mocks); default: global fetch
});
```

- `VECTRA_URL` / `VECTRA_TOKEN` env defaults are whitespace-stripped
  (copy-pasted tokens often carry a trailing newline that would
  otherwise guarantee a 401); a trailing slash on `baseUrl` is dropped.
- `client.lastSnapshot` holds the sequence number from the most recent
  `X-Vectra-Snapshot` response header (reads and awaits refresh it — and
  so do error responses from read endpoints, e.g. a 404 read: the server
  sets the header before writing the error body) — use it as the snapshot
  token for optimistic transactions.

## API

| Method | Request | Notes |
| --- | --- | --- |
| `health()` | `GET /v1/health` | liveness + leader/term; 503 "starting" until the FSM's first raft publish |
| `stats()` | `GET /v1/stats` | graph counters, raft term, `publishedSeq` |
| `node(key)` | `GET /v1/nodes/{key}` | typed 404 (`NODE_NOT_FOUND`) when missing |
| `neighbors(key, {direction, label})` | `GET /v1/neighbors` | `direction`: "out" (default) / "in" |
| `bfs(from, {maxDepth, labels, direction})` | `POST /v1/query` | `maxDepth` 0/omitted = unlimited |
| `path(from, to)` | `GET /v1/path` | `{found, hops, nodes}` |
| `reachable(from)` | `GET /v1/reachable` | keys of all reachable nodes |
| `cycles()` | `GET /v1/cycles` | elementary cycles as key lists |
| `cypher(query)` | `POST /v1/cypher` | union return — see [Typed cypher results](#typed-cypher-results) |
| `batch({nodes, edges, edgeDeletes, nodeDeletes})` | `POST /v1/batch` | auto-commit; fixed apply order node upserts -> edge upserts -> edge deletes -> node deletes |
| `txn({..., snapshot, ser, readNodes, readEdges})` | `POST /v1/txn` | optimistic commit; rejects with `TxnConflictError` |
| `awaitSeq(seq, timeoutMs)` | `GET /v1/await` | read-your-writes long poll; server caps the wait at 30s |
| `vectorSearch(index, {queryVector, similarTo, k, ef, label})` | `POST /v1/vector/search` | exactly one of `queryVector` / `similarTo`; empty vector/anchor counts as absent (else `TypeError`) |
| `vectorCreateIndex(name, prop, dim, {metric, m, efConstruction})` | `POST /v1/vector/indexes` | `metric`: "cosine" (default) / "l2" / "dot" |
| `vectorListIndexes()` | `GET /v1/vector/indexes` | declarations served from the published clone |
| `vectorDropIndex(name)` | `DELETE /v1/vector/indexes/{name}` | typed 404 (`VECTOR_INDEX_NOT_FOUND`) when unknown |
| `indexProperty(prop)` | `POST /v1/cypher` | `CREATE INDEX ON (n.prop)`; check `summary.indexesCreated === 1` |
| `indexFullText(prop)` | `POST /v1/cypher` | `CREATE FULLTEXT INDEX ON (n.prop)`; idempotent while pending |

Batch item shapes (see the server's `server/batch.go`): node
`{key, label, props}`; edge `{srcKey, dstKey, label, props, allowParallel}`;
edge delete `{srcKey, dstKey, label}`.

## Typed cypher results

READ statements answer with `count`; WRITE statements
(CREATE/SET/DELETE/MERGE/CREATE INDEX) answer with `summary` and **never**
a `count` key — the two faces never mix on the wire, so a type guard
discriminates them:

```js
import { isCypherWrite } from "@projanvil/vectra";

const res = await vectra.cypher("CREATE (n:Service {key:'api'}) RETURN n.key");
if (isCypherWrite(res)) {
  console.log(res.summary.createdNodes); // 1
}
```

`WriteSummary` carries exactly six counters: `createdNodes`,
`createdEdges`, `deletedNodes`, `deletedEdges`, `propertiesSet`,
`indexesCreated`. `propertiesSet` counts SET assignments — a CREATE
map-literal is part of node creation and does not count.
`indexProperty` / `indexFullText` are typed as always-write calls.

## Errors

Every non-2xx response maps onto a typed error class carrying `status`
(HTTP code) and `code` (the server's envelope code, e.g.
`"NODE_NOT_FOUND"`):

| Class | Maps | Typical codes |
| --- | --- | --- |
| `VectraError` | any other non-2xx | `PARSE`, `VALIDATION` |
| `VectraNotFoundError` | 404 | `NODE_NOT_FOUND`, `VECTOR_INDEX_NOT_FOUND` |
| `VectraConflictError` | 409 envelope | `VECTOR_INDEX_EXISTS` |
| `VectraAuthError` | 401/403 | `UNAUTHORIZED` |

### The two 409 faces

There are exactly two 409 shapes on the wire:

1. **Envelope 409** — `{"error": {"code": "VECTOR_INDEX_EXISTS", ...}}`,
   e.g. declaring a duplicate vector index. Surfaces as
   `VectraConflictError` with the server's own `code`.
2. **Txn conflict report 409** — `POST /v1/txn` answers 409 with a
   *non-envelope* body `{"committed": false, "conflicts": [...],
   "conflictTotal": n, "hint": "refresh snapshot ... and retry"}`.
   Surfaces as `TxnConflictError` (a `VectraConflictError` subclass)
   carrying `conflicts`, `conflictTotal`, `hint`, `committed: false`,
   and the synthesized `code: "TXN_CONFLICT"`:

```js
} catch (err) {
  if (err instanceof TxnConflictError) {
    // err.conflicts: string[] (node keys, or "src\0dst\0label" for edges)
    // err.conflictTotal: number (conflicts may be truncated to 100)
    // err.hint: retry guidance from the server
  }
}
```

A rejected txn applies nothing — refresh the snapshot
(`stats().publishedSeq` or `client.lastSnapshot`) and retry.

## Development

```bash
cd sdk/ts
npm install --no-save   # devDependencies only (typescript + @types/node)
npm run typecheck       # tsc --noEmit over src
npm test                # compile src+tests, run node:test E2E against a real server
npm run build           # tsc -> dist (also runs via prepare on install/publish)
```

The E2E suite builds the Go server (`go build ./server/cmd/vectra-server`
from the repo root), boots a single-node server on a free port with an
explicit loopback `-raft` address, bootstraps it with one write (health
stays 503 "starting" until the first raft write), and always tears the
process down in `after()`. When the go toolchain is missing the
server-backed tests `t.skip` with a clear reason.

Environment conventions:

- `VECTRA_SDK_TEST_BIN` — path to a prebuilt `vectra-server` binary;
  overrides the `go build` (the suite skips if the path is not a file).
- `VECTRA_URL`, `VECTRA_TOKEN` — client defaults (see Configuration).

## Publishing

Built and published from `sdk/ts` (documented, not executed here):

```bash
npm publish --access public
```

`prepare` runs `npm run build`, so the tarball ships `dist/`
(ESM + generated `.d.ts`) — `npm pack --dry-run` shows exactly what is
included.

## License

MIT — see [LICENSE](./LICENSE).
