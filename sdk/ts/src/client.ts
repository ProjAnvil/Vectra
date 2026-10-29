/**
 * Asynchronous, zero-dependency HTTP client for the Vectra REST API
 * (/v1/*). Transport is the native `fetch` (Node >= 18) — override it
 * through the constructor for tests/fetch-mocks. Every call carries a
 * timeout. Errors arrive as the typed classes from `./errors.js` (HTTP
 * status + server error code).
 */

import {
  TxnConflictError,
  VectraAuthError,
  VectraConflictError,
  VectraError,
  VectraNotFoundError,
} from "./errors.js";
import type {
  AwaitResult,
  BatchResult,
  BfsResult,
  CypherResult,
  CypherWriteResult,
  CyclesResult,
  EdgeDelete,
  EdgeRef,
  EdgeUpsert,
  HealthResult,
  NeighborsResult,
  NodeRecord,
  NodeUpsert,
  PathResult,
  ReachableResult,
  StatsResult,
  TxnResult,
  VectorIndexDeclaration,
  VectorSearchResult,
} from "./types.js";

/** Minimal fetch shape the client needs — the native fetch satisfies it. */
export type FetchLike = (url: string, init?: RequestInit) => Promise<Response>;

export interface VectraClientOptions {
  /** server base URL; defaults to `$VECTRA_URL` then
   * `http://127.0.0.1:8080` (whitespace-stripped, trailing slash dropped). */
  baseUrl?: string;
  /** bearer token; defaults to `$VECTRA_TOKEN` (stripped — copy-pasted
   * tokens often carry a trailing newline that would otherwise guarantee
   * a 401). The Authorization header is only attached when the token is
   * non-empty, matching auth-off servers. */
  token?: string;
  /** per-request timeout in milliseconds, applied to every call. */
  timeoutMs?: number;
  /** fetch implementation; defaults to the global fetch. */
  fetch?: FetchLike;
}

export interface BatchOptions {
  nodes?: NodeUpsert[];
  edges?: EdgeUpsert[];
  edgeDeletes?: EdgeDelete[];
  nodeDeletes?: string[];
}

export interface TxnOptions extends BatchOptions {
  /** snapshot token the caller's reads were based on (a raft log index
   * from `stats().publishedSeq` or `client.lastSnapshot`). */
  snapshot?: number;
  /** validate the SER read set the same way as the write set. */
  ser?: boolean;
  /** SER read set: node keys. */
  readNodes?: string[];
  /** SER read set: edges as `{src, dst, label}`. */
  readEdges?: EdgeRef[];
}

export interface BfsOptions {
  /** 0/omitted means unlimited hops. */
  maxDepth?: number;
  /** restrict traversal to edges carrying one of the given labels. */
  labels?: string[];
  /** "out" (default) or "in". */
  direction?: string;
}

export interface NeighborsOptions {
  /** "out" (default) or "in". */
  direction?: string;
  /** restrict to edges carrying this label. */
  label?: string;
}

export interface VectorSearchOptions {
  /** query vector; the dim must match the declaration (enforced by the
   * server as a 400). Exactly one of `queryVector`/`similarTo`. */
  queryVector?: number[];
  /** node key holding a valid vector on the indexed prop. */
  similarTo?: string;
  /** top-k (default 10). */
  k?: number;
  /** 0/omitted takes the server's adaptive default. */
  ef?: number;
  /** filter hits by node label. */
  label?: string;
}

export interface VectorCreateIndexOptions {
  /** one of "cosine" (default) / "l2" / "dot". */
  metric?: string;
  /** 0/omitted takes the engine default (16). */
  m?: number;
  /** 0/omitted takes the engine default (200). */
  efConstruction?: number;
}

interface RequestOptions {
  params?: Record<string, string | number | boolean>;
  body?: unknown;
  timeoutMs?: number;
}

function isJsonMap(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Vectra REST API client.
 *
 * ```js
 * import { VectraClient } from "@projanvil/vectra";
 * const vectra = new VectraClient({ baseUrl: "http://127.0.0.1:8080" });
 * await vectra.batch({ nodes: [{ key: "a", label: "Service" }] });
 * ```
 */
export class VectraClient {
  readonly baseUrl: string;
  readonly token: string;
  readonly timeoutMs: number;
  /** Sequence number from the most recent `X-Vectra-Snapshot` response
   * header (reads and awaits carry it); `null` until one is seen. Use it
   * as the snapshot token for optimistic transactions. */
  lastSnapshot: number | null = null;
  private readonly fetchFn: FetchLike;

  constructor(options: VectraClientOptions = {}) {
    this.baseUrl = (options.baseUrl ?? process.env.VECTRA_URL ??
      "http://127.0.0.1:8080").trim().replace(/\/+$/, "");
    this.token = (options.token ?? process.env.VECTRA_TOKEN ?? "").trim();
    this.timeoutMs = options.timeoutMs ?? 30_000;
    this.fetchFn = options.fetch ?? globalThis.fetch.bind(globalThis);
  }

  // ---- transport -------------------------------------------------------

  /** One request through the shared transport: build URL + headers,
   * JSON-encode the body, decode the response, map error envelopes onto
   * typed errors, and record the snapshot header when present. */
  private async request<T>(
    method: string,
    path: string,
    options: RequestOptions = {},
  ): Promise<T> {
    const url = new URL(this.baseUrl + path);
    if (options.params) {
      for (const [key, value] of Object.entries(options.params)) {
        url.searchParams.set(key, String(value));
      }
    }
    const headers: Record<string, string> = { Accept: "application/json" };
    let body: string | undefined;
    if (options.body !== undefined) {
      body = JSON.stringify(options.body);
      headers["Content-Type"] = "application/json";
    }
    if (this.token) {
      headers.Authorization = `Bearer ${this.token}`;
    }
    const res = await this.fetchFn(url.toString(), {
      method,
      headers,
      body,
      signal: AbortSignal.timeout(
        options.timeoutMs ?? this.timeoutMs,
      ),
    });
    const snapshot = res.headers.get("X-Vectra-Snapshot");
    if (snapshot !== null) {
      const parsed = Number.parseInt(snapshot, 10);
      if (!Number.isNaN(parsed)) {
        this.lastSnapshot = parsed;
      }
    }
    if (!res.ok) {
      throw VectraClient.mapError(res.status, res.statusText, await res.text());
    }
    const text = await res.text();
    return (text ? (JSON.parse(text) as T) : ({} as T)) as T;
  }

  /** Decode the response body and pick the error class.
   *
   * The `{"error": {"code", "message"}}` envelope maps by HTTP status:
   * 401/403 -> Auth, 404 -> NotFound, 409 -> Conflict, anything else ->
   * base VectraError. One documented deviation: a 409 whose body is the
   * txn conflict *report* (`committed`/`conflicts` keys, no envelope —
   * server/api.go handleTxn) becomes TxnConflictError carrying the
   * conflict fields. */
  private static mapError(
    status: number,
    statusText: string,
    raw: string,
  ): VectraError {
    let code = "";
    let message = statusText;
    let payload: unknown = null;
    try {
      payload = raw ? (JSON.parse(raw) as unknown) : null;
    } catch {
      // body was not JSON — keep the status text as the message
    }
    if (isJsonMap(payload)) {
      const envelope = payload.error;
      if (isJsonMap(envelope)) {
        code = String(envelope.code ?? "");
        const serverMessage = String(envelope.message ?? "");
        if (serverMessage) {
          message = serverMessage;
        }
      }
    }
    if (
      status === 409 &&
      code === "" &&
      isJsonMap(payload) &&
      "committed" in payload &&
      "conflicts" in payload
    ) {
      const hint = String(payload.hint ?? "");
      const conflicts = Array.isArray(payload.conflicts)
        ? payload.conflicts.map(String)
        : [];
      const conflictTotal = typeof payload.conflictTotal === "number"
        ? payload.conflictTotal
        : conflicts.length;
      message = "transaction conflict" + (hint ? `: ${hint}` : "");
      return new TxnConflictError(message, conflicts, conflictTotal, hint);
    }
    if (status === 401 || status === 403) {
      return new VectraAuthError(message, status, code);
    }
    if (status === 404) {
      return new VectraNotFoundError(message, status, code);
    }
    if (status === 409) {
      return new VectraConflictError(message, status, code);
    }
    return new VectraError(message, status, code);
  }

  // ---- reads -----------------------------------------------------------

  /** GET /v1/health — liveness plus leader/term.
   *
   * Gated on the FSM's first raft publish (503 "starting" before it,
   * i.e. before the first write); `awaitSeq` is the endpoint with no
   * readiness gate. */
  health(): Promise<HealthResult> {
    return this.request<HealthResult>("GET", "/v1/health");
  }

  /** GET /v1/stats — graph counters, raft term, publishedSeq. */
  stats(): Promise<StatsResult> {
    return this.request<StatsResult>("GET", "/v1/stats");
  }

  /** GET /v1/nodes/{key} — one node; rejects with VectraNotFoundError
   * on 404. */
  node(key: string): Promise<NodeRecord> {
    return this.request<NodeRecord>(
      "GET",
      `/v1/nodes/${encodeURIComponent(key)}`,
    );
  }

  /** GET /v1/neighbors — adjacent nodes ("out"/"in", optional label). */
  neighbors(
    key: string,
    options: NeighborsOptions = {},
  ): Promise<NeighborsResult> {
    const params: Record<string, string> = {
      key,
      dir: options.direction ?? "out",
    };
    if (options.label) {
      params.label = options.label;
    }
    return this.request<NeighborsResult>("GET", "/v1/neighbors", { params });
  }

  /** POST /v1/query — BFS from a set of start keys.
   *
   * maxDepth 0/omitted means unlimited hops; labels restrict traversal
   * to edges carrying one of the given labels. */
  bfs(from: string[], options: BfsOptions = {}): Promise<BfsResult> {
    const body: Record<string, unknown> = {
      from,
      direction: options.direction ?? "out",
    };
    if (options.maxDepth) {
      body.maxDepth = options.maxDepth;
    }
    if (options.labels && options.labels.length > 0) {
      body.labels = options.labels;
    }
    return this.request<BfsResult>("POST", "/v1/query", { body });
  }

  /** GET /v1/path — shortest path ("found", "hops", "nodes"). */
  path(from: string, to: string): Promise<PathResult> {
    return this.request<PathResult>("GET", "/v1/path", {
      params: { from, to },
    });
  }

  /** GET /v1/reachable — keys of all nodes reachable from `from`. */
  reachable(from: string): Promise<ReachableResult> {
    return this.request<ReachableResult>("GET", "/v1/reachable", {
      params: { from },
    });
  }

  /** GET /v1/cycles — elementary cycles as key lists. */
  cycles(): Promise<CyclesResult> {
    return this.request<CyclesResult>("GET", "/v1/cycles");
  }

  /** POST /v1/cypher — one Cypher-subset statement.
   *
   * READ statements answer with `{"columns", "rows", "count"}` (typed
   * CypherReadResult); WRITE statements (CREATE/SET/DELETE/MERGE/CREATE
   * INDEX) answer with `{"columns", "rows", "summary"}` and never a
   * `count` key (typed CypherWriteResult) — discriminate with
   * `isCypherWrite`. Parse and semantic errors surface as VectraError
   * with code `"PARSE"`. */
  cypher(query: string): Promise<CypherResult> {
    return this.request<CypherResult>("POST", "/v1/cypher", {
      body: { query },
    });
  }

  // ---- writes ------------------------------------------------------------

  /** POST /v1/batch — auto-commit changeset, applied in fixed order:
   * node upserts -> edge upserts -> edge deletes -> node deletes.
   *
   * Item shapes (see server/batch.go): node `{"key", "label", "props"}`;
   * edge `{"srcKey", "dstKey", "label", "props", "allowParallel"}`; edge
   * delete `{"srcKey", "dstKey", "label"}`. Returns applied counts. */
  batch(options: BatchOptions = {}): Promise<BatchResult> {
    return this.request<BatchResult>("POST", "/v1/batch", {
      body: {
        nodes: options.nodes ?? [],
        edges: options.edges ?? [],
        edgeDeletes: options.edgeDeletes ?? [],
        nodeDeletes: options.nodeDeletes ?? [],
      },
    });
  }

  /** POST /v1/txn — optimistic-concurrency (OCC) transactional commit.
   *
   * Item shapes and fixed apply order match `batch()`; the extra `txn`
   * guard carries the snapshot token the caller's reads were based on
   * (a raft log index — take it from `stats().publishedSeq` or
   * `client.lastSnapshot`, refreshed by every read/await) plus an
   * optional SER read set.
   *
   * Validation always covers the batch's own write set: a key written by
   * a raft entry newer than `snapshot` rejects the whole commit. With
   * `ser: true` the read set is validated the same way — `readNodes`
   * are node keys, `readEdges` are `{src, dst, label}` objects.
   * `snapshot: 0` means "read at epoch 0" and conflicts with any prior
   * write to the same keys, so always pass a real snapshot
   * (fresh never-written keys commit fine at 0).
   *
   * A rejected commit rejects with TxnConflictError — the txn 409 body
   * is a conflict report, not an error envelope — exposing
   * `conflicts`/`conflictTotal`/`hint`; refresh the snapshot and retry.
   * A commit returns the batch counters plus `committed: true` and the
   * raft `index`. */
  txn(options: TxnOptions = {}): Promise<TxnResult> {
    const guard: Record<string, unknown> = {
      snapshot: options.snapshot ?? 0,
      ser: options.ser ?? false,
    };
    if (options.readNodes && options.readNodes.length > 0) {
      guard.readNodes = options.readNodes;
    }
    if (options.readEdges && options.readEdges.length > 0) {
      guard.readEdges = options.readEdges;
    }
    return this.request<TxnResult>("POST", "/v1/txn", {
      body: {
        nodes: options.nodes ?? [],
        edges: options.edges ?? [],
        edgeDeletes: options.edgeDeletes ?? [],
        nodeDeletes: options.nodeDeletes ?? [],
        txn: guard,
      },
    });
  }

  /** GET /v1/await — read-your-writes long poll.
   *
   * Blocks server-side until `publishedSeq >= seq` or `timeoutMs`
   * elapses (the server caps the wait at 30s), then returns
   * `{"publishedSeq", "reached"}`. The `X-Vectra-Snapshot` response
   * header is recorded on `client.lastSnapshot`. The client-side timeout
   * is raised to cover the server wait plus a margin. */
  awaitSeq(seq: number, timeoutMs = 5000): Promise<AwaitResult> {
    return this.request<AwaitResult>("GET", "/v1/await", {
      params: { seq, timeoutMs },
      timeoutMs: Math.max(this.timeoutMs, timeoutMs + 5000),
    });
  }

  // ---- vector indexes + index DDL ---------------------------------------

  /** POST /v1/vector/search — approximate top-k over a declared index.
   *
   * Exactly one of `queryVector` (dim must match the declaration;
   * enforced server-side as a 400) or `similarTo` (a node key holding a
   * valid vector on the indexed prop) — anything else throws a TypeError
   * synchronously. An empty vector or empty anchor counts as absent,
   * mirroring the server's `(len===0)===(similarTo==="")` rule. Returns
   * `{"count", "hits"}` with hits of `{"key", "score"}` — smaller score
   * is better. Unknown index rejects with VectraNotFoundError
   * (VECTOR_INDEX_NOT_FOUND). */
  vectorSearch(
    index: string,
    options: VectorSearchOptions = {},
  ): Promise<VectorSearchResult> {
    const hasVector =
      options.queryVector !== undefined && options.queryVector.length > 0;
    const hasSimilarTo = options.similarTo !== undefined && options.similarTo !== "";
    if (hasVector === hasSimilarTo) {
      throw new TypeError(
        "vectorSearch: provide exactly one of queryVector or similarTo",
      );
    }
    const body: Record<string, unknown> = { index, k: options.k ?? 10 };
    if (hasVector) {
      body.query_vector = options.queryVector!.map((x) => Number(x));
    } else {
      body.similar_to = options.similarTo;
    }
    if (options.ef) {
      body.ef = options.ef;
    }
    if (options.label) {
      body.label = options.label;
    }
    return this.request<VectorSearchResult>("POST", "/v1/vector/search", {
      body,
    });
  }

  /** POST /v1/vector/indexes — declare a vector index (raft write).
   *
   * `metric` is one of "cosine"/"l2"/"dot"; `dim` 1..4096; zero
   * `m`/`efConstruction` take the engine defaults (16/200). A duplicate
   * name rejects with VectraConflictError carrying the server's envelope
   * code VECTOR_INDEX_EXISTS; a bad metric/dim rejects with VectraError
   * 400 VALIDATION. Returns `{"index": {...}}` — the declaration reaches
   * reads once published (await the seq bumped by this raft entry when
   * in doubt). */
  vectorCreateIndex(
    name: string,
    prop: string,
    dim: number,
    options: VectorCreateIndexOptions = {},
  ): Promise<{ index: VectorIndexDeclaration }> {
    const body: Record<string, unknown> = {
      name,
      prop,
      dim,
      metric: options.metric ?? "cosine",
    };
    if (options.m) {
      body.m = options.m;
    }
    if (options.efConstruction) {
      body.efConstruction = options.efConstruction;
    }
    return this.request<{ index: VectorIndexDeclaration }>(
      "POST",
      "/v1/vector/indexes",
      { body },
    );
  }

  /** GET /v1/vector/indexes — declared vector indexes.
   *
   * Each entry carries name/prop/dim/metric/m/efConstruction. Served
   * from the published clone. */
  vectorListIndexes(): Promise<VectorIndexDeclaration[]> {
    return this.request<{ indexes: VectorIndexDeclaration[] }>(
      "GET",
      "/v1/vector/indexes",
    ).then((res) => res.indexes);
  }

  /** DELETE /v1/vector/indexes/{name} — drop a declaration.
   *
   * Raft write; resolves without a value on success. Dropping an
   * unknown name rejects with VectraNotFoundError
   * (VECTOR_INDEX_NOT_FOUND). */
  async vectorDropIndex(name: string): Promise<void> {
    await this.request<unknown>(
      "DELETE",
      `/v1/vector/indexes/${encodeURIComponent(name)}`,
    );
  }

  /** Cypher `CREATE INDEX ON (n.prop)` — declare a typed index.
   *
   * Declarations are global per property key (label-scoped spellings are
   * rejected by the engine) and PENDING until the next Seal/publish —
   * typed lookups answer via compensation scan until the declaration
   * merges, then serve from the index. Idempotent: re-declaring still
   * counts one `summary.indexesCreated`. Returns the cypher write
   * response; check `summary.indexesCreated === 1`. */
  indexProperty(prop: string): Promise<CypherWriteResult> {
    return this.cypher(`CREATE INDEX ON (n.${prop})`) as Promise<CypherWriteResult>;
  }

  /** Cypher `CREATE FULLTEXT INDEX ON (n.prop)` — declare an FTS index.
   *
   * Same declaration contract as `indexProperty`: pending until the
   * next Seal, idempotent, counted in `summary.indexesCreated`. */
  indexFullText(prop: string): Promise<CypherWriteResult> {
    return this.cypher(
      `CREATE FULLTEXT INDEX ON (n.${prop})`,
    ) as Promise<CypherWriteResult>;
  }
}
