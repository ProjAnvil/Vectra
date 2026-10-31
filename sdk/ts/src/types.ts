/**
 * Wire types for the Vectra REST client (/v1/*).
 *
 * These are hand-written views of the server's JSON responses (see
 * server/api.go and server/batch.go) — the SDK ships no runtime
 * dependency and no code generation, so each interface documents the
 * exact keys the server writes.
 */

/** Arbitrary JSON object (node properties, cypher row cells). */
export interface JsonMap {
  [key: string]: unknown;
}

/** Node upsert for batch/txn writes: `{"key", "label", "props"}`. */
export interface NodeUpsert {
  key: string;
  label: string;
  props?: JsonMap;
}

/** Edge upsert for batch/txn writes:
 * `{"srcKey", "dstKey", "label", "props", "allowParallel"}`. */
export interface EdgeUpsert {
  srcKey: string;
  dstKey: string;
  label: string;
  props?: JsonMap;
  allowParallel?: boolean;
}

/** Edge delete addressed by its logical key:
 * `{"srcKey", "dstKey", "label"}`. */
export interface EdgeDelete {
  srcKey: string;
  dstKey: string;
  label: string;
}

/** Edge read-set entry for SER transactions — mirrors server/batch.go
 * `EdgeRef`: `{"src", "dst", "label"}`. */
export interface EdgeRef {
  src: string;
  dst: string;
  label: string;
}

/** GET /v1/health — liveness plus leader/term.
 *
 * Gated on the FSM's first raft publish (503 "starting" before it, i.e.
 * before the first write); `awaitSeq` is the endpoint with no readiness
 * gate. */
export interface HealthResult {
  status: string;
  isLeader: boolean;
  term: number;
  [field: string]: unknown;
}

/** GET /v1/stats — graph counters, raft term, publishedSeq. */
export interface StatsResult {
  nodes: number;
  edges: number;
  sccs: number;
  cyclicNodes: number;
  isLeader: boolean;
  term: number;
  publishedSeq: number;
  [field: string]: unknown;
}

/** GET /v1/nodes/{key} — one node. */
export interface NodeRecord {
  key: string;
  label: string;
  props: JsonMap;
}

/** GET /v1/neighbors — adjacent nodes ("out"/"in", optional label). */
export interface NeighborsResult {
  nodes: NodeRecord[];
}

/** One BFS row: POST /v1/query emits `{key, depth}` pairs. */
export interface BfsNode {
  key: string;
  depth: number;
}

/** POST /v1/query — BFS from a set of start keys. */
export interface BfsResult {
  count: number;
  nodes: BfsNode[];
  [field: string]: unknown;
}

/** GET /v1/path — shortest path. */
export interface PathResult {
  found: boolean;
  hops: number;
  nodes: string[];
}

/** GET /v1/reachable — keys of all reachable nodes. */
export interface ReachableResult {
  nodes: string[];
}

/** GET /v1/cycles — elementary cycles as key lists. */
export interface CyclesResult {
  cycles: { nodes: string[] }[];
}

/** POST /v1/batch — applied counters, in the fixed apply order node
 * upserts -> edge upserts -> edge deletes -> node deletes. */
export interface BatchResult {
  nodesApplied: number;
  edgesApplied: number;
  edgesSkipped: number;
  edgesDeleted: number;
  nodesDeleted: number;
  nodesSkipped: number;
}

/** POST /v1/txn commit — the batch counters plus `committed` and the
 * raft `index` of the entry. */
export interface TxnResult extends BatchResult {
  committed: boolean;
  index: number;
}

/** POST /v1/cypher READ response: `{"columns", "rows", "count"}`. */
export interface CypherReadResult {
  columns: string[];
  rows: JsonMap[];
  count: number;
}

/** The seven write counters POST /v1/cypher reports on WRITE statements
 * (CREATE/SET/DELETE/MERGE/CREATE INDEX/CREATE CONSTRAINT). */
export interface WriteSummary {
  createdNodes: number;
  createdEdges: number;
  deletedNodes: number;
  deletedEdges: number;
  propertiesSet: number;
  indexesCreated: number;
  constraintsCreated: number;
}

/** POST /v1/cypher WRITE response: `{"columns", "rows", "summary"}` —
 * never a `count` key. */
export interface CypherWriteResult {
  columns: string[];
  rows: JsonMap[];
  summary: WriteSummary;
}

/** POST /v1/cypher response: READ answers with `count`, WRITE with
 * `summary` — discriminate with {@link isCypherWrite}. */
export type CypherResult = CypherReadResult | CypherWriteResult;

/** Type guard: WRITE cypher responses carry `summary`, READ ones carry
 * `count` — the two faces never mix on the wire. */
export function isCypherWrite(result: CypherResult): result is CypherWriteResult {
  return "summary" in result;
}

/** GET /v1/await — read-your-writes long poll. */
export interface AwaitResult {
  publishedSeq: number;
  reached: boolean;
}

/** One approximate-nearest-neighbor hit — smaller score is better. */
export interface VectorHit {
  key: string;
  score: number;
}

/** POST /v1/vector/search response: `{"count", "hits"}`. */
export interface VectorSearchResult {
  count: number;
  hits: VectorHit[];
}

/** A declared vector index (POST/GET /v1/vector/indexes). */
export interface VectorIndexDeclaration {
  name: string;
  prop: string;
  dim: number;
  metric: string;
  m: number;
  efConstruction: number;
}
