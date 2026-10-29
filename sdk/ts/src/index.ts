/**
 * @projanvil/vectra — zero-dependency TypeScript client for the Vectra
 * REST API (/v1/*).
 *
 * ```js
 * import { VectraClient, VectraNotFoundError } from "@projanvil/vectra";
 *
 * const vectra = new VectraClient({ baseUrl: "http://127.0.0.1:8080" });
 * await vectra.batch({ nodes: [{ key: "api", label: "Service" }] });
 * ```
 */

export {
  VectraClient,
  type BatchOptions,
  type BfsOptions,
  type FetchLike,
  type NeighborsOptions,
  type TxnOptions,
  type VectraClientOptions,
  type VectorCreateIndexOptions,
  type VectorSearchOptions,
} from "./client.js";
export {
  TxnConflictError,
  VectraAuthError,
  VectraConflictError,
  VectraError,
  VectraNotFoundError,
} from "./errors.js";
export {
  isCypherWrite,
  type AwaitResult,
  type BatchResult,
  type BfsNode,
  type BfsResult,
  type CypherReadResult,
  type CypherResult,
  type CypherWriteResult,
  type CyclesResult,
  type EdgeDelete,
  type EdgeRef,
  type EdgeUpsert,
  type HealthResult,
  type JsonMap,
  type NeighborsResult,
  type NodeRecord,
  type NodeUpsert,
  type PathResult,
  type ReachableResult,
  type StatsResult,
  type TxnResult,
  type VectorHit,
  type VectorIndexDeclaration,
  type VectorSearchResult,
  type WriteSummary,
} from "./types.js";
