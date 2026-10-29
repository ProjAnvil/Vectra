/**
 * Typed errors for the Vectra REST API.
 *
 * Every non-2xx response carries the envelope `{"error": {"code": ...,
 * "message": ...}}` plus an HTTP status — with one documented exception:
 * a rejected `POST /v1/txn` answers 409 with a conflict *report* instead
 * of the envelope (see {@link TxnConflictError}). The client maps
 * responses onto these error types so callers can branch on
 * `instanceof` instead of parsing strings.
 */

/** Any non-OK Vectra API response. */
export class VectraError extends Error {
  /** HTTP status code from the response (0 when the error did not
   * originate from an HTTP response body). */
  readonly status: number;
  /** server error code from the envelope, e.g. `"NODE_NOT_FOUND"`
   * (`""` when the body was not a recognizable envelope). */
  readonly code: string;

  constructor(message: string, status = 0, code = "") {
    super(message);
    this.name = "VectraError";
    this.status = status;
    this.code = code;
  }
}

/** 404 — the addressed resource does not exist (`NODE_NOT_FOUND`,
 * `VECTOR_INDEX_NOT_FOUND`). */
export class VectraNotFoundError extends VectraError {
  constructor(message: string, status = 0, code = "") {
    super(message, status, code);
    this.name = "VectraNotFoundError";
  }
}

/** 409 — transaction conflict or duplicate declaration
 * (`VECTOR_INDEX_EXISTS`, and the txn face via {@link TxnConflictError}). */
export class VectraConflictError extends VectraError {
  constructor(message: string, status = 0, code = "") {
    super(message, status, code);
    this.name = "VectraConflictError";
  }
}

/** 409 from POST /v1/txn — optimistic validation rejected the commit.
 *
 * The txn endpoint is the one non-envelope 409 on the wire: instead of
 * `{"error": {...}}` the body is the conflict report `{"committed":
 * false, "conflicts": [...], "conflictTotal": n, "hint": "..."}`, so
 * `code` is synthesized as `"TXN_CONFLICT"`. Catch this class (or
 * {@link VectraConflictError}) and retry with a refreshed snapshot per
 * `hint`; every other 409 keeps the envelope face with the server's own
 * code.
 */
export class TxnConflictError extends VectraConflictError {
  /** the conflicting keys — node keys, or `"src\0dst\0label"` strings
   * for edges — truncated to the first 100 by the server. */
  readonly conflicts: string[];
  /** total conflict count before that truncation. */
  readonly conflictTotal: number;
  /** server retry guidance. */
  readonly hint: string;
  /** always `false` when this error is thrown. */
  readonly committed: false;

  constructor(
    message: string,
    conflicts: readonly string[] = [],
    conflictTotal = 0,
    hint = "",
    status = 409,
    code = "TXN_CONFLICT",
  ) {
    super(message, status, code);
    this.name = "TxnConflictError";
    this.conflicts = [...conflicts];
    this.conflictTotal = conflictTotal;
    this.hint = hint;
    this.committed = false;
  }
}

/** 401/403 — missing or rejected bearer token (`UNAUTHORIZED`). */
export class VectraAuthError extends VectraError {
  constructor(message: string, status = 0, code = "") {
    super(message, status, code);
    this.name = "VectraAuthError";
  }
}
