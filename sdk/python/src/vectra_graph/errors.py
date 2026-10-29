"""Typed errors for the Vectra REST API.

Every non-2xx response carries the envelope ``{"error": {"code": ...,
"message": ...}}`` plus an HTTP status — with one documented exception: a
rejected ``POST /v1/txn`` answers 409 with a conflict *report* instead of
the envelope (see :class:`TxnConflictError`). The client maps responses
onto these exception types so callers can branch on the exception class
instead of parsing strings.
"""

from __future__ import annotations

from typing import Optional


class VectraError(Exception):
    """Any non-OK Vectra API response.

    Attributes:
        status: HTTP status code from the response (0 when the error did not
            originate from an HTTP response body).
        code: server error code from the envelope, e.g. ``"NODE_NOT_FOUND"``
            (``""`` when the body was not a recognizable envelope).
        message: human-readable message from the envelope.
    """

    def __init__(self, message: str, status: int = 0, code: str = ""):
        super().__init__(message)
        self.message = message
        self.status = status
        self.code = code


class VectraNotFoundError(VectraError):
    """404 — the addressed resource does not exist (``NODE_NOT_FOUND``)."""


class VectraConflictError(VectraError):
    """409 — transaction conflict or duplicate declaration (``VECTOR_INDEX_EXISTS``)."""


class TxnConflictError(VectraConflictError):
    """409 from POST /v1/txn — optimistic validation rejected the commit.

    The txn endpoint is the one non-envelope 409 on the wire: instead of
    ``{"error": {...}}`` the body is the conflict report
    ``{"committed": false, "conflicts": [...], "conflictTotal": n,
    "hint": "..."}``, so ``code`` is synthesized as ``"TXN_CONFLICT"``.
    Catch this class (or :class:`VectraConflictError`) and retry with a
    refreshed snapshot per ``hint``; every other 409 keeps the envelope
    face with the server's own code.

    Attributes:
        conflicts: the conflicting keys — node keys, or
            ``"src\\x00dst\\x00label"`` strings for edges — truncated to
            the first 100 by the server.
        conflict_total: total conflict count before that truncation.
        hint: server retry guidance.
        committed: always ``False`` when this exception is raised.
    """

    def __init__(
        self,
        message: str,
        conflicts: Optional[list] = None,
        conflict_total: int = 0,
        hint: str = "",
        status: int = 409,
        code: str = "TXN_CONFLICT",
    ):
        super().__init__(message, status, code)
        self.conflicts = list(conflicts or [])
        self.conflict_total = conflict_total
        self.hint = hint
        self.committed = False


class VectraAuthError(VectraError):
    """401/403 — missing or rejected bearer token."""
