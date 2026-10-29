"""Typed errors for the Vectra REST API.

Every non-2xx response carries the envelope ``{"error": {"code": ...,
"message": ...}}`` plus an HTTP status; the client maps that pair onto these
exception types so callers can branch on the exception class instead of
parsing strings.
"""

from __future__ import annotations


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


class VectraAuthError(VectraError):
    """401/403 — missing or rejected bearer token."""
