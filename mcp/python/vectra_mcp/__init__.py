"""Vectra MCP server (Python/FastMCP): wraps a running Vectra HTTP service.

The Go binary `vectra-mcp` embeds the engine directly; this Python server is
the polyglot counterpart for teams standardizing on Python MCP hosts — it
talks to any Vectra server node (single or raft cluster) over its HTTP API.
"""
from .server import mcp, main  # noqa: F401
