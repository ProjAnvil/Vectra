"""E2E test harness: build and boot a real vectra-server for the SDK tests.

``build_server()`` compiles the Go binary once (or honors
``VECTRA_SDK_TEST_BIN``); ``ServerFixture`` is a context manager that boots a
single-node server on a free port with a temp data dir, waits for
``/v1/health``, and always tears the process and tempdir back down.
"""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.error
import urllib.request

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))

_BUILD_DIR = None  # tempdir holding the built binary; cleaned by cleanup_build()


def build_server() -> str:
    """Return the path to a vectra-server binary.

    ``VECTRA_SDK_TEST_BIN`` overrides the build and is returned as-is.
    Otherwise ``go build ./server/cmd/vectra-server`` runs against the repo
    checkout into a tempdir removed by :func:`cleanup_build`. Raises
    ``unittest.SkipTest`` when the go toolchain is missing (or the override
    does not exist) so the suite skips with a clear message.
    """
    global _BUILD_DIR
    override = os.environ.get("VECTRA_SDK_TEST_BIN", "").strip()
    if override:
        if not os.path.isfile(override):
            raise unittest.SkipTest(
                "VECTRA_SDK_TEST_BIN=%r is not a file" % override
            )
        return override
    if shutil.which("go") is None:
        raise unittest.SkipTest(
            "go toolchain not found on PATH; install Go or set "
            "VECTRA_SDK_TEST_BIN to a prebuilt vectra-server binary"
        )
    _BUILD_DIR = tempfile.mkdtemp(prefix="vectra-sdk-build-")
    binary = os.path.join(_BUILD_DIR, "vectra-server")
    proc = subprocess.run(
        ["go", "build", "-o", binary, "./server/cmd/vectra-server"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        shutil.rmtree(_BUILD_DIR, ignore_errors=True)
        _BUILD_DIR = None
        raise RuntimeError("go build failed:\n" + proc.stderr)
    return binary


def cleanup_build() -> None:
    """Remove the tempdir ``build_server()`` created (no-op for overrides)."""
    global _BUILD_DIR
    if _BUILD_DIR is not None:
        shutil.rmtree(_BUILD_DIR, ignore_errors=True)
        _BUILD_DIR = None


def _free_port() -> int:
    """Pick a free TCP port: bind to port 0, read the assigned port, close."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _first_token_line(path: str) -> str:
    """First non-blank trimmed line of a token file (the server's loader
    accepts one token per line; the fixture uses the first for bootstrap)."""
    with open(path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if line:
                return line
    return ""


class ServerFixture:
    """One single-node vectra-server bound to a free port and a temp dir.

    Context manager: ``__enter__`` spawns the process and polls
    ``/v1/health`` until it returns 200 (deadline-bounded); ``__exit__``
    terminates (then kills) the process and removes the data dir even when
    the test failed. Empty ``-join`` boots a single-node raft cluster.
    ``auth_token_file`` (optional) enables static bearer auth: the flag is
    passed through, and the readiness bootstrap write presents the file's
    first token (GET /v1/health stays open — the exempt probe route).
    """

    def __init__(self, binary: str, ready_timeout: float = 30.0,
                 auth_token_file: str = None):
        self.binary = binary
        self.ready_timeout = ready_timeout
        self.auth_token_file = auth_token_file
        #: bearer token for the readiness bootstrap write ("" = auth off)
        self.auth_token = (
            _first_token_line(auth_token_file) if auth_token_file else ""
        )
        self.url = ""
        self.port = 0
        self._proc = None
        self._data_dir = None

    def __enter__(self) -> "ServerFixture":
        self.port = _free_port()
        self.url = "http://127.0.0.1:%d" % self.port
        self._data_dir = tempfile.mkdtemp(prefix="vectra-sdk-data-")
        # Explicit loopback raft address: the -raft default ":9000" resolves
        # to an unspecified IP, which hashicorp/raft rejects as "not
        # advertisable" — a dedicated free port keeps parallel runs safe too.
        raft_port = _free_port()
        argv = [
            self.binary,
            "-id", "t1",
            "-data", self._data_dir,
            "-http", "127.0.0.1:%d" % self.port,
            "-raft", "127.0.0.1:%d" % raft_port,
        ]
        if self.auth_token_file:
            argv += ["-auth-token-file", self.auth_token_file]
        self._proc = subprocess.Popen(
            argv,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            self._wait_ready()
        except BaseException:
            # Readiness failed: __exit__ would never run, so tear the
            # process and tempdir down here to keep a failing harness from
            # leaking them.
            self.__exit__(None, None, None)
            raise
        return self

    def _wait_ready(self) -> None:
        """Drive the server to readiness.

        A freshly bootstrapped single-node server publishes its first
        snapshot (and turns /v1/health green) only after its first raft
        write, so readiness is: one bootstrap batch (503 NOT_LEADER while
        the election runs, connection refused before listening) followed by
        /v1/health 200.
        """
        deadline = time.monotonic() + self.ready_timeout
        headers = {"Content-Type": "application/json"}
        if self.auth_token:
            # auth gates every route but GET /v1/health; the bootstrap
            # write must present a valid token or the FSM never publishes
            headers["Authorization"] = "Bearer " + self.auth_token
        boot_request = urllib.request.Request(
            self.url + "/v1/batch",
            data=json.dumps(
                {"nodes": [{"key": "__harness_boot__", "label": "System"}]}
            ).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        while time.monotonic() < deadline:
            if self._proc.poll() is not None:
                raise RuntimeError(
                    "vectra-server exited early with code %s" % self._proc.returncode
                )
            try:
                with urllib.request.urlopen(boot_request, timeout=2.0) as response:
                    if response.status == 200 and self._health_ok():
                        return
            except urllib.error.HTTPError:
                pass  # 503 NOT_LEADER during the election window
            except urllib.error.URLError:
                pass  # not listening yet
            time.sleep(0.1)
        raise TimeoutError(
            "vectra-server not ready within %.0fs at %s" % (self.ready_timeout, self.url)
        )

    def _health_ok(self) -> bool:
        try:
            with urllib.request.urlopen(
                self.url + "/v1/health", timeout=2.0
            ) as response:
                return response.status == 200
        except (urllib.error.HTTPError, urllib.error.URLError):
            return False

    def __exit__(self, exc_type, exc, tb) -> bool:
        proc = self._proc
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        if self._data_dir is not None:
            shutil.rmtree(self._data_dir, ignore_errors=True)
        return False
