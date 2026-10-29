/**
 * E2E test harness: build and boot a real vectra-server for the SDK tests.
 *
 * `buildServer()` compiles the Go binary once (or honors
 * `VECTRA_SDK_TEST_BIN`); `ServerFixture` boots a single-node server on a
 * free port with a temp data dir, waits for readiness, and always tears
 * the process and tempdir back down in `stop()` — call it from `after()`
 * regardless of test failures. When the go toolchain is missing the
 * harness throws {@link SkipTestError}; each test passes its message to
 * `t.skip()` so the suite skips with a clear reason.
 */

import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import * as net from "node:net";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

/**
 * Repo root: the ancestor of this file that owns `server/cmd/vectra-server`.
 * Walked rather than hard-coded so the harness works from both the
 * source tree (`sdk/ts/tests`) and the compiled tree (`sdk/ts/dist-test/tests`).
 */
export const REPO_ROOT = findRepoRoot(
  dirname(fileURLToPath(import.meta.url)),
);

function findRepoRoot(start: string): string {
  let dir = resolve(start);
  for (let i = 0; i < 6; i += 1) {
    if (existsSync(join(dir, "server", "cmd", "vectra-server"))) {
      return dir;
    }
    const parent = dirname(dir);
    if (parent === dir) {
      break;
    }
    dir = parent;
  }
  throw new Error(
    "could not locate the Vectra repo root (server/cmd/vectra-server) " +
      `above ${start}`,
  );
}

/** Thrown to request a suite-wide skip (go missing / bad VECTRA_SDK_TEST_BIN). */
export class SkipTestError extends Error {}

let buildDir: string | null = null; // tempdir holding the built binary
let cachedBinary: string | null = null; // memoized path (override or build)

/** Pick a free TCP port: listen on port 0, read the assigned port once
 * the listening event fires, close, resolve. */
export function freePort(): Promise<number> {
  return new Promise((resolvePort, reject) => {
    const server = net.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (address === null || typeof address === "string") {
        server.close();
        reject(new Error("could not pick a free port"));
        return;
      }
      const { port } = address;
      server.close(() => resolvePort(port));
    });
  });
}

/**
 * Return the path to a vectra-server binary.
 *
 * `VECTRA_SDK_TEST_BIN` overrides the build and is returned as-is.
 * Otherwise `go build ./server/cmd/vectra-server` runs against the repo
 * checkout into a tempdir removed by {@link cleanupBuild}. Throws
 * {@link SkipTestError} when the go toolchain is missing (or the override
 * does not exist) so the suite skips with a clear message.
 */
export function buildServer(): string {
  if (cachedBinary !== null) {
    return cachedBinary;
  }
  const override = (process.env.VECTRA_SDK_TEST_BIN ?? "").trim();
  if (override) {
    if (!existsSync(override)) {
      throw new SkipTestError(`VECTRA_SDK_TEST_BIN=${override} is not a file`);
    }
    cachedBinary = override;
    return override;
  }
  const go = spawnSync("go", ["version"], { stdio: "ignore" });
  if (go.error !== undefined || go.status !== 0) {
    throw new SkipTestError(
      "go toolchain not found on PATH; install Go or set " +
        "VECTRA_SDK_TEST_BIN to a prebuilt vectra-server binary",
    );
  }
  buildDir = mkdtempSync(join(tmpdir(), "vectra-sdk-build-"));
  const binary = join(buildDir, "vectra-server");
  const proc = spawnSync(
    "go",
    ["build", "-o", binary, "./server/cmd/vectra-server"],
    { cwd: REPO_ROOT, encoding: "utf8" },
  );
  if (proc.status !== 0) {
    cleanupBuild();
    throw new Error("go build failed:\n" + (proc.stderr ?? ""));
  }
  cachedBinary = binary;
  return binary;
}

/** Remove the tempdir `buildServer()` created (no-op for overrides). */
export function cleanupBuild(): void {
  if (buildDir !== null) {
    rmSync(buildDir, { recursive: true, force: true });
    buildDir = null;
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** First non-blank trimmed line of a token file (the server's loader
 * accepts one token per line; the fixture uses the first for bootstrap). */
function firstTokenLine(path: string): string {
  for (const line of readFileSync(path, "utf8").split("\n")) {
    const trimmed = line.trim();
    if (trimmed) {
      return trimmed;
    }
  }
  return "";
}

export interface ServerFixtureOptions {
  /** Readiness deadline in ms (default 30s). */
  readyTimeoutMs?: number;
  /** Enables static bearer auth via `-auth-token-file`. */
  authTokenFile?: string;
}

/**
 * One single-node vectra-server bound to a free port and a temp dir.
 *
 * `start()` spawns the process and polls readiness (see {@link waitReady});
 * `stop()` SIGTERMs (then SIGKILLs) the process and removes the data dir
 * even when the tests failed — always call it from `after()`. Empty
 * `-join` boots a single-node raft cluster. `authTokenFile` enables
 * static bearer auth: the flag is passed through, and the readiness
 * bootstrap write presents the file's first token (GET /v1/health stays
 * open — the exempt probe route).
 */
export class ServerFixture {
  url = "";
  port = 0;
  /** bearer token for the readiness bootstrap write ("" = auth off) */
  readonly authToken: string;
  private readonly binary: string;
  private readonly readyTimeoutMs: number;
  private readonly authTokenFile?: string;
  private proc: ChildProcess | null = null;
  private exited: Promise<void> = Promise.resolve();
  private dataDir: string | null = null;

  constructor(binary: string, options: ServerFixtureOptions = {}) {
    this.binary = binary;
    this.readyTimeoutMs = options.readyTimeoutMs ?? 30_000;
    this.authTokenFile = options.authTokenFile;
    this.authToken = options.authTokenFile
      ? firstTokenLine(options.authTokenFile)
      : "";
  }

  async start(): Promise<void> {
    this.port = await freePort();
    this.url = `http://127.0.0.1:${this.port}`;
    this.dataDir = mkdtempSync(join(tmpdir(), "vectra-sdk-data-"));
    // Explicit loopback raft address: the -raft default ":9000" resolves
    // to an unspecified IP, which hashicorp/raft rejects as "not
    // advertisable" — a dedicated free port keeps parallel runs safe too.
    const raftPort = await freePort();
    const argv = [
      this.binary,
      "-id",
      "t1",
      "-data",
      this.dataDir,
      "-http",
      `127.0.0.1:${this.port}`,
      "-raft",
      `127.0.0.1:${raftPort}`,
    ];
    if (this.authTokenFile) {
      argv.push("-auth-token-file", this.authTokenFile);
    }
    this.proc = spawn(argv[0], argv.slice(1), {
      stdio: "ignore",
    });
    this.exited = new Promise<void>((resolve) => {
      this.proc!.once("exit", () => resolve());
    });
    try {
      await this.waitReady();
    } catch (err) {
      // Readiness failed: after() may never run, so tear the process and
      // tempdir down here to keep a failing harness from leaking them.
      await this.stop();
      throw err;
    }
  }

  /**
   * Drive the server to readiness.
   *
   * A freshly bootstrapped single-node server publishes its first
   * snapshot (and turns /v1/health green) only after its first raft
   * write, so readiness is: one bootstrap batch (503 NOT_LEADER while
   * the election runs, connection refused before listening) followed by
   * /v1/health 200.
   */
  private async waitReady(): Promise<void> {
    const deadline = Date.now() + this.readyTimeoutMs;
    const headers: Record<string, string> = {
      "Content-Type": "application/json",
    };
    if (this.authToken) {
      // auth gates every route but GET /v1/health; the bootstrap write
      // must present a valid token or the FSM never publishes
      headers.Authorization = `Bearer ${this.authToken}`;
    }
    const body = JSON.stringify({
      nodes: [{ key: "__harness_boot__", label: "System" }],
    });
    while (Date.now() < deadline) {
      // Fail fast when the process is already gone: a clean exit sets
      // exitCode, but a signal kill leaves exitCode null with signalCode
      // set — either way, continuing to poll would only burn the deadline.
      const exitCode = this.proc?.exitCode ?? null;
      const signal = this.proc?.signalCode ?? null;
      if (exitCode !== null || signal !== null) {
        throw new Error(
          `server exited during startup (exit=${exitCode}, signal=${signal})`,
        );
      }
      try {
        const res = await fetch(`${this.url}/v1/batch`, {
          method: "POST",
          headers,
          body,
          signal: AbortSignal.timeout(2000),
        });
        if (res.status === 200 && (await this.healthOk())) {
          return;
        }
      } catch {
        // not listening yet / election window — keep polling
      }
      await sleep(100);
    }
    throw new Error(
      `vectra-server not ready within ${this.readyTimeoutMs}ms at ${this.url}`,
    );
  }

  private async healthOk(): Promise<boolean> {
    try {
      const res = await fetch(`${this.url}/v1/health`, {
        signal: AbortSignal.timeout(2000),
      });
      return res.status === 200;
    } catch {
      return false;
    }
  }

  /** Terminate the process (SIGTERM, then SIGKILL after 5s) and remove
   * the data dir; safe to call multiple times and when start() failed. */
  async stop(): Promise<void> {
    if (this.proc !== null && this.proc.exitCode === null) {
      this.proc.kill("SIGTERM");
      const killTimer = new Promise<void>((resolve) =>
        setTimeout(resolve, 5000).unref?.(),
      );
      await Promise.race([this.exited, killTimer]);
      if (this.proc.exitCode === null) {
        this.proc.kill("SIGKILL");
        await this.exited;
      }
    }
    if (this.dataDir !== null) {
      rmSync(this.dataDir, { recursive: true, force: true });
      this.dataDir = null;
    }
  }
}
