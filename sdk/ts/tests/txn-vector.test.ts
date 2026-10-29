/**
 * End-to-end tests for POST /v1/txn (optimistic concurrency), the vector
 * index API, Cypher index DDL, and bearer auth.
 *
 * The txn guard carries the snapshot token the caller's reads were based
 * on — a raft log index, the same number GET /v1/stats reports as
 * `publishedSeq` and reads return as the `X-Vectra-Snapshot` header (kept
 * on `VectraClient.lastSnapshot`). A guarded commit rejected by
 * validation answers 409 with a NON-envelope body `{"committed": false,
 * "conflicts": [...], "conflictTotal": n, "hint": ...}`; the client
 * rejects with `TxnConflictError` (a `VectraConflictError` subclass
 * carrying the conflict fields) while every other 409 keeps the
 * `{"error": {...}}` envelope face (pinned here against
 * VECTOR_INDEX_EXISTS). The auth suite boots a SECOND server with
 * `-auth-token-file` to pin the bearer-token round trip and the typed 401.
 */

import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";
import {
  VectraClient,
  TxnConflictError,
  VectraAuthError,
  VectraConflictError,
  VectraError,
  VectraNotFoundError,
  isCypherWrite,
} from "../src/index.js";
import {
  ServerFixture,
  SkipTestError,
  buildServer,
  cleanupBuild,
} from "./harness.js";

let fixture: ServerFixture | null = null;
let client: VectraClient | null = null; // primary client
let other: VectraClient | null = null; // concurrent writer in conflict tests
// Decided at module load (like unittest's setUpModule): test registration
// below reads this value, so the go-missing skip must be known before the
// describe/it calls run — buildServer() is synchronous and memoized.
let skipReason = "";
try {
  buildServer();
} catch (err) {
  if (err instanceof SkipTestError) {
    skipReason = err.message;
  } else {
    throw err;
  }
}

before(async () => {
  if (skipReason) {
    return;
  }
  try {
    fixture = new ServerFixture(buildServer());
    await fixture.start();
    client = new VectraClient({ baseUrl: fixture.url });
    other = new VectraClient({ baseUrl: fixture.url });
  } catch (err) {
    if (fixture !== null) {
      await fixture.stop();
    }
    cleanupBuild();
    throw err;
  }
});

after(async () => {
  if (fixture !== null) {
    await fixture.stop();
  }
  cleanupBuild();
});

/** The shared client; throws when the fixture could not start. */
function c(): VectraClient {
  if (client === null) {
    throw new Error("server fixture not started");
  }
  return client;
}

function rival(): VectraClient {
  if (other === null) {
    throw new Error("server fixture not started");
  }
  return other;
}

/** Apply one auto-commit batch, then await its publish so later reads
 * (on any client) observe the write deterministically. */
async function sync(options: Parameters<VectraClient["batch"]>[0]) {
  const before = (await c().stats()).publishedSeq;
  const result = await c().batch(options);
  const aw = await c().awaitSeq(before + 1);
  assert.ok(
    aw.reached,
    `write not published: awaited ${before + 1}, got ${JSON.stringify(aw)}`,
  );
  return result;
}

/** Run a vector-index declaration (a raft write whose response carries
 * the declaration, not the raft index), then await its publication. */
async function declareAndAwait(declare: () => Promise<unknown>) {
  const before = (await c().stats()).publishedSeq;
  await declare();
  const aw = await c().awaitSeq(before + 1);
  assert.ok(
    aw.reached,
    `declaration not published: ${JSON.stringify(aw)}`,
  );
}

describe("txn commits", () => {
  it("guarded upsert and delete commit", { skip: skipReason || false }, async () => {
    await sync({
      nodes: [
        { key: "tc-seed-a", label: "Txn", props: { v: 1 } },
        { key: "tc-seed-b", label: "Txn", props: {} },
      ],
    });
    // guard snapshot taken from stats (publishedSeq == raft index)
    const snapshot = (await c().stats()).publishedSeq;
    const res = await c().txn({
      nodes: [{ key: "tc-seed-a", label: "Txn", props: { v: 2 } }],
      nodeDeletes: ["tc-seed-b"],
      snapshot,
    });
    assert.equal(res.committed, true);
    assert.equal(res.nodesApplied, 1);
    assert.equal(res.nodesDeleted, 1);
    assert.equal(res.edgesApplied, 0);
    assert.ok(res.index >= snapshot);
    // the committed entry publishes like any raft write
    const aw = await c().awaitSeq(res.index);
    assert.equal(aw.reached, true);
    const n = await c().node("tc-seed-a");
    assert.equal(n.props.v, 2);
    await assert.rejects(() => c().node("tc-seed-b"), VectraNotFoundError);
  });

  it("unguarded fresh keys commit at snapshot zero", { skip: skipReason || false }, async () => {
    // snapshot 0 reads "at epoch 0": fresh keys never written before have
    // no version entry, so a zero-snapshot guard on them commits cleanly
    const res = await c().txn({
      nodes: [{ key: "tc-zero", label: "Txn", props: {} }],
    });
    assert.equal(res.committed, true);
    assert.equal(res.nodesApplied, 1);
    assert.equal((await c().awaitSeq(res.index)).reached, true);
    assert.equal((await c().node("tc-zero")).key, "tc-zero");
  });
});

describe("txn conflicts", () => {
  it("stale write set conflicts as TxnConflictError", { skip: skipReason || false }, async () => {
    await sync({ nodes: [{ key: "tc-c1", label: "Txn", props: { v: 1 } }] });
    const snapshot = (await c().stats()).publishedSeq;
    // a second client commits past the snapshot...
    await rival().batch({
      nodes: [{ key: "tc-c1", label: "Txn", props: { v: 2 } }],
    });
    await c().awaitSeq(snapshot + 1);
    await assert.rejects(
      () =>
        c().txn({
          nodes: [{ key: "tc-c1", label: "Txn", props: { v: 3 } }],
          snapshot,
        }),
      (err: unknown) => {
        // the conflict face: VectraConflictError subclass, 409,
        // non-envelope body exposed as fields, code synthesized
        assert.ok(err instanceof TxnConflictError);
        assert.ok(err instanceof VectraConflictError);
        assert.equal(err.status, 409);
        assert.equal(err.code, "TXN_CONFLICT");
        assert.ok(err.conflicts.includes("tc-c1"));
        assert.ok(err.conflictTotal >= 1);
        assert.ok(err.hint.length > 0);
        assert.equal(err.committed, false);
        return true;
      },
    );
    // nothing partial was applied: the concurrent write is intact
    const n = await c().node("tc-c1");
    assert.equal(n.props.v, 2);
  });

  it("stale edge write conflicts", { skip: skipReason || false }, async () => {
    await sync({
      nodes: [{ key: "tc-e1", label: "Txn" }, { key: "tc-e2", label: "Txn" }],
    });
    const snapshot = (await c().stats()).publishedSeq;
    await rival().batch({
      edges: [{ srcKey: "tc-e1", dstKey: "tc-e2", label: "CALLS" }],
    });
    await c().awaitSeq(snapshot + 1);
    await assert.rejects(
      () =>
        c().txn({
          edges: [
            {
              srcKey: "tc-e1",
              dstKey: "tc-e2",
              label: "CALLS",
              props: { w: 2 },
            },
          ],
          snapshot,
        }),
      (err: unknown) => {
        assert.ok(err instanceof TxnConflictError);
        assert.equal(err.code, "TXN_CONFLICT");
        // edge conflicts report the "src\0dst\0label" key string
        assert.ok(err.conflicts.some((conflict) => conflict.includes("tc-e1")));
        return true;
      },
    );
  });

  it("refreshed snapshot retry commits", { skip: skipReason || false }, async () => {
    // the hint's contract: refresh the snapshot, retry, expect success
    await sync({ nodes: [{ key: "tc-r1", label: "Txn", props: { v: 1 } }] });
    const stale = (await c().stats()).publishedSeq;
    await rival().batch({
      nodes: [{ key: "tc-r1", label: "Txn", props: { v: 2 } }],
    });
    await c().awaitSeq(stale + 1);
    await assert.rejects(
      () =>
        c().txn({
          nodes: [{ key: "tc-r1", label: "Txn", props: { v: 3 } }],
          snapshot: stale,
        }),
      TxnConflictError,
    );
    const fresh = (await c().stats()).publishedSeq;
    const res = await c().txn({
      nodes: [{ key: "tc-r1", label: "Txn", props: { v: 3 } }],
      snapshot: fresh,
    });
    assert.equal(res.committed, true);
    assert.equal((await c().awaitSeq(res.index)).reached, true);
    const n = await c().node("tc-r1");
    assert.equal(n.props.v, 3);
  });
});

describe("txn SER read sets", () => {
  it("clean read set commits", { skip: skipReason || false }, async () => {
    await sync({ nodes: [{ key: "tc-s1", label: "Txn", props: { v: 1 } }] });
    // the read returns X-Vectra-Snapshot, recorded on lastSnapshot
    await c().node("tc-s1");
    const snapshot = c().lastSnapshot;
    assert.notEqual(snapshot, null);
    const res = await c().txn({
      nodes: [{ key: "tc-s1", label: "Txn", props: { v: 2 } }],
      snapshot: snapshot as number,
      ser: true,
      readNodes: ["tc-s1"],
    });
    assert.equal(res.committed, true);
    assert.equal((await c().awaitSeq(res.index)).reached, true);
    const n = await c().node("tc-s1");
    assert.equal(n.props.v, 2);
  });

  it("stale read set rejects", { skip: skipReason || false }, async () => {
    await sync({ nodes: [{ key: "tc-s2", label: "Txn", props: { v: 1 } }] });
    await c().node("tc-s2");
    const snapshot = c().lastSnapshot;
    // the read key is rewritten after the snapshot (write set below is a
    // fresh key, so ONLY the SER read set can produce the conflict)
    await rival().batch({
      nodes: [{ key: "tc-s2", label: "Txn", props: { v: 2 } }],
    });
    await c().awaitSeq((snapshot as number) + 1);
    await assert.rejects(
      () =>
        c().txn({
          nodes: [{ key: "tc-unrelated", label: "Txn", props: {} }],
          snapshot: snapshot as number,
          ser: true,
          readNodes: ["tc-s2"],
        }),
      (err: unknown) => {
        assert.ok(err instanceof TxnConflictError);
        assert.equal(err.code, "TXN_CONFLICT");
        assert.ok(err.conflicts.includes("tc-s2"));
        assert.equal(err.conflictTotal, 1);
        return true;
      },
    );
    // the guarded write set was not applied either
    await assert.rejects(() => c().node("tc-unrelated"), VectraNotFoundError);
  });
});

describe("vector lifecycle", () => {
  const INDEX = "vl-emb";

  before(async () => {
    if (skipReason) {
      return;
    }
    await declareAndAwait(() =>
      c().vectorCreateIndex(INDEX, "embedding", 3, { metric: "cosine" }),
    );
    await sync({
      nodes: [
        { key: "vl-a", label: "VecSvc", props: { embedding: [1.0, 0.0, 0.0] } },
        { key: "vl-b", label: "VecSvc", props: { embedding: [0.9, 0.1, 0.0] } },
        { key: "vl-c", label: "VecSvc", props: { embedding: [0.0, 1.0, 0.0] } },
        { key: "vl-d", label: "Other", props: { embedding: [0.0, 0.2, 0.9] } },
      ],
    });
  });

  it("create is visible in the declaration list", { skip: skipReason || false }, async () => {
    const listed = (await c().vectorListIndexes()).filter(
      (decl) => decl.name === INDEX,
    );
    assert.equal(listed.length, 1);
    assert.equal(listed[0].prop, "embedding");
    assert.equal(listed[0].dim, 3);
    assert.equal(listed[0].metric, "cosine");
    assert.ok(typeof listed[0].m === "number");
    assert.ok(typeof listed[0].efConstruction === "number");
  });

  it("list shape", { skip: skipReason || false }, async () => {
    const decls = await c().vectorListIndexes();
    assert.ok(Array.isArray(decls));
    for (const decl of decls) {
      for (const field of ["name", "prop", "dim", "metric", "m", "efConstruction"] as const) {
        assert.ok(field in decl, `declaration missing ${field}`);
      }
    }
  });

  it("search by queryVector", { skip: skipReason || false }, async () => {
    const res = await c().vectorSearch(INDEX, { queryVector: [1.0, 0.0, 0.0], k: 10 });
    assert.equal(res.count, 4); // all four nodes carry valid vectors
    assert.equal(res.hits[0].key, "vl-a"); // exact self-match first
    assert.deepEqual(
      new Set(res.hits.map((hit) => hit.key)),
      new Set(["vl-a", "vl-b", "vl-c", "vl-d"]),
    );
    for (const hit of res.hits) {
      // exact matches score 0, which JSON decodes as an integer
      assert.ok(typeof hit.score === "number");
    }
  });

  it("search by similarTo", { skip: skipReason || false }, async () => {
    const res = await c().vectorSearch(INDEX, { similarTo: "vl-a", k: 2 });
    assert.equal(res.count, 2);
    assert.equal(res.hits[0].key, "vl-a");
  });

  it("search label filter", { skip: skipReason || false }, async () => {
    const res = await c().vectorSearch(INDEX, {
      queryVector: [1.0, 0.0, 0.0],
      k: 10,
      label: "Other",
    });
    assert.equal(res.count, 1);
    assert.equal(res.hits[0].key, "vl-d");
  });

  it("drop then typed 404", { skip: skipReason || false }, async () => {
    // self-contained: uses its own index so test order never matters
    const name = "vl-drop";
    await declareAndAwait(() => c().vectorCreateIndex(name, "embedding", 3));
    assert.ok(
      (await c().vectorListIndexes()).some((decl) => decl.name === name),
    );
    // the drop is a raft write: read the pre-drop seq, then await the
    // entry so list/search observe the dropped declaration
    const before = (await c().stats()).publishedSeq;
    await c().vectorDropIndex(name);
    const aw = await c().awaitSeq(before + 1);
    assert.equal(aw.reached, true);
    assert.equal(
      (await c().vectorListIndexes()).some((decl) => decl.name === name),
      false,
    );
    await assert.rejects(
      () => c().vectorSearch(name, { queryVector: [1.0, 0.0, 0.0] }),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.status, 404);
        assert.equal(err.code, "VECTOR_INDEX_NOT_FOUND");
        return true;
      },
    );
    // dropping again answers the same typed 404
    await assert.rejects(
      () => c().vectorDropIndex(name),
      VectraNotFoundError,
    );
  });
});

describe("vector errors", () => {
  const INDEX = "ve-idx";

  before(async () => {
    if (skipReason) {
      return;
    }
    await declareAndAwait(() =>
      c().vectorCreateIndex(INDEX, "embedding", 2, { metric: "l2" }),
    );
  });

  it("duplicate create is the envelope 409 face", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().vectorCreateIndex(INDEX, "embedding", 2),
      (err: unknown) => {
        // the ENVELOPE face: the server's own code, not the txn face
        assert.ok(err instanceof VectraConflictError);
        assert.equal(err.status, 409);
        assert.equal(err.code, "VECTOR_INDEX_EXISTS");
        assert.equal(err instanceof TxnConflictError, false);
        assert.equal("conflicts" in err, false);
        return true;
      },
    );
  });

  it("bad metric is 400 VALIDATION", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().vectorCreateIndex("ve-bad", "embedding", 2, { metric: "manhattan" }),
      (err: unknown) => {
        assert.ok(err instanceof VectraError);
        assert.equal(err.status, 400);
        assert.equal(err.code, "VALIDATION");
        assert.ok(err.message.includes("metric"));
        return true;
      },
    );
  });

  it("dim mismatch is 400 VALIDATION", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().vectorSearch(INDEX, { queryVector: [1.0] }),
      (err: unknown) => {
        assert.ok(err instanceof VectraError);
        assert.equal(err.status, 400);
        assert.equal(err.code, "VALIDATION");
        return true;
      },
    );
  });

  it("unknown index is a typed 404", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().vectorSearch("ve-no-such-index", { queryVector: [0.5, 0.5] }),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.status, 404);
        assert.equal(err.code, "VECTOR_INDEX_NOT_FOUND");
        return true;
      },
    );
  });
});

describe("index DDL", () => {
  before(async () => {
    if (skipReason) {
      return;
    }
    await sync({
      nodes: [
        { key: "ddl-a", label: "DdlSvc", props: { tier: "gold", desc: "alpha" } },
        { key: "ddl-b", label: "DdlSvc", props: { tier: "silver", desc: "beta" } },
        { key: "ddl-c", label: "DdlSvc", props: { tier: "gold", desc: "gamma" } },
      ],
    });
  });

  it("indexProperty returns a write summary", { skip: skipReason || false }, async () => {
    const before = (await c().stats()).publishedSeq;
    const res = await c().indexProperty("tier");
    // write face: summary counters, never a count key
    assert.equal("count" in res, false);
    assert.equal(res.summary.indexesCreated, 1);
    assert.equal(res.summary.createdNodes, 0);
    const aw = await c().awaitSeq(before + 1);
    assert.equal(aw.reached, true);
  });

  it("typed lookup works after the declaration seals", { skip: skipReason || false }, async () => {
    // the declaration merges at the next Seal (each small-graph raft
    // entry publishes); once published the typed equality anchor works
    const res = await c().cypher(
      "MATCH (n:DdlSvc {tier:'gold'}) RETURN n.key ORDER BY n.key",
    );
    if (isCypherWrite(res)) {
      assert.fail("read query returned the write face");
    }
    assert.equal(res.count, 2);
    assert.deepEqual(
      res.rows.map((row) => row["n.key"]),
      ["ddl-a", "ddl-c"],
    );
  });

  it("indexFullText summary is idempotent", { skip: skipReason || false }, async () => {
    const res = await c().indexFullText("desc");
    assert.equal(res.summary.indexesCreated, 1);
    // pending declarations are idempotent to re-declare (still counted)
    const again = await c().indexFullText("desc");
    assert.equal(again.summary.indexesCreated, 1);
  });
});

describe("client validation (no server)", () => {
  // Pure argument validation: the TypeError raises synchronously before
  // any request is built, so these run even when the server-backed tests
  // skipped (go toolchain missing) — only spawn-dependent tests skip.
  const validator = new VectraClient({ baseUrl: "http://validation-only.invalid" });

  it("vectorSearch requires exactly one query mode", () => {
    assert.throws(() => validator.vectorSearch("any-index"), TypeError); // neither
    assert.throws(
      () =>
        validator.vectorSearch("any-index", {
          queryVector: [1.0, 0.0],
          similarTo: "anchor",
        }),
      TypeError, // both
    );
    // empty vector / empty anchor count as absent, mirroring the server's
    // (len === 0) === (similarTo === "") rule
    assert.throws(
      () => validator.vectorSearch("any-index", { queryVector: [] }),
      TypeError,
    );
    assert.throws(
      () => validator.vectorSearch("any-index", { similarTo: "" }),
      TypeError,
    );
  });
});

describe("stub transport (no server)", () => {
  // Fetch-override pins for the two behaviors the Python client pins with
  // its http.server stub (sdk/python/tests/test_unit.py): the
  // X-Vectra-Snapshot header is recorded even on error responses, and the
  // non-envelope txn 409 still constructs TxnConflictError when the body
  // lacks a usable conflictTotal. Serverless, so they never skip.

  it("snapshot header is recorded on an error response", async () => {
    const stubClient = new VectraClient({
      baseUrl: "http://stub.example",
      fetch: () =>
        Promise.resolve(
          new Response(
            JSON.stringify({
              error: { code: "NODE_NOT_FOUND", message: "node not found" },
            }),
            { status: 404, headers: { "X-Vectra-Snapshot": "7" } },
          ),
        ),
    });
    assert.equal(stubClient.lastSnapshot, null); // null -> set
    await assert.rejects(
      () => stubClient.node("missing"),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.code, "NODE_NOT_FOUND");
        return true;
      },
    );
    assert.equal(stubClient.lastSnapshot, 7);
  });

  it("txn 409 with absent conflictTotal falls back to conflicts.length", async () => {
    // The defensive parse: conflictTotal missing OR null must never leak
    // NaN/undefined into the error — the fallback is the list length.
    const bodies = [
      JSON.stringify({ committed: false, conflicts: ["k1", "k2"], hint: "h" }),
      JSON.stringify({
        committed: false,
        conflicts: ["k1"],
        conflictTotal: null,
        hint: "h",
      }),
    ];
    const expected = [2, 1];
    for (let i = 0; i < bodies.length; i += 1) {
      const stubClient = new VectraClient({
        baseUrl: "http://stub.example",
        fetch: () =>
          Promise.resolve(new Response(bodies[i], { status: 409 })),
      });
      await assert.rejects(
        () => stubClient.txn({ nodes: [{ key: "k1", label: "T" }], snapshot: 1 }),
        (err: unknown) => {
          assert.ok(err instanceof TxnConflictError);
          assert.equal(err.code, "TXN_CONFLICT");
          assert.equal(err.conflictTotal, expected[i]); // numeric fallback
          assert.equal(Number.isNaN(err.conflictTotal), false);
          assert.equal(err.committed, false);
          return true;
        },
      );
    }
  });
});

describe("auth round trip", () => {
  // Boots a SECOND server with -auth-token-file: good token round-trips
  // writes/reads; a wrong token gets the typed 401 envelope; GET
  // /v1/health stays open as the exempt probe route.
  const TOKEN = "sdk-e2e-token-7f3a";

  let authFixture: ServerFixture | null = null;
  let tokenDir = "";

  before(async () => {
    if (skipReason) {
      return;
    }
    tokenDir = mkdtempSync(join(tmpdir(), "vectra-sdk-token-"));
    const tokenFile = join(tokenDir, "token");
    writeFileSync(tokenFile, `${TOKEN}\n`, "utf8");
    authFixture = new ServerFixture(buildServer(), {
      authTokenFile: tokenFile,
    });
    try {
      await authFixture.start();
    } catch (err) {
      await authFixture.stop();
      rmSync(tokenDir, { recursive: true, force: true });
      tokenDir = "";
      throw err;
    }
  });

  after(async () => {
    if (authFixture !== null) {
      await authFixture.stop();
    }
    if (tokenDir) {
      rmSync(tokenDir, { recursive: true, force: true });
      tokenDir = "";
    }
  });

  it("good token writes and reads", { skip: skipReason || false }, async () => {
    const good = new VectraClient({
      baseUrl: authFixture!.url,
      token: TOKEN,
    });
    const before = (await good.stats()).publishedSeq;
    await good.batch({ nodes: [{ key: "auth-ok", label: "Auth", props: { v: 1 } }] });
    const aw = await good.awaitSeq(before + 1);
    assert.equal(aw.reached, true);
    const node = await good.node("auth-ok");
    assert.equal(node.label, "Auth");
    assert.equal(node.props.v, 1);
  });

  it("wrong token is a typed 401", { skip: skipReason || false }, async () => {
    const bad = new VectraClient({
      baseUrl: authFixture!.url,
      token: "definitely-wrong",
    });
    await assert.rejects(
      () => bad.batch({ nodes: [{ key: "auth-bad", label: "Auth" }] }),
      (err: unknown) => {
        assert.ok(err instanceof VectraAuthError);
        assert.equal(err.status, 401);
        assert.equal(err.code, "UNAUTHORIZED");
        return true;
      },
    );
    // every non-exempt route is gated, reads included
    await assert.rejects(() => bad.node("auth-ok"), VectraAuthError);
  });

  it("health stays open without a token", { skip: skipReason || false }, async () => {
    const bare = new VectraClient({
      baseUrl: authFixture!.url,
      token: "",
    });
    const h = await bare.health();
    assert.equal(h.status, "ok");
  });
});
