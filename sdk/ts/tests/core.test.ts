/**
 * End-to-end tests for the @projanvil/vectra client against a real Vectra
 * server: health/stats, node lifecycle, traversals, Cypher read/write
 * faces, the typed error envelope, await-seq, and constructor env/fetch
 * handling. The file-level before() builds the Go server binary (or
 * honors VECTRA_SDK_TEST_BIN), boots one single-node server on a free
 * port, and shares it across every test below; each test writes only its
 * own key/label prefix, so test order never matters. Read-your-writes is
 * made deterministic by awaiting the published sequence after every write
 * (the same contract SDK users get via `awaitSeq`).
 */

import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";
import {
  BatchResult,
  VectraClient,
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
let client: VectraClient | null = null;
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

/** Apply one batch, then block until that write is published.
 *
 * Single-node servers publish shortly after the raft apply; awaiting
 * `publishedSeq + 1` makes every later read in the test observe the
 * write deterministically. */
async function writeAndSync(
  options: Parameters<VectraClient["batch"]>[0],
): Promise<BatchResult> {
  const before = (await c().stats()).publishedSeq;
  const result = await c().batch(options);
  const aw = await c().awaitSeq(before + 1);
  assert.ok(
    aw.reached,
    `write not published: awaited ${before + 1}, got ${JSON.stringify(aw)}`,
  );
  return result;
}

describe("health and stats", () => {
  it("health reports ok leader with a term", { skip: skipReason || false }, async () => {
    const h = await c().health();
    assert.equal(h.status, "ok");
    assert.equal(h.isLeader, true);
    assert.ok(typeof h.term === "number");
  });

  it("stats carries the graph counters and publishedSeq", { skip: skipReason || false }, async () => {
    const s = await c().stats();
    for (const key of ["nodes", "edges", "sccs", "cyclicNodes", "isLeader", "term", "publishedSeq"] as const) {
      assert.ok(key in s, `stats missing ${key}`);
    }
    assert.equal(s.publishedSeq >= 1, true);
    assert.equal(s.isLeader, true);
  });
});

describe("node lifecycle", () => {
  it("batch upsert, read, update, delete with a typed 404", { skip: skipReason || false }, async () => {
    // upsert
    const res = await writeAndSync({
      nodes: [{ key: "lc-a", label: "Service", props: { env: "prod", w: 2 } }],
    });
    assert.equal(res.nodesApplied, 1);
    // read back
    let n = await c().node("lc-a");
    assert.equal(n.key, "lc-a");
    assert.equal(n.label, "Service");
    assert.equal(n.props.env, "prod");
    // upsert again: same key updates props
    await writeAndSync({
      nodes: [{ key: "lc-a", label: "Service", props: { env: "staging" } }],
    });
    n = await c().node("lc-a");
    assert.equal(n.props.env, "staging");
    // delete, then the read is a typed 404
    const del = await writeAndSync({ nodeDeletes: ["lc-a"] });
    assert.equal(del.nodesDeleted, 1);
    await assert.rejects(
      () => c().node("lc-a"),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.status, 404);
        assert.equal(err.code, "NODE_NOT_FOUND");
        return true;
      },
    );
  });
});

describe("traversals", () => {
  before(async () => {
    if (skipReason) {
      return;
    }
    // Dependency graph: api -> auth -> db -> cache, api -> db (direct),
    // auth =METRICS=> db, plus a 2-cycle and a disconnected island.
    await writeAndSync({
      nodes: [
        { key: "tv-api", label: "Service" },
        { key: "tv-auth", label: "Service" },
        { key: "tv-db", label: "Service" },
        { key: "tv-cache", label: "Service" },
        { key: "tv-island", label: "Service" },
        { key: "tv-ca", label: "Service" },
        { key: "tv-cb", label: "Service" },
      ],
      edges: [
        { srcKey: "tv-api", dstKey: "tv-auth", label: "CALLS" },
        { srcKey: "tv-api", dstKey: "tv-db", label: "CALLS" },
        { srcKey: "tv-auth", dstKey: "tv-db", label: "CALLS" },
        { srcKey: "tv-auth", dstKey: "tv-db", label: "METRICS" },
        { srcKey: "tv-db", dstKey: "tv-cache", label: "CALLS" },
        { srcKey: "tv-ca", dstKey: "tv-cb", label: "CALLS" },
        { srcKey: "tv-cb", dstKey: "tv-ca", label: "CALLS" },
      ],
    });
  });

  const keys = (nodes: { key: string }[]): Set<string> =>
    new Set(nodes.map((n) => n.key));

  it("neighbors out", { skip: skipReason || false }, async () => {
    const res = await c().neighbors("tv-api");
    assert.deepEqual(keys(res.nodes), new Set(["tv-auth", "tv-db"]));
  });

  it("neighbors in", { skip: skipReason || false }, async () => {
    const res = await c().neighbors("tv-db", { direction: "in" });
    assert.deepEqual(keys(res.nodes), new Set(["tv-api", "tv-auth"]));
  });

  it("neighbors label filter", { skip: skipReason || false }, async () => {
    const res = await c().neighbors("tv-auth", { label: "METRICS" });
    assert.deepEqual(keys(res.nodes), new Set(["tv-db"]));
  });

  it("bfs depths", { skip: skipReason || false }, async () => {
    const res = await c().bfs(["tv-api"]);
    assert.equal(res.count, 4);
    const depths = new Map(res.nodes.map((n) => [n.key, n.depth]));
    assert.equal(depths.get("tv-api"), 0);
    assert.equal(depths.get("tv-auth"), 1);
    assert.equal(depths.get("tv-db"), 1);
    assert.equal(depths.get("tv-cache"), 2);
  });

  it("bfs maxDepth", { skip: skipReason || false }, async () => {
    const res = await c().bfs(["tv-api"], { maxDepth: 1 });
    assert.equal(res.count, 3);
    assert.deepEqual(
      keys(res.nodes),
      new Set(["tv-api", "tv-auth", "tv-db"]),
    );
  });

  it("bfs label filter", { skip: skipReason || false }, async () => {
    const res = await c().bfs(["tv-auth"], { labels: ["METRICS"] });
    assert.equal(res.count, 2);
    assert.deepEqual(keys(res.nodes), new Set(["tv-auth", "tv-db"]));
  });

  it("bfs direction in", { skip: skipReason || false }, async () => {
    const res = await c().bfs(["tv-cache"], { direction: "in" });
    assert.deepEqual(
      keys(res.nodes),
      new Set(["tv-cache", "tv-db", "tv-api", "tv-auth"]),
    );
  });

  it("path found", { skip: skipReason || false }, async () => {
    const res = await c().path("tv-auth", "tv-cache");
    assert.equal(res.found, true);
    assert.equal(res.hops, 2);
    assert.deepEqual(res.nodes, ["tv-auth", "tv-db", "tv-cache"]);
  });

  it("path not found", { skip: skipReason || false }, async () => {
    const res = await c().path("tv-api", "tv-island");
    assert.equal(res.found, false);
    assert.equal(res.hops, 0);
  });

  it("reachable", { skip: skipReason || false }, async () => {
    const res = await c().reachable("tv-api");
    const nodes: string[] = res.nodes;
    for (const expected of ["tv-api", "tv-auth", "tv-db", "tv-cache"]) {
      assert.ok(nodes.includes(expected), `missing ${expected}`);
    }
    assert.equal(nodes.includes("tv-island"), false);
    assert.equal(nodes.includes("tv-ca"), false);
  });

  it("cycles", { skip: skipReason || false }, async () => {
    const res = await c().cycles();
    const found = res.cycles.map((cycle) => new Set(cycle.nodes));
    assert.ok(
      found.some((cycle) => cycle.has("tv-ca") && cycle.has("tv-cb")),
      "expected the tv-ca/tv-cb cycle",
    );
  });
});

describe("cypher reads", () => {
  before(async () => {
    if (skipReason) {
      return;
    }
    await writeAndSync({
      nodes: [
        { key: "cy-a", label: "CySvc", props: { w: 5, tier: "gold" } },
        { key: "cy-b", label: "CySvc", props: { w: 2, tier: "silver" } },
        { key: "cy-x", label: "CySvc", props: { w: 9, tier: "gold" } },
      ],
      edges: [{ srcKey: "cy-a", dstKey: "cy-b", label: "CALLS" }],
    });
  });

  it("MATCH RETURN columns and rows", { skip: skipReason || false }, async () => {
    const res = await c().cypher("MATCH (n:CySvc {key:'cy-a'}) RETURN n.key, n.tier");
    if (isCypherWrite(res)) {
      assert.fail("read query returned the write face");
    }
    assert.deepEqual(res.columns, ["n.key", "n.tier"]);
    assert.deepEqual(res.rows, [{ "n.key": "cy-a", "n.tier": "gold" }]);
    assert.equal(res.count, 1);
  });

  it("WHERE and ORDER BY", { skip: skipReason || false }, async () => {
    const res = await c().cypher(
      "MATCH (s:CySvc) WHERE s.w > 4 RETURN s.key ORDER BY s.key",
    );
    if (isCypherWrite(res)) {
      assert.fail("read query returned the write face");
    }
    assert.equal(res.count, 2);
    assert.deepEqual(
      res.rows.map((row) => row["s.key"]),
      ["cy-a", "cy-x"],
    );
  });

  it("read responses never carry a summary", { skip: skipReason || false }, async () => {
    const res = await c().cypher("MATCH (n:CySvc) RETURN n.key");
    assert.equal(isCypherWrite(res), false);
  });
});

describe("cypher write summary", () => {
  it("CREATE returns summary counters, never a count key", { skip: skipReason || false }, async () => {
    const before = (await c().stats()).publishedSeq;
    const res = await c().cypher(
      "CREATE (n:CwSvc {key:'cw-x', env:'prod'}) RETURN n.key, n.env",
    );
    if (!isCypherWrite(res)) {
      assert.fail("write query returned the read face");
    }
    // write face: columns/rows from RETURN plus summary — never count
    assert.equal("count" in res, false);
    assert.deepEqual(res.columns, ["n.key", "n.env"]);
    assert.deepEqual(res.rows, [{ "n.key": "cw-x", "n.env": "prod" }]);
    assert.deepEqual(Object.keys(res.summary).sort(), [
      "createdEdges",
      "createdNodes",
      "deletedEdges",
      "deletedNodes",
      "indexesCreated",
      "propertiesSet",
    ]);
    assert.equal(res.summary.createdNodes, 1);
    assert.equal(res.summary.createdEdges, 0);
    // propertiesSet counts SET assignments; a pure CREATE map-literal
    // is part of node creation and does not count (wire truth).
    assert.equal(res.summary.propertiesSet, 0);
    // a SET statement bumps propertiesSet
    const set = await c().cypher("MATCH (n:CwSvc {key:'cw-x'}) SET n.env = 'staging'");
    if (!isCypherWrite(set)) {
      assert.fail("SET query returned the read face");
    }
    assert.equal(set.summary.propertiesSet, 1);
    // the writes really committed: visible to reads once published
    // (CREATE and SET are one raft entry each)
    const aw = await c().awaitSeq(before + 2);
    assert.equal(aw.reached, true);
    const got = await c().cypher("MATCH (n:CwSvc {key:'cw-x'}) RETURN n.env");
    if (isCypherWrite(got)) {
      assert.fail("read query returned the write face");
    }
    assert.equal(got.rows[0]["n.env"], "staging");
  });
});

describe("error envelope", () => {
  it("cypher parse error maps to the base VectraError", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().cypher("MATCH (a) RETURN b"), // unbound variable
      (err: unknown) => {
        assert.ok(err instanceof VectraError);
        assert.equal(err.status, 400);
        assert.equal(err.code, "PARSE");
        assert.ok(err.message.length > 0);
        // 400 maps to the base class, not a not-found/conflict subclass
        assert.equal(err instanceof VectraNotFoundError, false);
        assert.equal(err instanceof VectraConflictError, false);
        return true;
      },
    );
  });

  it("unknown node is a typed 404", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().node("no-such-node-404"),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.status, 404);
        assert.equal(err.code, "NODE_NOT_FOUND");
        return true;
      },
    );
  });

  it("unknown neighbors key is a typed 404", { skip: skipReason || false }, async () => {
    await assert.rejects(
      () => c().neighbors("no-such-node-404"),
      (err: unknown) => {
        assert.ok(err instanceof VectraNotFoundError);
        assert.equal(err.status, 404);
        assert.equal(err.code, "NODE_NOT_FOUND");
        return true;
      },
    );
  });
});

describe("await seq", () => {
  it("await reaches after a write and records the snapshot header", { skip: skipReason || false }, async () => {
    const before = (await c().stats()).publishedSeq;
    const res = await c().batch({
      nodes: [{ key: "aw-1", label: "Service" }],
    });
    assert.equal(res.nodesApplied, 1);
    const aw = await c().awaitSeq(before + 1, 5000);
    assert.equal(aw.reached, true);
    assert.ok(aw.publishedSeq >= before + 1);
    // the X-Vectra-Snapshot response header is surfaced by the client
    assert.notEqual(c().lastSnapshot, null);
    assert.equal(c().lastSnapshot, aw.publishedSeq);
    const n = await c().node("aw-1");
    assert.equal(n.key, "aw-1");
  });

  it("await timeout reports not reached", { skip: skipReason || false }, async () => {
    const aw = await c().awaitSeq(1_000_000_000, 200);
    assert.equal(aw.reached, false);
    assert.ok(aw.publishedSeq < 1_000_000_000);
  });
});

describe("constructor env and fetch handling", () => {
  // No server needed: env defaults + the fetch override (which enables
  // fetch-mocks without breaking zero-dep) are exercised with a stub.
  it("VECTRA_URL and VECTRA_TOKEN are whitespace-stripped", () => {
    const savedUrl = process.env.VECTRA_URL;
    const savedToken = process.env.VECTRA_TOKEN;
    process.env.VECTRA_URL = "  http://127.0.0.1:9999/  \n";
    process.env.VECTRA_TOKEN = " tok \n";
    try {
      const envClient = new VectraClient();
      assert.equal(envClient.baseUrl, "http://127.0.0.1:9999");
      assert.equal(envClient.token, "tok");
    } finally {
      if (savedUrl === undefined) {
        delete process.env.VECTRA_URL;
      } else {
        process.env.VECTRA_URL = savedUrl;
      }
      if (savedToken === undefined) {
        delete process.env.VECTRA_TOKEN;
      } else {
        process.env.VECTRA_TOKEN = savedToken;
      }
    }
  });

  it("the fetch override carries URL, headers, and body through", async () => {
    const seen: { url: string; init: RequestInit }[] = [];
    const stub = (url: string, init?: RequestInit): Promise<Response> => {
      seen.push({ url, init: init ?? {} });
      return Promise.resolve(
        new Response(JSON.stringify({ status: "ok", isLeader: true, term: 1 }), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        }),
      );
    };
    const stubClient = new VectraClient({
      baseUrl: "http://stub.example",
      token: "stub-token",
      fetch: stub,
    });
    const h = await stubClient.health();
    assert.equal(h.status, "ok");
    assert.equal(seen.length, 1);
    assert.equal(seen[0].url, "http://stub.example/v1/health");
    assert.equal(seen[0].init.method, "GET");
    assert.equal(
      (seen[0].init.headers as Record<string, string>).Authorization,
      "Bearer stub-token",
    );

    await stubClient.batch({ nodes: [{ key: "s-1", label: "S" }] });
    assert.equal(seen.length, 2);
    assert.equal(seen[1].url, "http://stub.example/v1/batch");
    assert.equal(seen[1].init.method, "POST");
    assert.equal(
      (seen[1].init.headers as Record<string, string>)["Content-Type"],
      "application/json",
    );
    assert.deepEqual(JSON.parse(String(seen[1].init.body)), {
      nodes: [{ key: "s-1", label: "S" }],
      edges: [],
      edgeDeletes: [],
      nodeDeletes: [],
    });
  });
});
