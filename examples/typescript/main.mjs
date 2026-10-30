// Project knowledge graph — TypeScript SDK (@projanvil/vectra) example.
//
// One scenario against a running Vectra server: batch-upsert Person/
// Project nodes (one project carries a CJK name) and WORKS_ON
// edges, declare a FULLTEXT index, run a Cypher write with RETURN (CREATE
// and SET) plus a MATCH read, hit the CJK name through full-text, then
// declare a vector index, write one 4-dim vector through batch props, and
// answer a top-1 vector search.
//
// From examples/typescript (server on 18080 — see ../README.md):
//
//   VECTRA_URL=http://127.0.0.1:18080 node main.mjs

import { VectraClient, VectraNotFoundError } from "@projanvil/vectra";

const vectra = new VectraClient({
  baseUrl: process.env.VECTRA_URL ?? "http://127.0.0.1:18080",
  token: process.env.VECTRA_TOKEN ?? "",
});

// [1] Batch upsert: the project knowledge graph. The first write also
// bootstraps a fresh server — health answers 503 "starting" until the
// FSM's first raft publish.
await vectra.batch({
  nodes: [
    { key: "ada", label: "Person", props: { name: "Ada" } },
    { key: "linus", label: "Person", props: { name: "Linus" } },
    { key: "vectra", label: "Project", props: { name: "Vectra engine" } },
    { key: "graphdb", label: "Project", props: { name: "图数据库" } },
  ],
  edges: [
    { srcKey: "ada", dstKey: "vectra", label: "WORKS_ON", props: { since: 2025 } },
    { srcKey: "ada", dstKey: "graphdb", label: "WORKS_ON", props: { since: 2026 } },
    { srcKey: "linus", dstKey: "graphdb", label: "WORKS_ON", props: { since: 2026 } },
  ],
});
console.log("[1] connected:", await vectra.health());

// Full-text index on name: pending until the server's next Seal/publish;
// CONTAINS answers via full scan meanwhile (and hits the bigram buckets
// once the declaration merges).
await vectra.indexFullText("name");

// [2] Cypher write with RETURN: CREATE a person plus her edge, anchored
// on a MATCHed project. Writes return {columns, rows, summary} — summary
// carries the mutation counters.
const before = (await vectra.stats()).publishedSeq;
const created = await vectra.cypher(`MATCH (pr:Project {key:'graphdb'})
CREATE (p:Person {key:'zoe', name:'Zoe'})-[:WORKS_ON {since: 2026}]->(pr)
RETURN p.name AS person, pr.name AS project`);
console.log(
  `[2] cypher CREATE: createdNodes=${created.summary.createdNodes}` +
    ` createdEdges=${created.summary.createdEdges},` +
    ` ${created.rows[0].person} works on ${created.rows[0].project}`,
);

// [3] Cypher SET with RETURN (a null RHS would delete the property).
const updated = await vectra.cypher(
  `MATCH (p:Person {key:'zoe'}) SET p.role = 'engineer' RETURN p.key, p.role`,
);
console.log(
  `[3] cypher SET: propertiesSet=${updated.summary.propertiesSet},` +
    ` ${updated.rows[0]["p.key"]} is now an ${updated.rows[0]["p.role"]}`,
);
// Read-your-writes: take the sequence number BEFORE the writes, await it
// after — the two statements above are two raft entries.
await vectra.awaitSeq(before + 2);

// [4] Cypher read: who works on what.
const read = await vectra.cypher(`MATCH (p:Person)-[:WORKS_ON]->(pr:Project)
RETURN p.name AS person, pr.name AS project ORDER BY p.name`);
console.log("[4] who works on what:");
for (const row of read.rows) {
  console.log(`    ${row.person} -> ${row.project}`);
}

// [5] Full-text query: the CJK name. On an FTS-declared key the CONTAINS
// anchor scan uses the bigram buckets as a prefilter.
const fts = await vectra.cypher(
  `MATCH (n) WHERE n.name CONTAINS '图数' RETURN n.key, n.name`,
);
console.log(`[5] full-text 图数: ${fts.count} hit(s)`);
for (const row of fts.rows) {
  console.log(`    ${row["n.key"]} = ${row["n.name"]}`);
}

// [6] Vector search: declare a 4-dim cosine index, write one vector
// through batch node props (the Cypher subset has no list literals —
// vectors never ride Cypher), then query top-1. The node upsert replaces
// props wholesale, so name rides along with emb.
try {
  await vectra.vectorDropIndex("person-emb"); // rerun-safe: drop a previous declaration
} catch (err) {
  if (!(err instanceof VectraNotFoundError)) throw err; // first run: nothing to drop
}
await vectra.vectorCreateIndex("person-emb", "emb", 4);
await vectra.batch({
  nodes: [
    { key: "ada", label: "Person", props: { name: "Ada", emb: [0.10, 0.90, 0.20, 0.40] } },
  ],
});
const res = await vectra.vectorSearch("person-emb", {
  queryVector: [0.12, 0.88, 0.25, 0.35],
  k: 1,
});
const top = res.hits[0];
console.log(`[6] vector top-1: ${top.key} (score ${top.score.toFixed(4)})`);
