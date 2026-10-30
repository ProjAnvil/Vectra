"""Project knowledge graph — Python SDK (vectra-graph) example.

One scenario against a running Vectra server: batch-upsert Person/Project
nodes (one project carries a CJK name) and WORKS_ON edges,
declare a FULLTEXT index, run a Cypher write with RETURN (CREATE and SET)
plus a MATCH read, hit the CJK name through full-text, then declare a
vector index, write one 4-dim vector through batch props, and answer a
top-1 vector search.

From examples/python (server on 18080 — see ../README.md):

    VECTRA_URL=http://127.0.0.1:18080 PYTHONPATH=../../sdk/python/src python3 main.py
"""

import os

from vectra_graph import Client, VectraNotFoundError

client = Client(
    base_url=os.environ.get("VECTRA_URL", "http://127.0.0.1:18080"),
    token=os.environ.get("VECTRA_TOKEN", ""),
)

# [1] Batch upsert: the project knowledge graph. The first write also
# bootstraps a fresh server — health answers 503 "starting" until the
# FSM's first raft publish.
client.batch(
    nodes=[
        {"key": "ada", "label": "Person", "props": {"name": "Ada"}},
        {"key": "linus", "label": "Person", "props": {"name": "Linus"}},
        {"key": "vectra", "label": "Project", "props": {"name": "Vectra engine"}},
        {"key": "graphdb", "label": "Project", "props": {"name": "图数据库"}},
    ],
    edges=[
        {"srcKey": "ada", "dstKey": "vectra", "label": "WORKS_ON", "props": {"since": 2025}},
        {"srcKey": "ada", "dstKey": "graphdb", "label": "WORKS_ON", "props": {"since": 2026}},
        {"srcKey": "linus", "dstKey": "graphdb", "label": "WORKS_ON", "props": {"since": 2026}},
    ],
)
print("[1] connected:", client.health())

# Full-text index on name: pending until the server's next Seal/publish;
# CONTAINS answers via full scan meanwhile (and hits the bigram buckets
# once the declaration merges).
client.index_full_text("name")

# [2] Cypher write with RETURN: CREATE a person plus her edge, anchored on
# a MATCHed project. Writes return {columns, rows, summary} — summary
# carries the mutation counters.
before = client.stats()["publishedSeq"]
created = client.cypher(
    "MATCH (pr:Project {key:'graphdb'}) "
    "CREATE (p:Person {key:'zoe', name:'Zoe'})-[:WORKS_ON {since: 2026}]->(pr) "
    "RETURN p.name AS person, pr.name AS project"
)
summary = created["summary"]
print(
    "[2] cypher CREATE: createdNodes=%s createdEdges=%s, %s works on %s"
    % (summary["createdNodes"], summary["createdEdges"],
       created["rows"][0]["person"], created["rows"][0]["project"])
)

# [3] Cypher SET with RETURN (a null RHS would delete the property).
updated = client.cypher(
    "MATCH (p:Person {key:'zoe'}) SET p.role = 'engineer' RETURN p.key, p.role"
)
row = updated["rows"][0]
print(
    "[3] cypher SET: propertiesSet=%s, %s is now an %s"
    % (updated["summary"]["propertiesSet"], row["p.key"], row["p.role"])
)
# Read-your-writes: take the sequence number BEFORE the writes, await it
# after — the two statements above are two raft entries.
client.await_seq(before + 2)

# [4] Cypher read: who works on what.
read = client.cypher(
    "MATCH (p:Person)-[:WORKS_ON]->(pr:Project) "
    "RETURN p.name AS person, pr.name AS project ORDER BY p.name"
)
print("[4] who works on what:")
for r in read["rows"]:
    print("    %s -> %s" % (r["person"], r["project"]))

# [5] Full-text query: the CJK name. On an FTS-declared key the CONTAINS
# anchor scan uses the bigram buckets as a prefilter.
hits = client.cypher("MATCH (n) WHERE n.name CONTAINS '图数' RETURN n.key, n.name")
print("[5] full-text 图数: %d hit(s)" % hits["count"])
for r in hits["rows"]:
    print("    %s = %s" % (r["n.key"], r["n.name"]))

# [6] Vector search: declare a 4-dim cosine index, write one vector
# through batch node props (the Cypher subset has no list literals —
# vectors never ride Cypher), then query top-1. The node upsert replaces
# props wholesale, so name rides along with emb.
try:
    client.vector_drop_index("person-emb")  # rerun-safe: drop a previous declaration
except VectraNotFoundError:
    pass  # first run: nothing to drop
client.vector_create_index("person-emb", prop="emb", dim=4)
client.batch(
    nodes=[
        {"key": "ada", "label": "Person",
         "props": {"name": "Ada", "emb": [0.10, 0.90, 0.20, 0.40]}},
    ],
)
res = client.vector_search("person-emb", query_vector=[0.12, 0.88, 0.25, 0.35], k=1)
top = res["hits"][0]
print("[6] vector top-1: %s (score %.4f)" % (top["key"], top["score"]))
