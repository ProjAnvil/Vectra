# vectra-graph — Python SDK for Vectra

Python client for the Vectra REST graph API. Standard library only — the
wheel installs **zero runtime dependencies**.

> Stub: full documentation, packaging polish, and publishing land with Task 2.

## Quick start

```python
from vectra_graph import Client

client = Client(base_url="http://127.0.0.1:8080")  # or set VECTRA_URL / VECTRA_TOKEN

client.batch(nodes=[{"key": "gateway", "label": "Service", "props": {"env": "prod"}}])
aw = client.await_seq(client.stats()["publishedSeq"] + 1)
for row in client.cypher("MATCH (n:Service) RETURN n.key")["rows"]:
    print(row["n.key"])
```

## Tests

End-to-end tests boot a real `vectra-server` (skipped with a message when
the `go` toolchain is absent; `VECTRA_SDK_TEST_BIN` points at a prebuilt
binary):

```
cd sdk/python && python3 -m unittest discover -s tests -v
```
