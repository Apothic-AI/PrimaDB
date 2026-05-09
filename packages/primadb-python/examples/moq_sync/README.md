# MoQ Sync

This example uses the Python package's `create_primadb_moq_loopback(...)` helper to publish a
PrimaDB sync envelope over the same path/track/frame shape used by the browser and Node MoQ
helpers.

The current Python example is a deterministic loopback because the available Python MoQ bindings do
not yet expose stable generic byte tracks on Python 3.14. It is still useful for exercising the
SDK-level PrimaDB MoQ API and sync-envelope mapping.

## Run

```bash
cd /path/to/primadb/packages/primadb-python/examples/moq_sync
uv sync
uv run python main.py
```
