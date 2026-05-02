# Scripting Runtime Plan

## Goals

- Add explicit node-attached script execution without making replicated code automatically active.
- Store script attachments in the graph so they replicate, but enforce capabilities locally at execution time.
- Expose a sandboxed database facade for read/query/traverse/write/transaction operations.
- Prevent scripts from granting themselves access to graph data or encryption material.
- Keep the runtime engine abstract so QuickJS, Rhai, or another engine can be supported behind the same stored-script model.

## Initial Implementation

- Add a feature-gated `scripting` Rust feature.
- Use Rhai as the first embedded scripting engine because it is pure Rust and suitable for a controlled sandbox.
- Store node script attachments under an internal graph root keyed by node path hash.
- Add explicit APIs:
  - `attach_node_script(path, script)`
  - `node_scripts(path)`
  - `remove_node_script(path, script_id)`
  - `execute_node_scripts(path, options)`
- Pass scripts a context object with path, node value, node state, edge metadata, script metadata, and caller args.
- Expose facade functions such as `db_get`, `db_map`, `db_query`, `db_traverse`, `db_put`, `db_unset`, `db_set`, `db_remove`, and `db_increment`.
- Collect write operations as transaction steps and apply them atomically only when execution options grant transaction capability.

## Security Rules

- Attached script capability metadata is a request/upper bound, not authority.
- Local execution options grant the actual capabilities for a run.
- Effective access is the intersection of local grants and requested script grants when the script declares requested grants.
- No encryption keys, filesystem, environment, transport, or network APIs are exposed to scripts.
- Scripts are never run automatically as a side effect of replication.

## Follow-Up Work

- Add browser, Node, and Python bindings after the Rust core API and tests stabilize.
- Add optional signed-script verification with trusted author keys.
- Add a QuickJS runtime behind the same `ScriptRuntime` abstraction if cross-target build and sandbox behavior are acceptable.
- Add richer SDK-shaped script facades once the minimal deterministic operation facade is proven.
