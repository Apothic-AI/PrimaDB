---
title: Node-Attached Scripting
sidebar_position: 6
---

Use scripting when a node should carry executable logic that an application may run explicitly.

## Attach A Script

Node and browser use camelCase methods:

```ts
const path = { anchor: "notes", segments: ["welcome"] };
const capabilities = {
  read: [{ root: "notes", recursive: true }],
  write: [{ root: "derived", recursive: true }],
  transaction: [{ root: "derived", recursive: true }],
};

db.attachNodeScript(path, {
  id: "derive-title",
  runtime: "rhai",
  source: `
    fn main(ctx) {
      let note = db_get("notes/welcome");
      db_put("derived/welcome", #{ title: note.title, source: ctx.path.display });
      return #{ title: note.title };
    }
  `,
  capabilities,
});
```

Python uses snake_case methods:

```python
db.attach_node_script(path, {"id": "derive-title", "source": source, "capabilities": capabilities})
```

## Execute With Local Grants

The local app grants the actual capabilities for one run:

```ts
const results = db.executeNodeScripts(path, {
  capabilities,
  args: { requestedBy: "ui" },
});

console.log(results[0].value);
console.log(results[0].steps);
console.log(results[0].report);
```

If a script tries to read or write outside the granted paths, execution fails. If `applyWrites` is
`false`, write functions still produce transaction steps, but PrimaDB does not commit them:

```ts
const dryRun = db.executeNodeScripts(path, {
  applyWrites: false,
  capabilities: {
    write: [{ root: "derived", recursive: true }],
  },
});
```

## Capability Rules

- Local execution options grant authority.
- Script `capabilities` are requested bounds, not authority.
- `read` gates `db_get(...)` and `db_map(...)`.
- `query` gates `db_query(...)`.
- `traverse` gates `db_traverse(...)`.
- `write` gates functions that create transaction steps.
- `transaction` gates committing those steps when `applyWrites` is enabled.

## Limits

Execution options can override sandbox limits:

```ts
db.executeNodeScripts(path, {
  capabilities,
  limits: {
    maxOperations: 10_000,
    maxStringBytes: 16 * 1024,
    maxArraySize: 256,
  },
});
```

Use lower limits for untrusted or user-authored scripts.

See also:

- [Scripting concept](../concepts/scripting)
- [Transactions and strict scopes](transactions-and-strict-scopes)
- [Auth, encryption, and password keys](auth-encryption)
