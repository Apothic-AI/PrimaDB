# Gun Record Watch Comparison Progress

## Completed

- Confirmed Gun's user-facing watch path is chain-based: `.on(...)` delegates to `.get(...)`, registers a chain listener, and emits a graph GET.
- Confirmed Gun's root event bus sends GETs to RAM, storage adapters, and peers through the same message shape.
- Confirmed Gun storage adapters answer GETs by sending normal PUT graph fragments back through `root.on("in", ...)`.
- Confirmed Gun mesh transport receives remote messages and feeds them into the same root `in` path used by local/storage messages.
- Confirmed AXE records peer subscriptions from GET/ACK traffic and routes future PUT updates by graph soul/field subscription.

## Takeaway

- Gun avoids separate local/remote watch semantics by making storage and network transports adapters over one graph message bus.
- PrimaDB can follow that model by making record scans a first-class core pull/watch request and by routing invalidation over logical record keys, not hashed storage node paths.
