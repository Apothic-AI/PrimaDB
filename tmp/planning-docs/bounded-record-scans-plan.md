# Bounded Native Record Scans Plan

## Objective

Make native `SegmentFileStore` record enumeration honor scan bounds and limits while walking the
on-disk record-key trie, without changing public pagination or lazy overlay behavior.

## Scope

- Traverse record directories in logical key order and stop after the requested storage page limit.
- Apply prefix, range, direction, and cursor checks before reading ordinary record JSON files.
- Page storage results when an in-memory overlay may replace or remove persisted records.
- Verify forward, reverse, cursor, range, and large-directory behavior.
