# Progress

## 2026-08-27

- Added bounded heap selection for collection BM25 results, selecting only the
  requested page plus offset before materializing result details.
- Added direct one-shot candidate scoring that computes only query-term
  frequencies and document statistics instead of rebuilding `TextIndex`.
- Added regression coverage comparing indexed and one-shot scores and all
  result details, plus deterministic top-k ordering.
- Verification completed: `cargo fmt -- --check`, 111 default tests,
  139 all-feature tests, and `cargo check --all-targets --all-features` all
  pass. The all-target check reports one pre-existing dead-code warning.
