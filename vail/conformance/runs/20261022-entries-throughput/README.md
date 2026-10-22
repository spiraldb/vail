# Entries table throughput investigation

The October 21 cloud timing (28.834s entries count, 13.117s entries show) predates the already merged equal-row-key-run/partition-gap expansion. Current source does not globally sort expanded entries. It sorts shared column metadata once, permutes entry lists to those positions, streams equal-row-key runs, and executes disjoint key ranges in parallel. Historical exact local profile25 results for that fix are separately recorded in `../20261021-entries-ordering/results.md` (12.73→1.187s wall).

## Trusted native snapshot reads

Commit 91847785 skips `_validate_layout` at native snapshot read, trusting MatrixTable.write's positional entry/column invariant. That removes scanning all stored column identities and repeated semantic validation; Arrow/schema checks and public unlocalize boundary checks remain. Public tests cover filtered slots, valid missing-field entries, empty structs, and unsorted physical column order. Targeted entries/layout/VCF: 24 passed; full matrix_layout: 15 passed.

The 12 raw trial JSONs and `snapshot-validation-summary.json` are 3 interleaved fresh-process pairs per action, baseline Python 8892034b versus candidate 91847785; both use the identical reviewed native binary from the final coordinate wheel. This isolates the Python read change, not an exact full latest-main binary rebenchmark. Native SHA256 a11d7cc710d6ce250240d959b3ec6e2c0fee0b5ad6f07d2b247cfa44b68cd646. Baseline matrixtable.py SHA256 9b222639db0c2eed9504285a43976033c6d7e7e8620f45e615636d292ab3c253; candidate 4584f44835585cc708101b09e27a99b947e82115f41b3e88a7f7743756f1dfa5. The 3 pairs ran in a quiet window without competing builds/tests, after one discarded count warmup. The existing `/tmp/entries-profile25.mt` fixture is 24,885×2,535 = 63,083,475 entries; it was not regenerated. Raw fixture provenance is retained by the prior entries-ordering evidence.

| Action | Baseline wall | Candidate wall | Baseline process CPU | Candidate process CPU |
|---|---:|---:|---:|---:|
| entries()._force_count() |2.053s|1.997s|22.395s|21.069s|
| entries().show() |1.638s|1.326s|13.337s|10.264s|

Timing includes read/query construction and action, excludes Python import/terminal I/O; show renders its normal terminal-sized prefix to an in-memory stream. Process CPU is in-process Rust/Python query CPU. RSS is process high-water mark (not a sampled query-only peak). These are not Hail comparisons: the cloud Hail baseline ran on a different machine.

## Fresh profile and contiguous entry selection

`count-profile.json.gz` is a 1 kHz samply capture of trusted-read count on the same fixture and exact native above, taken after trials without competing builds/tests. It covers construction+action, not only streaming. The symbol sidecar and `embed_symbols.py` produce `count-symbolicated.json.gz`; 13,656 of 14,894 native-address frames resolved. A simple unweighted leaf-sample tally (20,889 samples) (including runtime wait leaves) finds memmove 18.1%,MutableArrayData.extend 10.4%,take_list(UInt64) 6.5%,take_bits(UInt64) 6.5%. These are sampled attribution, not exact per-kernel CPU measurements.

Commit 7a3a5722 replaces entry-field gathers with borrowed Arrow slices when the internally generated entry positions are a contiguous range. Filtered gaps and duplicate-key transposition still use Arrow take. Parent buffers remain alive while slices are consumed; this avoids copying but may retain the full source allocation. Two public nested-child/string/missingness tests passed as baseline oracles. Exact candidate native validation and final paired timings are pending the serialized build slot.
