# table_import_ints: remove per-record CSV preparation

Base: `701d5eb4774739324407b9897cda6e903d8a844c`.
Candidate source: `df3cd7667930fcc7597d73a1ba806ae3abed8366`.
Standing batch-execution rule: `AGENTS.md`, committed in `0c512752`.

The benchmark driver in `vail/benchmarks/profile_table_import_ints.py` extracts
and executes the unchanged upstream `test_table_import_ints` AST body. Input:
`/Users/danielking/projects/vail/vail/local-data/upstream-benchmark-20261015/many_ints_table/many_ints_table.tsv.bgz`.
No fixture download or preparation is included in query timing. The driver
records process CPU, wall time, process-lifetime peak RSS, native SHA-256, and
upstream benchmark source SHA-256. CPU includes all process threads.

Baseline profile: CSV ReaderBuilder::build consumes 64.07% of sampled CPU,
about 144 CPU-seconds, because PreparedCsv::append constructs a new CSV reader
and its state machine for every record. Prepared ReaderBuilder configuration
alone had not prepared the actual parser.

Unquoted imports now split borrowed fields directly into Arrow builders.
Quoted parsing configures one reader per batch, reusing its DFA, IO buffer,
cached headers, and record scratch. Between records only the borrowed input
slice and primitive parser position/EOF state change. Regex-separated input
also appends borrowed fields directly instead of allocating a field vector for
each row. Regex compilation and marker configuration remain outside hot loops.

The required reviewer inspected csv 1.4.0 seek_raw and csv-core reset callees
and cleared this design against the hard no-per-row/per-entry-setup rule.
Public tests cover record/field/quote/missing semantics, null rows, Unicode,
long records, multiline quoted fields, and changing row lengths. The new tests
also pass on the baseline (69 focused text/CSV tests), verifying their oracles.
The final release artifact passes 103 focused text/CSV/compression/MatrixTable
import tests in 3.50 seconds. No plan-shape or IR assertions were added.

Builds use maturin --release --locked, Rust 1.98.0, Python 3.12.12, and the
absolute checkout PYTHONPATH. Final native SHA-256:
`cf397e3d9b5c424ffc9cdff8074b9e9f94ab6cd2b73af92605fc237b9a28521d`.
A final packaging attempt ran out of disk space during stripping; after
removing redundant task-owned artifacts, the rebuilt release wheel succeeded.

Profiles are captured with samply record --save-only --unstable-presymbolicate
--rate 500. Stack percentages use threadCPUDelta weights across the full process
profile, including startup; query CPU/wall timings exclude Python imports.
Other builds competed with the baseline for CPU. Wall-time comparisons are
therefore uncontrolled; CPU savings and disappearance of parser setup from
the profile are the primary performance evidence.

Final actual-workload result (one profiled run each): query process CPU fell
from **224.285s to 43.321s**
(**5.18x lower CPU time**). Observed wall times
were 466.138s and 43.799s;
the host-load caveat above prevents treating that ratio as a controlled speedup.
Process-lifetime peak RSS was 365510656 and
374145024 bytes, respectively.

The after-profile contains **zero sampled CPU in CSV ReaderBuilder::build**.
Top leaf work is memory copying (17.8%), inflate (7.8%), and regex search (6.3%).
This verifies removal of the offending setup from the actual upstream workload.
The full process profiles contain 224.626s baseline sampled CPU and
43.534s candidate sampled CPU. Raw profiles, symbol sidecars,
query metrics, and hotspot summaries are included alongside this document.

The 100k-row uncompressed prefix (prepared outside timing) was also run three
times per artifact. Those raw timings are included as secondary evidence; they
were captured under differing host load and are not the primary reported ratio.

Required reviewer thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` approved exact
runtime revision `df3cd7667930fcc7597d73a1ba806ae3abed8366` after source and
matching-artifact test/profile inspection. No source/test findings remain.
Policy-only follow-up `788b6e8528e65f4596a3dd28406cf9c591594e89` records the
user's kache requirement and prohibition on manual native-artifact caching;
the reviewer explicitly cleared it with unchanged runtime validation.
An accurate evidence/review-only child is nonmaterial and requires no new
runtime gate. See review.json for scope and measurement limitations.
