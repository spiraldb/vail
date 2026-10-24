# H089: exact integer composite entry totals

Frozen production/tests24d4f6218020f74a261944e4030cc57b25703a83 on main de71b289. Native Expr.is_pure exposure + generic composite aggregate-forest lowering. Nested Struct/Tuple bundles containing only integer SUM and COUNT reduce row-local lists, then use ordinary DataFusion table SUM/projection packing. Existing typed row sum, count_where and native aggregate kernels are reused; no new scalar entry interpreter or arithmetic loop.

Eligibility and rewriting memoize retained native identities. Inline struct children require retaining one native view per node: repeated view() creates fresh Arc wrappers. The initial identity failure and its corrected normal-wheel/public gate are retained. Rebuilt native expression handles preserve shared DAG children; Python dictionaries alone do not suffice. Global owners from all row vectors are unioned explicitly.

## Semantics and deliberate boundaries

Int32/Int64 SUM produces Int64: widening occurs before each row reduction, existing wrapping Int64 arithmetic is associative modulo2^64, and outer SUM preserves its zero identity. COUNT reduces logical entry presence, not defined values of a selected field. Missing entry structs stay absent even for coalesce/constants/row/global captures. Public cases cover empty rows/columns, missing field values, sparse entries, coalescing, large Int64 precision and wrapping, global/row captures, native persistence, nested packing and lazy replay.

Floating sums retain the original aggregation tree: no reassociation of cancellation/overflow/NaN behavior. Mean/stddev/stats require proper merge states and remain unsupported by this rewrite. Whole-expression conservative fallback applies to lookup refs, column/position dependencies, scans, aggregate combinators, initializer/options changes and effectful inputs. Central native purity analysis covers RNG and value-domain errors. Seeded sparse-entry rand_int32(seed7) result189/count4 is frozen independently from the baseline and repeats identically. Out-of-bounds inputs stay behind entry-presence filtering: all absent returns0; present entries still error.

## Exact normal artifact and public gate

Same stable cloud source/target, kache/maturin release,16jobs, incremental0, RUSTFLAGS=-C target-cpu=native. Source overlay content-hashes changed Rust files and touches them after extraction to invalidate stale Cargo mtimes. Initial native compile23.49s; Python-only view fix normal cached compile0.39s plus normal wheel packaging/reinstall. No native artifact was manually copied. Candidate installed native SHA2563d104710412eb0fad317fa373b3102af5d225ea5cb4ba555e69908df61d7a891 equals native contents of its normal wheel. Baseline normally built H092 native4d46e99d unchanged. Source hashes independently match the local frozen source.

Matched broad gate: baseline340passed/3failed; candidate341passed/2failed. Shared-DAG depth8 construction and deep public value evaluation now passes (baseline cardinality failure). Remaining failures are identical historical behaviors: test_composite_floating_sum_retains_existing_aggregation_results (Expected2/Got1; standalone baseline also fails) and test_collection_aggregate_matrix_scope_and_lookup_context (Expected3/Got1). Neither is skipped or hidden. Original full-materialization DAG/float baseline failures and revised receipts are retained. No new failures in Call, genotype likelihood/split, stats/stddev, shared row reductions/CSE, RNG, integer/nullable/list reducers, matrix SUM and collection filter/capture suites.

## Representative native63M-entry evidence

PRIMARY AMD EPYC7B13 n2d-standard32; native profile25_mt.mt,24885rows×2535columns,63083475logical cells,784316678stored bytes. These measurements compare Vail before/after, not Hail-relative multipliers. Baseline/candidate processes run A/B/B/A, each query one warmup+3recorded iterations,6measured samples per version/query. No builds/tests/profile workloads overlap timings. Raw initial and paired iterations are retained, including the initial failed IO-harness launch with zero valid paired samples (NameError, subsequently corrected).

| query class | wall baseline→candidate | speedup | CPU baseline→candidate |
|---|---:|---:|---:|
| bare SUM(DP) control |210.6→212.9ms|0.99x|5.59→5.72s|
| SUM(DP),SUM(GQ) |356.8→228.6ms|1.56x|9.21→6.10s|
| SUM(DP),SUM(GQ),COUNT |362.7→215.2ms|1.68x|9.43→5.64s|
| SUM(DP+GQ),SUM(DP),COUNT |363.6→240.0ms|1.52x|9.48→6.29s|

All results match on every iteration (DP353709503,GQ1246497536,count63083475,computed1597657563). Composite core-seconds fall33.7–40.2%. PeakRSS falls about2.6–4.8%; no large memory-capacity claim. Composite minor faults fall59–74% in these observations; these are not measured allocation counts. Bare control is approximately unchanged (wall+1.1%,CPU+2.4%).

Linux /proc/self/io counters show roughly0.97–0.98GB rchar per query in both versions: no source-IO improvement. rchar counts syscall-delivered bytes, including repeated/metadata reads and the tiny counters read; it is not logical selected-column size or DRAM traffic. read_bytes is0 in warm-cache runs, not evidence that no input was read. Hardware cycles/instructions/cache-misses explicitly report unsupported; no ALU/cache/bandwidth saturation claim.

Separate99Hz software CPU-clock profiles use12evaluations and report zero lost samples:11473baseline/7004candidate. Baseline exclusive take_list9.38%,take_bits8.89%,MutableArrayData::try_extend7.02%,primitive extend5.19% disappear at reported0.01%threshold in candidate. This supports eliminating whole-entry expansion/gather work. GT UInt32→UInt16 conversion remains3.29%→5.41%relative: underlying source decoding is NOT solved. Percentages cover startup and different total completed CPU work; do not interpret relative increases as an absolute regression without CPU evidence.

## Remaining general work

Integer/count composites only; floating/statistical merge states, other outer expression shapes, effects and column/lookup context stay on established paths. Source pruning opportunity: aggregate row-list expressions bypass MapRows-on-Read nested dependency propagation (logical.rs1098); ArrayMap is an opaque logical_kernel boundary (logical_expr.rs755). COUNT presence conservatively requests whole entry struct, needing validity-only dependency support. This is queued separately, coordinated with encoded projection work.

## Artifacts and reproduction

measure.py is the paired harness including OSIO counters. The first profile/initial timing harness omitted IO counters but used identical queries. build-cloud.sh/provenance.py/baseline-broad.py document source/normal-build/public provenance. Full JUnit, all iterations, normal build logs, source overlay and compressed flat profiles retained in results/. Downloaded small archive SHA25659345c2ee7611bf40265dce99c9be4e95ad7d486ca4bda77c1f80567e61316f2 verified. Native provenance JSON SHA2565890280740e9a7b9ecc17346213f6652381d3739a4e709b6f74548f198be0c4f verified separately; every source hash matches this checkout.

Raw profiles downloaded at /private/tmp/h089-raw-profiles.tar.gz, SHA256326e0dafd7ba7f6e41cf58919ca6356fc4602ba8883a89cd9dced2da57f651eb. Raw baseline SHA256e727b76ba078d79da105023edf67ff22790eed7f34899fc64c6e355a2e70dfb6; candidate2be273d6663849879cdd763dedce8f1906e1275684cb1d7d08342f9fd4e987b9. Cloud files preserved in /home/danielking/h089-composite-totals. gcloud refresh expired during run; work continued through existing trusted direct SSH key/host alias, without credential/trust changes. Fresh4h guest shutdown guard remains armed; parent integration on Boolean main follows required isolated acceptance.
