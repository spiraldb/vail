# Real Hail workload bodies: local pre/post struct fusion

**These four actual workload cases show no substantial improvement from the new struct/fold emission work.** All final before/after wall changes are below0.6%, and core-second changes below0.7%. These descriptive measurements establish none of the earlier synthetic2–7× gains on this selection. No noise, causality or statistical significance claim.

Before: source`c82d03056`, native`415b2f35...`; after: production`fcd760817`, native`7c1e347f...` in local main`57221455f`. Both normal release wheels installed normally in isolated environments. All69 Python frontend files match byte-for-byte. No source/binary copying/rebuilding, production modification or cloud use during this run.

## Results

| Upstream case | Before wall ms | After wall ms | Wall change | Before core-sec | After core-sec |
|---|---:|---:|---:|---:|---:|
| filter_entries | 87.63 | 87.63 | -0.00% | 0.3526 | 0.3517 |
| filter_entries_unfilter | 144.54 | 143.72 | -0.57% | 0.5645 | 0.5607 |
| array_arithmetic | 55.08 | 54.83 | -0.46% | 0.2127 | 0.2124 |
| split_multi_hts | 162.11 | 162.08 | -0.02% | 0.4288 | 0.4288 |

Four independent processes/version/case, balanced before/after/after/before/before/after/after/before. Two warmup calls plus5 timed original-function calls per process. Metrics are medians of process medians. Original upstream read/build/force-count statements execute unchanged; import/decorator binding is outside timing. Full per-process samples and all initial observations remain included, not only favorable directions. Each original function AST also matches the vendored Hail function, independently recorded.

## Data and correctness

This is an existing **1000-row real profile-data slice with2535 samples and4 native partitions**, not the full upstream profile25 fixture or generated integer microbenchmark data. Native input13 files/28996318 bytes is fully SHA-pinned before each worker and reverified after the cohort. Array arithmetic retains974 biallelic rows; split produces1035 rows. Original Hail benchmark bodies run on Vail only: no Hail engine timing or direct comparison to earlier Linux full-suite numbers.

Before timing, each transformed MatrixTable is materialized through a separate localize_entries/to_arrow action and complete Arrow values+metadata are compared to the pre-change reference. Split also matches its previously established full reference. Other cases use baseline equivalence plus domain checks; no independent Hail semantic oracle is claimed for them. This DIFFERENT untimed materialization does **not** prove that the timed force-count action materializes every entry payload.

Native, all frontend files, source files, input file set/content/stats and each case reference hash are checked before queries. All32 final workers completed and each was reaped with a positive owned-PGID-absence check before advancing. All process-result/exit receipts are retained. Original first cohort had normal leader reaping, but only eventual group-absence evidence; it is separate exploratory evidence. Two initial array-arithmetic validator errors (incorrect1000-row postcondition, then named key-field select) occurred before any timing in those failed workers, are retained, and were corrected by independent Arrow biallelic row counting. No benchmark operation was fixed or retried after a timed failure.

## Coverage gap

The entry-filter plans are unchanged: filter_entries uses one ordinary array_transform; unfilter wraps it with a SECOND array_transform. Array arithmetic also retains the same physical plan. Plan text is diagnostic, not proof about opaque callback internals or executed split dependencies (split's diagnostic plan has an unbound action).

The prepared list emitter currently requires a flatmap or an admitted integer fold; ordinary map/filter-only shapes decline. Therefore merely implementing StructBuilder output did not connect these actual map/masking-map shapes to it. The earlier decision to retain all map/filter-only paths after cheap synthetic regressions was overbroad.

A true array filter_map can compact into final builders with output entries bounded by input length. MatrixTable filter_entries instead preserves column slots and marks rejected entries missing; it needs a fused masking map and parent/child validity handling. Its fixed slot count is useful too. Variable-size child payload byte counts are not generally bounded solely by the slot count. Conditional borrowed child fields also require appropriate masked descriptors rather than unsupported branch bindings. These are remaining optimization gaps, not Arrow builder limitations; no new production fix was implemented in this benchmark-only task.

## Output type clarification

Scalar fused registers already support Bool, Int32, Int64, Float32 and Float64. Final outputs also support nested Structs and copied-through Utf8/primitive List fields. The new fold shortcut alone is restricted to literal-seeded I32/I64 add/product, preserving wrapping/null semantics. This is conservative implementation coverage, not a fundamental inability to fuse floating-point folds or an Arrow type limitation. Unsupported callbacks/layouts retain the general executor.

## Resource and methodology limits

Low scheduling priority. RAYON_NUM_THREADS=4, BLAS/MKL/NUMEXPR/VECLIB threads=1, TOKIO_WORKER_THREADS=4 recorded; the compiled native runtime EXPLICITLY creates32 Tokio workers, so the environment is **not a global4-thread cap**. These queries read4 source partitions. process_time captures process CPU across its threads; no subprocess CPU is included. The6GiB watchdog samples the worker's macOS peak RSS every100ms; it is not a hard process-tree or total-machine RAM limit. Reported RSS peaks include untimed setup/validation and must not be attributed to timed query memory savings.

The controller has bounded timeouts, live-leader identity proof before TERM/KILL, finally cleanup and group-absence checks. All final paths completed normally. Repeated signals can interrupt cleanup, and the launch-to-identity failure window lacks an unresolved receipt; those unexercised robustness gaps are retained limitations, not retroactive claims of failure or historical cleanup proof.

No production code changed. The appropriate next implementation is admission/fusion of the real masking-map/filter_map shapes while preserving the existing fast cheap primitive path; benchmark wins are not established yet.
