# DP source and artifact assessment (no new run)

The DP algorithmic path is unchanged in source. For the concrete measured
`agg.sum(m.DP)` (original DP Int32 field, no lookup/initializer), MatrixTable's
plain numeric SUM early-return block is byte-identical between baseline8dcd and
candidate00a. It returns before the new owning-coordinate admission call.
The called reduce_list function, including direct_entry_sum, is also
byte-identical. Existing numeric-list/reduction helpers, aggregate kernels,
logical-expression lowering, agg.py and public array-sum frontend files are
unchanged. dp-source-proof.json records actual source-byte comparisons/hashes.

That route projects DP through the existing per-row list reduction and combines
row totals with the existing DataFusion global SUM. The new coordinate owner
adapter is not its reducer. Removing only newly added items from
array_aggregate.rs reproduces the original file exactly. Removing the new
private-name preparation guard from logical_aggregation.rs likewise reproduces
the original file. That guard is a query-preparation check, not a new per-entry
DP loop; ordinary numeric global SUM already selects DataFusion's SUM path.

This does **not** make the entire frontend/native artifact byte-identical:
MatrixTable._aggregate contains an added later admission branch, a containing
Rust factory has the new guard, and native whole-library hashes differ5a8613→
6cb1fe. No Linux disassembly/function-byte comparison was performed, so compiled
layout or instruction equality is not established merely by unchanged source.

Both normal builds use the recorded release/native-CPU flags, kache,
incremental0,8 jobs and the same stable target/source location. Cargo.lock and
the configured Rust toolchain file are byte-identical. Candidate uses its fresh
normally installed environment and a different PYO3_PYTHON environment path;
no native library was transplanted. Build-only PATH provisioning resolves
existing tools; it does not modify the query environment. Both versions in the
actual matched run use the identical frozen thread settings, driver, fixture,
process order and same running VM. Artifact enrollment verifies all69 frontend
files and actual native hashes before query execution.

The observed DP mixed-workload latency increase is real in this packet:
+7.7%/+31.85ms by version medians, all four paired wall ratios favoring baseline,
with+0.6% median CPU and identical full outputs. Unchanged reduction source does
not explain that observation or establish that it is harmless. The existing-data
query-order stratification is descriptive only; it establishes no allocator,
cache, frequency, scheduler, layout or measurement-noise cause. No isolated
DP-only run, source variant, profile or extra trial was performed.

Root has explicitly chosen to accept the **narrow measured tradeoff**: AD's
consistent wall/core benefit and lower whole-process memory justify this scoped
change while retaining the unresolved DP latency increase. This is not a
no-regression claim; RSS covers both queries and their warmups, not AD alone.
Final receipt verification/main merge remains root/reviewer-controlled. No VM
restart or further optimization is part of this assessment.
