# Shared native row-reduction contexts

Final material source: `b2532cb9`, merged onto main `a0996a3` (projection and CSE
already included). No main updates were made by this agent. Production changes
are limited to MatrixTable row projection planning; native execution remains Rust.

## Problem and general fix

Named compatible row aggregates already shared a logical struct. Extracting each
field, however, created an opaque GetField semantic kernel containing the entire
struct expression. Each sibling could reevaluate every aggregate in the bundle.
The candidate gives that shared result one ordinary relational projection before
extracting its fields. DataFusion now sees one computed struct column and the
existing field kernels consume that column. No new custom operator or bypass of
logical null propagation is introduced. This also benefits bundles without fractions.

Plain fractions now participate in the existing shared filtered-entry context
route. Their public empty-scope NaN identity is restored with ordinary coalesce
after extraction. Nested and filtered fraction identities retain their existing
contracts. Eligibility continues excluding RNG, lookups, position-sensitive
callbacks, scans, nested explode/fold/array aggregation and unsupported options.
Unrelated seeded aggregate scopes remain unchanged.

This does not fuse every primitive reducer into one loop: existing Arrow batch
reducers still run separately. It shares expensive projected/filtered contexts and
prevents reevaluation of whole bundles. Collection-group multi similarly calls
DataFusion GroupsAccumulator separately per reducer; adding a constant key there
would introduce hashing and owner buffers, so that shortcut was rejected.

## Isolated 500M native-entry results

Same sparse native input, 200,000 rows x 2,500 columns, 8 source partitions.
Filtered entries and missing values are both present. All results match independent
periodic Python oracles. Each case runs in a fresh process, one warmup plus three
measured actions; entries and query preparation use public APIs. Wall includes
query construction and execution. CPU sums process user and system time. Baseline
and candidate execute sequentially; no builds, tests or other timed workloads
run concurrently. Order alternates by case. OPENBLAS_NUM_THREADS=1 for both.

| Public pipeline | Main wall s | Candidate wall s | Wall speedup | Main CPU s | Candidate CPU s |
|---|---:|---:|---:|---:|---:|
| Four row fractions | 1.392 | 0.593 | 2.35x | 18.483 | 6.654 |
| Twelve row fractions | 4.340 | 1.454 | 2.98x | 55.973 | 18.444 |
| Mixed fractions/count/sum/mean | 2.268 | 0.795 | 2.85x | 28.830 | 10.076 |
| Numeric count/sum/mean, no fractions | 1.517 | 0.550 | 2.76x | 19.213 | 6.911 |
| Global sum control | 0.161 | 0.168 | 0.96x | 1.847 | 1.828 |

CPU falls about 2.8–3.0x across affected classes. The global sum control has
unchanged CPU and a small/noisy wall difference; this change does not touch that
path. These are local main-versus-candidate comparisons, not new Hail ratios.
Raw receipts: isolated500m/, summary.json. mixed.py and paired.py reproduce the
mixed/numeric variants and sequence; fraction/control queries use the committed
20261023-native-reduction-cse/measure.py. The latter also documents input creation.
The prepared input and SHA manifest are the same as that earlier run.

## Physical-plan and profile evidence

baseline-plan.log, rejected-wrapper-plan.log and final-plan.log retain actual
DataFusion physical plans. The final plan contains one named_struct projection
computing the shared fractions; each GetField above it reads the shared column.
The rejected plan embeds the complete aggregate expression in every GetField.
This change uses DataFusion's existing relational projection/CSE behavior rather
than teaching a new opaque scalar callback how to optimize itself.

Separate three-second macOS sample profiles run after warmup over repeated four
fraction actions. Baseline top-of-stack samples include CASE result builder 860,
CASE partial merge 916, Boolean equality 1,585 and MutableArrayData extend 694.
None appear in the candidate top-of-stack list (report threshold >=5 samples).
Candidate work concentrates more on Boolean bitmap construction and remaining
Arrow/Vortex reads. Memmove remains about 2,167 versus 2,169 samples in the fixed
sampling windows, while candidate completes more actions: these counts are not
per-query CPU percentages or a disjoint accounting. Profiles support removal of
repeated CASE/filter framework work; they do not establish that all remaining
allocation/copies disappear. Full samples are compressed, with query receipts and
sample command receipts retained. Profile runs are excluded from timing medians.

## What did not work

Simply admitting fractions and coalescing each extracted field regressed four
fractions to about 1.93 seconds / 24.24 CPU seconds, versus 1.39 / 18.47 on main.
The twelve-fraction attempt was stopped because repeated bundles grew much worse.
Those incomplete receipts are explicitly kept in rejected-wrapper500m/ and are
excluded from final summary. This exposed the existing opaque field-extraction
problem and motivated the ordinary relational projection in the final change.

Initial baseline direct-versus-user-struct shape diagnostics showed the opportunity
at 80M and 500M entries. A separate integration build was active during those early
diagnostics. They are retained as directional evidence only; final isolated500m
results above supersede them as the performance gate.

## Validation and provenance

100 public checks pass on the final candidate and on the main baseline: 99 checks
in final-public-tests.log plus the explicit seeded matrix scope check. Baseline
receipts comprise 98 checks plus the additional all-missing and RNG checks.
Coverage includes empty columns, fully filtered rows, present entries with all
missing predicates, mixed numeric reductions, nested identity preservation,
guarded invalid casts on filtered entries, aggregate composition, coordinate
callbacks, shared callback DAGs and the full RNG suite. Existing reused callback
DAG test takes 5.59 seconds; no new exponential planning regression was observed.

Normal kache/maturin develop --release --locked builds use the existing shared
Cargo target, jobs=2, incremental=0 and target-cpu=native. No extension binary was
copied as a build cache or runtime workaround. Dedicated candidate environment:
/private/tmp/vail-native-row-fusion-env. Baseline is the normal-built integrated
main runtime /private/tmp/vail-entry-fixes-integration-env. Native SHA is identical
for both (77b0c38a0f7bea5308f1651114d0bc8391d2da93beed0b0f4111bb01e7bd7d49)
because this candidate changes only Python planning; both normal builds and exact
source hashes are recorded. Candidate/baseline-provenance.json distinguish their
module paths and MatrixTable source. The initial env needed pip upgraded to
support maturin's dependency-group install; the successful normal build receipts
are build.log and final-build.log.

Required external review remains the root agent's gate before main.
