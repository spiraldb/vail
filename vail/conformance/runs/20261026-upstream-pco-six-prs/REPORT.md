# Six upstream PCO PRs: frozen validation and resource results

2026-10-26. All six remain draft; this report records completed agent work, not GitHub approval, CI waiver, or permission to merge. Required human reviews and current upstream CI remain separate gates.

## Scope and correctness

- Optional `logical_row_prefix.logical_row_ends`, no wire version or enum. Missing-prefix Take returns its existing fallback, sliced execution retains the baseline decompressor, and the baseline probe body is unchanged apart from indexed-only dispatch/state.
- Legacy decoder allocation/unsafe code is preserved exactly rather than bundling a latent bug fix. New selective decode/output storage is initialized.
- All added public semantic tests originate in PR2 and pass old production. No added metadata mutation, format/layout assertions, or unrelated invalid-request error tests. Feature PRs only adjust public initialization wiring.
- PR2, PR3, PR4, PR5 and PR6 each passed all 63 PCO library tests plus all-target/all-feature Clippy with warnings denied. Scoped file Clippy and compressed-validity oracle passed.
- Fresh actual compatibility: current PR3 writer → unchanged base `33675d9e39b6155dbad4e5fcc2122cf78d3f5385` reader, and base `33675d9e39b6155dbad4e5fcc2122cf78d3f5385` writer → current PR6 reader, both PASS for nullable unsliced and explicitly normalized sliced files. Executable/source/fixture SHA receipts are in the archive.

## Measurement protocol

AMD EPYC 7B13, 32 vCPU; normal Rust 1.99.1 builds with kache, 8 jobs, incremental disabled, `RUSTFLAGS=-C target-cpu=native`. No copied compiled libraries. Each version was normally built at the same source path.

Fixed 26 cases: one untimed oracle pass and three OS-timer samples per operation, sample size 1 / min time 0, one process per version. These are within-process samples, not ABBA process replicas or confidence intervals. Buffers are resident; no disk/network read savings are measured. First probes use a prepared owner, not complete file-open/index-build latency. Fresh defined/missing and retained warm states cannot establish a break-even point.

The narrow repeated control is fixed at 4,194,304 outputs across all versions. Final pilot medians were 21.17 ms dense and 3.247 ms nullable; the nullable control remains below the requested tens-ms target. Earlier313/340 µs and166/167 ms calibrations are preserved, not pooled. Optimized cases can fall below 10 ms.

## Explicit tradeoffs

- Nullable repeated Take:52.43→67.71 ms, +15.28 ms/+29.1%.
- Metadata-open/drop batches of 256:45.61→60.75 ms dense (+33.2%, approximately 59 µs extra/operation);39.82→52.41 ms nullable (+31.6%, approximately 49 µs extra/operation). This excludes IO and includes immediate destruction.
- Sparse/contiguous/range and fresh-probe gains do not make every query faster. Retained probes are near parity; small point-estimate differences are not strong causal claims. No weighted overall win, bandwidth saturation, or operation peak-memory claim is made.

## Original baseline versus final stack medians

| Operation | Baseline ms | Final ms | Baseline / final |
|---|---:|---:|---:|
| `first_probes/false` | 13.82 | 1.315 | 10.5× |
| `first_probes/true` | 14.17 | 0.1026 | 138× |
| `full/false` | 41.11 | 37.29 | 1.1× |
| `full/true` | 70.09 | 66.38 | 1.06× |
| `metadata_open_batch/false` | 45.61 | 60.75 | 0.751× |
| `metadata_open_batch/true` | 39.82 | 52.41 | 0.76× |
| `retained_probes/false` | 47.56 | 46.5 | 1.02× |
| `retained_probes/true` | 53.38 | 50.68 | 1.05× |
| `selection/("broad", false)` | 46.48 | 43.56 | 1.07× |
| `selection/("broad", true)` | 56.56 | 56.76 | 0.996× |
| `selection/("contiguous", false)` | 66.72 | 41.5 | 1.61× |
| `selection/("contiguous", true)` | 93.39 | 43.81 | 2.13× |
| `selection/("mixed", false)` | 46.51 | 43.18 | 1.08× |
| `selection/("mixed", true)` | 73.1 | 70.87 | 1.03× |
| `selection/("narrow_repeated", false)` | 20.02 | 19.55 | 1.02× |
| `selection/("narrow_repeated", true)` | 2.779 | 2.739 | 1.01× |
| `selection/("near_full", false)` | 39.49 | 31.4 | 1.26× |
| `selection/("near_full", true)` | 69.3 | 34.52 | 2.01× |
| `selection/("ranges", false)` | 42.85 | 0.02674 | 1.6e+03× |
| `selection/("ranges", true)` | 68.2 | 0.03143 | 2.17e+03× |
| `selection/("repeated", false)` | 48.61 | 38.86 | 1.25× |
| `selection/("repeated", true)` | 52.43 | 67.71 | 0.774× |
| `selection/("sparse", false)` | 41.9 | 0.6009 | 69.7× |
| `selection/("sparse", true)` | 71.27 | 0.5619 | 127× |
| `write/false` | 64.93 | 65.43 | 0.992× |
| `write/true` | 58.9 | 58.94 | 0.999× |

Writer/full/query results compare the original writer+reader to final new-prefix writer+reader on identical logical inputs; they do not compare two readers against identical old-format bytes or isolate individual commits. `first_probes/false` means defined; `/true` means missing. Their batches contain 256 probes. Retained batches contain 1,048,576 probes. Other boolean suffixes mean nullable.

## Whole-process resource limits

GNU time CPU/RSS includes fixture compression, unmeasured value checks and timed operations. It is not per-operation CPU or transient memory attribution. Peak RSS was approximately 230–231 MiB across versions; this does not bound retained scalar caches in arbitrary workloads.

## PR heads

| PR | Head |
|---|---|
| [#10824](https://github.com/vortex-data/vortex/pull/10824) | `2736d4cf926627c265bb8eba9a270183691d2a05` |
| [#10825](https://github.com/vortex-data/vortex/pull/10825) | `f56bcebfbeda930bd1619e09a79744282b12036c` |
| [#10827](https://github.com/vortex-data/vortex/pull/10827) | `326178f86d0056b9129fc9fdf3c2c7e973bbe174` |
| [#10828](https://github.com/vortex-data/vortex/pull/10828) | `1c60c36b96c0dd3f2bb09f619c5ef72f36054125` |
| [#10829](https://github.com/vortex-data/vortex/pull/10829) | `ff66413e87b0857f61c10a1e395e1e2f5a045f89` |
| [#10830](https://github.com/vortex-data/vortex/pull/10830) | `106106b14f0daf9534e95eaf5ce2a64b463f01e4` |

All historical pre-new-only results and adverse results are retained in `raw-evidence.tgz`. `provenance.json` records exact executable/source relationships; `timings-ms.json` contains all 130 case medians. No new GitHub workflow was added. Old monolithic PR10771 was closed only after all six replacements existed; independent sliced-serialization fix10834 is outside this stack.

## Current GitHub status snapshot

`ci-snapshot.json` records the immutable heads and observed checks. All PRs remain draft. At capture, PR1 had no failing checks; PR2 had a failing CodSpeed analysis, and PR3–6 had failing Rust platform/lint checks as well as performance/pending checks. A read-only audit is classifying the exact job logs; these are not waived or labeled green by this report. Canceled checks are retained separately in the raw snapshot. Two human reviews remain required.

## Concrete CI corrections after measurement

The multi-platform compact stock-ticker test reported actual 55,396 bytes versus the pre-existing 55,344 expectation (+52 bytes after optional PCO metadata); its preceding semantic roundtrip passed. Only that existing expectation was updated in PR3. PR5's indexed-slice condition received the pinned formatter's brace layout. No new tests, weakened equality, performance policy, or production behavior was introduced. New standard CI runs validate these corrections. Final PR6 production bytes remain identical to the measured artifact; measured and current heads are separately recorded in provenance.json.

PR6's SVE failure was a rustup network timeout before compilation, not a source failure. A fresh standard run is required rather than a source workaround.

CodSpeed performance checks remain real outstanding signals: metadata-open cost rises in its independent model, retained-probe cases regress about 12–14%, and the existing 1,024-row scalar_access case regresses about 11%. The locked 26 cloud cases do not directly include that original scalar_access case. Model costs are not OS wall time, and their disagreement with near-parity retained cloud timings does not justify dismissing either measurement. Unrelated performance flags also appeared on the test-only PR2, whose production tree is unchanged. Human maintainer judgment remains required; these checks are not waived as infrastructure.

## After-fix CI snapshot

`ci-after-fixes-snapshot.json` supersedes the earlier failure snapshot. The current heads contain the minimal expected-size/format corrections; observed Rust source/platform failures have cleared. PR3 and PR6 had TSAN jobs in progress at that historical snapshot; both subsequently completed successfully. CodSpeed performance gates remain failed on several PRs and are not waived; required human reviews remain outstanding. All six remain draft.

## Completed source CI audit

The final current-head snapshot is `ci-complete-snapshot.json`. Both TSAN jobs completed successfully (PR3 at 22:05:51 UTC and PR6 at 22:06:26 UTC). Source/platform/formatting/TSAN failures are cleared; remaining CodSpeed performance checks and required human review are not waived. All six stay draft. No implementation head changed during this documentation-only reconciliation.
