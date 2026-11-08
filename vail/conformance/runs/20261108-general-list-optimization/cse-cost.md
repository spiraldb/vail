# Binding versus recomputation

**Disposition: neither production experiment is enabled.** After cost review,
both changes were removed. Production behavior remains identical to accepted
`64f1f0db`; existing arithmetic CSE is unchanged. The experiments below explain
that decision. They are not performance claims about shipped changes.

Candidate `28aa1bd799a666c9361dbbdc3f3f2d629766d199` refines existing local CSE;
it introduces no bind operator. Native singleton-map/index bindings already
lower to lexical frames. The candidate removes those frames for canonical
array-backed input reads in strict, pure scopes, while sharing repeated
lowercase/uppercase conversion in strict struct projections. Computed/scalar
arguments, conditional scopes and effectful work retain their original route.

## Expression experiment

The public physical-expression example checks complete Arrow results once
before timing in each process. Each process records preparation separately
and five evaluation samples, each averaging repeated evaluations over at
least 20 ms. These are generated expression experiments, not Hail benchmarks;
they do not measure evaluation CPU. Both baseline and candidate cohorts use
eight fresh processes per case and size, balanced direct/bound within each
cohort. Cross-version cohorts ran separately, so variation between them must
remain visible.

For borrowed reads, explicit binding previously cost 1.79–1.97 times direct
reading. The candidate's bound evaluation uses 53.6–57.5% of that previous
time across 1, 16 and 65,536 rows. Repeated lowercase conversion uses
71.2%, 60.0% and 35.5% of its previous direct-expression time. Existing
arithmetic CSE was already comparable to explicit once-only binding: this
is not a new arithmetic speedup.

Preparation for repeated lowercase conversion increased from 140–152 us to
196–214 us. A single evaluation of a tiny batch does not amortize that cost;
the wide-batch saving does. Adverse expression observations include 4.6%
more evaluation wall time for wide arithmetic and 7.3% for already-bound
wide lowercase conversion. No noise or universal-win claim removes these.
All preparation and evaluation observations are retained in the archive.

## Actual workload controls

These unchanged upstream Hail benchmark function bodies run against Vail's
real native 1,000-row by 2,535-column fixture. Each version/case uses four
fresh processes in balanced B,C,C,B,B,C,C,B order, two warmups and five timed
calls per process. Timers include the original read/build/force-count action.
Complete Arrow values and metadata are compared outside timing; split also
matches the established full external reference. This does not independently
prove that every timed force-count materializes every field.

Baseline is the accepted masking implementation (`3fac5cc1` normal wheel);
the candidate is built and installed normally using kache. Values below are
changes in the median of per-process medians, with all adverse results kept.

| Workload | Wall change | Core-seconds change |
|---|---:|---:|
| Filter entries | +0.072% | +0.007% |
| Filter, then unfilter entries | -0.348% | -0.357% |
| Array arithmetic / PL dosage | -0.326% | -0.373% |
| Split multiallelic HTS | +0.262% | -0.074% |

These controls do not demonstrate an end-to-end workload speedup. No Hail
engine comparison or full-corpus rerun was performed for this candidate.
Process CPU includes worker threads. Four Rayon threads and one BLAS thread
do not cap Vail's 32 Tokio workers. RSS includes imports and validation;
the sampled 6 GiB worker guard is not a hard process-tree bound.

## Correctness and provenance

The exact candidate passes 125 Rust tests and the normal-wheel Python gate
(519 tests plus ten subtests). The sole previously reproduced correlation
allocation test remains deselected. Public tests cover nulls, slices, tuple
bindings, Unicode, complete repeated outputs, eager errors, lexical scope
and allocation no larger than an equivalent once-computed string result
plus 32 KiB. Two malformed new test fixtures were corrected; their failed
logs remain in the evidence. Production did not change for those corrections.

All 72 baseline and 72 candidate expression processes and all 32 actual
workload workers exited successfully with positive process-group absence.
Input file membership and final content hashes match the frozen fixture.
Builds, tests and timings ran sequentially at low scheduling priority.

`cse-cost-evidence.json` seals the archive and each payload. The archive
contains scripts, build/test logs, package/input identities, raw samples,
preparation observations, cleanup receipts and summaries. It excludes
compiled artifacts, environments and large Arrow reference files; their
identities are retained. Steps 3–7 of the worklist remain separate work.

## Narrowed read-only-binding trial

`135af4e973585d82bd6f72f916b8a19192937423` removed text sharing and retained
only cheap array-read inlining. Its exact normal wheel passes 124 Rust tests
and 519 Python tests plus ten subtests, with the same sole pre-existing
correlation exclusion. The same 72 expression processes and 32 actual workers
were rerun, sequentially with builds/tests, and final fixture hashes match.

| Workload | Wall change | Core-seconds change |
|---|---:|---:|
| Filter entries | +0.242% | -0.414% |
| Filter, then unfilter entries | -0.210% | +0.016% |
| Array arithmetic / PL dosage | +0.032% | -0.413% |
| Split multiallelic HTS | -0.315% | -0.842% |

Bound borrowed-read evaluation takes 53.6–63.0% of its old time. However,
one-row bound preparation was 8.5% higher in the separate candidate cohort;
unchanged direct-read evaluation was 5.5–8.8% higher, and wide arithmetic
4.6–4.7% higher. Those observations are retained rather than assigned a noise
explanation. Near-flat actual workload results and uncertain one-shot
preparation costs do not justify adding the rewrite to the default runtime.

`cheap-read-evidence.json` seals this second archive independently. The first
archive continues to describe the original text-sharing experiment. No
additional benchmark retries were used to obtain a favorable disposition.
The public null/slice/tuple/Unicode/eager-error/scope tests and diagnostic
example remain useful without either rewrite.
