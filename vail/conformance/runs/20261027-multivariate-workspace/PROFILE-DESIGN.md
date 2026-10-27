# H047 current-path profile protocol proposal — source only

No runtime/build/VM authorization is implied. This freezes the next investigation's scope; an executable driver and complete asset hashes still require review before root GO. Do not infer a speedup from old75-fit samples.

## One actual entry-heavy action

Use the same immutable native matrix path and three metadata hashes already enrolled in H012 cloud expected.json: profile25_mt.mt, 24,885 rows ×2,535 columns. Use accepted main2d05075a production517's normally installed Linux native4761b8ec4e70325e922bb330987ef87c7a9f50bfba4f93f9a69c0c761337244c in `/home/danielking/h012-matched-20261027/run-1/gate/candidate-env/bin/python`. The exact native/frontend/input hashes must pass before any action. No build or installation is needed; missing artifact/schema stops instead of substituting. Root may have advanced docs-only main; production equivalence must be recorded explicitly.

Proposed public query (the final executable must preserve this shape):

```python
m = hl.read_matrix_table(FIXTURE)
m = m.annotate_rows(__h047_x=hl.float64(m.locus.position) / 1000000.0)
m = m.annotate_cols(__h047_fit=hl.agg.linreg(
    hl.float64(hl.or_else(m.DP, 0)), [1.0, m['__h047_x']]))
c = m.cols()
results = c.select(fit=c['__h047_fit']).collect()
```

DP must actually be Int32 and present, locus valid and finite, exact matrix dimensions as enrolled. The producer may contain filtered entries: any such validity can legitimately choose the ordinary grouped fallback. Do not force all-validness, change filtering, or replace fields to make a desired fast path execute. Fully encode every column key and every result field into retained JSON; require exactly all2,535 unique column keys, full `n`/coefficient/statistics shape, no partial `.n` or first-fit-only consumption. Real-data fits are observations, not an independent numerical oracle. A later optimization would need independent analytic/tolerance tests before acceptance.

Bound path: public annotate_cols agg.linreg → coordinate aggregation kernel selection in array_aggregate.rs1215 → CoordinateLinreg / LinregGroupsAccumulator. Shared-predictor admission is only a hypothesis: exact runtime masks, aligned groups and shared-predictor flag govern it. A public diagnostic plan is context, not a semantic test or executed-route oracle. Actual call stacks decide whether update_shared_predictors, generic factor_linreg_flat or neither executes. Failure to reach a linreg kernel stops this package rather than choosing another query silently.

## Bounded execution proposal

One confirmation process, then one sample process, no comparison/timing claim. Reuse H012's existing positive group cleanup, provider STOP-first on unsafe/unknown restoration, guestguard, source/input/frontends enrollment and sudo-n temporary guest permission protocol. Provider<=30minutes STOP before boot; no competing jobs. Thread settings BLAS/OMP/MKL/NUMEXPR1 and Rayon/Tokio32. No install, new source artifact, query variants or perf hardware privileges.

Confirmation: one full action, external60s/action10s/RSS6GiB/free disk10GiB, retain complete output and action/core timing only as baseline diagnostic. Sample: one warm successful complete action, then normal-user `cpu-clock:u`99Hz DWARF for3s; at most12 total actions /6s after first completed action, same60s/10s/6GiB bounds. If one action exceeds a bound, preserve failure and STOP, no extension. Temporary guest paranoid4→2→4 restoreverified and all owned process groups (including confirmation/sampler/commands) positively clean; otherwise providerSTOP before reports/collection. No allocation counters derived from RSS or CPU samples. No generic/scalar extra workload in this initial profile.

Sampling can locate allocator/zero-init calls beneath Faer and Vail but cannot by itself quantify allocated bytes or prove q_coeff/MemBuffer sizes. If those frames are absent/negligible, exit no implementation. If material, a separately reviewed public allocation probe must isolate accumulator updates with fixtures/preparation outside accounting; its thread/process scope must be explicit. Do not introduce production counters or claim the inclusive factorization percentage is recoverable allocation cost.

## Exact workspace contract if evidence warrants it

Dynamic requirements must be recomputed at the valid update/shape boundary using the SAME pinned Faer APIs/params, not guessed `rows*p` bytes:

| Operation | Existing dynamic StackReq inputs | Coefficient owner |
|---|---|---|
| generic augmented QR | actual stacked nrows, d=p+1, recommended_block_size(nrows,d), Par::Seq, original QrParams (blocking_threshold=MAX) | block_size×d matrix |
| predictor-cache miss QR | actual nrows,p,recommended block,Par::Seq, original default QrParams | block_size×p; coefficients copied into cache |
| cached-response apply | nrows,cache.block_size,nrhs=1, original apply API | borrow cached basis/coefficients unchanged |
| shared-predictor QR | actual matrix_rows,p,recommended block,Par::Seq, original params blocking_threshold=MAX | block_size×p |
| shared-response apply | matrix_rows,block_size,nrhs=actual response width | live same-update design and q_coeff |

Obtain both StackReq byte size and alignment from the pinned library, checked for overflow. Reuse a buffer only if capacity AND alignment satisfy the new requirement. One workspace per existing active accumulator/worker; never one buffer per touched group, global pool or reuse across concurrent calls. No MemStack, MatMut/MatRef or raw pointer escapes the borrowing call. QR and Householder phases may share scratch only after all prior borrows end; q_coeff remains live through application and cannot alias scratch/design/response/cache. Retain documented initialized coefficient semantics (currently zeros), including dimensions/tails; don't assume the numerical routine fully overwrites all storage without source/API proof.

Proposed policy for review, NOT implementation: at most8MiB retained scratch plus1MiB coefficient capacity per accumulator; larger requirements use a temporary exact-size buffer released before update returns. Cached basis/predictor data are existing state, separately accounted, never confused with scratch. `Accumulator::size` and GroupsAccumulator::size must include actual retained capacity plus owner overhead, not current length. Total retained bound is9MiB×number of live accumulators (which DataFusion may multiply by partitions/aggregate instances); not a claimed fixed process-wide bound. Quantify actual concurrency/aggregate count from the real executed plan before accepting this cap. Avoid a new global quota framework. Small requirements reuse capacity without shrink/regrow churn; one large call does not evict retained small scratch, so large→small retains at most9MiB while transient peak is separately reported. A different cap needs explicit evidence/review, not benchmark tuning.

Preserve all existing normalization, QR/sign convention, finite/extreme checks, weights/centering, row/order/state/merge logic and exact predictor-bit cache admission. Workspace lifetime confers no cache validity; no stale q_coeff/basis or response state may carry across updates. H027 predictor dispatch and scalar DMatrix reuse remain out of this first scope. Public large→small/multi-update/merge/missing/rank/weight/extreme tests and retained-cap accounting would be required if implementation is later authorized.
