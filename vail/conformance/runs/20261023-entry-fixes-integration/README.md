# Native entry projection and callback identity integration

Combined assembly `d7017cf747882665b2999f8c043803ad0c13dc5c` integrates reviewed projection candidate `f7972c4ba85632c8dac1993c69899d0dce46bed5` and callback identity candidate `1c2786e58cb32a57dbb90b38fd51e935bdcadc1b` atop main `ec909af4963717a6d7a38b853ed16cd00b25450a`.

The merges are clean and contain no interaction edits. The production files remain byte-equivalent to the individually reviewed candidates. Combined source/test review cleared the assembly; exact runtime/evidence review is requested on the evidence commit containing this file.

## Normal build and validation

- Dedicated environment: `/private/tmp/vail-entry-fixes-integration-env`.
- Normal `maturin develop --release --locked`, kache wrapper, `CARGO_BUILD_JOBS=2`, `CARGO_INCREMENTAL=0`, `RUSTFLAGS=-C target-cpu=native`.
- Exclusively released existing target: `/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target`. No copied native artifacts or manual build cache.
- Installed native SHA256: `77b0c38a0f7bea5308f1651114d0bc8391d2da93beed0b0f4111bb01e7bd7d49`.
- `public-tests.log`: **226 passed**, no failures/skips, covering public entry projections, sparse/empty matrices, matrix layout, joins, interval reads, partition operations, count projection, broadcast, callback sharing/binding/coordinate DAGs, scalar extrema and aggregate execution/composition.
- `random-tests.log`: **41 passed**, no failures/skips, exercising complete public random-expression scope suite.
- Exact commands are in `validate.sh`; artifact/module origin and source fingerprints are in `provenance.json`.
- Existing upstream scalar-bind fixture was supplied unchanged at its expected external source path; SHA256 `00ee0d8eb2f419bacb7e910da6423f98551e393a771327a300451a41db303209`.

No combined performance claim is made. Preserve isolated projection measurements and their increased CPU usage (roughly 40–64%) when describing improved parallelism. Callback CSE gains were modest and workload-dependent; normalization is bounded and intentionally falls back to exact identity for larger programs. These combined checks establish interaction correctness, not new throughput or Hail ratios.
