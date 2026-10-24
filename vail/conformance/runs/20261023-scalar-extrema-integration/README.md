# Scalar integer extrema integration

Combined source revision: `55c14408bd2cd05a53ca3cdee14d835af1530ce9`.
Base main: `40de9e4ed185a6c3c9f027b949549d4d371aec5d` (parallel VCF export and
legacy join fixes). Reviewed scalar candidate:
`b1ba7160717a387c7042be5019a00e3d35f2f27c`.

The merge is clean and introduces no interaction edits. The production change
is 37 Rust lines across expression and logical expression lowering. Explicit
nonempty integer min/max argument lists with missing-value filtering lower to
DataFusion least/greatest, avoiding intermediate list construction. Floating,
empty, strict-missingness and general array cases retain the existing reducer.
The original source report retains performance measurements and their scope;
this integration does not claim fresh combined benchmark timings.

## Normal combined runtime gate

- Normal kache `maturin develop --release --locked` completed in **2m25s**,
  Python 3.12.12, two jobs, `CARGO_INCREMENTAL=0`,
  `RUSTFLAGS='-C target-cpu=native'`.
- Reused the completed build target sequentially with root's owner confirmation;
  no compiled artifacts were copied or manually cached.
- **338 public tests passed in 156.71s**, no failures/skips: 40 scalar extrema
  regressions, numeric coercion/core aggregate kernels, and the preceding 278
  export/join/reader/VCF tests. The suite includes concurrent legacy actions
  and real tabix queries. Exact invocation and complete output are preserved.
- `provenance.json` identifies the installed module, source revision and SHA.
  `source.sha256.json` fingerprints both Rust files and scalar public tests.

Required review thread `01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared the exact
combined source assembly. Root owns final evidence acceptance and main update;
the integration subagent did not update main.
