# Prepared nullable SUM and transparent collection integration

Base main: `53d59fb5104d896505ec5023b346aba9e2909285`.

Clean merge assembly `3ce12261cb4b0b302a1b584721eaba9425d54f04` combines reviewed nullable SUM `d435dfe3add352c434c86fb81b0d80acc99f6e0a` and collection identity `a8f4b080442e196e9bb1f8189d4d4b38ef023128`. SUM's prepared typed evaluator and collect's projection occupy distinct expression-lowering arms; no interaction edit was needed. The respective kernel/list/logical implementation files retain the isolated candidates' bytes.

Source/test assembly revision `dec8fea010bdb7aeb219f2af36416e1bbd804442` adds four public composition cases and closes the collector review finding: matrix aggregation collection order is unspecified, so nested values and persisted round trips now compare row-keyed multisets while preserving missingness/multiplicity. Array-local collection ordering assertions remain unchanged.

## Exact normal build and runtime gate

- Normal `maturin develop --release --locked` with configured kache wrapper, jobs=2, incremental=0, `RUSTFLAGS=-C target-cpu=native`.
- Exclusively released existing target `/Users/danielking/.codex/worktrees/post-scan-compat-integration/vail/vail/target`; no artifact copying or manual native cache.
- Dedicated environment `/private/tmp/vail-sum-collect-integration-env` and own worktree module origin.
- Installed native SHA256 **`c06fe286cac22339fb6c6bc928f4aca9f630cc224dac443bfdcc96e730b931ee`**.
- **285 passed, three failed**. All three failing nodes reproduce on the exact production-main baseline; no new failing node. No skips or fixture errors.
- Coverage includes all 29 nullable integer SUM cases, five collector cases, four combined nullable/strict/sliced/stored sparse entry cases, complete public RNG suite, matrix layout, aggregate numeric/composition/execution, list/collection kernels, callback sharing, binding and retained DAGs.
- The nine collector/composition cases also pass on the main baseline.

Known baseline failures, retained in `baseline-disposition.log`:

1. `test_collection_aggregate_matrix_scope_and_lookup_context`: expected-three/got-one semantic-kernel row count.
2. `test_integer_reducers_preserve_values_above_float_precision`: heterogeneous dictionary inference.
3. `test_reducers_on_empty_and_all_missing_tables`: heterogeneous dictionary inference.

`validate.sh`, normal `build.log`, complete `public-tests.log`, baseline receipts and `provenance.json` are retained. The upstream bind fixture was supplied unchanged with SHA256 `00ee0d8eb2f419bacb7e910da6423f98551e393a771327a300451a41db303209`.

No new combined throughput claim. Preserve isolated SUM/collect profiles, resource measurements and scope caveats. Collection identity removes work without adding a loop; SUM retains query-prepared static choices and batch-dependent logical validity handling. Remaining generic take/set preparation and unrelated ledger items remain open.
