# H092 final integration gate

Frozen approved source af97646b1391d722ec4c49e4f4e1702accc10078 is a direct descendant of main28406dfb543bfab418519f6e7092bed36a051fa8. The development integration branch fast-forwards exactly; no merge/source/test edits. This evidence-only commit does not change production. Changed surfaces are prepared integer list SUM/product/min/max and their query lowering; no new interaction changes to Call error barriers, stddev, effects or filtering.

## Artifact and scope

The exact approved normally built H092 environment was used in place: /home/danielking/h092-segmented-reducers/env. Installed native SHA2564d46e99d743e7acac6dc3fe3b9a8fe9e2fe70e93d87fc14357ee75634525fd9f was asserted before tests, matching the isolated normal wheel provenance in ../20261024-segmented-integer-reducers/results/native-provenance.json. Current remote source hashes for execution/Call/effects/stddev/list reducers, Python collection planner and Cargo.lock independently match this checkout. No source edit, rebuild, compiled-artifact copy or new timing claim was needed.

Fresh broad regression:333 Python tests pass; exactly one historical baseline failure remains, test_collection_aggregate_matrix_scope_and_lookup_context (Expected3/Got1), with zero errors or skips. Full JUnit/logs are retained. The suite includes all326 passing Python surfaces from the previous integrated gate plus seven new integer segmented cases: Call/genotype/index/split/likelihood, stddev/stats, shared row computations, reduction CSE, RNG, list collect/shared reducers, nullable integer SUM, matrix SUM, filtered collection aggregates, coordinate callbacks and matrix-array reductions. The pre-existing Call/stddev10Rust boundary receipts from main remain available in the prior integration run; they were not rerun or counted as fresh tests here.

The isolated98-pass gate and actual63M-entry performance/profile/optimized-assembly evidence were already approved and remain in the segmented-integer-reducers run. The fresh larger suite adds cross-surface regression coverage without recompiling identical approved production. It has the same single known failure as the exact isolated baseline and candidate; no failure exclusion or skip hides it.

## Execution and cleanup

PRIMARY owner released source/target and stopped the VM after isolated receipts; this gate restarted it only for correctness and verified it idle. Fresh four-hour shutdown deadline17:23:59UTC armed before execution. Python gate14.09s; no timed workloads. Downloaded logs/provenance/JUnit checksums verified against VM and local source hashes. VM stopped after verified download. Parent retains required final evidence acceptance and main merge responsibility.
