# Entry key-run laptop comparison, 2026-11-09

Three fresh workers per version/case in balanced B,C,C,B,B,C order; two warmups and four timed calls per worker. Same persisted 1,000-row × 2,535-column native fixture and Rayon four threads, BLAS one thread, and the existing 32-worker Tokio runtime. The `VAIL_EXECUTION_CONCURRENCY=4` environment entry is ignored by the runtime; it does not establish four execution workers. Rust mimalloc and PyArrow mimalloc fixed. Original read/query construction/action included; imports excluded. No builds/tests/profile overlapped timing. All 30 workers completed and exited; all 120 samples retained.

Baseline native `d7119f220c917bbb5fc2dc0511d89c04bdfc2947a9ad8e4e917775952112be6e` comes from the existing normal allocator-comparison build (source `1abc1ed86`). Candidate native `2723af5a47571de26bf525e3960aa3d27e09505b31a9d5b6e979716bf564338e` is the root normal combined build of source `46d95c159`, including parallel stable sort, legacy key propagation, and entry-span changes. These measurements do not isolate the entry-span commit.

| Workload | Baseline wall ms | Candidate wall ms | Wall speedup | Baseline CPU ms | Candidate CPU ms | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| unique_show | 26.360 | 25.173 | 1.047× | 95.666 | 93.405 | -2.36% |
| unknown_show | 140.413 | 123.231 | 1.139× | 225.612 | 276.370 | +22.50% |
| unknown_take | 156.751 | 123.598 | 1.268× | 242.555 | 276.119 | +13.84% |
| duplicate_show | 1360.883 | 1362.337 | 0.999× | 1630.264 | 1755.847 | +7.70% |
| unknown_count | 138.142 | 120.888 | 1.143× | 223.266 | 272.338 | +21.98% |

The fixture proves row-key uniqueness, so `unique_show` uses the existing Unnest path and its observed change cannot be attributed to entry-span emission. `unknown_*` adds a public identity row explosion (`replica=[0]`) to remove that uniqueness proof. `duplicate_show` explodes `[0,1]`, exercising duplicate runs. Show uses the normal formatter with output discarded; take materializes 8,192 entry records.

Important adverse/control results: unknown show/take/count spend more core-seconds despite lower wall time. The count path bypasses entry expansion and changes similarly to unknown show; the full show gain must not be attributed to copy removal. Duplicate show is essentially flat in wall time and consumes 7.7% more CPU. These are descriptive small-fixture results, not significance tests or full-suite extrapolations.

Production source removes per-singleton RecordBatch/KeyRun construction and concatenation; Arrow adjacent-key partitioning identifies equal runs once per batch, contiguous unique spans emit borrowed entry slices, and only the trailing run carries to the next batch. Duplicate transposition/spill and existing certified disjoint partition grouping remain unchanged. Range metadata is O(batch rows), slices retain backing owners, and duplicate/filtered output may still gather; this is not a universal zero-copy or reduced peak-RSS claim.

The retained baseline failure for the existing repartitioned multikey Python case (`ScalarSubqueryExpr evaluated before the subquery was executed`) is separate. No fix for it is bundled here. The combined candidate’s initially observed duplicate tie reversal was traced and fixed by the row-sort agent’s constant-metadata correction before this timing cohort; no failed candidate timing was accepted.

Raw `records.json`, `summary.json`, `worker.py`, and `run.py` accompany this report. Parent evidence directory also retains baseline plans, public reproduction scripts, and logs. Normal combined correctness/resource gates and their counterfactuals are recorded by root separately.

## Isolated entry-span counterfactual

One additional root-authorized comparison retained the final parallel sort, Hail key propagation, mimalloc and all other production code, restoring only `matrix_entries.rs` to the pre-span source. Counterfactual source `dae79e5c7`, normal native `9c88772e`, was compared with the unchanged final source `46d95c159`, native `2723af5a`. The warm-resource-test-only child does not change the final runtime. Same existing fixture and exact `unknown_show`/`unknown_count` worker bodies, six balanced fresh workers per case, two warmups and four timed calls: 12 workers/48 samples, all completed and exited. No compilation, correctness tests, or profiling overlapped timing.

| Workload | Without spans wall ms | Final wall ms | Without spans CPU ms | Final CPU ms | Final wall change | Final CPU change |
|---|---:|---:|---:|---:|---:|---:|
| unknown_show | 123.618 | 123.752 | 274.539 | 276.848 | +0.109% | +0.841% |
| unknown_count | 122.816 | 122.537 | 274.798 | 276.154 | -0.227% | +0.493% |

**The isolated cohort demonstrates no material throughput improvement on this small-fixture show workload.** Count is likewise essentially unchanged. The earlier combined 13.9% unknown-show improvement must not be attributed to entry-span copy removal. The independent warmed allocation counterfactual (root-run) detects the removed intermediate payload copy, but does not establish it dominates this show benchmark. All small adverse observations are retained; no significance/noise claim or retry was made.

Isolated raw data, full source/native/body identities and the runner are under `isolated/`. The runner uses the unchanged parent `worker.py`; run it from the original evidence directory or put that worker beside it. Root retains the resource-test logs separately.
