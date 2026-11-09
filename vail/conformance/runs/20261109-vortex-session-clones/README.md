# Direct Vortex session clones, 2026-11-09

Deleted the runtime-ID-keyed Mutex cache from the Iceberg adapter. Native SnapshotReadPlan prepares one session for nonempty snapshots, using CurrentTokioRuntime dispatch rather than a captured polling-runtime handle. Readers and file tasks receive cheap session clones. The generic ArrowReader fallback initializes a missing session only at its first actual Vortex task, in the serial task stream before file futures fan out. Supplied sessions are retained. Generic Parquet-only/empty scans and empty native snapshots avoid default Vortex setup.

Native snapshot planning already rejects non-Vortex data files. Preparation is eager for a nonempty native snapshot, including when a later query selects no files; that preparation tradeoff is not claimed away. Session construction still occurs for a fresh nonempty snapshot read plan. This removes the wrapper mutex, not every Vortex internal synchronization point or the cost of constructing registries. File/scan/execution/metrics state remains local.

Matched warmed laptop subset: three fresh workers per version/case in B,C,C,B,B,C order; two warmups then four timed calls each, median of worker medians. Same native 1,000-row × 2,535-column fixture/four partitions; generic explode uses400×100 values. Rayon4, existing Tokio32, BLAS1, Rust/PyArrow mimalloc. Query construction/read/action included. All60 workers exited and240 timed samples retained. No compiler, tests or profiler overlapped these comparative timings.

| Workload | Before wall ms | After wall ms | Wall change | Before CPU ms | After CPU ms | CPU change |
|---|---:|---:|---:|---:|---:|---:|
| entries_show | 17.523 | 18.299 | +4.43% | 55.362 | 55.594 | +0.42% |
| entries_prefix | 16.226 | 16.476 | +1.54% | 55.693 | 54.141 | -2.79% |
| take_entry | 8.171 | 8.337 | +2.02% | 30.218 | 30.018 | -0.66% |
| entries_count | 20.857 | 21.176 | +1.53% | 70.092 | 69.313 | -1.11% |
| entries_count_no_key | 24.041 | 24.984 | +3.92% | 74.274 | 75.745 | +1.98% |
| explode_prefix | 1.059 | 1.063 | +0.41% | 1.232 | 1.249 | +1.30% |
| explode_full | 0.664 | 0.637 | -4.05% | 0.867 | 0.830 | -4.27% |
| filter_entries | 48.309 | 47.930 | -0.78% | 196.531 | 195.426 | -0.56% |
| split_multi_hts | 147.485 | 145.668 | -1.23% | 386.301 | 384.846 | -0.38% |
| read_rows | 13.517 | 13.175 | -2.52% | 64.144 | 62.256 | -2.94% |

**No substantial throughput gain is demonstrated.** The ownership change is simpler and eliminates the observed wrapper lock; measured latencies are mixed. Entries-show wall +4.43% and CPU +0.42%; keyless count wall +3.92% and CPU +1.98%. Native read/count wall -2.52%/CPU -2.94%, split-multi wall -1.23%/CPU -0.38%. All adverse values are retained without causality or noise-dismissal claims. Generic sub-millisecond timings are weak evidence of throughput differences. This is not a full-suite/Hail comparison.

Worker result hashes match between versions for all ten cases. These cover counts/action returns, bounded display text and100Call repr values, not full Arrow/schema/value identity; the public correctness tests are separate.

Correctness: focused Python22PASS/80deselected, same known baseline ScalarSubquery exclusion retained. Public reader_batches3/3PASS, covering reader batch/order/state behavior and concurrent reads from independent polling runtimes while the Iceberg table's owner I/O runtime remains alive. The barrier aligns starts; it does not establish internal task overlap.

An initially broader test destroyed the owner I/O runtime and failed with RecvError in delete-file loading. The identical test also failed against oldb2 production, confirming Iceberg's existing runtime-lifetime precondition. Failure/counterfactual logs and original test source are preserved; this independent behavior was not fixed or silently ignored. The corrected test keeps the owner runtime alive.

Separate real five-second samply recording of the final native artifact:35threads/23,993samples/32,391frames, sampler exit0. Symbols contain read_vortex_task and Vortex functions, with no VortexSessionCache references; the cache source is deleted. The recording includes startup/two warmups then repeated ordinary entries-show, and contains blocked as well as running-thread samples. The symbol check is a sampled-function/source-removal check, not proof that all internal locks disappeared or a CPU-time attribution claim. Profile metadata/hashes are in profile-proof.json; full recording and symbol sidecar are under the durable thread directory entries-show-session-clones.

Baseline sourceb2a5bc55f (productionc02), normal native0ee6087f01e60b2ee87aabc573939d667b6942bf5ef5d40c16d7546479c1efb3. Final production61ecae51abb1efb6f1fb44969668f0ef2d4bcb23, public-test correction5654d789b697bedf23965599cef7412c7cc787c8, normal native4031dcb7146f593be5309dcedf0972231ebc021087a7a40dff0bc82bbb7132e0. Final artifact was built normally against frozen source with kache and normally installed/identity verified. An earlier build overlapping the empty-snapshot guard edit is retained as ready-first.json/build-first.log, excluded from admission; the final repeat build produced403. No manually reused compiled .so.

Raw benchmark scripts/worker records/build/test/counterfactual logs and source hashes: /private/tmp/vail-direct-vortex-session-20261109/. No local timing source/action changed between the two versions.
