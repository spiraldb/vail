# Consumed aggregate input projection

Frozen production/test source: `06defddc30de799d7b16592afb6c41155c91b552`, based on main `a47b8f170462c95d81c68f23ade08938acc769e1`. Normal installed candidate native SHA256 `d7d8f91914fb15cc069fe86a6782b05ad78ab047e11ceb8106ba9e71e66ba95c`; immutable baseline native `0c9d373cff054f916ad96474f89e36c07b53289b5e597694b6ddbaa8eb7ba980`. Wheel/native equality and source fingerprints are in native-provenance.json. Changed-content source overlay touching is recorded in source-overlay.json. Normal release maturin build uses existing kache target,16jobs,target-cpu=native; no binary copying.

The generic existing dependency analyzer now supplies nested source requests for consumed expressions on immediate native reads, including grouped keys. Value paths preserve their ancestor logical validity/shape. Uncovered validity/shape requirements conservatively request the complete container. Consumer-specific providers bypass table-only memoization. A prefix set avoids quadratic selected-path coverage checks. Filters and other intervening table nodes retain conservative existing behavior. Typed validity-only storage requests remain separate future work.

Public baseline new cases:5passed. Exact candidate broad gate:350passed,2failed; both failures are unchanged main failures: collection aggregate matrix scope/lookup context and composite floating aggregate cardinality. No new failures. Tests cover sparse entry logical validity with selected children missing, count-only fallback, grouped keys, same-read independent consumers, filter fallback, and32selected fields among64stored fields.

## Representative native entry work

24885rows×2535columns (63083475logical cells), same existing native matrix snapshot. Clean alternating ABBA processes,4iterations each; discard iteration0 and retain6samples/version/query. All outputs match. summary.json contains medians and all raw quiet-*.jsonl are retained.

| Query class | Main ms | Candidate ms | Speedup | CPU reduction |
|---|---:|---:|---:|---:|
| SUM(DP) |218.3|146.8|1.49×|33.8%|
| SUM(DP),SUM(GQ) |214.3|158.7|1.35×|26.1%|
| SUM(DP),SUM(GQ),COUNT |210.9|155.9|1.35×|29.8%|
| Computed integer SUM plus SUM/COUNT |246.4|175.8|1.40×|30.2%|

The earlier paired-*.jsonl run is explicitly excluded: provenance hashing overlapped its tail. quiet-*.jsonl is the clean repeated run with no builds, tests, profiling, exports, or provenance subprocesses during measurement.

Syscall rchar remains approximately0.98GB per query; warm disk read_bytes is0 in both. These counters do not establish lower logical column IO or DRAM bandwidth. The demonstrated improvement is CPU/materialization avoidance; there is no measured syscall-byte saving. Maximum process RSS falls about23–40MiB and minor faults fall substantially, with per-iteration receipts retained.

Separate matched cpu-clock profiles (99Hz,DWARF8192,12SUM/COUNT queries; zero lost samples) support actual decode avoidance: prior UInt32→UInt16/UInt8 unary casts consume7.39%flat baseline and0.02%candidate; full offset validation3.95%→0.06%. This removes unrelated GT/list conversion work when only DP/GQ and entry presence are consumed. Profiles include Python startup and OpenBLAS background activity, so percentages are descriptive sampled CPU rather than precise query-only attribution. Remaining work includes Vortex take/materialization, allocator traffic, array execution and integer reductions.

Raw profiles downloaded outside Git to `/private/tmp/stagea-raw-profiles.tar.gz`, SHA256 `90196c15b442ab7aee6fc0664d16282366f3fdeecb49da1d8bcf54b5cc24a6db`; compressed flat reports are committed. Cloud run root `/home/danielking/aggregate-input-projection`; normal stable source/target released to Mean owner after all measurements. Fresh4h guest shutdown guard was installed and ownership explicitly handed off. No new Hail comparison is claimed.
