# Local Rust allocator throughput comparison — 2026-11-09

Same Vail source1abc1ed86 and normal release settings for all three variants: Rust1.98.0, Xcode clang17/SDK26.2. Only Rust global allocator changes. Python default allocation and PyArrow mimalloc remain fixed. No C malloc interposition.

The same1000-row×2535-sample/four-partition native input is used for six upstream benchmark bodies. Three fresh processes per allocator/case, Latin-square9-process order,3warmups and5measured calls/process:54workers270samples. Original read/query construction/action timed; engine import and native-write temporary-directory creation/deletion excluded. No builds or correctness runs overlap timings. No additional correctness suites or output validation after the explicit user instruction.

| Workload | System ms | Mimalloc ms | Jemalloc ms | Mimalloc throughput change | Jemalloc throughput change | Mimalloc CPU change | Jemalloc CPU change |
|---|---:|---:|---:|---:|---:|---:|---:|
| Filter entries | 50.256 | 48.620 | 48.619 | +3.36% | +3.37% | -7.43% | -7.86% |
| Filter/unfilter | 108.994 | 105.381 | 105.934 | +3.43% | +2.89% | -4.68% | -5.18% |
| PL dosage | 54.990 | 52.561 | 59.638 | +4.62% | -7.79% | -8.58% | +5.58% |
| Split multi HTS | 183.147 | 148.802 | 167.403 | +23.08% | +9.40% | -15.98% | -13.19% |
| Row-key shuffle | 117.224 | 119.056 | 126.275 | -1.54% | -7.17% | -5.59% | -2.30% |
| Native write | 134.974 | 128.039 | 126.739 | +5.42% | +6.50% | -5.03% | -8.95% |

Throughput ratio is System median wall / variant median wall, using medians of per-process medians. CPU is whole-worker process CPU including all threads. RSS is sampled whole-worker import/warmup/action memory, not isolated operator allocation. This is a quick laptop subset comparison, not the full published fixture or a significance claim. Write uses warm filesystem caches and does not measure physical-disk/fsync throughput. The shuffle-named original force-count action does not itself establish materialized shuffle bytes. All cases and adverse observations are retained.

Mimalloc improves five of six observed wall throughputs and reduces CPU in all six; jemalloc is mixed. Production defaults were unchanged during measurement. After reviewing these results and the higher sampled whole-worker RSS, Daniel explicitly requested mimalloc as the default; commit def5d15aa makes that change for Rust allocations in the Python extension, retaining System and jemalloc build options.

Both alternatives showed higher sampled whole-worker RSS in all six cases. For split multi HTS, System/Mimalloc/Jemalloc peaks were 336/1007/622 MB; for native write, 461/1239/1213 MB (decimal). These include imports and warmup and do not isolate allocator retention. This memory tradeoff was disclosed before the default-change decision.

Normal release wheels were built and installed with maturin/uv using kache; no native binaries were manually copied or reused as a build cache. Binding versions: mimalloc0.1.52/libmimalloc-sys0.1.49(default native3.3.2),tikv-jemallocator0.7.0/sys0.7.1+5.3.1. Earlier wrong-toolchain/SDK setup attempts and the canceled correctness plan are excluded from these measurements.

Raw data: fast-records.json,fast-summary.json,fast-protocol.json,fast-pins.json,fast-post-hashes.json,fast-complete.json. Build/feature/native/symbol identities are ../pinned-sdk/identities.json and per-variant logs. All54owned process groups exited; input13-file hashes and native identities stayed unchanged. No cloud work, push, website update or PR.
