# Inner join follow-up

The typed-key migration removed the previous byte-key encoding from the hot join path. On the optimized release wheel, the 1M-row Vail inner join fell from a 0.9732 s median in the recorded baseline to 0.0709 s in the current comparison run (about 13.7× faster). The Hail 0.2.139 median at 1M rows was 0.2163 s, so Vail measured about 3.05× faster on this workload.

| Rows per side | Hail median | Vail median | Hail / Vail |
| ---: | ---: | ---: | ---: |
| 100,000 | 0.1633 s | 0.0285 s | 5.73× |
| 500,000 | 0.1780 s | 0.0482 s | 3.69× |
| 1,000,000 | 0.2163 s | 0.0709 s | 3.05× |

Each comparison used 14 requested partitions, two warmups, three measured iterations, and the same `benchmarks/compare_hail.py` workload. Runs were serialized by engine and size. Full machine and per-iteration metadata are in `multi-size/`.

The Vail run used Python 3.11.13 and the release wheel at `/private/tmp/vail-matrix-release-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl` (SHA-256 `aa81db98350de90b6fad833d6b662ecbd8bc20b5b934d89e508a78480577f266`); its loaded native extension SHA-256 was `864c9c32e58b3b8cbedc5e32cca7d1e5e8dc0bed01da8cc4ad36415b92510b02`. The Hail environment reported version 0.2.139.

The prior native profile attributed 11.65% of sampled CPU leaves to `vail_ordering::append_value`, with additional samples in the canonical nullable-key expression and binary-array allocation. The optimized path no longer uses that encoder for join key comparison. `samply` could not attach to the updated worker (`Unknown(1100)`), and macOS `sample` denied process inspection privileges. A Python `cProfile` snapshot is retained at `after-profiled-symbols/python-profile.pstats` with text output alongside it; it confirms that the measured action is dominated by the native `NativeTable.force_count` call (about 72 ms), but does not resolve Rust internals. The new cProfile snapshot also records the wheel hash in `after-profiled-symbols/worker.json`.

The comparison is a local synthetic benchmark, not a general performance guarantee. Hail timings include Spark startup only in its separate initialization field; the listed measurements cover the join action after warmup.
