# Coordinate callback aggregation evidence

This directory records an affected-case analog for the composite coordinate callback path: grouped MatrixTable row aggregation. It is a deterministic range-matrix test, not the upstream `random_doubles_mt` benchmark fixture.

## Query and method

The runner `benchmark_matrix_group.py` creates `range_matrix_table(100000, 128, n_partitions=16)`, annotates entry `x = row_idx + col_idx`, then runs `group_rows_by(new_idx=row_idx % 3).aggregate(x=mean(x))`. It warms the lazy pipeline once outside timed trials, times `_force_count_rows()` for three independent Python processes, and then validates all 384 aggregated entries against their expected means outside the timed scope. Each JSON file records the run’s wall time, Python process CPU time, and process peak RSS. Fixture and query construction are outside the timed scope; evaluating the lazy range matrix and query are inside it.

Runs were serial and interleaved baseline/candidate, with no other Python benchmark, pytest, or Rust build active. The system kache daemon was idle. Both sites ran through `/Users/danielking/projects/vail/vail/.venv/bin/python` (CPython 3.12, matching the `libpython3.12` profile). The baseline was `/tmp/linreg-coordinate-site`; the candidate was `/tmp/arrayagg-coordinate-site-final`.

| Site | Median wall | Median process CPU | Peak RSS (max of 3 runs) |
|---|---:|---:|---:|
| Baseline | 5.212 s | 13.822 s | 4.13 GiB |
| Candidate | 0.269 s | 2.673 s | 0.49 GiB |

On this deterministic-range grouped-matrix case, the candidate was 19.4× faster by wall time, used 5.2× less process CPU, and had 8.4× lower peak RSS. These numbers do not compare Hail and do not claim performance on the unavailable upstream random-doubles fixture. Raw per-run outputs are `baseline-{1,2,3}.json` and `candidate-{1,2,3}.json`.

## Artifact provenance

- Baseline source was `31ac0397f4c7b96d86852af826b63cd61569097c` (the pre-bundle baseline); wheel SHA-256: `d678a789e454b3438dc426a3636ad18b3748f18bea93dfd3998f802592051933`. The installed native module SHA-256 is `93114b1a296143af8cb7ac994a6794203079a076537256949b0c642472e1028b`; the installed `matrixtable.py` SHA-256 is `28e7ecf62392f0f5e5eb05769f7b09f684000b979903593394659af4595cc8ad`.
- Candidate production source is `eb406703cbb7327a3f18611e811e441bc0cbf63b`; final test-only cleanup is `7f263edb63b73a763310736aed3afdb594c5f56d`. The release wheel built with Rust 1.98.0 has SHA-256 `181a7a51ed5c83a00ba04e6f4bea27fb052654eab8027ec79060e78595390a6b`. The installed native module SHA-256 is `a11d7cc710d6ce250240d959b3ec6e2c0fee0b5ad6f07d2b247cfa44b68cd646`; the installed `matrixtable.py` SHA-256 is `03df28462fc3c601ce09931905833c7f16e07f85b641ebfe1689282011a6f6f6`.
- Command per trial: `VAIL_BENCH_ROWS=100000 VAIL_BENCH_COLS=128 VAIL_BENCH_REPEATS=1 PYTHONPATH=<site> /Users/danielking/projects/vail/vail/.venv/bin/python vail/conformance/runs/20261022-arrayagg-coordinate/benchmark_matrix_group.py`.

## 1 kHz profile

`profile.json.gz` and `profile.json.syms.json` are a 1 kHz samply capture of the same 100k×128 grouped-matrix query using the candidate. The capture was made in a quiet window; it contains 1,898 samples across 35 threads and 3.178 seconds of sampled thread CPU. Samply sidecar symbols identify the candidate `_native.abi3.so` by debug ID `b5107494-6486-3f58-9644-6252a8447ace`.

Leaf samples are weighted toward DataFusion expression rebinding/cloning and hashing: `CaseExpr::with_new_children` (5.3%), `aligned_matrix_map::rebind_body` (4.0%), SipHash writes (3.9%), and `HashMap` drop/insert (about 5.0% combined). Several allocator frames remain raw addresses in the sidecar summary. The inclusive stack includes `ProjectionStream::poll_next` (8.0%) and the aligned matrix map evaluation path (about 2%). This capture covers plan construction plus the forced query, so it is evidence of remaining work in this run, not a clean attribution of all cost to the aggregate kernel.

A separate `profile-linreg75.json.gz` capture was started for the 75-fit linreg regression workload while unrelated Rust builds and Python test workers were active; it was interrupted and must be treated as contaminated diagnostic data. Do not use it for timing or performance claims. The exact driver is `profile_linreg75_contaminated.py`.

## Correctness validation

The exact production candidate wheel previously passed the focused Python aggregation suites (175 passed) and focused Rust coordinate bundle tests (2 passed). After the final test-only fixture cleanup, the Rust coordinate tests were rerun (2 passed). The matrix benchmark additionally validates every grouped entry against its expected mean after each timed action.
