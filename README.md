# Vail workspace

This directory contains upstream source checkouts and the new Vail project:

| Directory | Purpose |
| --- | --- |
| `hail/` | Hail Python API, semantics, implementation, and reference tests |
| `datafusion/` | DataFusion execution and ordering implementation |
| `vortex/` | Vortex arrays, files, and DataFusion integration |
| `iceberg/` | Iceberg specification and Java reference implementation |
| `iceberg-rust/` | Iceberg native Rust implementation and integration points |
| `vail/` | Vail source, tests, recorded results, and documentation |

Start with [Vail's build instructions and implemented subset](vail/README.md), then [the implementation plan](vail/docs/PLAN.md) and [agent assignments](vail/docs/TASKS.md).

The five upstream directories are Git submodules pinned to the inspected revisions in [upstreams.lock.json](vail/upstreams.lock.json). They are research baselines, not a tested runtime dependency combination. The Vail runtime instead uses its Cargo lockfile and the documented patched Iceberg PR snapshot under `vail/patches/`.

Clone this repository and fetch the five upstream checkouts with:

```sh
git clone git@github.com:danking/vail.git
cd vail
git submodule update --init
```

Vail's tests need these upstream source checkouts; their own nested submodules are not required for the current Vail checks. Build commands run from `vail/`. Build outputs and local environments are ignored; written documentation and conformance run records are committed.

Vail's original code is licensed under [MIT](LICENSE). Upstream submodules and the vendored Iceberg patch retain their own licenses and notices.

## Benchmark results

A comparison started on **2026-11-12** measured Vail `8ed78f4f` and Hail `0.2.139-5e5eedb87ff8` on one 32-vCPU AMD EPYC VM. Each engine read its own native format; format-independent operations used common inputs. Across **111 paired execution cases**, the median per-case Hail/Vail ratio was **8.12× for wall time** and **8.21× for observed core-seconds**. Each case has equal weight in those medians.

The total of per-case medians for this fixed case bundle is:

| Sum of per-case medians | Vail | Hail |
|---|---:|---:|
| Wall-seconds | 195.5 | 762.8 |
| Observed core-seconds | 1416.6 | 5382.2 |

These sums describe that case bundle, rather than elapsed suite duration. Vail took longer in **2** cases: gzip reading (+4.2%) and the CPU-hash sentinel (+0.1%). It used more CPU in **8** cases, including the billion-row range join, integer import with imputation, and entry filtering/unfiltering. All losses remain in the report; three repetitions do not establish statistical significance.

Results are medians of three fresh-process first actions, with query planning/compilation included and engine initialization excluded. They describe this supported pool, rather than warmed steady-state throughput or full Hail compatibility. There are 39 policy exclusions and one separately reported planning-only case. Native layouts/compression differ, caches were not forced cold, and the Linux wheel required the documented loader setting. See [the complete results and method](BENCHMARKS.md), including all 112 scheduled cases and raw samples.
