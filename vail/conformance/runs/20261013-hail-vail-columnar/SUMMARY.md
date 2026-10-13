# Hail versus Vail after columnar arithmetic — 2026-10-13

Vail revision: `173aac1f4be07c163cd7f6b675cfeb9619b9c7be`, optimized release wheel. This includes arithmetic lowering to DataFusion/typed Arrow kernels and edition-aware Vortex compression. Installed binary hash is in `vail-build.json`. Stock Hail is the same PyPI 0.2.139 installation as the previous benchmark, with no source build.

All 15 benchmark cases passed their existing correctness checks on both engines. Vail was faster in 7 cases; Hail in 8. Each median is five measured iterations after three warmups, with fresh lazy query construction and action included; import and initialization are excluded.

| Workload | Hail now (s) | Vail before (s) | Vail now (s) | Faster now |
|---|---:|---:|---:|---|
| Range stream, 100M rows | 0.2595 | 0.0313 | 0.0165 | Vail 15.77× |
| Rekey/sort, 1M rows | 1.1797 | 0.3605 | 0.2291 | Vail 5.15× |
| Group ascending keys, 10M rows | 0.6575 | 1.3942 | 0.7958 | Hail 1.21× |
| Group descending keys, 10M rows | 0.6364 | 1.8343 | 0.7685 | Hail 1.21× |
| Annotate 100 fields, 1M rows | 0.2240 | 4.7039 | 0.0349 | Vail 6.42× |
| Numeric statistics, 10M rows | 0.1718 | 0.7228 | 0.7253 | Hail 4.22× |
| Inner join, 1M × 1M rows | 0.2031 | 1.6719 | 1.6408 | Hail 8.08× |
| Lookup, 1M probes / 1K targets | 2.5504 | 0.5371 | 0.3506 | Vail 7.27× |
| Prefix sum scan, 1M rows | 0.1570 | 0.9532 | 0.9243 | Hail 5.89× |
| Native write, 1M rows / 2 int32 fields | 0.4286 | 0.2368 | 0.2003 | Vail 2.14× |
| Native read, 1M rows / 2 int32 fields | 0.1686 | 0.0106 | 0.0094 | Vail 17.95× |
| Matrix row stream, 100K × 10 | 0.0780 | 0.0070 | 0.0064 | Vail 12.16× |
| Matrix entry sum, 100K × 10 | 0.1000 | 2.1248 | 2.0268 | Hail 20.28× |
| Matrix row/column filters, 100K × 10 | 0.1653 | 0.4852 | 0.4678 | Hail 2.83× |
| VCF import + DP sum, 50K × 10 | 0.2057 | 0.9219 | 0.8993 | Hail 4.37× |

Wide annotation improved from 4.7039 s to 0.0349 s (134.7×), and is now 6.4× faster than Hail. Grouping and lookups also improved. Numeric statistics, joins, scans, Matrix entry aggregation, and VCF aggregation remain performance work; their Vail timings changed little. These timings alone do not establish where those workloads spend their CPU time.

## Resources and measurement limits

- Same ARM Mac15,10, 96 GiB RAM, 14 cores, Python 3.11.13. Engines ran sequentially. Hail was configured as `local[14]` with 48 GiB driver heap; runtime verified 14 workers and maximum heap of 51,539,607,552 bytes. Sources requested 14 partitions except VCF import, which uses reader defaults. Vail uses its existing runtime/execution parallelism; requested partitions do not mean 14 active workers.
- The primary results are `vail-quiet.json` and `hail-quiet.json`. The initial `vail.json`/`hail.json` are excluded: a concurrent Rust build consumed several cores during Hail measurements. The clean rerun paused the competing Cargo process and compiler descendants, with `finally` restoration and an independent 300-second resume watchdog. All seven paused processes received SIGCONT and were verified resumed or exited. The control record is `quiet-control.json`.
- Ordinary desktop background activity remained, including a Node development server using about one core; this is not an isolated server benchmark. Modest differences from the previous run should not be attributed solely to code changes. The 135× annotation improvement is much larger than this noise and agrees with the separate native profile.
- Native storage compares Hail .ht against Vail Vortex/Iceberg snapshots. Reads are warm-cache reads. Synthetic Matrix/VCF workloads have the same dimensions and actions as the earlier run, not realistic genomic datasets. See `../20261012-hail-vail/SUMMARY.md` for complete workload definitions and the runner for assertions.
- Arithmetic no longer extracts ScalarValue operands per input element. Some aggregate accumulators still do per-row ScalarValue extraction; this report does not claim all operators have been made columnar.

## Reproduction

Use `benchmarks/compare_hail.py` with `--warmups 3 --iterations 5 --cores 14`, each engine in its recorded Python environment, sequentially and without concurrent compiler workloads. Hail requires the recorded Java 11 installation and `SPARK_LOCAL_IP=127.0.0.1`. The exact local control driver is saved as `quiet_driver.py`; its worktree-specific pause targets should be adapted before reuse.
