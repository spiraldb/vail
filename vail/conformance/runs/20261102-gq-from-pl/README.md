# Single-pass genotype quality

Candidate source: `a38dfd48c`, based on `5dcf0b445`.

`gq_from_pl` now traverses borrowed PL offsets/primitive values once, tracking two
int32 minima initialized to 99 as Hail does. It allocates only the primitive
output column, not filtered, extended or sorted PL intermediates. Missing input
lists remain missing; missing elements are skipped, empty lists produce zero,
and subtraction wraps like the JVM. The existing public helper and
`split_multi_hts` call site use this kernel; no split-specific dispatch was added.

## Correctness

- Exact-source normal release wheel, built with maturin and kache.
- 62 public Python tests passed: GQ, genotype likelihoods/index calls, split HTS.
- 4 Rust tests passed: sliced/nullable GQ and existing variant operations.
- Same-wheel old-helper baseline passed all 11 new public GQ cases.
- Both fresh old normal artifact and final candidate passed all 12 full-value
  split oracle cases with hash
  `5b0b24d3e636fe2e2fc82106f5544b645c51a7d3ca7726ff2912e755449b5fee`.

## Local matched timings

These are laptop subset measurements, not the full Hail benchmark. Both versions
use the **same normally built native wheel**. The baseline package restores only
`genotype_likelihoods.py` to its exact `5dcf0b445` contents; its native kernel is
present but unused. This isolates the helper replacement, avoiding comparison
against an older native binary/toolchain. File hashes are in the receipt.

Each case used four fresh processes in B,C,C,B order, one second of warmup and
seven timed samples per process. Micro/control samples batch ten complete queries
and retain both batch and per-query timings; split samples execute one action.
Runs used nice 10, Rayon 4 and OMP/OpenBLAS 1. No agent builds/tests or other
benchmark workloads overlapped these timings. Every timed result was checked
against an independent numeric/count expectation. Full split values were checked
separately by the oracle above.

Numbers are medians within processes, then medians across the two processes per
version. CPU means process core-seconds. All raw samples and both paired ratios
are retained, including adverse control observations.

| Workload | Baseline wall s | Candidate wall s | C/B wall | Baseline CPU s | Candidate CPU s | C/B CPU |
|---|---:|---:|---:|---:|---:|---:|
| GQ, 30,000 lists of length 3 | .002997 | .001595 | .532 | .005777 | .002253 | .390 |
| GQ, 30,000 lists of length 128 | .025142 | .014061 | .559 | .088578 | .048738 | .550 |
| Split, 200 real rows × 2,535 samples → 204 rows | .268399 | .227899 | .849 | .540912 | .395596 | .731 |
| Split, 1,000 generated rows × 64 samples → 2,000 rows | .132385 | .117491 | .887 | .192528 | .153113 | .795 |
| Unrelated arithmetic aggregate | .001170 | .001165 | .996 | .001243 | .001244 | 1.000845 |

Both split pairs improved wall and CPU. The unrelated control's first pair was
1.38% slower wall / 1.48% higher CPU; its second pair was 2.12% faster wall /
1.25% lower CPU. Its aggregate CPU result is 0.084% adverse. These results do not
establish universal non-regression, a 2× split improvement, or superiority to
Hail. They do not implement the separate entry-expansion copy-elimination work.

`evidence.tar.gz` contains scripts, complete raw timings, tests/build logs,
full-value oracle results and hashes; `evidence.sha256` identifies the archive.
