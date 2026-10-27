# Final three-repetition comparison

The frozen native Vail build is **4.77× faster than Hail at the median**, with **34/110 cases at least10× faster**. Median speedup over old native Vail is1.087× across111cases. This measures native4761/production517c8753, **before** the newer pruning fix73201cc4c.

Hail=1.0 below; each entry is the median per-case normalized ratio, lower is better:

| Input | Cases | Hail wall | Old Vail wall | Frozen Vail wall | Hail observed CPU | Old Vail CPU | Frozen Vail CPU |
|---|---:|---:|---:|---:|---:|---:|---:|
| Native |110|1.000|0.272|0.210|1.000|0.235|0.201|
| Legacy |67|1.000|0.408|0.307|1.000|0.349|0.285|

Native wall time is slower in14/111 cases versus old Vail (all median slowdowns<6.4%), and6/110 versus Hail. Observed CPU is higher in26/111 versus old and11/110 versus Hail. All cases and all adverse observations remain visible, regardless of size; smaller differences are not labeled noise. The exportVCF native CPU regression is15.1% with all three repetitions higher; legacy exportVCF CPU is23.4% higher. These are descriptive observations, not significance tests.

- [Full report: all regressions and >=10× lists](THREE-REP-REPORT.md)
- [Every native case, Hail-normalized wall/CPU and variability](native-all-cases.csv)
- [Every legacy case, same metrics](legacy-all-cases.csv)
- [Every adverse metric with repetition directions](all-adverse-with-directions.json)
- [Histogram bins](native-speedup-histogram.csv) and [empiricalCDF](native-speedup-ecdf.csv),110native pairs
- [Complete statistics and input hashes](summary.json)

Three action repetitions completed.111native old/current pairs,110native Hail pairs,67legacy pairs; one intentionally lazy planning benchmark is separate. The isolated HailgVCF helper failure was attempted once then cached twice, not counted as three performance samples.85supplemental cases omitted.

All17,568input files passed canonical-set/fullSHA integrity checks, independently accepted at5c5675749; VMterminated. Old Python3.11 versus frozen Python3.12 is a whole-bundle comparison. Observed CPU is a10Hz lowerbound including observed descendants/JVM/telemetry, not complete hardware/core utilization. Summed sampledRSS is not PSS, allocatedbytes or initialization peak. Fixedcyclic order/uncontrolledOScache; allcross_engine_result_compared=false, execution-only tiers preserved. The existing count15 assertion validates only that count.

## Reproduce calculations only

From this directory, use the repository's frozen scan root as --root:

```sh
python3 analyze.py --root .. --repetitions 1,2,3 --out /private/tmp/vail-final-report-reproduction --final-sha-accepted --acceptance-record INTEGRITY-ACCEPTANCE.json
python3 render.py /private/tmp/vail-final-report-reproduction
```

No query, build, VM access or benchmark is performed by these scripts. DERIVED-SHA256.json authenticates generated outputs. PACKET-SHA256.json authenticates scripts, acceptance record and all derived files.
