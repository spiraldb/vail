# Matrix entry-sum and filter benchmarks

The plain numeric `aggregate_entries(agg.sum(...))` route now maps each row's
existing cell list directly to a scalar list. It masks absent cells as missing,
casts each value to the aggregate result type, reduces with the Arrow typed sum
kernel, then sums row subtotals with the native table aggregate. This avoids
unnesting entries and resolving each column record for this common expression.
Expressions with column-field references retain the positional reducer path;
other aggregate shapes retain the generic table path.

## Matched 100K x 10 comparison

Both Vail paths used the same optimized release wheel and native extension.
The generic baseline was a temporary `sitecustomize.py` override of
`MatrixTable._aggregate` to invoke the prior `_context`/`_lower` table route.
The optimized path used the public API. Each unprofiled run used three warmups
and five measured iterations. The instrumented runs used the same counts, 10 Hz
process telemetry, and 500 Hz Samply sampling.

| Path | Unprofiled median | Profiled median | Median CPU | Instructions | Cycles |
| --- | ---: | ---: | ---: | ---: | ---: |
| Generic table route | 0.245952 s | 0.239336 s | 0.236726 s | 4.620 B | 0.887 B |
| Direct row-list sum | 0.150245 s | 0.155123 s | 0.153461 s | 3.025 B | 0.574 B |

The direct path reduced unprofiled time by 38.9%, instructions by 34.5%, and
cycles by 35.3%. Median RSS was 456 MB generic and 407 MB direct. The sampled
maximum physical footprint varied (153 MB generic, 237 MB direct), so it does
not support a memory improvement claim.

The generic profile attributed 61.2% inclusive CPU to DataFusion
`Projector::project_batch` and 30.9% to the Vail physical expression evaluator.
After the direct path, `Projector::project_batch` accounted for 66.9% of the
smaller total. Leaf samples shifted from Arrow array copying (`MutableArrayData::try_extend`
10.9%, `memmove` 8.5%) to 13.0% and 9.5%, respectively. This indicates that
projection and Arrow array construction remain the main costs; their larger
share after the change is a share of less total CPU time.

## Additional shapes and filter workload

| Workload | Generic Vail | Direct Vail | Hail |
| --- | ---: | ---: | ---: |
| Entry sum, 25K x 100 | 1.606989 s | 0.574181 s | 0.144609 s |
| Entry sum, 500K x 10 | 0.945871 s | 0.591761 s | not run |
| Row/column filters, 100K x 10 | — | 0.068441 s | 0.137089 s |

Those runs used three warmups and five iterations. Filter Vail is about twice
as fast as Hail in this run. At 100K x 10, entry sum is substantially faster
than its generic Vail route but remains about 36% slower than the recorded Hail
median of 0.1108 s. At 25K x 100, Hail is much faster; the direct Vail path
still improves substantially over the generic route, but does not close that
gap.

## Reproduction artifacts

- Release wheel SHA-256: `2182415a32c40a9db2efb8140356afb08950d86ddc9e6ad0617b03bba33b9522`
- Embedded native extension SHA-256: `aa0403afd9460a4a75019f559753a37aeafb3baa7338f2dd1d5bf643f5aeda23`
- Python: 3.11.13; NumPy 2.4.6; PyArrow 26; 14 source partitions
- Matched timing records: `generic-unprofiled.json`, `direct-unprofiled.json`,
  `generic-profile/worker.json`, `direct-profile/worker.json`
- Native profiles and symbolized SVGs: `generic-profile/profile.json.gz`,
  `direct-profile/profile.json.gz`, `generic-profile/flamegraph.svg`,
  `direct-profile/flamegraph.svg`
- Telemetry: each profile folder's `metrics.csv` and `iterations.json`
- Scaled timing records: `generic-25k-100.json`, `direct-25k-100.json`,
  `hail-25k-100.json`, `generic-500k-10.json`, `direct-500k-10.json`
- Filter timing records: `filter-vail.json`, `filter-hail.json`

The SVGs were generated successfully. The optional PNG renderer is unavailable
in the benchmark environment because `matplotlib` is not installed.
