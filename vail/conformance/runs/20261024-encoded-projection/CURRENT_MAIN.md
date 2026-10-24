# Current-main assembly gate

Clean automatic merge source `e2bf8fdc6ceb2c081aba4531538b7571a602a472`:
main `a47b8f170462c95d81c68f23ade08938acc769e1` plus accepted isolated
`d621389d2f50d75a4c2b0b5f68a6062a97acbda6`. No conflict resolutions or additional
production changes. Normal kache `maturin develop --release --locked`, jobs2,
native CPU flags, completed in2m29s. Installed native SHA256
`1193eb43b60995d3a8f00f875f348829a0557eee070899c91d8340b73d200c71`.
No compiled artifact was copied or saved as a cache.

Combined public Python gate:168PASS,1FAIL across14files, including current-main
call access, collection filters, composite entry totals, finite integer stddev,
segmented integer/Boolean reducers, shared list reducers, randomness, and all59
isolated pushdown regressions. Public Rust gate:9PASS.

The failure is
`test_composite_floating_sum_retains_existing_aggregation_results`. It fails
alone on both this assembly and the historical normally built H078 baseline,
with the exact same semantic-UDF hash and expected2rows/got1row error. Its input
is `range_matrix_table`; there is no nativeRead, so neither new computed-source
entrypoint can execute. This existing float/broadcast failure is retained and
reported, not removed or marked as a pass. Logs include the full gate and both
standalone differential reproductions.

## Throughput confirmation

Same500M-entry fixture, independent exact sum checked on every action,12warm
actions per engine, no concurrent build/tests/profile. Values are medians.

| Query | Combined assembly | Recorded accepted isolated | Combined CPU | Recorded isolated CPU |
|---|---:|---:|---:|---:|
|Certified eligible|62.287ms|61.595ms|0.737285s|0.739922s|
|Filtered ordinary fallback|77.788ms|77.559ms|0.920271s|0.912890s|

The recorded isolated artifact was normally overwritten by the combined build;
it was not copied aside. These are explicitly separate runs on the same laptop,
not a simultaneous exact-artifact A/B. Differences are~1.1%/0.3%wall; CPU is
comparable. The isolated acceptance report remains the source of the17%gain claim
relative to its historical0eec/H078 baseline.

For machine-state context, the stable H078 reference was run immediately beside
the combined measurements: eligible74.393ms/0.838472CPU s; filtered75.294ms/
0.898782CPU s. This reference predates current-main typed reducers and Boolean
forests; it is **not an exact a47 main baseline**. No such local artifact was
available, and no new baseline build was performed solely to repeat the scan.
All raw records carry their actual native SHA256.

The isolated proof/resource/startup/Pco coverage and tradeoffs in [FINAL.md](FINAL.md)
remain unchanged: this assembly modifies no Vortex/storage/capability production
code relative to that accepted candidate. The unsupported forced native-Pco
producer boundary is still explicit. Current-main merge requires the parent's
final review disposition of this combined gate and the existing baseline failure.
Raw logs, records and checksums are in [current-main](current-main/).
