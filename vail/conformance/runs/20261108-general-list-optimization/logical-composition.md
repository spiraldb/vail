# Logical list composition experiment

Status: development prototype; not admitted to main. The broad map/filter output-sink extension was rejected and removed. Existing flatmap/fold sink admission is unchanged.

The prototype exposes pure, single-parameter, canonical list maps and filters as DataFusion higher-order expressions before projection optimization. A late pass reconstructs typed native list regions, preserving lexical scopes, aliases, full value/null semantics and the existing native executor. Captures, effects, virtual accessors, shared nontrivial DAG bodies and unsupported layouts retain their prior path. This is not universal list fusion.

The first 96 fresh-process experiment measured logical composition; a second balanced 144-process experiment compared old lowering, logical composition and a default-off map/filter-only direct-output sink. Full independent Arrow outputs were checked before timing, fixture setup was excluded, all process groups were confirmed absent, and source/executable hashes were frozen. Each evaluation sample averaged repeated calls for at least 20ms; five samples per process, four processes per mode/case/size. CPU was not measured. These are generated projection pipelines, not Hail workloads.

| Pipeline | Lists | Logical evaluation / old | Sink evaluation / old | Logical prepare + first / old |
|---|---:|---:|---:|---:|
| maps | 1 | 0.734 | 0.657 | 1.342 |
| maps | 16 | 0.747 | 2.638 | 1.386 |
| maps | 4096 | 0.955 | 27.825 | 1.342 |
| filter_map | 1 | 0.954 | 0.514 | 1.538 |
| filter_map | 16 | 0.968 | 1.574 | 1.548 |
| filter_map | 4096 | 0.919 | 3.969 | 1.318 |
| identity | 1 | 0.747 | 0.830 | 1.221 |
| identity | 16 | 0.755 | 2.822 | 1.315 |
| identity | 4096 | 0.922 | 39.769 | 1.348 |
| single | 1 | 1.015 | 1.012 | 1.278 |
| single | 16 | 1.000 | 1.002 | 1.261 |
| single | 4096 | 1.000 | 1.000 | 1.262 |

Lower is better. All cases, including adverse controls, are retained. Logical composition improved repeated evaluation of some pipelines, but preparation and preparation-plus-first evaluation regressed materially. It remains on performance hold. The map/filter sink was much worse on larger inputs, including approximately 4x slower filter-map and 40x slower borrowed identity; it is unsuitable as the default for these shapes. No actual Hail or whole-workload speedup is claimed.

Correctness: the focused gate passed 129 Rust tests, with the experimental sink enabled separately for its 39 relevant tests. The broader Rust suite stopped on an untouched CSV error-message assertion; its raw failure is retained and the same assertion was subsequently reproduced on accepted `562` source; it was not fixed or bundled (the Boolean-fill evidence retains that classification). The whole-Call test constructs its expected physical value through the public cast API rather than assuming storage packing. No new format/name/plan assertions are used as semantic oracles.

The archive includes both cohorts, pins, full output-oracle results, controller code, process cleanup receipts, exact prototype sources and build/test logs. Its JSON companion records SHA256 and length for all 742 payloads. Artifact SHA256: `b421a907b65fe5afae8f00937133c035ef181e0124f7946f9a9e47611b554444`. The archived source includes the rejected sink switch so the measured implementation is recoverable; current development source removes that switch.
