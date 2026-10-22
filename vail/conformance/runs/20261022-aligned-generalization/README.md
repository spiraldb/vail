# Broader aligned-map routing benchmark

This records the initial broader experiment. The subsequent narrow production
implementation and integration evidence are in [TARGETED.md](TARGETED.md).

Recommendation: do not replace the general array-lambda path wholesale. The
strong measured candidate is `filter_entries` with global captures. Row-only,
column-only and entry-only cases do not justify a general switch in this study.
No production routing or main branch was changed.

## What already uses which path

At base `138f830b33cb61816bd5b7c9ce4b4b03126cc8df`, entry-axis projections
through `_project_axis` use `AlignedMatrixMapExpr` when deterministic expressions
capture a row, column, global or column count. This includes `annotate_entries`,
`select_entries`, `transmute_entries` and applicable entry projections reached
through other helpers. Lookups, aggregates, column-position access and random
expressions remain excluded.

Entry-only projections and `filter_entries` use the general DataFusion
`ArrayMap`/`array_transform` route. Its Arrow55 implementation already flattens
list values and evaluates the lambda once across them. This experiment is NOT
a comparison of per-row evaluation against batch evaluation. Both paths batch.
The aligned implementation creates run-end captures, explicitly decodes them
once per captured binding and passes dense arrays to ordinary kernels; it is
not compressed execution end to end.

The prototype patch broadens deterministic entry-only projection eligibility
and routes eligible entry filters through the existing aligned expression.
This is isolated Python routing only, without monkeypatches, native changes,
private probes or counters. All alternatives import the SAME native artifact.
The broad patch is an experiment, not a proposal to ship every broadened case.

## Artifact and protocol

Native SHA256:
`68254f5ee21c520edc7e3be33a2d74b496c7a41c477d2408106a7262bda8e12b`.
Source is the previously validated main integration artifact (executable source
ac709e13, same executable tree as main138f830b). Each process records its
Python routing module SHA256 too; see summary.json and raw JSONL.

Four deterministic Vail-native fixture configurations were prepared once with
the baseline package, then read by both alternatives. They have numeric entry
values, filtered entry slots, 16-field nested row metadata, scalar global shift,
column coefficient and optional wide column strings. Materializing inputs
prevents the proposed routing from also changing the setup operations timed
inside the query. Exact Python formula oracles independently check every count,
sum and requested string length; merely constructing/counting a matrix cannot
satisfy these oracles. Nested-map outputs are summed, requiring their values.

Initial sweep: 14 matched workload/shape pairs, one warmup and three timed
actions in a fresh process per alternative, serial execution, alternating
variant order by case. Follow-up: three additional fresh processes per variant
for each of global-only, row-only and combined captured filters at4096×128.
Thus 46 fresh measured processes /184 actions including warmups; all results
passed exact oracles. Timers include aggregate action planning/execution and
Python result decoding, but exclude initial fixture read and matrix expression
construction (recorded separately). Peak RSS is cumulative process high-water
memory including setup/warmup. It is not allocation attributed to one kernel.
Host was shared; small sub-millisecond differences are not robust regressions.
No concurrent builds or test jobs were launched by this task during timings.

## Main results

For4096×128 (524288 entry positions before filtered slots):

| Operation | Existing route | Aligned route | Existing / aligned | Peak RSS existing / aligned |
|---|---:|---:|---:|---:|
| Filter with global capture |164.56ms|18.45ms|8.92×|4343 /747MiB|
| Filter with row + global capture |170.58ms|23.04ms|7.40×|4372 /956MiB|
| Filter with row capture only |20.98ms|21.02ms|1.00×|845 /869MiB|
| Filter with entry only |19.14ms|20.00ms|0.96×|915 /936MiB|
| Select entry-only arithmetic |33.64ms|34.03ms|0.99×|1835 /1848MiB|
| Select entry-derived strings |42.09ms|44.31ms|0.95×|1863 /1877MiB|
| Already-aligned row/global projection control |38.12ms|38.18ms|1.00×|1963 /1947MiB|

Captured filter rows above use the median of9 measured actions across3 fresh
processes per variant; other rows use3 measured actions from1 process. Raw
per-process medians, CPU times, native/routing hashes and exact results are
retained in summary.json and JSONL rather than only the aggregate ratios.

Smaller512×32 tests were tied/slightly slower for entry maps, nested maps,
entry-only/missing-negated filters and column filters; the combined captured
filter improved only1.17×. Tall16384×8 transmutation was7.79/8.16ms. Wide unused
column annotations128×16 with4096-byte strings did not establish a broader
win: annotation8.38/9.43ms, column filter8.53/8.38ms. See summary.json for all
cases; these small differences do not motivate dispatch thresholds.

## Interpretation and limits

The follow-up separates global capture from row capture: the large benefit
tracks globals, not all captures. Source inspection gives a likely explanation:
`_lower_cell` binds globals through `index_globals`, while the general lambda
projects enclosing input columns and `array_transform` expands captured arrays
with `take_arrays`. That can repeat an enclosing global struct containing
unreferenced matrix column metadata. `_aligned_matrix_map` explicitly builds
`global_values` only from requested global field names, avoiding that enclosing
capture. This attribution is inference from source and ablation, not a sampled
profile of these runs.

The experiment does not isolate the encoding format from capture pruning,
validity handling or specialized physical lowering. It does NOT establish that
building a RunArray and decoding it beats directly gathering the same narrowly
projected fields. Improving field-demand pruning in the general lambda path is
another promising way to remove the same waste, potentially beyond matrices.

The evidence supports a narrow global-capturing entry-filter fast path, followed
by production review and appropriate regression coverage. It does not support
broadly switching entry-only maps, nested array transforms, column filters,
aggregates, random/lookup expressions, or all ordinary array.map operations.
No Hail comparison or cohort-scale claim is made here. Large RSS remains even
on the aligned path, so throughput alone is not sufficient evidence.

The broad isolated prototype passed the same173 public tests (23.24s), including
scan, metadata, optimizer, VDS golden/functions, aligned maps and matrix layout.
This is useful regression evidence, not exhaustive validation of broader routing.
See prototype-tests.log. Production implementation and main are unchanged.

## Reproduce

From the recorded source revision and matching release native artifact:

```sh
python prepare_packages.py --native /absolute/path/_native.abi3.so --package-root /tmp/vail-general-map-packages
VAIL_GENERAL_PACKAGES=/tmp/vail-general-map-packages python run.py
VAIL_GENERAL_PACKAGES=/tmp/vail-general-map-packages python repeat_captures.py
python summarize.py
```

Use the documented Python environment or update run.py's interpreter path.
Fixtures are stored at `/tmp/vail-aligned-generalization-ROWS-COLS-PAYLOAD.mt`;
remove or use a fresh location when deliberately changing their contents.
