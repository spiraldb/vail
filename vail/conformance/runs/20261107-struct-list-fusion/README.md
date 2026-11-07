# Struct/list output and integer fold fusion

Production source: `fcd760817e4898fe0455ffa35482022ccbec3f81`. Normal installed native SHA256: `7c1e347f74d6a3e8cc353f468961d607541973dd84255ec3b02bbb039fbf2694`. The prior full Hail/Vail benchmark is in `../20261106-full-benchmark`; these are NEW local kernel measurements, not that full benchmark rerun or an end-to-end Hail comparison.

The existing typed block list program now writes Struct columns, nested Structs, borrowed strings and primitive child lists through Arrow StructBuilder/ListBuilder trees. Field layout and fanout writers are prepared once. Maps and filters compose through constructed fields; emitted payload goes into final child buffers without constructing intermediate Struct/List arrays. Missing parents, defined all-missing children, empty Structs, sliced inputs and hidden null-owner spans retain their semantics. Exact parent/child data types and checked List32/Utf8 output limits remain enforced.

An additional sink consumes admitted integer add/product folds directly. It preserves exact binder roles, native width/wrapping, literal seed once per original row, strict nulls, empty/missing distinction and state across blocks. Arbitrary/nonassociative/float/capture/effectful folds use the ordinary executor. Unknown/erroring/effectful producer fields cannot be discarded: the ORIGINAL expression DAG is certified before projection. These are typed generic expression rules; no workload or field-name matchers.

## Local measurements

64 lists ×1024 generated Int32 elements (65536 values), with two output Struct fields. Nullable variants include missing elements and parents. Same release executable, enabled versus **ALL list fusion disabled**, not an isolated incremental comparison solely attributable to this commit. Fold results include both fused producer and reduction sink.

Each case uses8 independent processes in off/on/on/off/off/on/on/off order,3 warmups and7 measurements per process. Complete independent Arrow output oracle checks run outside every evaluation timer. Preparation is recorded separately. All inherited VAIL_* runtime flags are removed except deliberate disabling; thread variables are fixed at2. No benchmark overlaps a build/test/profile. Fixtures, preparation, validation and process startup are excluded. Metric is **evaluation wall time only**; CPU/core-seconds are unmeasured. Values below are medians of process medians; the tiny cohorts do not establish broad query speedups.

| Case | Disabled ms | Enabled ms | Speedup | Enabled wall change |
|---|---:|---:|---:|---:|
| maps | 0.0425 | 0.0442 | 0.961× | +4.02% |
| filters | 0.1160 | 0.1153 | 1.007× | -0.65% |
| maps-flatmap | 2.9191 | 1.0861 | 2.688× | -62.79% |
| filters-flatmap | 1.4594 | 0.6577 | 2.219× | -54.94% |
| flatmaps | 3.0303 | 0.6494 | 4.666× | -78.57% |
| fold | 2.5285 | 0.3374 | 7.494× | -86.66% |
| maps-flatmap-null | 3.2796 | 2.0595 | 1.592× | -37.20% |
| filters-flatmap-null | 1.4962 | 0.8979 | 1.666× | -39.99% |
| flatmaps-null | 3.7101 | 1.3275 | 2.795× | -64.22% |
| fold-null | 2.8355 | 0.7466 | 3.798× | -73.67% |
| simple | 0.0062 | 0.0063 | 0.980× | +2.02% |

Map/filter-only chains retain the existing whole-column backend. Earlier prototypes replacing those kernels regressed cheap maps by16× and filters by2.6×; both raw prototype cohorts remain included. All final adverse controls remain in this table/results, with no noise waiver. No blanket throughput improvement is claimed for cheap maps or every Vail query.

## Correctness and allocations

The exact normal build passes71 Rust library tests,34 public list/Struct/fold tests and519 Python tests plus10 subtests. The same named pre-existing corr failure is deselected, with independently established same-environment baseline evidence included; no new exclusions. Fresh wheel build/import/source pins and exact commands/results are retained.

Calling-thread evaluation allocation volumes INCLUDE final output, exclude fixture construction and preparation. With equal empty final output, growing inputs4096→1048576 keeps Struct allocations724178→724178 bytes and fold697984→697984 bytes. The disabled counterfactual preserves those logical outputs but grows166802→39073682 bytes (Struct) and92196→21504036 bytes (fold); its two expected failures are ONLY the allocation-growth assertions. These are allocation volumes, not RSS, zero-copy, zero-allocation or input-owner-release claims.

Prepared writers bind types/fields once and execute typed kernels per block. Primitive registers, emission tuples, per-leaf masks and per-depth validity masks share the conservative4MiB heap BLOCK-payload budget. Fixed stack reference/nullable-copy masks, collection headers, per-row metadata, inputs/retained owners and final output are separate. Scratch holds no input/output owners. Final output can exceed that scratch bound within Arrow offset limits. Nullable borrowed child validity scratch is initialized once per block and reused across spans.

## Profile and scope

The one-second normal-user sample succeeded; its full raw call tree is retained in profile-sample.txt. The665 main-thread stack observations run through ListFlatMap.evaluate, execute_pointwise, typed compute and final OutputBlock.append/Arrow null-buffer writes. No Compiler/project/lowering/field-name setup appears in that sampled steady-state call graph. This is descriptive sampling outside timing, not an allocation total or proof of zero setup cost.

Supported output shapes currently include primitives, recursive Struct fields, strings and borrowed List-of-primitive fields. Other nested collection layouts and opaque UDFs retain existing execution; this is not universal list/UDF fusion. Borrowed selected-span checks are resource evidence, not a requirement for any arbitrary logical-null representation. Preparation timing is retained; moving effect certification after cheap rejection does not claim to solve pre-existing repeated shape/type analysis.

All work and measurements ran on the laptop. Both cloud VMs stayed off. No GitHub PR or push was performed. Main integration requires independent reviewer acceptance of the exact source and this evidence.
