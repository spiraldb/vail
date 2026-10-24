# H078: honor native projection without demanding unused ordering keys

Source candidate `4ee74ae39911c6ece18795dda971c81297366663`, refreshed cleanly onto reviewed main `e688a4a4` as assembly `2da14077ca1c03ca0ec09e3bf36de1c03e99144a`.

The native table provider previously appended **all declared keys** to every requested projection, even when neither the consumer nor residual filter needed them. This converted available source ordering into a demand for unused key I/O/materialization. Remove that append loop. Keep filter-referenced fields, the private source-partition tag and requested output order exactly as before.

## DataFusion contract and properties

Pinned DataFusion 55 `TableProvider::scan` documents that projection selects returned columns, filters precede projection, and filter-only fields may need internal retention. Our residual field loop stays unchanged. There is no requirement to read unused columns merely to advertise optional ordering.

No new operator or special query matcher. Existing `SourceExec` already advertises full source key ordering only when all key roots survive the projected schema intact. Existing indexed `ReadKeyFilteredExec` only adds key ordering when it can construct it against the actual projected schema. `PartitionSetExec` projects/reconciles child equivalence properties and retains source-tag partitioning. Missing ordering remains absent; consumers with ordering requirements still request their keys and DataFusion enforces those requirements. This patch does not invent a new ordering certificate or change row order.

Unchanged paths: legacy/text providers; interval pruning and residual predicates; native source field-ID mapping; nested pruning conservatism for retained keys; tag placement/cardinality; limit handling. Composite key prefix-order improvements remain a separate possible enhancement.

## Validation

**207 public tests passed**, no failures/skips. Scope includes new narrow native aggregation/zero-column cardinality/filter/head/scan/alias/join/partition-selection cases plus existing count projection, broadcast, joins, interval reads/joins, partition operations, matrix layout, entry projection, complete RNG and computed-key/matrix-reader/distinct ordering suites. The four new cases and 16 existing ordering cases also pass on the exact main baseline. Tests use public table actions and independent values, not private plan assertions or mocked column-read counters.

## Resource evidence

Native stored fixture: one million rows, key is a struct containing unique row ID plus 32 random Int32 values; separate narrow integer payload. Total native snapshot size 124,601,283 bytes, hashes in `fixture-files.json`. This deliberately makes unused key materialization visible; it is **not** a representative key-size assumption for all matrices.

Query: ordinary global SUM over the payload. One warmup and four measured evaluations per fresh process, sequential quiet slots (other agent's build safely paused for baseline; candidate after all builds/tests):

| | Main baseline | Candidate |
|---|---:|---:|
| Median wall | 21.05 ms | 12.65 ms |
| Median CPU | 74.25 ms | 43.74 ms |
| Peak RSS | 306 MB | 314 MB |
| Result | 5,999,994 | 5,999,994 |

About **1.66× faster wall / 41% less CPU**. Peak RSS increased ~3%; no memory reduction claimed. OS cache warm; fresh-process startup excluded. No Hail comparison or extrapolation to entry-heavy matrices with small row keys.

Separate three-second process samples retain baseline recursive Arrow Struct/List materialization beneath Vortex conversion, while candidate conversion is narrower. Samples include startup and unequal completed iteration counts; no disjoint percentage/counter attribution is claimed. Actual disk/DRAM bytes and allocation counts were not measured; file size is not read-byte telemetry. Source field-ID pruning, profile removal of nested unused-key conversion, correctness and CPU receipts provide the resource evidence. This change adds no hot loop, kernel or SIMD work: it lets the existing reader skip a column.

## Normal build/provenance

Dedicated `/private/tmp/vail-native-key-projection-env`, normal kache/maturin release, jobs=2, incremental=0, target-cpu=native. Existing clean managed integration checkout was switched to a new development branch after its previous main gate was accepted, preserving source paths and avoiding redundant dependency rebuilds. No native artifact was copied, saved or manually reused as a cache workaround. Current native SHA256: `69a06f57d61746e815cd843bb58a08efb912d214bb3c6da650d7c62e05d825bf`.

Exact baseline normal main native `c06fe286...` was measured before this checkout's normal candidate reinstall; `baseline-provenance.json` records its identity. Source/native hashes, reproduction script, build log, test logs, JSON iterations and compressed samples are retained. No production changes beyond the reviewed provider projection loop removal.
