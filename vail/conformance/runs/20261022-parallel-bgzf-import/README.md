# Parallel BGZF and typed integer import

Candidate source: `a84e61950cd8dfadd8a695fe87f155f1079cb9a1`, incorporating
main `f64e5f5605be94f5a70de0379f098fcff5b05988`. Final validation below uses a
normal release build with `RUSTC_WRAPPER=kache`, `maturin develop --release
--locked`, and this checkout's default Cargo target. No native library is
manually saved, copied, or linked.

## Behavior

Local BGZF sources prepare disjoint line-aligned virtual ranges and execute
independent blocking scan workers. TBI chunk/linear anchors are optional hints,
validated against the file; sparse/invalid/stale hints fall back to bounded
forward BGZF-header discovery. This follows Hail's forward resynchronization,
rather than searching upstream. CSI hints are currently ignored. Ordinary gzip
retains its sequential concatenated decoder, and `file_per_partition=True`
preserves one source partition per file. Source partition defaults account for
compressed size and available execution concurrency.

Workers reuse file handles, compressed/decoded buffers, inflater, and checksum
state; Arrow builders and type dispatch are prepared per batch. The typed
parser covers scalar int32/int64 and JSON arrays of those types. Its inner
loops scan borrowed input and append primitive values/nulls directly to Arrow.
Other import types retain their existing parser. No per-row or per-entry
configuration, compilation, or parser construction is introduced.

Input identity is checked around preparation and scanning. Growth, rewrite,
or replacement after preparation errors rather than silently omitting data.
This is detection, not an atomic snapshot guarantee for concurrent writers.
CRC, size, and full DEFLATE consumption are checked for BGZF blocks.

## Workload and retention gate

Unchanged upstream `test_table_import_ints`, full existing
`many_ints_table.tsv.bgz` fixture (714,710,623 compressed bytes), local machine.
Python imports and fixture preparation are outside query timing. Paired
comparison changes only the two integer-expression constructors, loading the
generic versions from baseline `64924362055e69d3a45c51fd98479942aa3b5e1e`.
Both modes use the same native artifact, scanner, and input; ordering alternates.
Acceptance requires at least 20% median CPU reduction and a material wall-time
reduction. This is a measured workload gate, not a claim about all formats.

Pre-integration artifact `50cc99622e8e02d6cece80364965bb8e6375b2805e669ff29cd5bfb2eab9f5c6`
used 14 actual source partitions. Three paired runs gave generic median
12.162s wall / 74.291s CPU and typed 2.763s wall / 19.689s CPU: 77.3% less
wall and 73.5% less CPU. The parser agent independently checked all six raw
runs and that the unchanged force-count action drains the projections.
Disposition: **keep the typed parser**; matching integrated checks below agree.

Single-pair serial diagnostic using public `file_per_partition=True` gave
generic 69.639s wall / 61.322s CPU and typed 18.481s wall / 18.229s CPU.
This isolates a source-concurrency control; it is not the parser retention gate.

The earlier local Hail 0.2.139 reference ran the same full upstream benchmark
in Spark `local[14]`, Java 11, 16 GiB heap: 19.597s wall / 85.981s CPU.
This is one fresh-JVM query measurement, not a repeated paired engine study;
setup was excluded. Raw report is `hail-local-reference.json`.

## Correctness and review

Before integration: 151 public import tests passed. Coverage includes indexed
and unindexed splits, sparse/invalid/stale indexes, CR/LF/CRLF, split UTF-8,
long lines, empty members, corruption, repeated actions, changed files,
ordinary gzip, explicit source partition behavior, integer bounds, and errors.
A real `bgzip`/`tabix` generated 100,000-row fixture matched exactly with and
without its TBI (seven requested source partitions).

Required reviewer thread: `01a14128-21b8-7a53-b399-4ad8f9b31f4f`.
Source/test clearance received for d19259b9 and integrated exact a84e6195;
no source/hot-loop blocker. Final merge approval remains pending at this draft.

## Integrated-artifact validation

Native SHA256:
`4519816e6a4ac3e2cd662ed5410d8cdad387f5bac29eec9d6df4f2de5af05af7`.
Normal incremental release build finished in 2m16s. All 176 targeted public
tests passed (151 import cases plus relevant tests from the integrated main).
Real tabix validation again matched all 100,000 rows with and without index,
seven partitions each. Source and native artifact stayed fixed throughout.

Three alternating parser pairs again used 14 raw source partitions:

| Parser | Median wall seconds | Median CPU seconds |
| --- | ---: | ---: |
| Generic baseline constructors | 6.971026 | 73.216605 |
| Typed direct-to-Arrow | 1.934043 | 20.276682 |

Retention gate passes: **72.26% wall and 72.31% CPU reduction**. Raw six-run
report: `parser-integrated.json`. Wall times across build stages reflect
different machine load; only within-report paired comparisons are the gate.
The typed median uses about 10.5 CPU cores on average, rather than one.

A separate 500 Hz samply capture of the unchanged full workload recorded
2.040s query wall / 20.279s CPU. `profile-query.json` records the measured
epoch interval; `iterations.json` feeds the existing
`render_annotate_profile.stacks` CPU-delta-weighted analysis. Raw profile and
symbol sidecar are included. Analysis captured 20.278 CPU seconds within the
query interval, overwhelmingly in worker threads. Inclusive samples include
22.25% typed integer traversal, 30.20% prepared CSV append, 28.67% BGZF scan,
and 10.35% BGZF block read. These overlap and must not be added. No sampled
ReaderBuilder or Regex::new frame remains; serde_json totals only 0.0019s
inclusive across measured stacks. Source review confirms preparation is
outside hot loops. Sampling assigns CPU deltas to sampled stacks, so parked
frames can receive weight; these are approximate attribution, not lock timing.

Reproduction (from repository root, with the normally installed editable
package on PYTHONPATH):

```sh
python vail/benchmarks/compare_integer_import.py --baseline-revision 64924362055e69d3a45c51fd98479942aa3b5e1e --input "$fixture" --output parser-integrated.json --pairs 3
samply record --save-only --unstable-presymbolicate --rate 500 --output profile.json.gz python vail/benchmarks/profile_table_import_ints.py --input "$fixture" --output profile-query.json --iterations 1
```

Tests: `test_bgzf_lines`, `test_text_import_export`, `test_csv_import`,
`test_import_lines_compression`, `test_import_support`,
`test_import_prerequisites`, `test_matrix_import`,
`test_genotype_likelihoods`, `test_native_branch_substitutions`, and
`test_matrix_strict_entry_inputs` under `vail/python/tests`.
