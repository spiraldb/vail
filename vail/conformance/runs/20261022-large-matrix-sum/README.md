# Large range MatrixTable column sum investigation

The exact upstream query creates 10,000 rows and 500,000 columns in 2,500
partitions, annotates each entry with row_idx + col_idx, computes each column's
SUM, and forces column count. This evaluates five billion entry positions.

The initial 1 kHz local diagnostic capture used normally built audit native
SHA256 59c30b4f8ae085289647ce7990c9f247dcb4f5c2c5590733f7bb839c9de25632
and main ae88922d MatrixTable Python. The native differs in unrelated BGEN/text
reader changes from ae889; this is not a complete exact-ae889 artifact claim.
This contended laptop diagnostic is not compared to cloud Hail timings.
Query observations: 10.912 s wall, 106.609 process CPU seconds, 4,773,904,384
bytes process high-water RSS (includes construction/profile startup).

126,088 sampled stacks, dominant leaf sample attribution:
- memmove 15.93%
- DataFusion SUM grouped validity accumulator 15.84%
- Arrow take bitmap construction 13.42%
- Boolean equality under CASE 11.74%
- Arrow take Int32 values 9.92%
- aligned-map run-end decoding of row captures 8.78%
- native Int32 addition 4.93%
- Int32-to-Int64 cast 4.67%

The CASE comparison/take/scatter is entry-presence masking, not inherent
addition. Dense batches can evaluate the existing prepared body directly when
entry-parent null_count is zero. Sparse batches must retain lazy guarded
execution, including avoiding errors from absent-entry hidden payloads.

A Python-only direct-column-SUM guard removal alone yielded 10.138 s wall /
105.781 CPU seconds in another diagnostic run: no meaningful CPU improvement
established. It is not sufficient to fix the major aligned annotation CASE.

Candidate validation and post-fix capture pending normal release build.

## First candidate diagnostic

Production bfef2f66, test fixture correction 8c546116. Normally built with
Rust 1.98 release, unchanged Cargo kache configuration and a worktree-local
target. 21 public Python tests pass (strict entry inputs, entry sum, aligned maps,
coordinate aggregation). Promoted-type public Rust lower/evaluate test pending.

The exact-query post-fix profile overlaps another compiler/test build. It has
53,872 sampled stacks; the previous Boolean equality leaf under CASE has no
samples. This substantiates removal of that path but not throughput improvement.
Observed 14.279 s wall /110.480 CPU seconds /5,315,444,736 high-water RSS are
contended diagnostics, not a speedup claim. Remaining leaf costs include memcpy
18.55%, grouped SUM null-state18.32%, take validity16.37%, take Int32 values11.42%,
run-end row expansion9.27%, addition5.49%, Int32→Int64 cast5.20%.

Nearly all memmove samples are under ScalarValue::list_to_array_of_size:
ExternalGlobal column records are cross-joined into each source row upstream,
then the aligned physical expression borrows only the first repeated list.
Therefore this patch does not remove the major column/global metadata broadcast.
The next general design requires keeping singleton global input separate from
row batches, binding its scalar buffers once per action rather than expanding
it through a cross join. No benchmark-specific sum constant folding is proposed.

## Native scalar-context path and final validation

Projection-only external captures now use DataFusion55's native uncorrelated
ScalarSubqueryExpr/ScalarSubqueryExec instead of an explicit cross join. Existing
scan/RNG indexed-input paths remain unchanged. Public tests exercise nested
scalar collections, null parent records, empty arrays, separate same-schema
captures, column permutations and repeated actions.

Semantic kernels retain one prepared program/schema per most recent immutable
scalar-binding combination. This is a bounded execution cache, not expression
identity: retained Arrow owners prevent address recycling, and new owners or
scalar values reprepare. Tests invoke the same public UDF with changed/empty and
restored captures and concurrent row batches, checking outputs only. The cache
is bounded by one combination, not by a fixed byte limit.

The first scalar-subquery diagnostic regressed to53.06wall/572.26CPU because
DataFusion ScalarSubqueryExpr::Display formats the entire resolved nested value.
Projection metric creation called that Display per partition. A scoped physical
projection label decorator now leaves native evaluation intact while avoiding
resolved payloads in metric labels. Its expression-DAG detection is memoized.

A further scalar Struct GetField fast path prevents our field extractor from
re-expanding singleton nested values after scalar binding. Parent nullness and
physical descriptors remain preserved; array-valued parent handling is unchanged.

Final production ac676b82, test-only76b9e893, current-main integration base0976b1bc.
Normal Rust1.98 release/kache native SHA256:
c45023e1475d248016795eea3f938111520a9415e582a53721d8fcac6c8c90d1.
60public Python tests passed;2public Rust lower/evaluate or UDF runtime tests
passed (promoted dense/sparse output types, changing scalar bindings/concurrency).
The final actual5Bentry profile observed10.724wall/83.202CPU seconds and process
high-water RSS6,074,793,984bytes. Other laptop compilation was active; these are
diagnostic observations rather than controlled wall-time/Hail comparisons.
The initial diagnostic had10.912wall/106.609CPU seconds. Process RSS high-water
includes setup and uncontrolled scheduling, so no memory improvement is claimed.

Final47,530sample attribution:
- native grouped SUM validity accumulation21.22%
- Arrow take bitmap construction17.76%
- take Int32 values12.08%
- memmove10.61%
- run-end row metadata decoding10.28%
- native addition6.25%, Int32→Int64 cast6.00%
- Boolean CASE equality has no sampled leaf
- all formatting inclusive0.048%; lower_uncached3samples(0.006%)
- bind_scalar_captures has no samples

Thus the introduced formatting/repeated-preparation work and redundant dense
presence CASE disappear from the hot path. Remaining column-field repetition,
row-capture decoding and native grouped SUM are still material costs; this is
not a claim that the benchmark is now faster than Hail on the cloud host.
