# BGEN bulk Arrow decoding

Production `a5c50c825230e7d8907d2b6f73f8592c3d748aad`, based on current main
`61c1a3b2ea7bbeb56365a70e60a1e99f911829c0` via clean merge `86a5b2b3`.
Test-only child `a54076da` adds the zero-sample oracle. Required review chat
`01a14128-21b8-7a53-b399-4ad8f9b31f4f` cleared source/tests through `a5c50c82`;
final artifact/evidence approval was requested separately.

## Problem and change

The actual upstream `test_import_bgen_force_count_all` workload spent 85% of
sampled process CPU in entry construction. Leaf samples attributed 25.2% to
`NullBufferBuilder::append`, 8.1% to `BooleanBuilder::append_value`, and 5.4% to
null-buffer materialization. This was updating Arrow builder validity per field
and per sample, including invariant sample IDs, ploidy and phase.

The reader now writes typed GP/dosage/allele buffers, builds validity with Arrow's
word-sized `BooleanBuffer::collect_bool`, and shares immutable IDs, GP offsets,
ploidy and phase across source variants. Field bindings and decompression
contexts/buffers are prepared once per scan. Existing Arrow concatenation packs
small multi-variant batches; one-variant batches keep the child arrays directly.
The pinned Arrow bitmap collector visits indices once in ascending order; public
oracles cover 0, 5, 64 and 65 samples.

Fully missing entry batches reuse a scan-owned all-null entry array. This is a
general parent-validity optimization: child values under an absent entry are
unobservable. It is significant on this actual fixture, whose first five
variants are entirely missing. Partially missing and defined entries retain the
normal decoding path.

As explicitly requested by the user, redundant per-sample semantic probability
sum/ploidy validation is removed. Global format/header/dimension and physical
length checks necessary to decode safely remain. Signed/float arithmetic avoids
underflow for semantically malformed probabilities. Public tests independently
check trusted decoded values and rejection of structurally inconsistent blocks.

## Build and correctness

Normal worktree-local Rust 1.98 release `maturin develop --release --locked`,
with Cargo's configured kache wrapper and isolated `vail/target`. No manually
copied/reused native library was used for candidate validation.

Final native SHA256:
`33602cbc126d1a9b07a32dd38c0b6604938f4ab797c63f4ef073530716e9c853`.

The integrated affected suite passes (BGEN, strict entry inputs, native branch
substitutions, variant QC, BGZF lines). BGEN covers zlib/zstd, compressed/raw,
GP/GT/dosage, ties, missing entries, empty structs, multiple batches/partitions,
whole/partial bitmap words, zero samples and indexed variant selection.
`tests-accepted.log` is the final aggregate result.

`check_fixture.py` also independently decompresses the first populated actual
fixture variant and compares native GP/dosage sums, call counts/classification,
and physical sample order against the byte-level oracle. Variant
22:16050688 contains 906 present entries among 50,000; every value agrees.
`fixture-check-final.json` records the summaries, not the input data.

## Actual workload profiles

The official cached benchmark input was downloaded directly from
`https://storage.googleapis.com/hail-common/benchmark/sim_ukb.bgen`, avoiding the
active cloud VM. It contains 249,115 variants and 50,000 samples. SHA256:
`12a22f950950d3a7b11465445a11993c679721a2bd7d8c7b4759380ee0aba9a9`.
The public query imports all GT/GP/dosage fields with eight source partitions and
forces all rows, just as the upstream benchmark does.

| Local 1 kHz diagnostic capture | Wall seconds | Process CPU seconds |
|---|---:|---:|
| Prior normally built native artifact | 118.290 | 572.921 |
| Final bulk/bitmap/all-missing candidate | 6.622 | 43.548 |

These are single CPU-weighted profiles collected while other laptop work could
compete, **not controlled timing repetitions or a Hail comparison**. Their
purpose is attributing work and verifying removal of the offending hot path.
The baseline native artifact hash is
`8f6f07458ad85a4a53e10645411d6f9707eb836f02c9fa9c6111e00000faa1a0`,
the existing arithmetic reference installation; no exact source revision is
asserted for that pre-existing artifact. The relevant pre-change BGEN implementation
is unchanged since its reviewed import implementation on main.

Final sampling records 43.655 process CPU seconds. Per-value NullBufferBuilder
and BooleanBuilder append frames are absent from the hot table. Remaining leaf
attribution includes file read 26.5%, seek 10.0%, entry presence bitmap 9.9%,
GT decoding/output packing 12.3%, and other entry decoding 9.2%. The GT bitmap
collector's closure also writes allele outputs; its percentage is not solely
bitmap bookkeeping. These numbers have different denominators from the baseline.

The raw profiles plus saved symbols and embedded-symbol profiles are included.
`embed_symbols.py` reproduces the viewer-ready profiles. No source line/inlining
attribution is fabricated. Separate same-VM cloud repetitions remain the final
comparative benchmark.

## Resource scope

Final process-wide peak RSS is 1,057,734,656 bytes (0.985 GiB), including Python
planning and index metadata. It is not a sampled query-only peak or a controlled
before/after RAM comparison.

The actual fixture's largest compressed frame is 11,971 bytes (median 181 bytes).
Eight source workers therefore retain at most 95,768 bytes of compressed buffer
capacity for this fixture, plus 150,010 decompressed bytes per worker. Buffers
retain their maximum encountered size per scan. No universal bounded-memory
claim is made for arbitrary larger valid BGEN frames. Output arrays and shared
sample-layout buffers are additional memory, accounted by the observed RSS.
