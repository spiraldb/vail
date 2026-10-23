# Preserve certified reader ordering through MatrixTable rows

Production `25b44cbd`, tests `823e9de1`, based on reviewed main `8e26b507`.
This is the clean cherry-pick of reviewed `5f9bc87a` / `438973ae` without the
abandoned range-constructor experiment. **98 public Python tests pass**: the
five new cases and BGEN, VCF, PLINK, matrix-layout, shared-annotation suites.

BGEN import already stable-sorts indexed full `(locus, alleles)` keys and assigns
contiguous ranges to sources. Each source seeks offsets in that indexed order.
The previous MatrixIR dropped that certificate when nesting row fields; `rows()`
then called `key_by` and introduced a global stable sort and ordinal window.
The new helper projects complete key aliases onto the owning core and attaches
existing trusted Table key metadata. It claims ordering, not uniqueness. VCF
and PLINK still perform their necessary initial sort; `_sort_rows` now retains
that actual sort's certificate as well. Reserved internal key names retain the
existing fallback. Persisted native matrices already carry row-index keys.

Public output tests cover unsorted physical BGEN input, duplicate-key stable
encounter order across 1/3/8 partitions, row annotations, filters/selections,
empty rows, native write/read, VCF/PLINK sorting and rekeying. The physical plan
is diagnostic evidence, not a test assertion. `plan.txt` confirms the exact
info-score query has eight source partitions without the redundant SortExec,
BoundedWindowAgg ordinal, or OrderedConcat boundary.

## Exact cost comparison

Official cached BGEN: 249,115 variants, 50,000 samples, GP-only decode, eight
partitions, `annotate_rows(info_score=agg.info_score(GP))`, then
`rows().select('info_score')._force_count()`. Every attempt returns 249,115.
Shared-result binding is enabled on both sides. Both use the same normally built
native SHA256 `8e85c492309e290f5cb273a553d6b6a8be0bde3af210e17709ba3d822d1461a5`
and identical Python except the BGEN reader certificate call. Both have the
unchanged two-batch queue and eight input partitions. No higher concurrency
setting or additional buffers were introduced.

Three alternating serial repetitions per mode, query-only measurements:

| Metric | Previous reader | Certified order |
|---|---:|---:|
| Median wall seconds | 41.730 | 5.940 |
| Wall min–max | 40.299–43.263 | 5.940–6.242 |
| Median core-seconds | 71.804 | 68.252 |
| Core-seconds min–max | 69.895–73.499 | 66.924–68.294 |
| Median retired instructions | 864.534 billion | 558.821 billion |
| Median peak RSS | 3.128 GiB | 1.037 GiB |
| Maximum observed peak RSS | 3.170 GiB | 1.041 GiB |
| Median average cores | 1.722 | 11.519 |

Wall decreases 85.8% (7.02×); CPU decreases 4.9%; instructions decrease 35.4%;
median peak RSS decreases 66.9%. This is primarily removal of a serialization
boundary, plus less sorting/stream handling and buffering. Parallel scheduling
uses existing source/read tasks; it is not an increase to execution settings.
macOS activity (desktop, kache monitoring, transient git work) was observed, so
ranges are descriptive, not confidence intervals. Hardware instruction counters
are process counters, not direct DRAM/cache-traffic measurements.

An additional 1 kHz symbolicated profile returns 249,115 in 6.046 wall / 67.901
core-seconds. No SortExec or sort-stream frames remain. Of 66,113 nonblocking
sampled leaf stacks, 43.0% are under semantic expression kernels and 40.5% under
BGEN scan/decode/build (inclusive categories overlap). This is not proof of zero
unsampled overhead. Saved symbols and an embedded-name profile are supplied.

[Profile viewer](https://profiler.firefox.com/from-url/http%3A%2F%2F127.0.0.1%3A3083%2F0f75p0a5g1xsbs3rjll7mzqi82mi8l367zind5g%2Fprofile.json/)

## Scope and pre-existing limitation

A broader RangeMatrix constructor certificate experiment exposed a native
planning-time scalar-subquery initialization gap in keyed repartition after
column masks. That experiment was reverted and is **not in this production
diff**. A BGEN annotate-cols/filter-cols/repartition case reports the same error
with both the original reader and this candidate; both raw logs are retained.
It is a pre-existing limitation, not claimed fixed. Do not generalize this patch
to every constructor without auditing its actual order and transform domain.

The binary was loaded directly from its original normal build path; it was not
copied or reused as a compilation-cache artifact. No Rust source changes exist
between its build base and this candidate's main base. Python candidate source,
original reader snapshot, scripts, hashes, 10 Hz telemetry and counter receipts
are included. No cloud VM or Hail build/run was needed for this Vail before/after
comparison; the full-run Hail baseline remains a separate measurement.
