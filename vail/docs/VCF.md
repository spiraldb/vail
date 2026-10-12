# Native VCF reader

`vail.import_vcf` reads local VCF, gzip, and concatenated gzip/BGZF input without
Hail, Spark, or Java. Headers determine sample count, INFO fields, and entry types.
Rows are decoded lazily in Rust directly into Arrow buffers for the owning
MatrixTable relation. A bounded worker queue retains at most two output batches;
batch size is limited by sample count as well as row count.

Each distinct FORMAT string has a cached field-position plan. Delimiters are
found once per record; each typed field is then parsed across the sample stripe.
Type dispatch is outside the sample loop. Genotype calls use stack storage rather
than a heap allocation per cell. This layout permits specialized parsing kernels
and SIMD delimiter discovery without generating a giant regular expression.
No runtime machine-code generation is implemented.

Supported reader options include multiple files/globs, a header override,
`drop_samples`, call fields, float32/float64 entries, reference genomes and contig
recoding, missing array elements, invalid-locus skipping, line filtering and
replacement. INFO float fields remain float64. FORMAT layouts may vary between
records; omitted trailing values and absent fields are missing. Missing calls
include partially missing calls. Calls with ploidy greater than two fail explicitly,
matching the currently tested Hail behavior.

Ordinary counts can use a minimal source projection; forced counts validate
retained fields. MatrixTable lookup lowering uses direct column positions and
owning entry lists, avoiding nested metadata scans.

Limitations: local files only; no tabix or BGZF byte-range partition reader; the
partition options repartition decoded output rather than splitting compressed
input; Java-only regular expression features are unsupported. A basic local
uncompressed exporter supplies reader round trips and header fixtures; parallel,
compressed, and indexed export are not implemented. Variant splitting remains
in progress and is recorded as a failing dependency in the inventory.

## Validation

The [complete test inventory](../conformance/runs/20261012-vcf/INVENTORY.md) lists
all 61 definitions in Hail's VCFTests class and the 38 import-focused definitions
attempted with their original assertion bodies. Original Hail-format golden
snapshots are not runtime-readable; `conformance/vcf_golden.py` extracts them
using Hail and independently compares all imported values and schemas with Vail.
The three golden comparisons cover 9, 346, and 300,070 rows, respectively.

Reader regression tests are in `python/tests/test_vcf.py`. Rust parsing tests are
in `crates/vail-vcf/src/lib.rs`. The full Python suite passed 855 tests before the
last shared expression/reference adapters; the later focused Matrix/VCF run
passed 54 tests, and the six reader regression tests pass at this commit.
