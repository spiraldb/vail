# Matrix entries ordering benchmark

The candidate emits entries in row-key/column-key order without globally sorting all expanded entry rows. It uses typed key-bound proofs to execute strict-gap row-key ranges independently and coalesces ranges that share an equal boundary key. Where the proof is unavailable or inconsistent it retains a single ordered run. A shared cache evaluates the column globals once for all output partitions, and singleton expansion bounds both backing-batch memory and queued row wrappers.

On the exact local `profile25_mt` fixture (24,885 rows × 2,535 columns; 63,083,475 entries), the benchmark action was `entries()._force_count()`. The detached base revision returned the expected count in a median 12.73 s wall / 33.58 s process CPU. The reviewed candidate returned the same count in a median 1.187 s wall / 14.89 s CPU: 10.73× lower wall time and 2.25× lower process CPU time on this fixture. The three measurements and raw counts are in [raw-metrics.json](raw-metrics.json).

The fixture was regenerated from the repository's `profile.vcf.bgz` (SHA-256 `cdacd409124e0f647e08b2eff0dda7705759e971d88d6c9b054dc252c91d4b93`) with 16 partitions. It is not a generic guarantee: its row keys were not unique and column-order metadata was absent, so it exercises duplicate-capable row-key handling and one-time shared column ordering.

Final-wheel validation passed: 17 `test_matrix_table.py` entries tests and three targeted VCF slot/order/duplicate-boundary/repeated-action tests. The duplicate-key VCF test is imported with two partitions and persists equal row-key bounds across the split; it checks nondecreasing composite keys and an exact multiset that ties sample, genotype, depth, and record ID together. The candidate shared-global path was source-reviewed for reset behavior; the repeated-action case also collects the same entries table twice.

The sampled profile is [candidate-sample.txt](candidate-sample.txt). It is a four-second macOS `sample` capture spanning setup, idle time, and one entries count; it is not a benchmark profile suitable for attributing precise percentages. macOS reported a 1.5 GB footprint at sampling time and 7.2 GB peak footprint. The baseline was not sampled, so this peak is documented without implying a comparative memory result.

Build and wheel hashes, exact revisions, fixture provenance, and validation commands are recorded in [provenance.json](provenance.json). No cloud VM was used.
