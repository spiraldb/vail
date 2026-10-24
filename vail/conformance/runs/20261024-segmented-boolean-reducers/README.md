# Prepared segmented Boolean reducers

Source candidate f11b40b5ef4144121fed9a8d59c52d621e97d07b on dk/prepared-boolean-reducers, based on main de71b289. Required review and integration pending.

The query-prepared list reducer now also selects Boolean any/all/count_where/fraction evaluators. Each batch binds packed Boolean values and logical child/list masks, normalizing all-valid masks once. Each segment borrows Arrow BitChunks over the actual bit range. No BooleanArray/NullBuffer/BooleanBuffer slice, Arc refcount, per-list validity count, or configured reducer is constructed. Segment offset/length alignment is actual data work; type/opcode/null-presence decisions are outside the loops. any/all short-circuit Boolean words, while count/fraction use Arrow bitmap iterators and ordinary count_ones. Dense ALL handles the last word's zero padding explicitly. There is no handwritten SIMD or unsafe code.

Contracts: a missing list gives missing output; present empty list gives ANY=false,ALL=true,count0,fraction missing. Missing child values are excluded from Boolean truth decisions and true count, while fraction's denominator includes all child positions. Thus a nonempty all-missing list gives ANY=false,ALL=true,count0,fraction0. Short-circuiting operates on already-evaluated Boolean buffers, so it does not reorder evaluation/errors of expressions producing those buffers.

Public tests have independent Python expected results and cover dense/nullable words at0/1/63/64/65/127/128/129bits, sliced offsets, all-missing lists, mixed values and stored sparse matrix entries. Both baseline and candidate gates have102passes plus the same known matrix literal-array callback cardinality failure; no skips/new failures. Normal warm kache/maturin build1m25. Native source/wheel provenance and measurements are recorded under results/ after the resource gate.

The remaining AD-filtered-SUM profile bitmap setup is a separate DataFusion ArrayFilter offset-building path: ArrayFilter→OffsetBuffer::from_lengths→BooleanBuffer::count_set_bits. This patch targets Boolean aggregate pipelines; it does not claim to eliminate that filter work or floating-point reducer setup.

## Results

| Native63M-entry pipeline | Baseline wall | Candidate wall | Speedup | CPU-seconds baseline→candidate |
|---|---:|---:|---:|---:|
| Per-entry AD ANY(x>0), global count |318.9ms|284.4ms|1.12×|8.47→7.39 (12.8%less)|
| Per-entry PL ANY/ALL/count/fraction bundle, global sums |1952.2ms|1177.7ms|1.66×|52.46→31.86 (39.3%less)|

A/B/B/A fresh processes, one warmup and three recorded iterations per query/process; medians of the two process medians/version. These are improvements over main de71b289, not Hail-relative multipliers. Exact integer results match; parallel floating-point sums agree within the documented tolerance. Full values and individual measurements are in results/paired-summary.json and JSONL receipts. MaxRSS changed−1.6%/−0.9%, no substantial capacity claim; minor faults were flat for ANY and+6.6% for the bundle, so this does not establish a page-fault/allocation win. Zero major faults in both.

Software CPU-clock99Hz profiles (12bundle evaluations plus process startup) have zero lost samples:636.5→386.3CPU-seconds, about63K→38K samples. Baseline BooleanArray::slice15.36%, BooleanBuffer::slice8.26%, UnalignedBitChunk::new21.48%, BooleanBuffer::count_set_bits5.56%, and BooleanArray::true_count3.79% disappear from the candidate report above0.01%threshold. Candidate Arrow BitChunks::new is9.09%: actual offset/length range alignment and checked borrow work remains. It is not schema/opcode preparation, though a future once-per-batch borrowed word view could reduce it further. The normal output-builder append calls remain.

Optimized count assembly is included. The word loops use compiler-generated popcnt/bitwise operations, not per-Boolean-value dispatch or hand-written SIMD. Source type/opcode is selected during query preparation, while logical nullable/dense variants are selected once per batch. Remaining branches depend on actual segment lengths, bitmap-word values, nulls and safe bounds. Any/all can stop after the first decisive word. No per-list Arrow array/bitmap header or null-count scan is constructed.

Source SHA256 values match f11b40b5. Baseline installed native4d46e99d and candidate74be3d4d each match their normally built wheel. No native library was manually copied. Same stable source/target, interpreter ABI, features and RUSTFLAGS were used; changed source was hashed/touched after overlay. Raw perf files remain on PRIMARY under h092-boolean-reducers/results, with hashes in raw-profile-sha256.json; compressed reports are included here. Hardware PMUs are unavailable on this VM, so no ALU saturation, DRAM bandwidth or cache-miss claim is made.

PRIMARY compute/source/target ownership was released to the H089 integration agent after all timings/profiles and before source/provenance packaging finished. Only read-only receipt downloads continued. Required review/main integration remains the parent's responsibility. Numeric mean, floating-point reducers, integer_list_sum same-width and the separate DataFusion filter-offset path remain open.
