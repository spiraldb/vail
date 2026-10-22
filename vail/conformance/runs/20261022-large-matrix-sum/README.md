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
