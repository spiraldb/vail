# Dense coordinate SUM milestone

Production source `fc0b711e`, normal Rust 1.98 release artifact SHA256 in provenance.json. No Hail timings in this evidence; paired laptop benchmarks follow reviewed integration.

Coordinate SUM uses one typed Arrow vector and native `binary_mut` updates/merges, without coordinate IDs or grouped null-state machinery. Aligned maps borrow shared column arrays and read row captures as scalars. A per-batch evaluation frame prepares only audited scalar kernels; collection/grouping/fold callbacks retain the batch fallback. Struct descriptors and numeric types are prepared during lowering. Global-free total aggregates omit the singleton globals input join and use an effective empty globals schema.

The exact upstream 10,000 by 500,000 range matrix query (2,500 partitions) returned 500,000 columns. The 1 kHz symbol-embedded profile measured 4.219 seconds wall and 33.663 core-seconds. These are diagnostic profile timings under uncontrolled laptop activity; no fair speedup claim. RSS highwater was 7.90 GiB; no memory improvement claim.

Profile leaves: native in-place Int64 vector SUM 39.84%; Int32-to-Int64 widening 20.86%; scalar-plus-array arithmetic 9.17%; memmove 12.42%. Of 18,055 total samples, 2,212 (12.251%) copy produced map values during concat; 22 (0.122%) are projection broadcast; 9 (0.050%) other copies. No memmove samples remain under CrossJoin scalar-list replication. Prior row metadata RunEnd decoding, column takes, and generic grouped SUM null-state hotspots are absent.

123 public Python tests passed, including sparse entries, nested captures, empty axes, replay, integer wrapping, Boolean widening, global scope, RNG, folds and lazy initializers. Two native coordinate tests also passed on the final production source. An additional 97 expression, numeric, empty-struct and parent-validity tests passed for the shared struct/numeric preparation changes.

Other reducers still need direct vector-state adapters. Count, Boolean reductions, product, min/max, mean/fraction and stddev have compatible fixed-size vector state; ordered collection, regression and call-statistics require algorithm-specific coordinate state. No claim that those fallbacks are fixed here.
