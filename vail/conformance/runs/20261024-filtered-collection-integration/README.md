# H058 integration on reviewed Call/stddev main

Source assembly0379cd059927aaf3893967b956206b534186eba7 merges approved H05840677993 onto main3a7183a7. The only automatic overlap is expressions.rs: H058 adds public is_pure() near the Expr helpers while main retains CallIndex lower/evaluate/tests elsewhere. No manual source edits. Other H058 production/test files match the approved candidate exactly; Call kernels and stddev source remain byte-identical to main (source-composition.json).

## Interaction review

ArrayIndex remains MayError in expression_effects, so prepared direct Call indexing is conservatively excluded from the pure filter rewrite when evaluation errors could change. H058 additionally classifies the virtual unphased-diploid-index field MayError, preserving non-diploid errors. Random/effectful input, predicate or random reducer body stays on its historical evaluation/UID path. Pure filtering uses ordinary native ArrayFilter and preserves schema/scope. H043 aggregate simplification and float/shared cast math are untouched.

## Exact normal build and gate

PRIMARY warm normal kache/maturin source/target,16jobs, incremental0, RUSTFLAGS=-C target-cpu=native. Fresh four-hour shutdown deadline armed on the idle VM;106GB free before build. Changed Rust source/test files explicitly touched after extraction. Dedicated environment normally installed the resulting wheel; native SHA256415b3b9724cab05f7cdf94ef881d67a71de75ba35938f7084e7f94716b1e4ed4 equals native contents in its wheel. Source hashes independently match this checkout and downloaded logs/provenance/JUnit checksums match the VM.

326 Python cases pass plus10 Rust cases pass (336 total). Python gate covers H058 filters, randomness/capture/errors, existing collection/coordinate/matrix reducers, Call indexing/likelihood/split pipelines, stddev, shared row reductions/CSE, list collect, nullable integer SUM and matrix SUM. Rust Call functional/allocator and stddev merge/retract/filter/emission contracts pass. Real allocator remains33256bytes for8192Calls.

Exactly one known baseline failure remains: test_collection_aggregate_matrix_scope_and_lookup_context, Expected3/Got1. It is recorded in full in python-tests.log/tests.xml/test-summary.json, with no skips or errors. The isolated H058 baseline and candidate have exactly that same failure, documented in ../20261024-filtered-collection-aggregate/results/test-comparison.json and baseline-public-tests.log; this gate does not reclassify it as a new regression or hide it.

No new performance claim for integration. Exact isolated H058 resource/profile evidence remains in its run; combined source introduces no further hot-loop or buffer change. No manually copied native artifact and no local shared-target build. After verified evidence download, cloud source/target ownership is handed to H092 agent under the shutdown deadline, avoiding a redundant VM stop/restart. Parent performs final review and main update.
