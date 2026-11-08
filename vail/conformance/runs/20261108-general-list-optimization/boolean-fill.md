# Boolean null filling without selected-row gathering

Production candidate: `1dad1531c474b59136dc26238434a976e2e87e35`, based on accepted `562f8e38`. No binding operator, evaluated-value cache, genotype-specific matching or map/filter-sink extension.

For a pure Boolean value `v`, `if is_defined(v) then v else false` becomes the existing DataFusion truth test `v IS NOT DISTINCT FROM true`. A true fallback uses `v IS DISTINCT FROM false`; the inverse missingness-guard form is also recognized. The two references must share their exact native Arc identity. A query-local memo checks full-DAG purity before removing the second evaluation. Errors, randomness, unknown effects and unmatched shapes retain the original CASE. Logical lowering sends this narrow pattern through the same native preparation. No purity or schema preparation occurs in row/entry loops.

The resource test evaluates a Boolean field predicate inside a nullable record with an unused string sibling. With256 rows and payload8→16384 bytes/row, accepted baseline allocation grows15388→4207644 bytes on native lowering and16424→4208680 on logical lowering. The candidate stays1532→1532 and2050→2050 bytes, respectively. Fixtures, compilation and warmup are outside allocation counting; complete expected Boolean arrays are checked. This measures allocation independence from unused payload, not zero allocation, ownership release, IO or peak RSS.

Validation:127 focused Rust tests passed, including public scalar/runtime literal, true/false/null, inverse guards, empty and sliced batches;524 Python tests and10 subtests passed from the normal wheel. The sole preexisting correlation-allocation exclusion is explicit. The broad Rust suite encountered the untouched CSV field-count error-message assertion, which was independently reproduced against accepted562 with the same environment. Its failure is retained and no CSV code is changed. The initial new-test compile error is also retained.

| Unchanged upstream workload | Wall before → after | Wall change | Core-seconds before → after | CPU change |
|---|---:|---:|---:|---:|
| test_matrix_table_array_arithmetic | 0.055611 → 0.055246 s | -0.657% | 0.214960 → 0.213546 | -0.657% |
| test_matrix_table_filter_entries | 0.068864 → 0.050664 s | -26.429% | 0.280247 → 0.196292 | -29.958% |
| test_matrix_table_filter_entries_unfilter | 0.125934 → 0.107121 s | -14.939% | 0.492643 → 0.405404 | -17.708% |
| test_split_multi_hts | 0.162481 → 0.162562 s | +0.050% | 0.426231 → 0.427472 | +0.291% |

Four fresh processes/version/workload in balanced B,C,C,B,B,C,C,B order; each runs the original upstream benchmark body after two warmups, with five measured calls. Values and Arrow metadata are independently compared before timing against the pre-change output; split-multi also checks its established external reference. Input is the same public1000-row×2535-column native fixture, four partitions;13 input-file SHA256s and all reference/source/frontend/native identities are pinned and rechecked. The reference matches values, not a new Hail engine rerun.

Process CPU includes all threads. The native runtime has32 Tokio workers; RAYON4/BLAS1 settings are not a global four-thread cap. RSS includes imports and full-output validation and is not isolated operator memory. The worker’s sampled6GiB guard is not a hard process-tree limit. All32 worker process groups were confirmed absent. Builds, correctness gates and timings ran sequentially. Negative controls and their small adverse results are retained; no noise waiver or universal-win claim. This cohort compares the candidate with accepted masking production, not the original pre-mask version.

Evidence: the companion manifest hashes every archived payload. The archive contains raw32-process records, values/metadata oracle status, plans as diagnostics, package/native hashes, input pins, post-input SHA checks, cleanup, source restoration, candidate/baseline allocation tests, CSV baseline failure, normal build/Python gate and exact source snapshot. Compiled libraries, virtual environments and the large reference Arrow tables are excluded. Reference table identities and full-oracle receipts are retained.

Archive SHA256: `caf535cff7a2e6bc348b1797d951812cccc766455c8f2ec8448b5bd648d6d289`; 256 payloads, 169723 compressed bytes.
