# Matrix entry annotation and sum profile

This run compares the prior indexed-list lowering with the `arrays_zip` lowering for entry annotation. Both used the same optimized native extension and Python 3.11 environment; the old path was enabled by a temporary `sitecustomize.py` monkeypatch replacing `_indexed_cells` with the former `range(length(cells)).map(index -> {cell: cells[index], position: index})` implementation. No native source or dependencies differed between the compared runs.

The earlier direct row-list aggregation change has a separate matched comparison in `../20261013-matrix-arrow-row-sum/STATUS.md`. Together the available Vail results are:

| Workload | Generic table path | Direct row-list sum | Old indexed annotation on direct path | `arrays_zip` annotation | Hail |
|---|---:|---:|---:|---:|---:|
| 100K × 10 | 0.245952 s | 0.150245 s | 0.149875 s | 0.116710 s | 0.1108 s |
| 25K × 100 | 1.606989 s | 0.574181 s | 0.536261 s | 0.337611 s | 0.144609 s |

Generic→direct is the matched row-sum experiment from the linked report; old-indexed→`arrays_zip` is the same-wheel comparison below. The 100K × 10 Hail gap is now small (about 5.3%), while the wide 25K × 100 case remains about 2.33× slower than Hail. Do not interpret the cross-report direct→old-indexed values as a controlled comparison: they use different release wheels and source snapshots.

- Python: `/private/tmp/vail-matrix-benchmark/bin/python` (3.11.13)
- Wheel: `/private/tmp/vail-matrix-release-wheels/vail-0.1.0a1-cp311-abi3-macosx_11_0_arm64.whl`
- Wheel SHA-256: `71bac3f412e444701fac6ea50537a28c404a57a425ebb77dbe01f78b1faf5d33`
- Native extension SHA-256: `0ea9e6446a0592417a865faf1876eb849dd65d2e92864c196a0d58c61ce9c44b`
- Benchmark protocol: 3 warmups and 5 measured iterations; medians from the saved `results.json` files.

| Matrix | Old indexed-list median | `arrays_zip` median | Change | Hail reference |
|---|---:|---:|---:|---:|
| 100K × 10 | 0.149875 s | 0.116710 s | 22.1% faster | 0.1108 s |
| 25K × 100 | 0.536261 s | 0.337611 s | 37.0% faster | 0.144609 s |

The comparison shows a meaningful improvement from removing repeated outer-list indexing/capture, but the 25K × 100 case remains about 2.33× slower than Hail. The 100K × 10 case remains slightly slower than Hail. These results do not establish that all Matrix costs are resolved.

The current-path profile is in `new-100k-10/profile-escalated/`; the symbolized flamegraph is [`flamegraph.svg`](new-100k-10/profile-escalated/flamegraph.svg) and the sampled profile is [`profile.json.gz`](new-100k-10/profile-escalated/profile.json.gz) (worker median 0.119082 s). Main inclusive hotspots were `Projector::project_batch` (59.2%), `ArrayTransform::invoke_with_args` (43.6%), and `arrow_select::take::take_list` (28.7%). Those percentages are nested inclusive costs and must not be summed. The profile indicates list projection/lambda evaluation remains material, but does not by itself identify which exact captured value causes the `take_list` cost.

Focused public Matrix correctness tests against this release extension passed 9/9, including `test_entry_annotation_zips_cells_with_positions_after_column_reordering`, which asserts the public result and that the lowered core IR contains `arrays_zip`; it also covers column choice/filtering/reordering, row/column/entry references, missing cells, and zero-width lists. The focused public sum suite (`python/tests/test_matrix_entry_sum.py`) covers empty axes, all-missing entries, filtered entries, `_localize=False`, Int64 precision and wrapping, and Float32 promotion. The native arrays_zip dispatcher and table crate tests also passed in the integrated test run (see parent validation report). Exact test invocations/results are retained in the task log. The profile run and per-case timing JSON files are retained alongside this report. The optional profile renderer's matplotlib export was unavailable; sampled profile, symbolized data, folded stacks, SVG flamegraph, and hotspots are present.
