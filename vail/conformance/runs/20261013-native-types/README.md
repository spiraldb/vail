# Native operator and logical-type validation

Validated 13 October 2026 against DataFusion 55.0.0 / Arrow 59.2.0 / Vortex 0.86.1.

- Release extension build succeeded.
- Entire local Python suite: **891 passed, 1 skipped, 19 subtests passed** in 66.63 seconds. Output: [pytest.txt](pytest.txt).
- Standalone Rust packed-Call codec: **2 passed**, including Hail word fixtures, phased/unphased roundtrips, signed-word boundaries, invalid ploidy, and representation overflow.
- The installed PyPI Hail oracle confirms that duplicate expression-valued set keys retain the first value in the existing dictionary regression (`10`). No Hail build was used.

Regression coverage includes direct DataFusion comparisons/grouping, scoped aggregates with missing keys, joins across two partitions and stored files, interval-query boundaries, nested lookups, packed-Call construction/accessors/storage/VCF/genomic aggregates, reference-genome metadata validation, and explicit unsupported Set/Dictionary keying.

```sh
PYTHONPATH=/Users/danielking/projects/vail/vail \
  /private/tmp/vail-columnar-benchmark/bin/python -m pytest python/tests -q
rustc --edition 2024 --test crates/vail-ordering/src/call.rs -o /private/tmp/vail-call-codec-tests
/private/tmp/vail-call-codec-tests
```

The local suite includes upstream-backed cases but is not a rerun of the entire upstream Hail inventory. The intentional policy changes are recorded in [the incompatibilities document](../../../docs/INTENTIONAL_HAIL_DIVERGENCES.md).
