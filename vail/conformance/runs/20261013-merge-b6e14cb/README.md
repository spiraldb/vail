# Merge validation: b6e14cb

Merged `b6e14cb7894a391033b3aa7999c8bf19a3eae79e` into `3afe38f1e7fdd062dfaec1727ea041a3424b7ab5`.

The only merge conflict was in Python `Call` initialization. The resolution preserves the incoming list-valued public `alleles` property backed by an immutable tuple, while retaining packed-call representation validation. Existing codec and storage changes remain intact.

- Release extension build succeeded.
- Full local Python suite: **980 passed, 1 skipped, 19 subtests passed** in 72.06 seconds.
- [Captured pytest output](pytest.txt).

Validation command (against the newly built and installed release wheel):

```sh
PYTHONPATH=/Users/danielking/projects/vail/vail \
  /private/tmp/vail-columnar-benchmark/bin/python -m pytest python/tests -q
```

This validates the merged local suite, including the new tests from the incoming branch. The incoming complete upstream inventory report remains a separately dated artifact; this merge check does not claim to rerun that inventory.
