# Integrated test run

All six suites completed and `summary.json` plus each suite stdout/stderr log were written. The recorder then raised a `NameError` while printing its final aggregate because the `totals` variable was out of scope. This run is retained as diagnostic evidence; the recorder process exit was 1 even though the saved summary records the child-suite classifications and clean source/extension integrity. See `recorder-error.json`. A corrected recorder run is being prepared separately; no outputs in this directory were changed.
