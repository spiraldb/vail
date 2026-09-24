# Integrated verified test run

The corrected recorder completed all suites and wrote `summary.json`, per-suite stdout/stderr logs, source hashes, and native extension fingerprints. Its overall exit is 1 because the upstream original-body runner still has one assertion failure (`test_annotate`, `AttributeError: _same`). The native fixture set also has one explicitly unsupported fixture, and all four oracle fixtures are unavailable. These are kept separate in the structured summary. `recorder-result.json` records the command and exit basis.
