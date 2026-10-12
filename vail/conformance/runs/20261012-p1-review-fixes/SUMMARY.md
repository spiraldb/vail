# P1 review fixes

Both P1 findings in the October 12 broad review are fixed.

- Tuple inputs require a Python tuple with exact declared arity, including empty tuples. Nested members are validated before Arrow conversion. Malformed input raises instead of silently padding or truncating data.
- Population standard deviation uses DataFusion's accumulator and Arrow centering kernels, including partial-state mean translation for grouped and scan execution. Empty/all-missing inputs remain NaN; finite singletons return zero; nonfinite values propagate NaN. This numerical improvement intentionally differs from the live Hail result, as documented in `docs/INTENTIONAL_HAIL_DIVERGENCES.md`.

Validation after native rebuild: **728 Python tests passed, 1 skipped, 19 subtests passed; 111 Rust tests passed**. The new regression module contains 30 cases covering tuple boundaries and large-offset standard deviation across total, grouped, and scan aggregation.

Commands from the Vail project directory:

```sh
PYTHONPATH=python uv run --no-sync python -m pytest python/tests -q
cargo test --offline -p vail-execution -p vail-table
```

Logs and source hashes are saved alongside this summary. The P2 findings remain open. These changes are separate from the repeated-key_by fix committed as `f030996072a86cb91c3ef9af1ee3850da406e93c`.
