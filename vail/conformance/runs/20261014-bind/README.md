# Lexical bind and restored model construction

`bind` no longer substitutes its argument expressions into the callback.
Arguments are placed in a one-element tuple array and evaluated outside a
native array lambda. The callback references the lambda's bound tuple fields,
and the single result is extracted. This supplies scalar lexical binding using
existing native operators; it does not call the ordinary array-map frontend,
which would introduce an additional repeated-element random identity.

Consequently, a draw bound before a nested loop retains its value throughout
that loop. A draw bound inside an enclosing loop still varies with that loop's
identity. Multiple arguments, nested bindings, missing values, row sources,
global references and keyed lookups are retained. Invalid captures of ordinary
bind variables across aggregation/scan scopes raise ExpressionException.
Hail's internal `_ctx` AggLet interface remains unimplemented; this change fixes
normal scalar lexical binding, not that separate aggregation-context feature.

The model now uses Hail's Table._generate construction again, including binding
the ancestral draw before the nested population-frequency map. Vail still takes
an integer partition count rather than Hail's interval partition list, and row
key metadata is attached explicitly to the already ordered generated rows.

Validation:

- 13 dedicated binding tests pass, including four original upstream scalar
  bind/rbind bodies and semantic nested-RNG/scope/reference checks.
- 141 model/sampling/randomness checks pass. The pre-existing randomness suite's
  IR adapter limitations remain as documented in preceding reports.
- Full local Python/harness suite: 1,223 passed, 2 failed, 1 skipped, plus
  19 passing subtests. Both failures are file-pruning cases previously
  reproduced on the requested base.
- The original fixed phased-genotype reference still fails without assertion
  changes. Its first differing genotype is now index 6 rather than index 0.
  This is evidence of corrected construction/binding, not a claim of full Hail
  seeded-output equivalence. See upstream-phased-reference.json.

No Rust code changed. Source hashes and complete logs are saved alongside
this report. Commands from vail/:

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_bind.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_simulation.py python/tests/test_randomness.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests conformance/test_harness.py -q
```
