# Sampling and Balding-Nichols follow-up

Implemented `Table.sample`, `MatrixTable.sample_rows`, and `sample_cols` as
probability validation followed by the corresponding `rand_bool` filter, as
in upstream Hail.

Ported Hail's Python Balding-Nichols algorithm and validation into
`python/vail/simulation.py`, with its source attribution and MIT license.
The Vail adaptation generates native matrix axes and stores the ancestral
frequency before the nested population-frequency draws. This preserves the
lexical binding that Hail implements with IR `bind`; inline substitution would
resample that ancestral expression in the nested RNG context. Row keys are
already ordered by global position, so attaching their metadata preserves the
requested partition count. Genotype draws are retained in intermediate entry
fields to avoid duplicating random expressions throughout call construction.
Standard, mixture, and phased models are supported.

Added call-expression `phased` and `n_alt_alleles()` accessors needed by the
upstream statistical checks. Updated an obsolete local test that expected
`sample_rows` to remain unsupported.

## Validation

- 18 new sampling/model tests passed, including four original upstream model
  bodies: dimensions/partitions/metadata, repeatability in both modes,
  allele-frequency bounds, and population/frequency/genotype statistics.
- 135 existing randomness and native-type checks passed. The pre-existing
  randomness adapter's IR limitations remain as documented in earlier reports.
- Original upstream Table sampling, entry-Table sampling, and MatrixTable row
  sampling bodies pass. The latter uses the existing sample-fixture adapter.
- Full local Python/harness suite: 1,210 passed, 2 failed, 1 skipped, plus
  19 passing subtests. Both failures are file-pruning cases previously reproduced
  on the requested base, not new simulation failures. See `python-full-final.log`
  and the earlier `20261014-randomness/baseline-partition-pruning.log`.

The original upstream `test_balding_nichols_model_phased` was also attempted
without changing its assertions. Its phased-property assertion passes, but the
fixed genotype list differs. **Seed-for-seed Hail output compatibility remains
incomplete.** See `upstream-phased-final.json`; this failure is not counted as
passing or silently replaced by the semantic phased/replay checks.

No native code was changed for this follow-up. `source-sha256.json` records
implementation/test hashes and the native extension used. `summary.json`
contains machine-readable counts.

From `vail/`:

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_simulation.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_randomness.py python/tests/test_native_type_policy.py -q
PYTHONPATH=python .venv/bin/python -m pytest python/tests conformance/test_harness.py -q
```
