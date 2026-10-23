# Small Python API compatibility gaps

Base: frozen main `e79cf6ffeb624756cc27769cd6031b0f12858651`.
Candidate source: `bab0027baf89a5b72dc964ffaef81b294d4d5255`.
Upstream Hail source: `e10cc355bc8cd0de9c5f29d5182d31527898bae1`.

The patch adds struct-expression `values()` and simultaneous `rename()`, Python
Call value predicates/counts/one-hot/genotype-index/display methods, Hail's
`frozendict` value utility, and standalone NumPy `array_windows`.

Struct operations build existing typed native expressions. Missing parent
structs stay missing after rename. Python Call methods operate only on already
localized Python values, not inside engine row/entry execution. `array_windows`
is the upstream local NumPy utility and introduces no ndarray/BlockMatrix engine
dependency. Copied Hail helpers retain the existing MIT license attribution.

Deliberately remaining: `locus_windows` and `_locus_windows_per_contig`; automatic
Python Mapping/frozendict literal inference (a separate numeric/coercion patch);
row-regression implementations, even though their initial struct `values()`
blocker is removed. These changes do not establish that downstream regression
tests pass.

Validation uses a normal isolated maturin release build with kache and two Cargo
jobs, Python 3.12, environment `/private/tmp/vail-python-api-env`. No saved or
copied native extension is used. `upstream_bodies.py` executes unchanged selected
upstream function bodies, including the seven parametrized invalid-window
cases. Selected bodies have no Hail fixtures or class setup dependencies; this
is targeted body validation, not a claim that entire upstream modules pass.

Results: **87 public tests passed** (20 new compatibility cases plus existing
Call/dictionary tests), **17 unchanged upstream cases passed**. Normal release
build completed successfully in 12m25s. Native path/hash and exact source are
recorded in `provenance.json`; build/test logs are adjacent. `git diff --check`
passed. No Rust source changed, and no performance improvement is claimed.
