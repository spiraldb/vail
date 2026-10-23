# Small Python API compatibility gaps

Base: frozen main `e79cf6ffeb624756cc27769cd6031b0f12858651`.
Candidate source: `42fb93ceffb3ad05f9bbf03649c475e37dd36b6a`.
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

Results: **94 public tests passed** (27 new compatibility cases plus existing
Call/dictionary tests), **17 unchanged upstream cases passed**. Normal release
build completed successfully in 12m25s. Native path/hash and exact source are
recorded in `provenance.json`; build/test logs are adjacent. `git diff --check`
passed. No Rust source changed, and no performance improvement is claimed.


## Required review fixes

- NaN radii are rejected explicitly, including empty arrays.
- Signed-integer arrays use a separate linear two-pointer scan, comparing exact
  Python-integer coordinate distances to `floor(radius)`. This preserves
  adjacent int64 coordinates above 2**53 with floating radii and avoids integer
  subtraction overflow. Integer endpoint arithmetic is checked against the
  input dtype domain; positive infinite radii produce whole-array windows.
- The localized Python Call genotype-index API intentionally returns an
  unbounded Python integer, matching upstream Hail's value contract. The
  65535/65535 boundary returns 2147516415. It is not silently wrapped to the
  native expression API's Int32 result; that native result-type limitation is
  separate from these Python conveniences.

Public regressions cover NaN radii, fractional/zero/integral floating radii
above 2**53, int64 endpoint domains, infinite radii, and the large Call index.
After these Python-only changes, normal `maturin develop --release --locked`
completed in 0.53s and the 94 public/17 upstream cases passed again against that
installation. See `python-api-gaps-review-build.log`.
