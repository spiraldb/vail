# Random expressions

The implementation follows Patrick Schultz's randomness work:

- [PR 11605: reproducible randomness infrastructure](https://github.com/hail-is/hail/pull/11605)
- [PR 11847: Python infrastructure and explicit dynamic identities](https://github.com/hail-is/hail/pull/11847)
- [PR 12139: enable the new RNG](https://github.com/hail-is/hail/pull/12139)
- [The design document](../../hail/dev-docs/hail-query/randomness.md), pinned with the Hail source checkout.

Each random function captures a static call-site identity when the Python
expression is constructed. An explicit seed uses Hail's `-seed - 1` mapping;
otherwise identities come from a counter. `reset_global_randomness` resets that
counter. Reusing an expression therefore preserves its call-site identity.

The native kernel implements the fixed-key, 20-round Threefry block cipher and
PMAC stream construction. An invocation's identity contains its session nonce,
static call-site identity and dynamic context words. The kernel owns a fresh
local stream per invocation, with no shared mutable execution RNG. Integer
sampling and scalar uniform/normal sampling follow Hail's bit consumption.
The remaining distribution samplers preserve distribution semantics and seeded
reproducibility within Vail; their samples are not promised to match Hail's
JDistlib algorithms bit for bit.

The frontend binds a row context through an explicit ordered ordinal operator
before random projections, filters and aggregate inputs. Its executable boundary
is optimized once and retained when consumers form partition views, so a
stateful upstream repartitioner is consumed once per action. Collection callbacks
receive their element position, including when elements have equal values.
Nested callbacks prepend their enclosing contexts. Group result randomness is
evaluated after grouping, independently from randomness inside reducers.

All 13 public `hl.rand_...` functions are exported. Normal and uniform draws
support two-dimensional ndarray sizes. Distributions also include categorical,
Dirichlet, gamma, beta (including truncation), Poisson, bivariate normal,
hypergeometric and multivariate hypergeometric. `shuffle` uses the same stream
identity mechanism. Distribution kernels use Rust numeric routines; they do not
call Python or NumPy during query execution.

The randomness tests extract behavioral test bodies from the pinned Hail
checkout. Hail compiler-node assertions are adapted to checking native Vail IR
construction. Value, uniqueness and repeated-evaluation assertions are retained.
Additional tests cover the exported API inventory, exact Hail integer/uniform
values, deterministic seeds, missingness, ndarray shapes, distribution support
and moments, and multivariate constraints.

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_randomness.py -q
```
