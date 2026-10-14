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
checkout. The current IR adapter does not preserve full upstream structural
coverage: many table assertions search corresponding native nodes, but matrix
and several other assertions only check that IR exists. Those checks are too
weak to establish that the intended operation was constructed. Value, uniqueness
and repeated-evaluation assertions are retained; their passes do not establish
that the complete upstream tests pass. See the
[full validation follow-up](../conformance/runs/20261014-randomness-full/README.md).
The subsequent [entire upstream Python API run](../conformance/runs/20261014-upstream-entire-report/README.md)
attempts all 62 modules and separately all 1,546 test definitions. The broader
randomness consumers do not all pass.
Additional tests cover the exported API inventory, exact Hail integer/uniform
values, deterministic seeds, missingness, ndarray shapes, distribution support
and moments, and multivariate constraints.

`Table.sample`, `MatrixTable.sample_rows`, and `sample_cols` validate their
probability and filter with `rand_bool`, following Hail. The Python
`balding_nichols_model` implementation uses Hail's `_generate` construction and
lexical `bind`; standard, mixture, and phased models pass the statistical
and replay checks. Hail's fixed phased-genotype reference list still differs.
See the [sampling/model verification](../conformance/runs/20261014-sampling-bn/README.md).

Scalar `bind` evaluates its arguments outside the callback's lexical scope.
Reusing a bound random value inside nested array callbacks retains that value;
the binding itself does not add a new random element identity. It lowers to a
single-element native array lambda with bound variables, preserving globals and
lookup references. See the [binding verification](../conformance/runs/20261014-bind/README.md).

Nested table aggregations now use the same logical compiler as ordinary
aggregations. `array_agg` expands elements and their positions with logical
`Unnest`, reduces each position, and collects results with an ordered
`array_agg`. `group_by` reduces the extended group keys and collects dictionary
entries. Logical joins preserve the enclosing group domain and empty-input
identities; array length validation remains a native aggregate kernel.
Consequently, nested reducers consume the same logically planned random row
identities as their ordinary counterparts.

Fold sequence/combine expressions still execute in the native accumulator.
Their inputs and aggregate operators are logically planned, with ordered input
and one application of the initial state. Runtime configuration from globals
uses an explicit logical initialization boundary: evaluate scalar parameters,
then compile the aggregate with their values through the same logical compiler.
Synchronous legacy operators bridge to that compiler rather than rebuilding
aggregate inputs with a separate physical planner. The old physical table
aggregation implementation has been removed.
See the [logical aggregation validation](../conformance/runs/20261014-logical-nested/README.md).

```sh
PYTHONPATH=python .venv/bin/python -m pytest python/tests/test_randomness.py -q
```
