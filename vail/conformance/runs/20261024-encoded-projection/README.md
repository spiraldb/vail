# Encoded projection prototype: first measurements

This prototype is **not merge-ready**. Its first measurement regressed; these
results are retained rather than replaced by an expected improvement.

Production revision `386c990029dca0552770fd40fd2e372800a2deda`, based on approved
main `0eec407731c5b39829316f8edf3f463e544b6888`. Normal kache/maturin release
build, native CPU, two build jobs, incremental disabled; build took 14m27s.
Candidate native SHA256
`f6dae80baaabbfc9e84d93641cd766f221e588c88f306b0f33b279c5ad36fd4e`.
Baseline is the normally built H078 assembly `2da14077`, production-equivalent
to main `0eec407`, native SHA256
`69a06f57d61746e815cd843bb58a08efb912d214bb3c6da650d7c62e05d825bf`.

Same existing synthetic native fixture: 200,000 rows × 2,500 columns, 500M
entry slots, 32 source partitions, 500,686,160 stored Vortex bytes including
column metadata files. Input and OS cache were warm. One warmup and three
measured queries each, sequential environments, no builds/tests/exports during
measurements. Every action returned the independent expected sum 20,969,995,813.

| Median | Main baseline | First prototype |
|---|---:|---:|
| Wall seconds | 0.076326 | 0.132074 |
| CPU seconds | 0.911501 | 1.575259 |
| Maximum process RSS bytes | 910,114,816 | 1,081,491,456 |

The prototype took 1.73× as long and used 1.73× the CPU seconds. This small first
comparison establishes a regression, not a final throughput claim. Raw actions
and artifact provenance are in the adjacent JSONL files.

A separate eight-second Time Profiler recording sampled the computed source.
Numeric dictionary take was 21.85% of leaf CPU samples, Boolean take 23.22%,
generic masked grouped SUM 11.08%, and bitmap iteration 11.03%. These are sampled
CPU fractions, not hardware utilization. Caller stacks split numeric takes
between Arrow conversion of retained original entry columns and canonicalization
inside scalar Mask beneath the grouped accumulator. The intended dictionary
grouped reduction was not the hot implementation. The source exposed unused
passthrough columns across an aggregate boundary that retains its input schema;
it therefore computed the reduction while also decoding original entries.

The first public Python gate had 54 passes and two failures in new wide-schema
native tests: Utf8View physical strings did not match the declared Utf8 schema.
The existing normalizer was missing at the computed output boundary. The
earlier release Rust runtime gate passed five tests before the final resource
refinements; that earlier run is not claimed as an exact-revision final gate.

Raw trace `/private/tmp/encoded-pushdown-first.trace`; exported stack XML
`/private/tmp/encoded-pushdown-first-stacks.xml`, SHA256
`e71692c11c0950bb6b06a0f3d8112cd0675099239093c0f035db8f664dc869c4`.
The workload JSONL SHA256 is
`c4385b07f2ec0671858f774276d7bfa3ef3937465f29a93f703b0e6a27ddd7ae`.

Subsequent fixes require fresh builds, semantic checks and measurements. Review
also requires clipping visible child spans before ancestor extraction/masking,
preserving session batch/entry budgets, and resolving underparallelized sources.

## Second prototype, still slower

Production source `3959714d`, native SHA256
`75858264238d9df730bbbcba1a55529a414e26f1f88e87d8da006f9801bac15a`.
Normal follow-up build took 5m34s. Repeated the same warmup + three action
comparison in quiet conditions: baseline median 81.724ms / 0.891919 CPU seconds;
candidate 99.898ms / 1.135964 CPU seconds. The prototype improved versus its
first version but still took 1.22× wall time and 1.27× CPU time versus main.
Every action returned the exact expected sum. Public Python gate: 56 passed,
including the two formerly failing wide-schema cases and native string
passthrough. This does not establish final resource acceptance.

Second profile: Boolean take 15.74%, numeric take 14.83%, generic masked grouped
SUM 14.38%, bitmap iteration 14.90% of leaf CPU samples. Unused Arrow conversion
was removed. The dictionary grouped kernel still did not become the hot path.
Pinned MaskReduceAdaptor declines reduction unless its Boolean mask is concrete
Bool/Constant; encoded logical masks therefore prevented the Dict rule. The next
prototype changes mask execution order to resolve only that bitmap first and
reapply the existing encoding-specific mask rules. No improvement from that
next change is claimed before its own build, correctness checks and timings.
