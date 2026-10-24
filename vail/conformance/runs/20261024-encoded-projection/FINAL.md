# Certified encoded projection: final isolated candidate

Production source: `dc5ab65863931dc37d9abf41a20a598aa87d4f3b`, based on
`0eec407731c5b39829316f8edf3f463e544b6888`. This is an isolated development
candidate; final review/assembly onto current main remains the parent's gate.
Normal `maturin develop --release --locked` with kache, jobs2,
`RUSTFLAGS='-C target-cpu=native'`, no incremental builds or profile overrides.
Installed native SHA256:
`27c233d6031152e7943651864fe210d434e6c18e8a0912f800c56830425a02a2`.
Baseline is the normally built H078/main-equivalent artifact, SHA256
`69a06f57d61746e815cd843bb58a08efb912d214bb3c6da650d7c62e05d825bf`.

All59public Python tests and9public Rust integration tests pass. These include
widening, logical ancestor nulls, empty axes/lists, sliced/overlapping/distant
lists, all physical dictionary code widths, missing codes/values, ordered keys,
aliases, globals, rejected global-dependent arithmetic, batch-size1, and valid
stored numeric constants that must use the ordinary route. The initial fallback
bug's51pass/6fail receipt and the two stored-constant failures are retained;
none is silently omitted from the final59pass result.

## Actual throughput and CPU

M3Max14cores/96GiB. Native500M-entry fixture:200Krows×2500columns,32source
partitions, exact independent DP sum20,969,995,813. One untimed warmup then12
queries per engine, warm filesystem caches. No concurrent build, test, profile,
or other timed workload. CPU is process user+system core-seconds.

| Query | Baseline wall | Candidate wall | Wall speedup | Baseline CPU | Candidate CPU | CPU speedup |
|---|---:|---:|---:|---:|---:|---:|
| Dense entry structs, nullable DP; certified encoded route |72.068ms|61.595ms|1.170×|0.855248s|0.739922s|1.156×|
| Filtered entry structs; original route |76.235ms|77.559ms|0.983×|0.899137s|0.912890s|0.985×|

These are medians. Every measured query checked the independent exact sum.
Final-artifact CPU samples show the typed dictionary grouped SUM on the eligible
fixture and `original_scan`/Arrow conversion on the filtered fixture. This is
actual encoded execution, not an all-fallback success. The filtered result is a
~1.7%wall/~1.5%CPU regression and remains disclosed.

The eligible query averages~12active CPU cores from CPU/wall. That is parallel
CPU consumption, not evidence that arithmetic/SIMD units or DRAM bandwidth are
saturated. No new PMU or bandwidth saturation claim is made by these samples.
The preceding hardware-counter/assembly evidence remains explicitly tied to its
older source/artifact in README.md; the typed reduction kernel is unchanged.

## Startup and small queries

All files are in warm filesystem cache; **no disk-cold-cache claim**. Fresh-read
queries create a new native source each time, so each pays metadata/preparation.
Seven warm-process samples after the first process action:

| Files | Baseline fresh-read query | Candidate fresh-read query |
|---|---:|---:|
|1|2.179ms|2.590ms|
|128|30.495ms|30.569ms|

The1-file query regresses0.411ms (~19%). First process actions, including native
initialization, were157.5→158.0ms and186.1→184.2ms respectively.

A separate run initializes the ordinary native engine first, then separates
source read/setup, first uncached query preparation, and repeated queries on the
same source. This does not subtract unrelated timings to invent a precise
preflight-only duration:

| Files/engine | Read setup | First query | Same-source warm median |
|---|---:|---:|---:|
|1 baseline|1.175ms|2.996ms|1.358ms|
|1 candidate|1.125ms|3.524ms|1.502ms|
|128 baseline|3.902ms|24.326ms|25.197ms|
|128 candidate|3.809ms|25.550ms|26.199ms|

No benchmark-specific eligibility threshold was introduced to hide startup cost.

## Bounded resources and the Pco boundary

Public Vortex API probes, rebuilt normally from the exact source, exclude fixture
preparation and check independent exact results.128visible values in distant
list groups over10K/1M/40M retained owners take30.21/29.96/30.00µs. Oversized
primitive dictionaries take29.67/29.58/30.29µs; bitpacked dictionaries take
36.79/36.54/36.54µs. These retain the bounded clipping/gathering fixes.

Public native tables with128visible values and a missing middle list retain
10K/1M/10M physical owners (~40KiB/884KiB/8.4MiB stored). Their ordinary-route
warm medians are baseline1.399/1.919/4.155ms versus candidate1.595/2.015/2.549ms.
Their semantics match independent sums; the candidate does not introduce the
previous full-owner mask preparation.

The direct public Pco dictionary probe **still scales badly**:128visible values
across10K/1M/10M retained owners take97.6µs/3.893ms/38.232ms. This unsupported
physical case is deliberately outside the certified route. The source proof
admits only the closed bounded primitive/constant/patch-free-bitpacked/small-
dictionary combinations; Pco or oversized/sparse/unknown descendants return
UNKNOWN before computed payload evaluation. Valid unsupported numeric constants
also return UNKNOWN rather than an invalid-input error.

**Coverage limitation:** the public native Vail producer accepts Arrow batches,
canonicalizes dictionaries, and exposes no custom Vortex layout/compressor or
commit-existing-Vortex-file API. It cannot persist a128-code/10M-value retained
Pco dictionary. Therefore the receipts do not claim an end-to-end LocalRead
certificate-fallback test on that exact forced-Pco shape. The direct public Pco
hazard, fail-closed source proof, native sparse fallback, and supported-encoding
resource probes cover distinct boundaries. No test hook, fabricated buffer,
protected-metadata rewrite or manifest edit was introduced to bridge this gap.

## Prepared capability and semantics

Preparation checks immutable snapshot/file identity, full source coverage,
checked constant-step list offsets exactly covering their child, logical
ancestor validity, and descendant decoded-work/memory bounds. A Constant Boolean
validity uses the pinned scalar validator on at most16bytes, not a bitmap scan.
Small dictionary preparation is capped at64KiB including validity/decoder scratch.
Metadata work is capped and positive/negative physical proofs are cached per
snapshot/operand/range. The proof never admits arbitrary Pco through a generic
Take fallback.

A private-field `PreparedComputedProjection` owns the proof, exact expression,
and result schema. Execution cannot substitute another expression/schema. The
trusted cross-crate preparation boundary explicitly requires the closed
translator's row-preserving operator/shape/dependency/effect/schema proof.
Unknown cases retain original typed expressions in an ordinary source plus
standard DataFusion ProjectionExec before any computed output. DataFusion still
owns global SUM and relational consumers; source ordering and TaskContext batch
caps are preserved.

Raw JSONL, source harnesses, sample stacks, build/test logs and SHA256 receipts
are in [final-certified](final-certified/). Broader predicate/statistics/validity-
only pushdown, session reuse and source scheduling remain separate work; this
candidate does not claim to implement the entire pushdown queue.
