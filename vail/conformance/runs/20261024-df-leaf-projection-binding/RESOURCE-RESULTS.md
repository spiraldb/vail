# Fix-only resource observations and explicit remaining limits

Source productione1a006e1; identical public DF test/resource driver91afabdb2e2619f8f0a834432728dde5492d69029dc7b0aaa3ee0db6e27ffb8d. Baseline2e479637 is approvedmain66 plus driver, unpatched DF55; fixed4d5240d0 uses the same driver plus the project-pinned six-line library repair. Cached relinks18.09s/16.08s; standalone binaries/hash receipts retained. No revoked Vail GetField route present. Source normal wheelcc4/nativeae1a and431Python correctness are separate fixed-only evidence.

Public controls use DataFrames, actual schemas/null counts/checksums, default optimizer. Rename is the supported alias case. Wide keeps a GetField and an uncaptured arithmetic sibling over passthrough idx; both outputs observed. Many observes N distinct field outputs, not a width/single-output substitute. The source shape does not assert a private optimizer branch was selected. Timers labeled planning_only or plan_collect_validate_end_to_end. External CPU/RSS includes fixture construction and correctness warmup; none is a hot-kernel, PMU, ALU, SIMD, cache or disk-throughput measurement.

Protocol seed94020261024, four balanced independent baseline/fix process pairs per case. Original trial completed64processes, then the first candidate many256/40planning-iteration child reached its15-second cap. That is retained as a CENSORED FAILURE with incomplete original throughput gate, not replaced by a successful shorter run. The first runner did not preserve partial stdout/stderr for that timed-out child; its command/protocol/censor receipt and all completed raw processes are retained. Exact-owned process check afterward found no live child.

Root authorized ONE completion-only protocol, unchanged binaries/source: many256 with TWO iterations on BOTH sides plus the two originally unrun end-to-end controls. It completed24processes in19.47s, all public correctness checks passed. These reduced-iteration observations are a separate metric, not a substitute for the original40-iteration gate or evidence of linear scaling. No further repeats or variants were run. Whole fixed test suite4/4 passes,0ignored/filtered.

Ratios are fixed/baseline means over four processes per side; small-case noise is not presented as a speedup claim.

| Public control | Wall ratio | Whole-process CPU ratio | Whole-process peak ratio |
|---|---:|---:|---:|
| Rename planning16 fields |0.936|0.731|0.998|
| Rename planning64 fields |1.006|1.007|0.996|
| Rename planning256 fields |1.003|1.002|0.995|
| Wide+arithmetic planning16 fields |0.828|0.856|0.995|
| Wide+arithmetic planning64 fields |0.827|0.835|0.989|
| Wide+arithmetic planning256 fields |0.827|0.830|0.975|
| Many-output planning16 |0.991|0.990|0.997|
| Many-output planning64 |1.005|1.005|0.999|
| Many-output planning256, SEPARATE TWO-iteration completion |1.018|1.013|1.083|
| Rename end-to-end100k rows/64fields/8iterations |0.988|0.974|1.003|
| Wide+arithmetic end-to-end100k rows/64fields/8iterations |0.841|0.880|0.997|

Many256 is a disclosed memory regression: process peak152.47→165.09MB, +12.62MB/+8.3%. Its two-plan observed window1.558→1.587s; whole CPU2.359→2.389s. Many64 already costs about2.01s per40plans in both libraries; the original256 timeout and reduced completion show expensive existing planning scaling. There is NO end-to-end linearity claim and no attribution of all that cost to this six-line branch. No resource win is inferred from the original failing schema-replacement cases. Wide passing observations improve, while other large passing wall/CPU controls are approximately flat; peak regression and censored original gate require explicit reviewer assessment.

Shared source/target/timing was released immediately after completion, before packaging. Final af8-library composition remains mandatory before acceptance/merge. Transparent Vail route remains separately rejected/unmerged until its own reviewed composition and complete regression/resource gate.
