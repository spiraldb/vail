# Matched cloud result: coordinate gain, unresolved DP wall regression

Frozen production00a8d7d7 and executable protocol91aa68d6, after the documented
exact-asset provisioning correction. **No extra trial, source variant or query
substitution was run.** VM is verified **TERMINATED**; downloaded archive and
all73 raw receipt-file hashes verify. Earlier failures remain separate.

## Correctness and artifacts

Normal Linux wheel/install, all8 public Rust tests (including combined hidden
outer/inner/sibling allocation and lifecycle/merge fixtures), and all435 public
Python cases passed BEFORE any timings. All2,097 source hashes,69 frontend
files/version, original fixture metadata and DP/AD schema checks passed.

- Baseline source8dcd426b, production-equivalent to current maindc13b054; native
  `5a8613c794bbb932bfc4711f2d041a0ee850239fede532eb84120ed163f77ef2`.
- Candidate source00a8d7d7; native
  `6cb1fe232e4b352e1f41caf92530445ec58cfa364ddbdf93c58a7c667cf051ec`.
- Candidate normal wheel
  `ee0b7ca594939fa2a2cb792df4aa47fff331385c65a60faa02c8252128b9830c`.
- Exact Linux lock is raw/gate/artifact-lock.json. Gate completed in42.13seconds
  on the existing normal cache; this is build/test time, not query throughput.

## Fixed matched measurements

Eight independent processes in B,C,C,B,B,C,C,B order: **four/version**, each with
two warmups and four repeated timed observations/query. These are not16
independent trials. Every one of80 total consumed actions passed full-output
checks;64 were timed. AD output `[315761156,29304213]` is the previously recorded
full baseline output, not an independent oracle. DP output345065369 was frozen
from the first baseline warmup and matched throughout, also not an independent
oracle. The original63M-cell native fixture and biallelic row filter were fixed.

The table reports medians of the four per-process medians/version. Ratios are
baseline/candidate; all raw observations and the four adjacent balanced pairs
are retained in raw/comparison, actions.csv and analysis.json.

| Query | Baseline wall | Candidate wall | Wall ratio | Baseline core-sec | Candidate core-sec | CPU ratio |
|---|---:|---:|---:|---:|---:|---:|
| Global AD coordinate SUM |0.68743s|0.47603s|1.444x|15.76634|10.58486|1.490x|
| Unchanged-path DP SUM control |0.41298s|0.44483s|0.928x|9.90029|9.96375|0.994x|

AD wins in all four paired comparisons:1.423–1.542x wall and1.481–1.523x CPU.
This is approximately30.8% less wall time and32.9% less process CPU time.

**The DP control is adverse and unresolved.** Candidate median wall time is7.7%
higher, and every paired DP wall ratio favors baseline (0.863–0.966x).
Candidate median CPU is0.6% higher; individual CPU pairs range0.963–1.023x.
Do not label the control flat, clear it as noise, or infer no broader regression.
No cause or independent DP-only behavior is established by this interleaved run.

Median whole-process peak RSS falls from3,898,535,936 to2,855,649,280bytes
(3.63→2.66GiB,26.8% lower). This includes construction, warmups, both workloads
and input backing; it is NOT coordinate-only or per-action incremental memory.
No IO/decode-byte saving, new CPU profile, Hail multiplier or scaling claim is
made. No perf permission/sysctl setting changed.

Existing-data descriptive stratification in descriptive-control-order.json
shows DP wall0.402→0.453s following AD and0.416→0.436s following DP. Each category
has only two correlated observations/process; this does not establish a cause
or remove the overall control regression. It motivates assessment rather than
another unapproved trial. No rerun has been performed.

## Disposition

Correctness and coordinate-resource gain are concrete. Main/performance
acceptance remains pending independent review of the adverse DP control.
All results, including failures and control regressions, are preserved without
selection. VM is stopped; root alone decides further investigation or merge.
