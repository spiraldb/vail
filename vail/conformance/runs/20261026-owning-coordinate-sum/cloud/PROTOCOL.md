# Frozen executable H089 resource protocol — NOT RUN

Root authorization and independent protocol review are required before starting
or querying a VM. No script starts a VM or changes perf/privilege settings.

## Fixed source, artifacts, input and queries

`expected.json` freezes candidate production00a8d7d7, its2,097 Rust/Python/Cargo
source fingerprints, and all69 frontend-file fingerprints for both versions.
Baseline is the existing normally built Linux source8dcd426b/native5a8613…
environment. `baseline-source-equivalence.json` records an empty production diff
from8dcd to current maindc13. No dependency/PCO-stack adoption is involved.

Candidate Linux native bytes cannot be known before normal compilation.
`prepare.py` verifies the full frozen source manifest, builds/installs one normal
wheel into a fresh environment, then runs ALL8 public Rust ownership tests and
ALL435 public Python cases. The Rust gate includes the combined excluded outer
gap + included null inner hidden-span + unused-sibling allocated-byte fixture;
real AD/DP RSS cannot substitute for that test. Only after both gates pass does
it lock the exact new Linux wheel/native fingerprints and verify every installed
frontend file. The measurement runner rejects absent gates or changed locks.
No copied native binaries or imports through PYTHONPATH are used.

Before building, `preflight.py` verifies original baseline native/frontend and
three fixture metadata fingerprints, opens only the original matrix/schema and
requires the original DP Int32 control and AD Int32-array field. Missing or
changed DP is a STOP, never a substituted control. No dimension count query is
added;24,885×2,535 is a stored-metadata statement.

The original fixture path and exact public actions are frozen in expected.json
and worker.py. Row filter is `m.alleles.length()==2`.

- Coordinate action: `m.aggregate_entries(h.agg.array_agg(lambda x: h.agg.sum(x), m.AD))`.
  Expected `[315761156,29304213]` is the **previously recorded full baseline
  output**, not an independently derived oracle.
- Unchanged-path control: `m.aggregate_entries(h.agg.sum(m.DP))`. Its complete
  first baseline warmup result is recorded, then required for every subsequent
  baseline/candidate action. It is an empirical reference, not an independent
  DP oracle. No replacement query is allowed.

## Fixed order and independent units

Independent process order is exactly B,C,C,B,B,C,C,B: **four independent
processes/version**. Each process runs coordinate then DP once as untimed
warmups. Four timed rounds follow, in query order coordinate/DP, DP/coordinate,
coordinate/DP, DP/coordinate. Thus each process has ten actions total and four
repeated timed observations per query. These are **not16 independent trials**.
Every full result is consumed, checked and recorded. No query/body/field/action
count adaptation or retries follow failures.

Both versions use exactly the expected.json environment: BLAS/OMP/MKL/NUMEXPR1,
Rayon32 and Tokio32; the VM must expose32 CPUs. Driver/settings are identical.
Action wall time and process CPU time (all process threads) exclude construction
and warmup. Per-process medians summarize each set of four repeated actions;
four adjacent balanced process pairs supply the reported ratios. Every raw
observation remains available; no speedup/CI/asymptotic claim is assumed.

RSS is sampled from /proc every50ms and also recorded through Linux ru_maxrss.
The process peak includes construction, both warmups, both workloads and input
backing. It is not per-action incremental allocation or coordinate-only memory.
Monitoring overhead is not subtracted. The separate public allocated-byte test
has its own explicitly narrower consumer scope.

## Guards, budgets and cleanup

Each query process is capped at60seconds externally; in-flight ACTION starts
are watched for a10second limit, and whole-process live RSS for6GiB. A completed
action also checks10seconds/cumulative6GiB. Complete fixed outputs/actions are
mandatory. All subprocesses have owned groups and outer-finally bounded
TERM3seconds→KILL3seconds cleanup, with status/failure receipts. Failed cleanup
or any constructor/output/guard/action failure stops the protocol without retry.
No other compiler or measurement workload overlaps timing.

Normal build uses the accepted `/home/danielking/vail-presence-20261026/target`,
kache, native CPU flags, incremental0 and8 jobs. Budgets are: venv90seconds,
dependencies180, wheel900, install90, complete Rust8 gate300, public435 gate120,
and two artifact probes30each. Baseline schema preflight30 precedes the build.
The host caps all preparation at1800seconds, and comparison at510seconds.
Every stage has a10GiB disk floor. A timeout is failure, not permission to
restart a cold graph or choose another variant.

Root sets a provider STOP bound of at most45minutes and an earlier guest safety
shutdown before starting the exclusive VM. host_run.py verifies the provider
STOP configuration and RUNNING status. It does not change configuration/start
anything. In its outer finally, it packs/downloads/hashes receipts only (never
venvs/wheels/native binaries) and **requests proactive VM STOP even on any gate,
query or download failure**, then verifies TERMINATED. If API/download fails,
that failure is reported; provider STOP remains the safety bound and remote
receipts persist on disk. No successful cleanup is claimed without verification.

## Commands after explicit root GO

Upload the reviewed protocol and candidate production archive only after root
starts and hands off the VM. Use the existing stable source path with a verified candidate overlay to reuse
the accepted cached graph; do not change the baseline environment. The candidate archive must
satisfy expected.json exactly. Keep the stable target as specified above.

On the host, Python3.12+:

```sh
python cloud/host_run.py \
  --remote-source /home/danielking/vail-presence-20261026/source \
  --remote-protocol /home/danielking/h089-matched-20261026/protocol \
  --remote-out /home/danielking/h089-matched-20261026/run \
  --local-out /private/tmp/h089-matched-20261026-receipts
```

Source/root paths are frozen operational locations, not benchmark dispatch.
The host wrapper uses the configured gcloud projectpyspiral-dev/us-east1-b and
VMvail-bench-20261021. No current VM command or benchmark has been executed.
