# Unrun matched cloud resource gate

No VM is started by this proposal. Root must authorize the exact runtime protocol
and normal Linux candidate build before execution. No local benchmark is proposed.

## Artifacts and input

Use the existing exclusive `vail-bench-20261021` VM with a bounded provider STOP
and guest shutdown guard. Preserve the accepted baseline environment/native from
`/home/danielking/vail-presence-20261026`: source8dcd, normal Linux native SHA256
`5a8613c794bbb932bfc4711f2d041a0ee850239fede532eb84120ed163f77ef2`, production-equivalent
to this candidate's dc13 base. Reverify saved frontend/native/input metadata
hashes and absence of PYTHONPATH before any action; stop on mismatch.

Build source `00a8d7d7` normally into a separate environment using the existing
cloud cached target, native flags/kache and bounded jobs; preserve exact Linux
wheel/native/frontend hashes. No native-library copying or baseline replacement.
The new Linux artifact must pass the relevant public correctness gate before
measurement. Root chooses the build budget from current warm-target state.

Existing real input only:
`/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt`.
Metadata dimensions are24,885×2,535 (63,083,475 logical cells before row filter).
Use saved input metadata fingerprints; do not generate a synthetic replacement.

## Same consumed public query

```python
m = h.read_matrix_table(path)
m = m.filter_rows(m.alleles.length() == 2)
def coordinate_query():
    return m.aggregate_entries(h.agg.array_agg(lambda x: h.agg.sum(x), m.AD))
```

Every completed action must return the full independently established output
`[315761156, 29304213]`; stop on a constructor, plan or action failure. A private
plan may be saved diagnostically, never substituted for public output. Do not
change coordinate bodies/field names/options to make admission succeed.

An unchanged-path control is `m.aggregate_entries(h.agg.sum(m.DP))` on the same
fixture. Freeze its complete result from the first baseline action and require
identical subsequent baseline/candidate results. This checks a neighboring
entry-heavy pipeline that does not enter the new admission; it is not an
independent DP correctness oracle (the public suite supplies that).

## Bounded balanced comparison

Use independent processes with the identical driver/environment settings for
baseline and candidate, balanced order B,C,C,B repeated twice (eight processes).
Each process performs one untimed warmup for each query and four consumed timed
actions of each query: ten total actions, at most12. Record per-action elapsed
wall time and process CPU time (all process threads), full outputs, and sampled
whole-process RSS. Record phase boundaries so construction/warmup are excluded
from action latency; whole-process RSS includes them and input backing memory.
No RSS claim is based on an allocation counter or an old profile.

Retain the accepted watchdog constraints: each action10seconds, RSS6GiB,
external process60seconds, owned process groups with bounded TERM→KILL cleanup
and failure receipts. Stop on any failed guard/output/action; do not tune/retry
variants. No other workloads overlap measurement. Report every process/action,
paired wall and core-second ratios and process memory; keep adverse controls and
state uncertainty if the short durations are noisy. Do not substitute the older
0.64second baseline for this matched run or promise an improvement.

After an accepted timing gate, a separate explicitly authorized single candidate
CPU sample could reuse the reviewed18d protocol: unprivileged cpu-clock:u99Hz,
DWARF, requested3seconds after the first successful ACTION, maximum6seconds/
12actions, the same10second/6GiB/external60second guards. Any temporary guest
perf setting requires its existing captured-original/finally-restore-and-verify
protocol and separate root GO. No such setting or profile is implicit here.

Download/hash all small receipts and stop the VM proactively when work ends.
Source/result review decides whether the change is worthwhile; this protocol is
not a plan to expand the benchmark scan or broaden admission.
