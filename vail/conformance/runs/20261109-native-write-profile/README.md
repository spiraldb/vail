# Native-write profile, 2026-11-09

A bounded laptop profile of the original upstream `test_write_profile_mt` read-and-write body on the existing 1,000-row × 2,535-column, four-partition native fixture. The normally built release extension uses mimalloc (native SHA `d7119f22`, source `1abc1ed86`). One warmup precedes a 3-second, 1 ms macOS `sample` window; the worker completes 26 writes over roughly five seconds. Imports, fixture generation, and output deletion are outside the sample; all outputs are deleted after worker exit. A Rust build overlaps this diagnostic, so these results establish call-stack attribution, not latency, throughput, or CPU-budget comparisons.

## What stands out

The sample contains 868 rounds across 33 threads, or 28,644 leaf observations. Removing explicitly identified parking syscalls leaves 6,848 nonparked leaf observations; this does not establish that every remaining observation represents on-CPU execution. Counts below are descriptive samples, not measured CPU-time fractions.

| Function or stack | Observations | Interpretation |
|---|---:|---|
| `IntegerStats::generate_opts_fallible`, self | 1,252 | Compression statistics are the largest individual nonparked leaf. |
| Primitive min/max, self | 495 | Additional value/statistics scans. |
| Bit-width histogram, self | 486 | Compression encoding work. |
| `__fcntl`, self | 387 | Caller chains include durable file close and immutable metadata writes. |
| Normalization, inclusive | 86 | Present, but not a leading cost in this sample. |
| Nested schema evolution, inclusive | 69 | Rebuilding transformer/schema state is not the dominant issue here. |
| Field-ID metadata adaptation, inclusive | 10 | Minor in this sample. |
| Key certification, inclusive | 1 | Not a demonstrated bottleneck here. |

Inclusive categories overlap and must not be added. Sequence-ID bookkeeping has 3,992 inclusive observations, but 3,845 end in parked calls; only 147 are nonparked. Presenting its inclusive count as CPU work would be misleading. The sample also includes decoder/read-side work, since the original benchmark reads the native matrix before writing it.

## Code analysis and decision

`ArrayAndStats` already caches typed integer statistics once per compression-site bundle. Derived arrays correctly get new statistics, and sampled scheme estimation and full-data encoding are different inputs. No repeated computation on an identical immutable array was proven. Sequence-ID registration/locking operates per layout/chunk, not per input value. The local file close path flushes then calls `sync_all`; immutable metadata writes also sync. They must not be silently removed to improve a benchmark.

The writer does reconstruct `RecordBatchTransformer` and nested schema adapters per batch, and Vail normalizes both before and inside the storage writer. Those are possible future preparation improvements, but this profile does not justify prioritizing them or claiming they explain the full-suite write slowdown. Observed compression/statistics stacks are more frequent.

**No production change is proposed from this diagnostic.** No small, evidence-backed redundant-work fix was found. Changing compression policies or broader Vortex sequence scheduling would be separate work with compression-ratio, read-throughput, and durability tradeoffs; this investigation does not alter those policies.

The first sandboxed sampler failed to attach (exit 255), while its worker succeeded. A fresh same-user unsandboxed run succeeded without sudo or host-setting changes. Both worker and sampler exited successfully and the temporary output tree was removed. Raw profile paths and SHA-256 hashes are recorded in `summary.json`; raw stacks remain in `/private/tmp/vail-native-write-profile-20261109-evidence/attempt2`. No build, benchmark comparison, cloud VM, push, or PR was performed by this agent.
