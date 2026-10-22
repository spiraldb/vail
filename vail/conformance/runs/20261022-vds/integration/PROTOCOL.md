# Aligned matrix map experiment protocol

Base source: a5415231 (reviewed VDS source06f47aa9 plus metadata).
Separate managed worktrees for runend and mutable-hole approaches. No main merge.

Compare unchanged baseline and both implementations in release builds with the
same Rust toolchain, Arrow59.2.0/DataFusion55.0.0 and compiler flags. All target
directories and Python packages are private; shared native binaries untouched.
Record native hashes and source diffs. Compilation is not a measurement.

Common public-API harness: compare.py. Each process records plan time, per-action
wall/CPU time, process peak RSS, exact result digest and expected-value checks.
Run end-to-end VDS (81variants x5samples) plus numeric512x8, nested512x8, and
nested512x128 matrix maps with row/column/global captures, strings and filtered
entry slots. One warmup plus three measured actions. Workload sizes may be
bounded if baseline execution is impractical; identical sizes must be used for
all alternatives. Timings run serially after both experiment builds finish.

Validate public aligned-map tests, missing/filtered entries, lazy CASE branches,
shared expression DAGs, empty rows/lists, repeated action evaluation and concurrent
independent evaluation where feasible. Compare digests against baseline; VDS
also has independently extracted pinned-Hail golden output.

Runend: construct actual run-end row/global metadata and evaluate the body once
per flattened input batch. Report native-kernel support, any decode fallback,
when decoding happens, and expanded-memory costs. Do not describe a decoded
fallback as fully compressed execution.

Holes: expression-local bindings owned per evaluate(batch), compiled tree bound
once, scalar row inputs updated between sequential row-list evaluations. Hash
and equality must not depend on mutable values; no shared cross-call slots.
Record synchronization or copying costs rather than hiding them.

Peak RSS is process-wide, including engine/runtime. Record shared-host and build
limitations. These measurements choose an approach; they do not establish full
VDS capability, Hail throughput acceptance, or main-merge approval.

After initial small cases pass, include nested4096x128 (524288 nominal entries)
serially for all three release variants to distinguish fixed action overhead
from sustained map throughput and observe expanded-metadata peak memory.
