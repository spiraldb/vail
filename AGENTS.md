# Batch execution is mandatory

Standing user instruction (2026-10-22): **NO per-row or per-entry configuration,
setup, compilation, or preparation.** This is a hard performance requirement.

Build parsers, state machines, regexes, schemas, expression evaluators, kernels,
and other execution machinery outside row/entry loops. Reuse prepared machinery
across Arrow batches. Use batch kernels and Arrow buffers; do not replace batch
execution with scalar interpreter dispatch or temporary objects for each value.
Primitive traversal of input buffers must not perform configuration, setup,
compilation, or preparation for each row or entry.

Implementation and review must inspect hot loops and their callees for hidden
setup, including constructors and helpers that rebuild machinery. Profile the
actual workload and verify that the offending setup disappears from the hot
path before claiming a performance fix. Apply this requirement to adjacent
execution paths touched by the change as well.

# Build caching

Standing user instruction (2026-10-22): use **kache** for Rust build caching.
Do not manually save, copy, hard-link, or reuse compiled `.so` files as a build
cache or artifact-saving workaround. Build normally with kache and use the
result of that build for validation. If builds are too slow, tell the user so
that a better workflow can be agreed; do not invent artifact-reuse schemes.

# Required review before main

Standing user instruction (2026-10-15): before committing or merging code into
`main`, obtain a review from Codex thread
`01a14128-21b8-7a53-b399-4ad8f9b31f4f`
(codex://threads/01a14128-21b8-7a53-b399-4ad8f9b31f4f).

Send the concrete candidate changes and validation evidence to that thread.
Address review findings and obtain follow-up review for material fixes before
updating `main`. Record the reviewed revision and disposition of findings.
A review of an earlier revision does not cover subsequent material changes.
If review cannot be obtained, leave the work on its development branch and
report the blocker; do not bypass this requirement.

Development-branch commits are allowed to make work concrete and reviewable.
Subagents must not commit to, merge into, or otherwise update `main` themselves.
