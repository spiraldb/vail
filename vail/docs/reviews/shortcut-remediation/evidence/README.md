# Intermediate validation evidence

These are intermediate build6 observations, not final acceptance or approval.
The matching wheel was extracted into `/private/tmp/vail-shortcuts-package6`;
its native library SHA256 is
`e08932277a6f2eb853b8a5c63cfce8ac9ae9523e6b2b5b33ad71fe93563bcff5`.
The Python source workspace subsequently gained a native method and must not be
mixed with that older binary. Run the profile scripts with the matching package
on `PYTHONPATH`. Each script takes a row count and writes files under `/private/tmp`.

`vcf-profile` includes VCF input sorting. `vcf-native-profile` uses generated
keyed rows, but localization still sorts them. `vcf-unkeyed-profile` removes the
row key on an already ordered generator to isolate the streaming sink. All keep
20 sample columns constant while increasing rows. RSS
is whole-process macOS peak bytes, not a measurement of the sink alone. Timing
was observed while other Rust compilation jobs ran and is not a controlled
throughput comparison. Successful output is atomically published on completion.

The focused run has 30 passes and 1 failure, an actual randomness/scan consumer
distribution-contract failure. That failure and the separate externally timed-out
depth64 planning test still block acceptance; planned fixes require build7.

The isolated unkeyed source used 169.3 MB peak RSS for 100k rows and 221.2 MB for 2M rows.
These are whole-process peaks and show 20x row growth without corresponding memory
growth; they do not eliminate the keyed pipeline's upstream sorting cost.

Build8 subsequently passed 246 native tests and failed 2 old function-display
assertions. Replacement tests check actual distinct values and missingness; they
await build9. Full Python build8 had 1,333 passes / 10 failures (details in its log).
Review remains REQUEST CHANGES until the later scalar capture, scoped binding,
and predicate-once fixes are rebuilt and checked.

Intermediate rejected full-suite runs are summarized in `intermediate-validation.json`
with hashes and local raw-log paths. The final candidate10 logs are kept in full.
