# First matched baseline: unsupported public producer shape

Exact cde7af8252206b95fad2b0778040c015f0d99097 is main585 plus ONLY authored public tests. Normal relink26.135s. Whole34serial:31passed/3failed in0.30s. Every failed semantic fixture panicked while writing a nullable TOP-LEVEL Struct: FileStatsAccumulator requires NonNullable. No candidate production was built or run, and no reader result from those three cases exists.

Public correction45d48528 preserves each nullable record inside a nonnullable table envelope and projects the normal record field. Independent masks/oracles remain unchanged; no statistics/edition bypass or production change. That correction is source-only and requires review/new authorization before a new baseline/candidate run.

All raw output, exact commands/environment, executable hash and original protocol/source hashes are retained. The shared target was released immediately after failure for current-main work.
