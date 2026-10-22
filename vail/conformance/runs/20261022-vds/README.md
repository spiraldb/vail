# VDS attempt disposition

Stopped on the development branch at the user's performance boundary. This is
an experimental partial port, not a completed VDS module or a main candidate.

The implemented algorithms use ordinary outer joins, `multi_way_zip_join`,
existing array expressions, and ordered scans. No Hail IR emulation, native
operator, general UDF API, or Python genomic-row fallback was introduced.
`MatrixTable.distinct_by_row` needed a small fix to preserve an existing physical
key index when validating a matrix after write/read. Its two existing focused
regressions pass, as do 48 VDS/functions/stream tests.

The direct dense translation matches the fresh independent Hail golden on all
81 variants of the five-sample fixture. Hail's entry struct field order depends
on Python set iteration, so comparison checks fields/types and named values,
not that unstable ordering. NaNs compare equal. Original Hail storage was
converted only by the test fixture generator; production reads have no adapter.

Exploratory timings are recorded in the JSONL files. Hail execution took
1.57/0.41 seconds; Vail took 51.27/44.57 seconds on equivalent fixture values.
These are shared-host smoke measurements, not a controlled throughput study.
They do establish that this attempt has not met the requested speed goal.
No performance improvement is claimed. Further work needs investigation of
planning/execution costs; no particular bottleneck is asserted without a profile.

A full module also needs Hail-native MatrixTable storage interoperability,
the restartable combiner, and the advanced APIs listed in the module README.
The attempt did not add those large features after encountering the stopping
condition. The new module explicitly rejects legacy Hail storage and requested
VDS partition planning rather than silently emulating them.

The general struct-annotation missingness fix explored during development was
reverted because it affected existing stream key preservation. The final VDS
implementation guards a missing reference entry explicitly before supplying
a default genotype; the existing stream tests pass unchanged.

See `summary.json`, the module README, `tests.log`, `distinct-tests.log`, and
`conformance/fixtures/vds/provenance.json` for scope and reproducibility.

Review of `fc1abd16` found two correctness gaps: interval reads could omit
covering blocks beginning before the requested interval, and G-numbered allele
expansion could overflow int32 triangular intermediates. The follow-up rejects
interval reads entirely and bounds both global/local G allele counts at 32767.
Canonical LA mapping preconditions are documented. Nineteen targeted tests pass
(including seven new regressions); follow-up review is requested. These fixes
do not change the experimental-only disposition or authorize a main merge.
