# Current disposition

The user accepted the limited VDS implementation and requested integration on
2026-10-22. The initial stopped-attempt report below is historical, including
its debug timings and previous development-only review restrictions.

The accepted candidate includes batch aligned-map execution, cached kernel
hashes/scan descriptors, and direct fold-state finalization. No public backend
API was added. The supported subset and explicit unsupported APIs remain
listed in `python/vail/vds/README.md`; this is not a complete Hail VDS port.

Fresh serial release medians on the small five-sample fixture were 3.04s before
the final cleanup, 1.66s after it, and 1.54s for Hail. Host load varied strongly;
these do not establish a Hail win or general throughput. Larger synthetic
aligned-map results, memory costs, exact merged artifact provenance, runtime
tests and required review disposition are recorded in `integration/README.md`.

# Historical initial attempt

# VDS implementation and integration disposition

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

Follow-up review cleared the limited source/test fixes at exact
`06f47aa91456b36b8f2d721be62bb17a1bb78e8d` and closed both scoped correctness
findings. This is not merge approval or performance acceptance; the port remains
stopped on its development branch. Reviewer performed no builds, tests, or edits.
