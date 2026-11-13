# Full supported native-format comparison, 2026-11-12–13

The human-readable [complete report](../../../../BENCHMARKS.md) covers 111 paired execution cases plus one separately labelled planning case. All 672 scheduled timing observations in the authoritative **attempt2** cohort passed: 666 execution measurements and 6 planning measurements. Current-Vail preflight passed 112/112. Engine initialization is excluded; original first-action query construction, planning/compilation and actions are included. Each engine uses its own native persisted format; format-independent cases use common inputs.

`RUN.json` records the cohort and integrity scope; `summary.json` contains three-sample medians/ranges and every loss; `samples.csv` preserves each timed observation. Ratios require three eligible measurements per engine. The packet is execution evidence, not full cross-engine value/schema equivalence. The prior 39 policy exclusions and 85 supplemental omissions remain explicit in the protocol/report.

## Authoritative evidence

- `protocol-attempt2.tar.gz`: frozen 112-case pool/schedule, upstream bodies, policy/tiers, ownership/measurement/stage code and metadata-only runtime gate. Paths are specific to this isolated VM and must be adapted explicitly to repeat the run.
- `runtime-gate-attempt2.tar.gz`, `enroll-attempt2.tar.gz`, `preflight-attempt2.tar.gz`: full runtime initialization, normal installed artifact identities, complete canonical input SHA/stat roster and 112 passing preflight actions.
- `rep1-attempt2.tar.gz`, `rep2-attempt2.tar.gz`, `rep3-attempt2.tar.gz`: all three fresh-process matched repetitions, raw measurements, commands and positive worker/stage cleanup receipts. The small admission JSONs are local packaging/cleanup checks, not separate correctness tests.
- `final-attempt2.tar.gz`: final complete 17,593-file content rehash/canonical-set/stat check and unchanged runtime/artifact identities.
- `post-pack-seals-attempt2.tar.gz`: finally-written stage-success records, archive metadata and final protocol asset reconciliation. Stage archives are packed before their outer finally writes its own result, so that result is preserved here.
- `provenance/`: observed guest guards/compiler-idle checks and selected provider status/stop-operation receipts. Full provider receipts remain local; network/SSH metadata is omitted from these status projections. Provider status was positively verified as TERMINATED.
- `source-lock.json`, `source-manifest.json`, `bootstrap.sh`, `startup-guard.sh`: source/build and shutdown provenance. The code is available at the recorded Git commit; compiled binaries are not included in this packet.
- `helpers/`: exact local reduction/rendering/admission and metadata-collector scripts retained for audit. Their original host paths/layout are recorded in source and require adaptation outside that workspace. They do not modify benchmark bodies or add timed forcing actions.

## Retained setup history, excluded from ratios

The normal wheel built successfully but failed import under the default static-TLS reserve. An initial 65,536-byte shared reserve allowed ordinary imports but failed full Hail JVM startup (process-reaper StackOverflow/fixture timeout). That cohort was stopped and aborted; its two recorded observations were never pooled into attempt2. `runtime-recovery.tar.gz` and `aborted-timing-attempt1.tar.gz` retain the failures. Attempt2 used a 16,384-byte reserve for both engines, passed full Hail init/stop and Vail import, and repeated all initial gates and all three timing rounds. No source rebuild or fixture-data change occurred.

`fixtures-hail.tar.gz` and `fixtures-native.tar.gz` retain the original generation and 14 count/prefix conversion checks. Original Hail generation ran without the loader tunable; native conversion used the earlier reserve. The data files' complete SHA/stat identities were unchanged in the authoritative attempt2 enrollment and final rehash. Counts/prefixes are limited validation, not full conversion equality.

`enroll.tar.gz` is the superseded setup roster that omitted sidecars. `enroll-sidecars.tar.gz` corrected required BGEN index/present VCF tabix roots before timing; `enroll-attempt2.tar.gz` is authoritative for the final cohort. Intermediate protocol archives preserve that chronology; **only protocol-attempt2** defines the final timing cohort.

Verify the retained file bytes with `shasum -a256 -c SHA256SUMS`. See `FINAL-INTEGRITY-ACCEPTED.json` for the independent final report/seal review.
