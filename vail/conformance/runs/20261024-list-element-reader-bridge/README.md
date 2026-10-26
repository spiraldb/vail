# Typed list-element validity reader: exact public gate

Runtime source bd97e7b98676051fce1b0b2d9067f7276f3c34ea (private-equivalent35e69188), base acceptedaf8ae9c7. Normal native-only/jobs2/locked target build. Initial299.46s build compiled production but failed two authored resource-test compile errors. The corrected test-only relink completed22.112s. Whole encoded_projection target:30passed,0failed/ignored,0.33s. Exact artifact/source hashes and raw logs are preserved.

Public writer/reader coverage adds direct Bool map/SumMap, observable logical ancestors, repeated same body, original raw sibling, distinct body sibling, bounded row range with sparse mask and exact declared types. Shared-DAG preparation depths8/16/30 observed4.58/5.08/0.92microseconds, with independent depth6 values. These are tiny preparation-only observations, not end-to-end scaling or throughput claims.

**NO payload-read reduction was demonstrated.** The4,214,260byte fixture requested4,277,547bytes for BOTH raw full read and presence SumMap, with4,214,252unique bytes for both. Open-observed65,535bytes can overlap asynchronous prefetch. These are application-reader requests/completions, not phase-isolated physical-device IO. Scan+result materialization/check observations were0.924/0.806ms in one concurrent unit-test process; no throughput speedup claim. Root layout Debug shows Zoned but omits its child layout; full nested layout remains unestablished.

No storage capability/translator/native admission changes, no selected-owner proof, no new native wheel, no entry-count speedup or terminal count operator. Reader cache is reader-lifetime and grows with distinct exact expression identities; it does not retain values/masks/output data, but selector/schema hashing still occurs on lookups. Original nested-element fallback traversal is not a universal DAG-linearity guarantee.

The shared target/source was released immediately after the whole30 run to the independent DF fix-only final gate. No main update has been made by this agent; independent final review is required.

## Route-proof correction (source inspection,2026-10-26)

The initial30test fixtures used bare WriteOptions. Pinned WriteStrategyBuilder defaults list decomposition OFF unless VORTEX_EXPERIMENTAL_LIST_LAYOUT=1. TableStrategy then routes List through its leaf writer rather than ListLayoutStrategy. The initial root Debug is insufficient to reconstruct its full children, and these tests do NOT establish execution of the newly modified ListReader. The passing results and equal bytes remain valid ordinary-fallback observations. This receipt is not final acceptance of the reader optimization.

Actual Vail storage writing already explicitly enables structural lists (vail-storage/src/lib.rs488 and patched Iceberg vortex_writer.rs208–217). Test-only87937ebe changes the four new reader fixtures to use the public structural writer and records both flat/structural variants plus compact actual layout-child IDs in the IO receipt. That corrected source has NOT been built or run. It requires independent review and an explicit cached relink slot after the DF final gate. No native admission was expanded.

## Actual structural fixture gate (not yet successful)

Exact794a2127 relink completed26.07s. Whole30:25passed/5failed in0.11s. Every structural fixture failed during writing with `Layout encoding vortex.list not permitted by ctx`, before reading. Default control now explicitly shows root Zoned→Flat and the same requested bytes. Actual structural ListReader execution remains unproved. Test-only27695137 mirrors Vail's public dedicated structural-list edition opt-in (register/enable only vortex.list, no disable-editions bypass); that correction is source-only and awaiting independent review/build authorization.

## Actual edition-corrected structural reader proof

Exactf99894de8c8d93fe908e302f0c8b6cb4e0b3e55c uses the same public structural-list edition declaration already enabled by Vail's writer; all other edition enforcement stays enabled. Normal cached relink24.06s. Whole30target:30passed/0failed/0ignored in0.13s. The actual layout outline is Zoned→List→Struct→nestedStruct with independent flat validity children and payload fields. The direct map, sums, logical ancestors, raw/distinct siblings, bounded mask, and shared-DAG correctness controls now exercise real structural files.

Application-reader traffic: flat control still4,277,547requested bytes for both queries. Structural full read4,277,895requested/4,221,520unique; structural presence SumMap2,180,011requested+unique bytes,49.04%fewer requested. This is a partial payload omission result, NOT bitmap-only: about2MBremains and source audit is pending. Both public query outputs match independently calculated presence counts.

These are one concurrent unit-test execution, not a controlled throughput benchmark. Tiny scan+materialization+checks timings and interleaved DAG-preparation diagnostics are retained raw but do not establish wall/core-second speedup. Open snapshots can overlap prefetch; these counters are application-reader requests/completions, not physical device reads. No native capability/translator/admission expansion, selected-owner bound or new native wheel. Target released immediately after runtime.
