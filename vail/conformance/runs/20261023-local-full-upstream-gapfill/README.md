# Exact cached-input gapfill plan

Initial full108inventory run remains immutable and uses frozen source1c00d2d6/native3f8b5061. Its cached Vail BGEN index is incompatible with Hail's reader: Hail reportsNegativeArraySizeException inIndexReader. These four Hail setup failures are not a Hail performance regression and are excluded from paired ratios.

After the initial three-repetition timing phase finishes (never concurrently with its query CPU):

1. Create a Hail-specific raw BGEN symlink path, generate its native Hail index, verify publicimport count. Preserve the original Vail raw/index path.
2. Materialize Hailprofile25 andmanyints once from existing cachedrawVCF/TSV; reuseexistingVailnativefixtures.
3. Materialize canonical10Mrange tables at10/100/1000partitions once perengine; aliasesht/ht1/ht2 shareidenticalupstreamfixturevalues.
4. Under2GiBdiskreserve/48GiBRSS/1800sperfixture, run54affectedexactcases3repetitions. Reuseonlyalready-successful Vail samples from initialphase with matching exactcommit/nativehash, so info_score's3×80sdoesnotrepeat unnecessarily. FreshHailsamples execute3times.
5. Renderlatest overlay by selectingexactlyone sample perengine/case/repetition; followupcasesreplaceinitialreports. ReusedVail samplesretainoriginalrawtiming/telemetryprovenance and are neverdoublecounted. Initialbadindexfailuresremainarchived.

No fullgnomad/randomdoubles/manystrings generation/download is included. Those15caseusesremainexplicitmissingfixtures. No representative substitute. Stagehelperrefuseswhileinitialtimingrunneractive;source/artifactremainfixedthroughbothphases. This preparation was syntaxchecked only; runtimefixture/index receipts andfinalbenchmarkresults willfollow.

## Runtime setup and phase start

All9boundedpreparationscompleted successfully: separateHailBGENindex249115variantsverified; cachedrawHailprofile25/manyints; canonicalranges10/100/1000partitionsforbothengines. No query CPU ran during preparation. Storage used~1GiB;5.2GiBfree atgapstart.

A one-case validation exercised actualHailsetup/reusedVailreceipt and per-case scratchcleanup. GapHailSpark/Hailtempfiles now live in an explicit owned `rep-*/scratch/engine/case` directory, removed by the supervisor only after worker/JVM reaping. This prevents repeated-worker scratchgrowth without deleting unproven sharedtmpfiles. Per-caseHail.log is retained with telemetry. This affects temporary locations/cleanup, not query logic or inputs.

Actual54casegapfill started, same1c00d2d6/native3f8b5061,3freshHailrepetitions and needednewVailrepetitions. ExistingVailPASS samples reused only withmatchingexactsource/artifact; rawprovenance retained. Gap uses remaining time from the initial6hbudget,2GiBdiskreserve,48GiBRSS. Receipts, validation, and actual invocation/provenance committed; complete timing results follow. Initial full108inventoryphase3repetitions is immutable and remainsarchived separately.
