# Local full upstream benchmark preparation (not run)

108 exact upstream cases discovered from benchmark_table.py + benchmark_matrix_table.py;37 fixture-free,4 upstream skips. All dimensions remain exact; no smoke/representative substitution.

Prepared runner reuses cloudworker harness (fresh isolated process per attempt, timeout+RSS guards,10Hz process-tree telemetry retaining exited descendants) with explicit existing-fixture manifest. Default forbids fixture downloads/materialization. `--allow-fixture-generation` is an explicit later control, not currently selected. Hail runs only when paired Vail passes;3fresh attempts each. JSON/control/telemetry only; query data remains native Arrow/storage.

Existing fixtures:
- raw manyints TSV1.3GB + BGZ682MB; Vail native table available.
- Vail many-partition10Mrow tables for10/100/1000 partitions; equivalent names may share fixture after matching schema/partition check.
- exact profile25 Vail v3 matrix807MB `/tmp/entries-profile25.mt`; raw214MBVCF `/tmp/vail-matrix-decode-count/fixtures/profile25/profile.vcf.bgz`.
- official BGEN143MB/sample/index `/tmp/bgen-throughput-data`.
- Hail matching representative grouped-collect20k matrix newly prepared by separate authorized narrowpair, NOT the fullgnomadfixture.

Missing full fixtures:
- exact randomgnomad250k×1000 native matrices forbothengines (6cases)
- random_doubles native/rawTSV (2cases)
- manystrings raw/native (7caseuses)
- Hail native manyints and partitiontables/profile25.

Current disk1.8GiB: full generation unsafe. Need substantial cleanup/space assessment before enabling missing fixtures. Historical twofixtures attempt524GBallocation aborted;96GiBhost cannot support unrestricted construction. Retain explicitresource-limit outcomes, do not silently downscale.

Freeze plan:
1. Root waits writer/reducers reviewed+merged; records exact finalmainSHA.
2. Normalrelease/kache build in own isolatedtarget into finalenv; verify nativeSHA/wheel/source, stockHailPyPIversion+Java11. Never sharetarget orcopySO.
3. Read-only compatibility smoke ofexistingfixtures, exactschema/count/metadata receipts; configuremanifest/cache links without regenerating compatibledata.
4. Decide missing-fixture resourcebudget. Entries remaininventory evenif unavailable; record unsupported/resourceblocked results separate fromtimingpairs. A runmissingfixtures is a fullinventoryattempt, not all108successfulcomparisons.
5. Finish builds beforetimings;serialenginecases; recordbackgroundactivity. HailSpark/Vailparallelism32matchesrecentlocalcomparisons, heap24g,48GiBprocesstreeRSS cap, query250s/fixture1800s. Upstreamxtimeoutcurrentlyoverridesdefault; documentfinitecasebound.
6. Three repetitions; wall/core/RSSmedian+ranges, individualN, H/Vratios/geomeans/ratioofsums, failuresseparate. RenderHTML+fulltable andchecksumartifacts. NoVM/cloud.

Example final command (DO NOT RUN until finalsource/space/fixturefreeze):
```
VAIL_BENCHMARK_UPSTREAM=/Users/danielking/projects/vail/hail/hail/python/benchmark/hail \
 /private/tmp/linreg-hail-pypi-latest/bin/python /tmp/full-local-upstream-prep-20261022/run_local_upstream.py \
 --vail-python FINAL_NORMAL_ENV/bin/python \
 --hail-python /private/tmp/linreg-hail-pypi-latest/bin/python \
 --commit FINAL_REVIEWED_MAIN_SHA \
 --upstream /Users/danielking/projects/vail/hail/hail/python/benchmark/hail \
 --data-dir /tmp/full-local-upstream-cache-20261022 \
 --output /Users/danielking/projects/vail/vail/conformance/runs/20261022-local-full-upstream \
 --fixture-manifest /tmp/full-local-upstream-prep-20261022/fixture-manifest.json \
 --repetitions 3 --cores 32 --heap 24g --rss-gib 48 --timeout 250
```

Only syntax compilation/inventory performed. Runner still needs finaldryvalidation/resourcepolicy and aggregationrenderer wiring; no fullsuite benchmark, build, download, or missingfixturegeneration performed by this preparation.

## Ready runner and validated blockers, updated October23

The runner has now been exercised end-to-end with normal positional nativeSHA041e67e865745f7c987d8f14b2daedda805c68f7c03e4f41ff3d4152f7797ca8 (source485b7e57) and stockHail0.2.139. `dry-final/` contains one fixture-free case passed on both engines, plus one missing exactgnomadfixture reported asunsupported andHailgated. Per-repetition interactiveHTML/10Hzmetrics, aggregateHTML/CSV, independentN/ratios, and explicitnonpassesJSON were produced. This is harnessvalidation, not a newbenchmarkcomparison orfullsuite.

The concrete runner defaults are now offline/existing-fixture-only, completion-marker-aware, serialengines,3repetitions,48GiBRSS,8GiBfreediskreserve,250squery/1800sfixture bounds,6hdispatchbudget. Disk/RSS are monitored at10Hz; resource exits preserve explicitstatuses. The dispatchbudget stops newcases at6h; an activecase retains its finitequery/fixturetimeout. Resume verifies source/config/native/harnesshashes. Query-completion shutdown waiting is .5s instead of5s; completeworker/JVMtree cleanup precedes nextengine. No fullrun started.

**Correction to earlier inventory:** the many-partition paths were incomplete/removed and cannot be reused. Manifestnow requires `_vail_table.json`, `_vail_matrix.json`, or Hail`_SUCCESS`. See fixture-availability.csv for all108exactcases. Current exact-ready counts (including4skips): Vail71,Hail43. No representativefixtures count toward these totals.

Budget stages:
1. Existingdisk/raw only:43cases potentiallypaired (4areskips), Vail71fixture-ready. Existing807MiBVailprofile andraw214MiBVCFreused, not regenerated.
2. Nativefixtures fromexistingraw plus3canonical10Mrange tables/engine: prepareHailprofile+manyints andrangefixtures once,aliasht/ht1/ht2 tosamecanonicalfixture. This can bring bothengines to93fixture-readycases. Storageestimate roughly1–3GiB additional plus transientSpark/writebuffers; require at least8GiBreserve AFTER fixtures, so target12GiBfree before thisstage. Estimate only; monitor enforcesreserve.
3. Remaining15caseuses:manystrings7,gnomad6,randomdoubles2. HEADchecked publiccompressedinputs: manystrings341,994,020bytes; randomdoubles1,001,296,960bytes. Both are absentlocally and offlinegeneration intentionally refusesdownloads. Gnomad250MInt32payload is1GB/engine beforestorage/validity/metadata; streamingbuildbounded but runtime unknown. Randomdoubles previously aborted on524GBallocation duringnativefixturematerialization; unrestrictedretry is inappropriate. Exactboundedattempt should remainexplicitfailure/resource-limit if unchanged. Do not downscale or label representativeasfull.

`prepare_cache_links.py` plans all availablealiases withoutcopying and skips missing/incomplete sources; `--apply` is explicit. `prepare_fixture.py` prepares only one explicitfixture, offline fromrawcache, under48GiBRSS/diskreserve/1800stimeout with process-group/JVMcleanup and receipt. It has not been invoked. No fixturegeneration is authorized until root confirms the diskbudget/finalplan.

Owncleanup: removed inactive `/private/tmp/range-join-env`320MiB and ownuntracked compilednative190MiB after paired evidence committed. Logical510MiB removal; dfavailable3.8→4.1GiB (~.3GiB actual dueAPFSsharedblocks). Preserved all committedlogs/profiles, reusablefixtures, otherchats'builds, activeSUMtarget andfullharness. Currentfree4.1GiB remains below8GiBreserve.

## Full inventory run started

Root authorized a real full inventory milestone run. Frozen exact main `1c00d2d6fb8a5b029b3014f57dd3942e904afce8`; normal release rebuild6.62s withkache,26public smoke passes, nativeSHA3f8b5061f4d9fc1b0e8b9ec26e68fb3847d547511fdd87c72ae6ec06182b2201. Receipt logs/provenance in freeze-1c00d2d6/.

Actual output `/Users/danielking/projects/vail/vail/conformance/runs/20261023-local-full-upstream`, live log `/tmp/full-local-upstream-prep-20261022/full-run.log`. All108inventorycases,3Vail+3eligibleHail attempts, serialfreshworkers. Missingexactfixtures explicitlyunsupported (no representative substitution). Nativeartifact/source remain fixed; otherheavyworkpaused.48GiBRSS,2GiBdisk guard for bounded existing-input querywrites,6hdispatchbudget. Sourceprepared helper/manifest frozen by hashes; exactcachedraw/native aliases only. Currentfree9.2GiB atstart, no missingfixturegeneration inside run. Fixture CPUpreparation will not compete withtimings: planned optional gapfillphase follows initialattempts with explicitreports/sourceprovenance.
