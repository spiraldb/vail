# H085 controlled replication attempt1 — incomplete profile, STOP/defer

The exact reviewed f217236a1 protocol stopped because the sample process finished its twelve allowed actions before the requested3second recording completed. `scope-result.json` reports `RuntimeError('Query ended before complete requested sample')`. No retry, adaptive replication, extra action, query variant, production change, or implementation followed. This is **not a completed valid profile**, and the partial recording is not used for hotpath attribution, materiality, or an optimization recommendation.

## Gates that actually completed

The exact original Hail JSON and eleven configuration/script inputs passed their enrolled hashes. Existing normal production517/main2d Linux native4761 and69frontend pins passed. Controlled4096replication native materialization and readback checks completed in2.733238seconds, monitor maxRSS635,621,376bytes, persisted fixture10,443,888bytes (well below the separate10GiBoutput cap). No build or installation.

The persisted331776×5fixture has483328defined entries, missing LA/LAD/n on defined entries0, local lengths479232at2/4096at3, global lengths475136at2/8192at3, preserving exact source distribution. Complete118defined one-replica public expanded arrays matched independently expanded original405cell outputs (including absent positions), keyed without duplicates. Full405original arrays are retained in the setup stdout's `complete_one_replica`; source JSON is an enrolled protocol asset. Generated schema/file hashes were frozen in the guest fixture lock before confirmation; see evidence limitation below.

All thirteen complete consumed actions (one confirmation, twelve sample-process actions) matched the independent Python expansion/fold aggregate reference:

```
defined_arrays 483328
elements 974848
defined_elements 974848
total 3248128
weighted 4984832
missing_entries 1175552
```

Absent owners are counted as1658880 minus public present-owner count. Missing expanded arrays are never substituted for absent owners; setup separately proves every defined LA/LAD/n is defined. Absolute integer-sum/weighted bounds passed. Aggregate statistics are full-consumption checks, not a uniqueness theorem for arbitrary arrays; direct tiny full-array contract tests and complete one-replica outputs supply per-element correctness.

Confirmation action0.284238s wall/2.156699core-seconds. Twelve sample action durations range0.232837–0.298563seconds; first-to-last completed-action window2.747863seconds. These are repeated actions in one process, with sampling overhead and warm caches, not independent trials/comparative throughput. They explain the fixed stop condition but do not measure dictionary-specific work. The fixture is identity-heavy:4096/483328defined entries actually expand; it cannot establish sparse/high-allele cohort performance.

## Partial recording and scope

Perf captured2,112samples/17.125MB before bounded cleanup, but failed the complete-duration gate. No perf report was run or analyzed, no executed dictionary-route assertion, allocation-byte/StackReq inference, source-I/O claim, or speedup forecast follows. The query could have taken a different optimized path; this incomplete evidence does not decide it. Respect the protocol exit: defer this route rather than altering scale until success. Broader H085/H015 remain open.

Guest originalparanoid4→temporary2→restored/verified4; setup, confirmation, query, sampler and command groups positively clean, pendinggroups empty. Download original receipt archive SHA verified. Host reports failure with `scope_verified:true`; VM proactively stopped and independently verified **TERMINATED**, provider1800sSTOP/guest28minute guard. All resources released.

## Evidence locations and limitation

`host/receipts.tar.gz` is the original downloaded archive, including partial perf.data and all collected guest receipts. `raw/` contains exact unpacked receipts except duplicate perf.data; SHA256.json authenticates every packaged original. Original setup and source file hashes/commands are retained. **The full generated fixture-lock JSON lives outside the guest attempt directory**, `/home/danielking/h085-profile-20261027/fixture-lock.json`, so the unchanged packer did not include it; the native fixture is `/home/danielking/h085-profile-20261027/fixture.mt`. Both remain on the stopped VM persistent disk. The downloaded setup stdout retains its SHA256 `50f254140d90946ca4116e00059466e1bbda75cb0a6aba731bd54c980ab0b9af`, complete source output arrays, distribution and fixture byte count, but not all generated file hashes/schema records from that lock. Do not claim a fully downloaded generated-input provenance packet. Recovering that original small lock would need a separately authorized metadata-only VM boot/recovery; this attempt does not restart the VM implicitly. No successful-profile acceptance is requested.
