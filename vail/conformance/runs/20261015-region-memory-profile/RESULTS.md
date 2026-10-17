# Large zero-array map/filter profile

Exact upstream region-memory map/filter body:30 source rows annotated with a100M-element integer zero array, filtered to15 rows, then `_force_count`. Matched extracted package native SHA256 `180f6f25bf286f96fd346450d9d2fcbc54d8903b5e705827527667d6ce6cafd9`. Samply500Hz,10Hz process counters, no Hail rerun. This uses the older expression boundary.

The external guard killed the query at41.5 observed seconds when physical footprint reached6.17GiB (6GiB limit); peak sampled RSS6.58GiB. Observed process CPU consumption39.15seconds, converted from raw Mach ticks using the recorded125/3nanosecond timebase. These are a bounded failed attempt, not a completed benchmark timing.

Among18,286main-thread samples,15,555(85.1%) include logical optimization. Leaf costs include20.7%formatting,17.8%memmove,14.5%Arrow Int32 display,14.0%string writes,10.3%list hashing and7.6%array hashing. Categories overlap when discussed inclusively; leaf symbols are listed individually in `map-filter-action-hotspots.json`. There are another2,051worker samples, with formatting also prominent.

The profile shows the large constant list is repeatedly formatted and hashed during planning even after removing all logical-plan rendering from the harness. This is a more immediate bottleneck than the source-audited array-repeat index allocation. Any proposed fix needs to avoid materializing huge constants into optimizer expression identity/naming without falsely declaring pure operations volatile. Coordination was requested with the chat owning planner/kernel identity changes before implementing an overlapping fix.

Source-only followup: locked DataFusion55.0.0 array_repeat constructs UInt64 take indices per repeated primitive before building the dense child values. Avoiding those indices is a general secondary opportunity. Ordinary Arrow List32 has a per-batch offset limit; it does not prevent a table exceeding that total across multiple batches. Dense output child storage remains necessary under the current representation; small output batches may bound peak memory. `_force_count` drains full output batches and does not insert an output-pruning count projection.

`map-filter-final` was an earlier invalid harness run: plan formatting expanded the literal into a300MB diagnostic string. It is excluded from these results. The corrected run is `map-filter-action`, with independently active memory/deadline guards and explicit CPU counter units.

Artifacts: profile and symbols `map-filter-action-profile.json.gz`/`.json.syms.json`, analyzed hotspots, `map-filter-action/rss.csv`, counter-units metadata and guard result. The guard was also smoke-tested with a nonexistent PID to confirm it creates its output and exits cleanly.
