# H089: current query confirmed; CPU profile permission denied

Exact reviewed protocol753cb1b1 ran once on vail-bench-20261021. Normal installed Linux native5a8613c7, all six frontend source hashes/import paths, and fixture metadata hashes matched. Accepted source8dcd426b is production-byte-equivalent to current main1e19d9e4. Public unfiltered dimensions confirm24885rows×2535columns. No build/install/binary copy, source variant, retry, privilege or global configuration change.

The actual public action is biallelic filter→aggregate_entries(array_agg(sum,AD)). All four completed actions returned the same full list **[315761156,29304213]**. This is repeated-output consistency, not a new independent Hail correctness oracle. The unfiltered63Mlogical cells describe the fixture; we did not separately count cells surviving the row predicate, so no processed-entry throughput rate is claimed.

| Action | Wall seconds | Summed-thread CPU seconds | Whole-process high-water bytes |
|---|---:|---:|---:|
|0,warm-up|4.079753|10.298660|2511626240|
|1|0.655303|15.568667|3629207552|
|2|0.639741|16.143660|3848785920|
|3|0.619272|15.320760|3919241216|

Three warmed observations: **median0.639741s wall /15.568667CPU-s**. Their ratio is about24.3process core-equivalents, not ALU/SIMD/memory-bandwidth saturation. Process peak is about3.65GiB and includes import/input/earlier actions. Four cumulative high-water observations do not prove a leak or steady allocation footprint. These are bounded diagnostic observations, not a paired speedup/Hail comparison; warm-up and cache/allocator effects are not independently isolated.

## Current callpath evidence

The saved plan explicitly identifies itself as **UnboundActionExec diagnostic**: shared aggregate singleton dependencies were not executed. It was obtained by reconstructing the source-confirmed fallback, so it is not a fully bound executed-plan oracle. It contains:

```
coordinate SUM Final
  CoalescePartitions
    coordinate SUM Partial
      projected entry
        opaque GetField(entry context)
          presence Filter
            Unnest
              matrix cells
                biallelic row Filter
                  67 native source partitions
```

The coordinate SUM input is also an opaque semantic array-transform expression. This confirms the current frontend still constructs the hypothesized entry relation; it does **not** quantify its CPU cost or prove that unrelated nested payload was decoded. Standard DataFusion partial/final aggregation and source partition parallelism are already present. There is no evidence here for a forced global sort or a serial source bottleneck. The displayed scan does not enumerate nested requested field IDs, so no AD-only versus full-entry IO claim follows.

## Profile failure and exact stop

The separate sample child completed its first public ACTION with the same result. Only then did the monitor launch its single3second99Hz userspace cpu-clock/DWARF recording. Perf exited255: access denied with perf_event_paranoid4. The reviewed monitor terminated only the owned query group (exit-15), recorded abort_reason and empty cleanup_pending_pids, and stopped. No second profiler, sudo, capability/sysctl change, alternate event, longer sample or workload retry occurred. The empty perf.data and complete stderr are retained. **No current CPU-stack attribution is available.** Do not reuse historical H050 percentages as though this attempt measured them.

## Resource shutdown and disposition

The provider30minute STOP guard was confirmed before work. All10raw remote receipt files (15471bytes) were downloaded and independently SHA256-verified; same-user idle check showed only systemd/SSH/ps, no query/perf/compiler. The VM was proactively stopped and provider status **TERMINATED** verified. Local command/provider/source receipts and manifest are retained; no compute remains owned.

H089 is now supported by current public execution/diagnostic callpath and absolute wall/CPU/peak observations, but its proposed outer-list adapter remains **unimplemented and not performance-approved**. The key missing proof is where the15.6CPU-seconds go on this current artifact. A separately authorized permitted profiler would be needed to answer that; this protocol did not authorize changing observability permissions. Source-only design can continue, but no claim that Unnest/take/concat dominates, or that a rewrite will improve throughput, is justified by this failed profile.
