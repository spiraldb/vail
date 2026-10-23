# Generic composite column coordinate lowering

Python source168bee3d, unchanged normal native c5adb1d8.148publictests pass, including actual missing-entry slots, row/column captures and zero row/column axes with native reducer identities. Required source/test review cleared; evidence retained here.

The10k×1k/250partition representative matrix result matches Hail. Vail3-rep median **0.065646wall/0.497743core seconds**. Reused fresh unchanged stockHail3-rep baseline **2.684925/13.411449** (H/V40.90wall/26.94CPU). Same explicit partitions/32concurrency, serial engines, query construction/action timed; imports/init excluded. JIT and laptop activity caveats apply.

Guard-only previous Vail median.554483wall/2.334259core; generic coordinate callback median.065646/.497743. The earlier full-column-list broadcast version was7.260326/63.570749. These are representative-case measurements, not universal speedups.

Diagnostic plan now uses native vail_coordinate_bundle Partial/Final aggregations with gby=[] over aligned entry lists. The10Mentry UNNEST/CID hash grouping has been removed from this path. Small column-axis materialization and the native empty-input default aggregate remain. Nested grouping/explode/fold, random expressions, lookups and indexed result callbacks explicitly retain their existing fallback. Product/Boolean/min/max/stddev still need the separately queued direct-state adapters; they can use this generic coordinate route already.

The one-shot profile is short and includes initialization; it supports attribution only at coarse scale. Embedded names and actual diagnostic plan are included. Raw3Vailattempts, reused3Hailbaseline,10Hztelemetry and RSS/JIT spreads are retained.
