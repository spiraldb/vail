# Matrix composite follow-up on reviewed main

Six of six attempts pass. Same10k×1k representative range matrix,250partitions, composite SUM/COUNT/MEAN/FRACTION and missing entry-field values. Both output first/last records agree. Engines serial,32targetconcurrency,3repetitions each.

Median Vail **0.554483 wall / 2.334259 core seconds** versus stockPyPIHail **2.684925 / 13.411449**. H/V 4.842wall/5.745CPU. First-rep JIT effects and unrelated laptop activity are retained in raw spreads, not hidden.

Before grouped unused-global pruning, Vail medians were7.260326wall/63.570749core. After reviewed main's guard, .554483/2.334259. Different retained process heaps mean before/after RSS is not an isolated memory comparison.

The1kHz matched profile reduces total memmove samples from88.50% to1.47%; the dominant list-child-copy families shrink from88.21% toabout.37%. Frontend entry unnest and coordinate hash grouping still remain and are the next general lowering opportunity. This does not claim all matrix aggregation overhead has been removed.

Runtime source is exactly reviewed main e6abbe86; own duplicate guard was discarded during reconciliation.79publicgrouped/global/fold/lazy/nested tests pass. Full source/native hashes, build/test logs, profile symbols,10Hztelemetry and individual attempts are retained.
