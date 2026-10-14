# Concurrent partition execution

Vail's Python runtime has 32 Tokio workers. DataFusion's target partition count
is the source count capped at 32. Automatic round-robin repartitioning remains
disabled because source partitions are observable through the Table API.

Ordinary associative reductions use DataFusion's partial/final aggregation.
Sequence-sensitive and unknown reducers retain an ordered input boundary.
That boundary belongs before scope and grouping columns are appended: its
private partition tag is the final source column.

Ordered consumers start up to 32 partition producers and consume their output
in partition order. Each producer uses DataFusion's receiver stream with room
for two batches. Dropping the action aborts producers; errors propagate to the
consumer. Native source adapters also limit concurrent streams to 32 per source.
These limits bound stream concurrency and prefetching, not total query memory.

An adapter that executes child partition zero must declare a single-partition
input requirement when that child represents a complete result. DataFusion may
parallelize the child during optimization; the distribution requirement then
inserts the necessary final merge without serializing upstream computation.
A partition view of a genuinely partitioned source instead selects its intended
source partition. Singleton views retain their complete-result contract through
child rewrites.

Empty logical tables retain zero Table partitions, while their physical source
offers one empty stream so DataFusion can evaluate global aggregates normally.

Native Rust embedders supply their own runtime and need a multithreaded runtime
for CPU parallelism. No benchmark rerun accompanied this change; verification
uses correctness, physical-plan, bounded-concurrency, and cancellation tests.
