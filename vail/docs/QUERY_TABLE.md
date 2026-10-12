# Lazy keyed table queries

`hl.query_table(path, point_or_interval)` returns an expression containing all
matching full rows, including their key fields. No matching rows yields an empty
array. Missing point values match missing keys, as confirmed against Hail; a
missing whole interval yields a missing array. Duplicate matches preserve table
order. Missing endpoint values participate in the existing extended key ordering
(missing points last), rather than making the whole interval missing.

Queries can use a scalar first key, a positional struct prefix of a compound
key, or an interval over a key prefix. Struct field names need not equal key
field names, but their positional types must match. An interval whose type is
itself the first key type is an exact point query. An outer interval over
interval-valued keys compares those keys by their ordering, not by overlap.

Constant and global queries use the interval-read/filter path and a lazy
collection expression. Constant bounds therefore prune source fragments before
reading their data. Query bounds are evaluated when constructing this path,
like the existing `filter_intervals` frontend; the matching table rows remain
lazy.

Row-varying point queries reuse the DataFusion sort-merge all-matches lookup
path. Row-varying range queries reuse its DataFusion non-equi join path, with a
predicate testing whether the query interval contains the target key. The
payload is the complete target row. Rows and results use Arrow; no data is
serialized to JSON.

The row-varying paths currently read target provider partitions and do not push
per-row bounds into individual file readers. They provide correct lazy query
semantics but are not yet a low-latency indexed seek for each outer row.
Dynamic interval/file pruning remains an execution optimization to implement.
