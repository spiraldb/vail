# MatrixTable frontend

A MatrixTable consists of globals (including private column metadata) and one
native Table relation. Each owning row contains its row annotation and an aligned
list of entry structs. Entry structs carry a presence bit and the entry value;
column labels identify column metadata only. No internal row identities,
coordinate relations, or partition callbacks are used by MatrixTable lowering.
Ordinary Arrow lists are the accepted storage representation.

`rows()`, `cols()`, and `entries()` are lazy views. Entry annotation and filtering
map the owning row's list; row aggregations reduce that list. Column grouping
uses the column metadata, and grouped rows aggregate aligned entry lists.
Public row keys may contain duplicates. They do not identify owning rows.

Keyed entry lookups resolve the target entry list once per caller row and the
target column position once per caller column, then index directly into the
list. This avoids nested scans and repeated column metadata inside cell loops.
Lookups whose keys depend on individual entry values still require native
correlated lookup support and fail explicitly.

Snapshots use a version-3 `vail-matrix` manifest pointing at one generation's
core Table snapshot. Column metadata is stored in that Table's globals. There
are no separate coordinate snapshots or migration readers. On-disk compatibility
with previous Vail prototypes or Hail `.mt` files is not supported.

Native list reducers support common numeric, collection, nested aggregation,
and call-statistics operations. Ordered take, fold, and several advanced reducers
still need native list kernels. Caching/persistence and random sampling need
shared execution support. These limitations are reported explicitly.

VCF import is a lazy native source that produces this owning-row representation
directly. See [VCF.md](VCF.md) and the [VCF test inventory](../conformance/runs/20261012-vcf/INVENTORY.md).
Historical MatrixTable runs are retained in `conformance/runs/20261012-matrix-core`;
those earlier run reports do not describe the current representation.
