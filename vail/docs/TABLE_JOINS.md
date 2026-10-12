# Table joins

Vail's Table joins use keyed row matching and preserve the key and schema rules
exposed by Hail's Table API.

`join` supports `inner`, `left`, `right`, and `outer`. By default it matches
the complete keys; corresponding key component types and order must agree,
although key field names can differ. The result keeps the left key fields and
omits the right key fields. Right-side row and global fields that collide with
left-side names receive a `_1` suffix (and further suffixes as needed). Rows
with equal nonmissing keys are combined as a Cartesian product when either
side has duplicate keys. Missing in any key component prevents a match; each
unmatched row is emitted according to the selected join mode. Float keys use
Vail's IEEE total ordering: bit-identical NaNs match, distinct NaN payloads do
not, and positive and negative zero remain distinct.
`inner` emits matched groups only, `left` keeps all left rows, `right` keeps
all right rows, and `outer` keeps rows from both sides.

The private `_join_key` option matches the first N key fields and permits
matching types in that prefix. It can intentionally leave suffix fields from
both keys in the output and duplicate rows across differing suffixes. The
result key is the left key followed by the unmatched suffix of the right key.

`semi_join` and `anti_join` retain the original left rows and schema. The right
table must have a nonempty key no longer than the left key; each right key type
must equal the corresponding leading left key type. Membership is checked on
that key prefix. A left row is retained at most once per original row even
when the right side has duplicate matches. Missing key components do not
establish membership.
Left execution partitions and globals are preserved. Ordinary joins concatenate
both global structs after applying right-side naming collision rules.

`Table.multi_way_zip_join(tables, data_field_name, global_field_name)` requires
at least one input and exact matching key, row, and global types. It produces
one row per union key, with an array of optional non-key row structs in input
order; missing inputs occupy missing array elements. The named global field is
an array of each input's globals, also in input order. Duplicate keys are
outside the method's contract: Hail documents the selected duplicate row as
undefined. A missing component in a key does not match another missing key;
such rows remain separate with the other inputs' array elements missing.
Nested row and key values remain nested structs rather than being flattened.

All of these operations are lazy table plans. Constructing a result does not
collect rows; execution occurs when an action such as `collect` or `count` is
called.

Rust lowers ordinary joins to DataFusion `HashJoinExec`, canonical key
expressions, projections, and sorting. Zip joins use DataFusion `WindowAggExec`
and filtering to select duplicate representatives before full hash joins.
The keyless zip domain also has one representative per input. No row matching
or grouping runs in Python. Hash joins currently build their left side in
memory; global join outputs use one execution partition. Distributed join
planning and partial execution remain future work.

Regression tests live in `python/tests/test_table_joins.py` and
`crates/vail-table/tests/joins.rs`. Pinned Hail runtime observations are saved
in [table-joins-hail-oracle.json](research/table-joins-hail-oracle.json).
Some private `_join_key` shapes trigger schema errors in the pinned Hail
frontend; Vail supports their well-defined native prefix-join schema rather
than reproducing those frontend failures.
