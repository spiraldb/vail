"""Matrix semantics over native lazy Table plans; no Python cell evaluator."""

from vail.expressions import IR_NODE_TYPES

import json
import math
import subprocess
import os
import sys

import pytest
import vail as hl


def matrix(nr=3, nc=4, partitions=2):
    m = hl.utils.range_matrix_table(nr, nc, partitions).annotate_globals(offset=5)
    return m.annotate_entries(x=m.row_idx * 10 + m.col_idx + m.offset)


def test_row_scoped_entry_projection_and_filter_skip_column_context_zip(monkeypatch):
    m = matrix(2, 3)
    original = hl.MatrixTable._cell_contexts

    def unexpected_column_context(self, base):
        raise AssertionError("row-scoped entry expression zipped column records")

    monkeypatch.setattr(hl.MatrixTable, "_cell_contexts", unexpected_column_context)
    annotated = m.annotate_entries(row_value=hl.int32(m.row_idx + m.x))
    selected = annotated.select_entries("row_value")
    filtered = annotated.filter_entries(annotated.row_idx == 1)
    transmuted = annotated.transmute_entries(row_value=hl.int32(annotated.row_idx + 1))
    monkeypatch.setattr(hl.MatrixTable, "_cell_contexts", original)

    assert [row.row_value for row in selected.entries().collect()] == [5, 6, 7, 16, 17, 18]
    assert [row.row_value for row in filtered.entries().collect()] == [16, 17, 18]
    assert [row.row_value for row in transmuted.entries().collect()] == [1, 1, 1, 2, 2, 2]


def test_named_column_entry_projection_does_not_construct_positions():

    m = matrix(2, 3).choose_cols([2, 0, 2])
    m = m.filter_entries(m.col_idx != 0)
    result = m.annotate_entries(y=m.row_idx + m.col_idx + m.x)
    assert result.y.collect() == [9, 9, 20, 20]


@pytest.mark.parametrize("nr,nc", [(0, 0), (0, 3), (3, 0), (2, 3)])
def test_range_dimensions_schema_and_empty_axes(nr, nc):
    m = hl.range_matrix_table(nr, nc, 2)
    assert m.count() == (nr, nc)
    assert m.entries().count() == nr * nc
    assert m.row.dtype == hl.tstruct(row_idx=hl.tint32)
    assert m.col.dtype == hl.tstruct(col_idx=hl.tint32)
    assert m.entry.dtype == hl.tstruct()
    assert list(m.row_key) == ["row_idx"]
    assert list(m.col_key) == ["col_idx"]


@pytest.mark.parametrize("n_rows,n_partitions", [(0, 3), (3, 5), (11, 4)])
def test_matrix_scan_count_preserves_source_order_across_partitions(n_rows, n_partitions):
    mt = hl.utils.range_matrix_table(n_rows, 2, n_partitions)
    rows = mt.annotate_rows(prefix=hl.scan.count()).rows().collect()
    assert [(row.row_idx, row.prefix) for row in rows] == [
        (i, i) for i in range(n_rows)
    ]


def test_matrix_scan_count_preserves_union_source_order():
    mt = hl.utils.range_matrix_table(7, 2, 3)
    rows = mt.union_rows(mt).annotate_rows(prefix=hl.scan.count()).rows().collect()
    assert [(row.row_idx, row.prefix) for row in rows] == [
        (i, prefix) for prefix, i in enumerate([0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6])
    ]


def test_matrix_scan_count_preserves_head_prefix_order():
    mt = hl.utils.range_matrix_table(10, 2, 4)
    before_scan = mt.head(5).annotate_rows(prefix=hl.scan.count()).rows().collect()
    after_scan = mt.annotate_rows(prefix=hl.scan.count()).head(5).rows().collect()
    expected = [(i, i) for i in range(5)]
    assert [(row.row_idx, row.prefix) for row in before_scan] == expected
    assert [(row.row_idx, row.prefix) for row in after_scan] == expected


def test_matrix_scan_count_after_repartition_union_and_head():
    mt = hl.utils.range_matrix_table(12, 2, 4)
    combined = mt.repartition(3).union_rows(mt)
    expected = [
        (row_idx, prefix)
        for prefix, row_idx in enumerate([0, 0, 1, 1, 2, 2, 3, 3])
    ]

    before_scan = combined.head(8).annotate_rows(prefix=hl.scan.count()).rows().collect()
    after_scan = combined.annotate_rows(prefix=hl.scan.count()).head(8).rows().collect()
    assert [(row.row_idx, row.prefix) for row in before_scan] == expected
    assert [(row.row_idx, row.prefix) for row in after_scan] == expected


def test_matrix_scan_count_after_grouped_and_global_aggregate():
    table = hl.utils.range_table(12, n_partitions=4)
    table = table.annotate(label=hl.str(table.idx), payload=hl.struct(x=table.idx))
    grouped = table.group_by(k=table.idx % 3).aggregate(
        n=hl.agg.count(),
        labels=hl.agg.collect(table.label),
        payloads=hl.agg.collect(table.payload),
    )
    grouped = grouped.annotate(prefix=hl.scan.count()).collect()
    assert [
        (row.k, row.prefix, row.n, sorted(row.labels), sorted(item.x for item in row.payloads))
        for row in grouped
    ] == [
        (
            key,
            key,
            4,
            sorted(str(idx) for idx in range(key, 12, 3)),
            sorted(range(key, 12, 3)),
        )
        for key in range(3)
    ]

    global_aggregate = table.group_by().aggregate(n=hl.agg.count())
    global_aggregate = global_aggregate.annotate(prefix=hl.scan.count()).collect()
    assert [(row.n, row.prefix) for row in global_aggregate] == [(12, 0)]


def test_annotations_actions_globals_and_native_plan():
    m = matrix()
    assert m.aggregate_entries(hl.agg.sum(m.x)) == 198
    assert hl.eval(m.offset) == 5
    assert hl.eval(m.index_globals()).offset == 5
    assert m.row_idx.collect() == [0, 1, 2]
    assert m.col_idx.collect() == [0, 1, 2, 3]
    assert m.x.take(3) == [5, 6, 7]
    assert hl.eval(m.x.collect(_localize=False)) == m.x.collect()
    assert _has_native_op(m._mir.core._native.ir(), "array_map")
    assert "__vail_matrix_row_id" not in m._mir.core._schema
    assert m.entries().collect() == m.entries().collect()
    with pytest.raises(ValueError):
        hl.eval(m.x)


def test_entry_annotation_zips_cells_with_positions_after_column_reordering():
    mt = hl.range_matrix_table(3, 5, 2).choose_cols([4, 1, 3])
    mt = mt.filter_cols(mt.col_idx != 3)

    def cell_index_count(node):
        if isinstance(node, IR_NODE_TYPES):
            is_cell_index = (
                node.get("op") == "array_index"
                and node.get("value")
                == {"op": "column", "name": "__vail_matrix_cells"}
            )
            return int(is_cell_index) + sum(cell_index_count(value) for value in node.values())
        if isinstance(node, list):
            return sum(cell_index_count(value) for value in node)
        return 0

    previous_cell_index_count = cell_index_count(
        mt._mir.core._native.ir()
    )
    mt = mt.annotate_entries(
        base_value=hl.if_else(
            mt.row_idx == 1,
            hl.missing(hl.tint32),
            hl.int32(mt.row_idx * 100 + mt.col_idx),
        )
    )
    mt = mt.annotate_entries(
        value=mt.base_value + mt.row_idx + mt.col_idx
    )

    # The remaining column order is [4, 1]. The projected entry values must
    # use the current cell position for column lookup, not the original ID.
    assert mt.col_idx.collect() == [4, 1]
    assert [row.value for row in mt.entries().collect()] == [2, 8, None, None, 204, 210]
    assert mt.aggregate_entries(hl.agg.sum(mt.value)) == 424
    core_ir = mt._mir.core._native.ir()
    assert _has_native_op(core_ir, "nested_function", function="arrays_zip")
    assert cell_index_count(core_ir) == previous_cell_index_count

    empty = hl.range_matrix_table(2, 0, 1).annotate_entries(value=hl.int32(1))
    assert empty.entries().count() == 0
    assert empty.aggregate_entries(hl.agg.sum(empty.value)) == 0


def test_expression_scope_and_source_errors():
    m = matrix(2, 2)
    with pytest.raises(hl.ExpressionException):
        m.annotate_rows(bad=m.col_idx)
    with pytest.raises(hl.ExpressionException):
        m.annotate_cols(bad=m.row_idx)
    with pytest.raises(hl.ExpressionException):
        m.annotate_globals(bad=m.x)
    with pytest.raises(hl.ExpressionException):
        m.filter_entries(hl.agg.count() > 0)
    with pytest.raises(hl.ExpressionException):
        m.aggregate_rows(hl.agg.sum(m.col_idx))
    with pytest.raises(hl.ExpressionException):
        m.aggregate_entries(m.x)
    other = matrix(2, 2)
    with pytest.raises(hl.ExpressionException):
        m.annotate_entries(bad=other.x)
    with pytest.raises(hl.ExpressionException):
        m.annotate_rows(bad=other.row_idx)
    with pytest.raises(ValueError):
        m.annotate_rows(row_idx=1)
    with pytest.raises(TypeError):
        m.filter_rows(3)
    with pytest.raises(TypeError):
        m.count_rows(_localize=3)
    with pytest.raises(TypeError):
        m.foo = 2


def test_matrix_indexed_write_preserves_sorted_source_keys(tmp_path):
    matrix = hl.range_matrix_table(20, 5, 4)
    path = tmp_path / "matrix.mt"
    matrix.write(path)

    descriptor = json.loads((path / "_vail_matrix.json").read_text())
    core = hl.read_table(path / descriptor["generation"] / "core")
    index_key = descriptor["row_index_keys"][0]
    assert index_key == "row_idx"
    assert core._keys == (index_key,)
    assert core[index_key].collect() == list(range(20))
    assert set(core._schema) == {"__vail_matrix_row", "__vail_matrix_cells", "row_idx"}

    intervals = [
        hl.Interval(2, 7, includes_start=True, includes_end=False),
        hl.Interval(7, 12, includes_start=True, includes_end=False),
    ]
    interval_read = hl.read_matrix_table(path, _intervals=intervals)
    pruned_read = hl.read_matrix_table(path, _intervals=intervals, _filter_intervals=True)
    assert pruned_read.row_idx.collect() == list(range(2, 12))

    source = hl.read_matrix_table(path)
    filtered = source.filter_rows(source.row_idx >= 11)
    assert filtered.row_idx.collect() == list(range(11, 20))

    rows = source.rows()
    assert set(rows._schema) == {"row_idx"}
    assert rows.annotate(extra=rows.row_idx + 1).extra.collect() == list(range(1, 21))
    assert rows.select(extra=rows.row_idx + 2).extra.collect() == list(range(2, 22))
    pandas_rows = rows.to_pandas(flatten=False)
    assert list(pandas_rows.columns) == ["row_idx"]
    rows_path = tmp_path / "rows.ht"
    rows.write(rows_path)
    reloaded_rows = hl.read_table(rows_path)
    assert set(reloaded_rows._schema) == {"row_idx"}
    assert reloaded_rows.row_idx.collect() == list(range(20))


def test_matrix_reserved_row_key_aliases_are_collision_free(tmp_path):
    matrix = hl.range_matrix_table(5, 2, 2)
    matrix = matrix.key_rows_by(
        __vail_matrix_row=matrix.row_idx,
        __vail_matrix_row_key_0=matrix.row_idx + 10,
    )
    path = tmp_path / "reserved-keys.mt"
    matrix.write(path)

    descriptor = json.loads((path / "_vail_matrix.json").read_text())
    assert descriptor["row_index_keys"] == ["__vail_matrix_row_key_0_1", "__vail_matrix_row_key_0"]
    result = hl.read_matrix_table(path)
    assert result.rows().collect() == [
        hl.Struct(row_idx=i, __vail_matrix_row=i, __vail_matrix_row_key_0=i + 10)
        for i in range(5)
    ]


def test_matrix_indexed_read_maps_composite_open_prefix_intervals(tmp_path):
    matrix = hl.range_matrix_table(12, 2, 4)
    matrix = matrix.key_rows_by(group=matrix.row_idx // 4, row_idx=matrix.row_idx)
    path = tmp_path / "composite.mt"
    matrix.write(path)

    full_key_type = hl.tstruct(group=hl.tint32, row_idx=hl.tint32)
    prefix_type = hl.tstruct(group=hl.tint32)
    open_before = hl.Interval(
        None,
        hl.Struct(group=1, row_idx=6),
        includes_start=True,
        includes_end=True,
        point_type=full_key_type,
    )
    open_after = hl.Interval(
        hl.Struct(group=1),
        None,
        includes_start=True,
        includes_end=False,
        point_type=prefix_type,
    )

    with pytest.raises(hl.ExpressionException, match="missing endpoint"):
        hl.read_matrix_table(path, _intervals=[open_before])
    with pytest.raises(hl.ExpressionException, match="missing endpoint"):
        hl.read_matrix_table(path, _intervals=[open_after])

    bounded_key = hl.Interval(
        hl.Struct(group=0, row_idx=0),
        hl.Struct(group=1, row_idx=6),
        includes_start=True,
        includes_end=True,
        point_type=full_key_type,
    )
    bounded_prefix = hl.Interval(
        hl.Struct(group=1),
        hl.Struct(group=2),
        includes_start=True,
        includes_end=False,
        point_type=prefix_type,
    )
    assert hl.read_matrix_table(path, _intervals=[bounded_key]).row_idx.collect() == list(range(7))
    assert hl.read_matrix_table(path, _intervals=[bounded_prefix]).row_idx.collect() == list(range(4, 8))


def test_matrix_indexed_read_preserves_expression_intervals(tmp_path):
    matrix = hl.range_matrix_table(8, 2, 4)
    path = tmp_path / "expression-intervals.mt"
    matrix.write(path)
    interval_type = hl.tarray(hl.tinterval(hl.tint32))
    intervals = hl.literal(
        [hl.Interval(2, 5, includes_start=True, includes_end=False)], interval_type
    )

    assert hl.read_matrix_table(path, _intervals=intervals).row_idx.collect() == [2, 3, 4]
    filtered = hl.read_matrix_table(path, _intervals=intervals, _filter_intervals=True)
    assert filtered.row_idx.collect() == [2, 3, 4]


def test_filtered_cells_distinct_from_missing_and_dimensions_retained():
    m = matrix(2, 3)
    filtered = m.filter_entries(m.col_idx == 0).annotate_entries(y=1)
    assert filtered.count() == (2, 3)
    assert filtered.aggregate_entries(hl.agg.count()) == 2
    assert filtered.aggregate_entries(hl.agg.sum(filtered.y)) == 2
    empty = m.filter_entries(False)
    assert empty.count() == (2, 3)
    assert empty.entries().collect() == []
    restored = empty.unfilter_entries()
    assert restored.entries().count() == 6
    assert restored.aggregate_entries(hl.agg.count()) == 6
    assert restored.x.collect() == [None] * 6
    missing = m.annotate_entries(x=hl.missing(hl.tint32))
    assert missing.aggregate_entries(hl.agg.count()) == 6
    assert m.filter_entries(hl.missing(hl.tbool), keep=False).entries().count() == 0


def test_row_column_aggregation_and_empty_initialization():
    m = matrix(3, 2)
    rows = m.annotate_rows(total=hl.agg.sum(m.x), mixed=m.row_idx + hl.agg.count())
    assert [r.total for r in rows.rows().collect()] == [11, 31, 51]
    assert [r.mixed for r in rows.rows().collect()] == [2, 3, 4]
    cols = m.annotate_cols(total=hl.agg.sum(m.x))
    assert [c.total for c in cols.cols().collect()] == [45, 48]
    empty = m.filter_entries(False)
    annotated = empty.annotate_rows(
        total=hl.agg.sum(empty.x), n=hl.agg.count(), values=hl.agg.collect(empty.x)
    )
    assert [(r.total, r.n, r["values"]) for r in annotated.rows().collect()] == [
        (0, 0, [])
    ] * 3
    assert m.filter_rows(hl.agg.sum(m.x) > 20).row_idx.collect() == [1, 2]
    assert hl.eval(m.aggregate_cols(hl.agg.count(), _localize=False)) == 2


def test_row_boolean_aggregations_handle_missing_and_empty_values():
    m = hl.range_matrix_table(1, 3, 1)
    predicate = hl.if_else(
        m.col_idx == 0,
        True,
        hl.if_else(m.col_idx == 1, False, hl.missing(hl.tbool)),
    )


    def aggregate(matrix):
        return matrix.annotate_rows(
            n=hl.agg.count_where(matrix.predicate),
            any=hl.agg.any(matrix.predicate),
            all=hl.agg.all(matrix.predicate),
            fraction=hl.agg.fraction(matrix.predicate),
        ).rows().collect()[0]

    mixed = aggregate(m.annotate_entries(predicate=predicate))
    assert (mixed.n, mixed.any, mixed.all, mixed.fraction) == (1, True, False, 1 / 3)

    all_missing = aggregate(m.annotate_entries(predicate=hl.missing(hl.tbool)))
    assert (all_missing.n, all_missing.any, all_missing.all, all_missing.fraction) == (
        0, False, True, 0.0
    )

    empty = aggregate(m.annotate_entries(predicate=predicate).filter_entries(False))
    assert (empty.n, empty.any, empty.all) == (0, False, True)
    assert math.isnan(empty.fraction)


def test_row_aggregation_context_projection_preserves_values():
    m = hl.range_matrix_table(2, 3, 1)
    m = m.annotate_cols(sex=hl.if_else(m.col_idx == 0, "F", "M"))
    m = m.annotate_entries(
        record=hl.struct(
            gt=m.col_idx,
            nested=hl.struct(keep=m.col_idx > 0, unused=m.row_idx),
        ),
        unrelated=m.row_idx + m.col_idx,
    )
    rows = m.annotate_rows(
        selected=hl.agg.count_where(
            (m.record.nested.keep) & (m.sex == "M")
        )
    )

    assert rows.selected.collect() == [2, 2]

    ancestor_and_leaf = m.annotate_rows(
        selected=hl.agg.count_where(
            hl.is_defined(m.record) & m.record.nested.keep
        )
    )
    assert ancestor_and_leaf.selected.collect() == [2, 2]

    missing_parent = m.annotate_entries(
        record=hl.if_else(
            m.col_idx == 0,
            hl.missing(m.record.dtype),
            m.record,
        )
    )
    missing_rows = missing_parent.annotate_rows(
        selected=hl.agg.count_where(hl.is_defined(missing_parent.record.nested.keep))
    )
    assert missing_rows.selected.collect() == [2, 2]

    whole = m.annotate_rows(records=hl.agg.collect(m.entry))
    assert whole.records.collect()[0] == [
        {"record": {"gt": 0, "nested": {"keep": False, "unused": 0}}, "unrelated": 0},
        {"record": {"gt": 1, "nested": {"keep": True, "unused": 0}}, "unrelated": 1},
        {"record": {"gt": 2, "nested": {"keep": True, "unused": 0}}, "unrelated": 2},
    ]


@pytest.mark.parametrize(
    "limits,expected",
    [((1, 2), (1, 2)), ((1, None), (1, 4)), ((None, 1), (3, 1)), ((0, 0), (0, 0))],
)
def test_head_tail_limits(limits, expected):
    m = matrix()
    assert m.head(*limits).count() == expected
    tail = m.tail(*limits)
    assert tail.count() == expected
    if limits == (1, 2):
        assert [(r.row_idx, r.col_idx, r.x) for r in tail.entries().collect()] == [
            (2, 2, 27),
            (2, 3, 28),
        ]


def test_filter_dimensions_column_order_duplicate_keys_and_choices():
    m = matrix(2, 3)
    m = m.key_cols_by(sample=hl.str(2 - m.col_idx))
    assert m.sample.collect() == ["2", "1", "0"]
    assert [r.sample for r in m.cols().collect()] == ["0", "1", "2"]
    chosen = m.choose_cols([2, 0, 2])
    assert chosen.sample.collect() == ["0", "2", "0"]
    assert chosen.count() == (2, 3)
    assert chosen.x.collect() == [7, 5, 7, 17, 15, 17]
    assert chosen.cols().count() == 3
    filtered = m.filter_rows(m.row_idx == 1)
    filtered = filtered.filter_cols(filtered.col_idx != 1)
    assert filtered.count() == (1, 2)
    assert filtered.x.collect() == [15, 17]
    with pytest.raises(ValueError):
        m.choose_cols([3])
    with pytest.raises(TypeError):
        m.choose_cols([True])


def test_select_transmute_drop_rename_and_indices():
    m = matrix(2, 2)
    m = m.annotate_rows(a=m.row_idx + 1).annotate_cols(b=1)
    selected = m.select_rows().select_cols()
    selected = selected.select_entries(z=selected.x + 1)
    assert list(selected.row) == ["row_idx"]
    assert list(selected.col) == ["col_idx"]
    assert list(selected.entry) == ["z"]
    transmuted = m.transmute_entries(z=m.x * 2)
    assert list(transmuted.entry) == ["z"]
    dropped = m.drop("a", m.b)
    assert list(dropped.row) == ["row_idx"]
    assert list(dropped.col) == ["col_idx"]
    renamed = m.rename({"row_idx": "r", "col_idx": "c", "x": "value", "offset": "base"})
    assert list(renamed.row_key) == ["r"]
    assert list(renamed.col_key) == ["c"]
    assert renamed.value.collect() == [5, 6, 15, 16]
    assert hl.eval(renamed.base) == 5
    indexed = m.add_row_index("ri").add_col_index("ci")
    assert indexed.ri.collect() == [0, 1]
    assert indexed.ci.collect() == [0, 1]


def test_localize_filtered_entries_and_column_order():
    m = matrix(2, 3).choose_cols([2, 0, 1])
    m = m.filter_entries(m.col_idx != 0)
    t = m.localize_entries("es", "cs")
    assert [c.col_idx for c in hl.eval(t.cs)] == [2, 0, 1]
    assert [[e.x if e is not None else None for e in r.es] for r in t.collect()] == [
        [7, None, 6],
        [17, None, 16],
    ]
    with pytest.raises(ValueError):
        m.localize_entries("row_idx", "cs")


def test_grouped_rows_and_columns_with_empty_cells():
    m = matrix(3, 2)
    grouped = (
        m.group_rows_by(group=m.row_idx % 2)
        .aggregate_entries(total=hl.agg.sum(m.x))
        .aggregate_rows(n=hl.agg.count())
        .result()
    )
    assert [(r.group, r.n) for r in grouped.rows().collect()] == [(0, 2), (1, 1)]
    assert [(r.group, r.col_idx, r.total) for r in grouped.entries().collect()] == [
        (0, 0, 30),
        (0, 1, 32),
        (1, 0, 15),
        (1, 1, 16),
    ]
    cols = m.group_cols_by(group=m.col_idx % 2).aggregate(total=hl.agg.sum(m.x))
    assert cols.count() == (3, 2)
    empty = m.filter_entries(False)
    grouped_empty = empty.group_rows_by(group=empty.row_idx % 2).aggregate(
        n=hl.agg.count()
    )
    assert grouped_empty.n.collect() == [0, 0, 0, 0]


def test_table_and_matrix_lookups_and_semi_anti_joins():
    m = matrix(3, 2)
    t = hl.Table.parallelize([{"row_idx": 1, "value": 99}], key="row_idx")
    a = m.annotate_rows(value=t[m.row_idx].value)
    assert a.value.collect() == [None, 99, None]
    assert m.semi_join_rows(t).row_idx.collect() == [1]
    assert m.anti_join_rows(t).row_idx.collect() == [0, 2]
    ct = hl.Table.parallelize([{"col_idx": 0}], key="col_idx")
    assert m.semi_join_cols(ct).col_idx.collect() == [0]
    assert m.anti_join_cols(ct).col_idx.collect() == [1]
    a = m.annotate_entries(copy=m[m.row_key, m.col_key].x)
    assert a.copy.collect() == m.x.collect()
    a = m.annotate_cols(rowlookup=m.index_rows(m.col_idx))
    assert list(a.rowlookup.dtype) == []


def test_from_parts_and_rows_only():
    m = hl.MatrixTable.from_parts(
        globals={"g": 3},
        rows={"a": [1, 2]},
        cols={"b": ["a", "b"]},
        entries={"x": [[1, 2], [3, 4]]},
    )
    assert m.count() == (2, 2)
    assert m.x.collect() == [1, 2, 3, 4]
    assert hl.eval(m.g) == 3
    t = hl.Table.parallelize([{"k": 1}, {"k": 2}], key="k")
    rows_only = hl.MatrixTable.from_rows_table(t)
    assert rows_only.count() == (2, 0)
    assert rows_only.rows()._same(t)
    with pytest.raises(ValueError):
        hl.MatrixTable.from_parts(
            rows={"a": [1]}, cols={"b": [1, 2]}, entries={"x": [[1]]}
        )


def test_matrix_storage_overwrite_snapshot_and_fresh_process(tmp_path):
    m = matrix(2, 3).choose_cols([2, 0, 1])
    m = m.filter_entries(m.col_idx != 0)
    path = tmp_path / "matrix"
    m.write(path)
    r = hl.read_matrix_table(path)
    assert r.count() == (2, 3)
    assert r.col_idx.collect() == [2, 0, 1]
    assert r.x.collect() == [7, 6, 17, 16]
    assert r._same(m)
    with pytest.raises(FileExistsError):
        m.write(path)
    smaller = m.head(1, 1)
    smaller.write(path, overwrite=True)
    assert hl.read_matrix_table(path).count() == (1, 1)
    assert r.count() == (2, 3)
    code = "import vail as hl,sys,json; m=hl.read_matrix_table(sys.argv[1]); print(json.dumps([m.count(),m.x.collect()]))"
    output = subprocess.check_output(
        [sys.executable, "-c", code, str(path)], text=True, env=os.environ, timeout=30
    )
    assert json.loads(output) == [[1, 1], [7]]


def test_matrix_checkpoint_empty_dimensions_and_repartition(tmp_path):
    for nr, nc in [(0, 0), (0, 2), (2, 0)]:
        m = hl.range_matrix_table(nr, nc, 3)
        r = m.checkpoint(tmp_path / f"{nr}-{nc}")
        assert r.count() == (nr, nc)
        assert r.entries().count() == 0
    m = matrix(4, 2).repartition(3)
    assert m.x.collect() == [5, 6, 15, 16, 25, 26, 35, 36]


def test_union_rows_columns_distinct_and_outer_missing_cells():
    m = matrix(2, 2)
    rows = m.union_rows(m)
    assert rows.count() == (4, 2)
    assert rows.x.collect() == [5, 6, 5, 6, 15, 16, 15, 16]
    assert rows.distinct_by_row()._same(m)
    cols = m.union_cols(m)
    assert cols.count() == (2, 4)
    assert cols.x.collect() == [5, 6, 5, 6, 15, 16, 15, 16]
    assert cols.distinct_by_col()._same(m)
    left = m.filter_rows(m.row_idx == 0)
    right = m.filter_rows(m.row_idx == 1)
    outer = left.union_cols(right, row_join_type="outer")
    assert outer.count() == (2, 4)
    assert outer.x.collect() == [5, 6, None, None, None, None, 15, 16]
    assert left.union_cols(right).count() == (0, 4)


def test_explode_axes_and_collect_columns_preserve_alignment():
    m = matrix(2, 2)
    m = m.annotate_rows(items=hl.array([m.row_idx, m.row_idx + 10]))
    exploded = m.explode_rows("items")
    assert exploded.count() == (4, 2)
    assert exploded.items.collect() == [0, 10, 1, 11]
    assert exploded.x.collect() == [5, 6, 5, 6, 15, 16, 15, 16]
    m = matrix(2, 2)
    m = m.annotate_cols(items=hl.array([m.col_idx, m.col_idx + 10]))
    exploded = m.explode_cols("items")
    assert exploded.items.collect() == [0, 10, 1, 11]
    assert exploded.x.collect() == [5, 5, 6, 6, 15, 15, 16, 16]
    collected = exploded.collect_cols_by_key()
    assert collected.items.collect() == [[0, 10], [1, 11]]
    assert collected.x.collect() == [[5, 5], [6, 6], [15, 15], [16, 16]]


def test_unkey_after_rekey_keeps_row_order_even_when_fields_dropped(tmp_path):
    m = matrix(3, 2)
    m = m.key_rows_by(k=-m.row_idx).key_rows_by().select_rows()
    assert m.x.collect() == [25, 26, 15, 16, 5, 6]
    r = m.checkpoint(tmp_path / "unkeyed")
    assert r.x.collect() == m.x.collect()


def test_grouped_column_order_and_localization():
    m = matrix(2, 3)
    grouped = m.group_cols_by(group=2 - m.col_idx).aggregate(total=hl.agg.sum(m.x))
    assert grouped.group.collect() == [0, 1, 2]
    assert grouped.total.collect() == [7, 6, 5, 17, 16, 15]
    localized = grouped.localize_entries("es", "cs")
    assert [[e.total for e in row.es] for row in localized.collect()] == [
        [7, 6, 5],
        [17, 16, 15],
    ]
    assert grouped.localize_entries()._same(grouped.rows())


def test_make_table_filter_stats_and_explicit_blockers():
    m = matrix(2, 2)
    m = m.key_cols_by(s=hl.str(m.col_idx))
    t = m.make_table()
    assert [r["0.x"] for r in t.collect()] == [5, 15]
    assert [r["1.x"] for r in t.collect()] == [6, 16]
    assert hl.eval(t.offset) == 5
    f = m.filter_entries(m.col_idx == 0).compute_entry_filter_stats()
    assert [r.entry_stats_row.n_filtered for r in f.rows().collect()] == [1, 1]
    assert [r.entry_stats_col.n_remaining for r in f.cols().collect()] == [2, 0]
    with pytest.raises(NotImplementedError):
        m.cache()
    assert m.sample_rows(1, seed=0)._same(m)


def test_grouped_empty_cells_keep_fixed_axis_values():
    m = matrix(2, 3).filter_entries(False)
    grouped = m.group_rows_by(group=m.row_idx % 2).aggregate(
        value=m.col_idx + hl.agg.sum(m.x)
    )
    assert grouped.value.collect() == [0, 1, 2, 0, 1, 2]
    grouped = m.group_cols_by(group=m.col_idx % 2).aggregate(
        value=m.row_idx + hl.agg.sum(m.x)
    )
    assert grouped.value.collect() == [0, 0, 1, 1]


def test_table_to_matrix_validation_and_row_major_roundtrip():
    t = hl.range_table(4, 2)
    t = t.annotate(foo=t.idx, bar=2 * t.idx, baz=3 * t.idx)
    m = t.to_matrix_table_row_major(["bar", "baz"], "entry", "col")
    assert m.count() == (4, 2)
    localized = m.localize_entries("entries", "cols")
    roundtrip = localized.transmute(
        **{
            c.col: localized.entries[i].entry
            for i, c in enumerate(hl.eval(localized.cols))
        }
    )
    roundtrip = roundtrip.drop("cols")
    assert roundtrip._same(t)
    with pytest.raises(ValueError):
        t.to_matrix_table_row_major(["bar", "baz"])
    with pytest.raises(ValueError):
        t.to_matrix_table([], ["idx"])
    with pytest.raises(ValueError):
        t.to_matrix_table(["idx"], ["foo"], ["bar", "baz"])


def test_genomic_aggregate_literal_and_per_row_initializers():
    m = (
        hl.range_matrix_table(2, 3)
        .annotate_rows(alleles=["A", "C"])
        .annotate_entries(GT=hl.call(0, 1))
    )
    m = m.annotate_rows(cs=hl.agg.call_stats(m.GT, 2))
    assert [r.cs.AC for r in m.rows().collect()] == [[3, 3], [3, 3]]
    initialized = m.annotate_rows(other=hl.agg.call_stats(m.GT, m.alleles))
    assert [r.other.AC for r in initialized.rows().collect()] == [[3, 3], [3, 3]]


def test_sparse_column_collection_preserves_missing_slots():
    m = matrix(1, 2)
    m = m.key_cols_by(group=0)
    m = m.filter_entries(m.col_idx == 0)
    collected = m.collect_cols_by_key()
    assert collected.x.collect() == [[5, None]]
    assert collected.col_idx.collect() == [[0, 1]]


def test_collect_cols_by_key_keeps_column_and_entry_values_aligned():
    m = hl.range_matrix_table(2, 3, n_partitions=3)
    m = m.annotate_cols(column_value=m.col_idx)
    m = m.annotate_entries(entry_value=m.row_idx * 100 + m.col_idx)
    collected = m.key_cols_by(group=0).collect_cols_by_key()

    columns = collected.column_value.collect()[0]
    entries = collected.entry_value.collect()
    assert columns == [0, 1, 2]
    assert entries == [[0, 1, 2], [100, 101, 102]]
    assert all([value - row * 100 for value in values] == columns
               for row, values in enumerate(entries))


def test_nested_explode_and_empty_column_choice():
    m = matrix(2, 2)
    m = m.annotate_rows(nested=hl.struct(xs=hl.array([m.row_idx, m.row_idx + 10])))
    exploded = m.explode_rows(m.nested.xs)
    assert exploded.nested.xs.collect() == [0, 10, 1, 11]
    assert exploded.x.collect() == [5, 6, 5, 6, 15, 16, 15, 16]
    empty = m.choose_cols([])
    assert empty.count() == (2, 0)
    assert empty.entries().count() == 0


def test_matrix_local_uri_and_remote_rejection(tmp_path):
    m = matrix(1, 1)
    uri = (tmp_path / "local").as_uri()
    m.write(uri)
    assert hl.read_matrix_table(uri)._same(m)
    with pytest.raises(NotImplementedError, match="local paths"):
        m.write("s3://bucket/matrix")
    with pytest.raises(NotImplementedError, match="local paths"):
        hl.read_matrix_table("https://example.com/matrix")


def test_localization_inverse_sparse_reordered_and_repeated_actions():
    m = matrix(3, 3).choose_cols([2, 0, 1])
    m = m.filter_entries(m.col_idx != 0)
    localized = m.localize_entries("cells", "samples")
    restored = localized._unlocalize_entries("cells", "samples", ["col_idx"])
    assert restored.count() == (3, 3)
    assert restored.col_idx.collect() == [2, 0, 1]
    assert restored.x.collect() == [7, 6, 17, 16, 27, 26]
    assert restored._same(m)
    assert restored._same(m)


@pytest.mark.parametrize("partitions", [1, 2, 5])
def test_coordinate_conversion_sparse_missing_keys_duplicate_coordinates(partitions):
    t = hl.Table.parallelize(
        [
            {"r": 1, "c": "b", "x": 2},
            {"r": 1, "c": "a", "x": 1},
            {"r": 2, "c": "b", "x": 4},
            {"r": 1, "c": "a", "x": 9},
            {"r": None, "c": "b", "x": 5},
        ],
        schema=hl.tstruct(r=hl.tint32, c=hl.tstr, x=hl.tint32),
        n_partitions=partitions,
    )
    m = t.to_matrix_table(["r"], ["c"], n_partitions=2)
    assert m.count() == (3, 2)
    assert m.entries().count() == 4
    values = {(r.r, r.c): r.x for r in m.entries().collect()}
    assert values == {(1, "a"): 9, (1, "b"): 2, (2, "b"): 4, (None, "b"): 5}
    assert m.entries().collect() == m.entries().collect()


def test_struct_row_major_and_unlocalize_dimension_validation():
    t = hl.range_table(3, 2)
    t = t.annotate(foo=t.idx, bar=hl.struct(v=t.idx * 2), baz=hl.struct(v=t.idx * 3))
    m = t.to_matrix_table_row_major(["bar", "baz"])
    assert m.count() == (3, 2)
    assert m.v.collect() == [0, 0, 2, 3, 4, 6]
    with pytest.raises(ValueError):
        t.to_matrix_table_row_major(["foo", "bar"])
    with pytest.raises(ValueError):
        t.to_matrix_table_row_major(["bar"], entry_field_name="v")
    bad = hl.Table.parallelize([{"r": 0, "cells": [{"x": 1}]}], key="r")
    bad = bad.annotate_globals(cols=[{"s": "a"}, {"s": "b"}])
    mt = bad._unlocalize_entries("cells", "cols", ["s"])
    with pytest.raises((RuntimeError, ValueError), match="length differs"):
        mt.entries().collect()


@pytest.mark.parametrize("nr,nc", [(0, 0), (0, 2), (2, 0), (2, 2)])
def test_zero_field_entry_localization_inverse(nr, nc):
    m = hl.range_matrix_table(nr, nc, 2)
    localized = m.localize_entries("e", "c")
    restored = localized._unlocalize_entries("e", "c", ["col_idx"])
    assert restored.count() == (nr, nc)
    assert restored._same(m)
    assert restored.entries().count() == nr * nc


def test_union_reordered_unkeyed_rows_without_scan_ids():
    m = matrix(3, 2)
    m = m.key_rows_by(k=-m.row_idx).key_rows_by().select_rows()
    union = m.union_rows(m)
    assert union.count() == (6, 2)
    assert union.x.collect() == [25, 26, 15, 16, 5, 6, 25, 26, 15, 16, 5, 6]


def test_row_aligned_views_and_transformations_do_not_index_rows(monkeypatch, tmp_path):
    import vail.matrixtable as module

    def unexpected_index(*args, **kwargs):
        raise AssertionError("row-aligned operation requested coordinate identities")

    monkeypatch.setattr(module, "_index_relation", unexpected_index, raising=False)
    m = matrix(3, 3)
    m = m.key_rows_by(k=0)
    m = m.filter_rows(m.row_idx != 1)
    m = m.choose_cols([2, 0, 2])
    m = m.filter_entries(m.col_idx != 0)
    assert set(m._mir.core._schema) == {module._ROW, module._CELLS}
    assert m.rows().count() == 2
    assert m.x.collect() == [7, 7, 27, 27]
    localized = m.localize_entries("cells", "columns")
    restored = localized._unlocalize_entries("cells", "columns", ["col_idx"])
    assert restored._same(m)
    exploded = restored.annotate_rows(items=hl.array([0, 1])).explode_rows("items")
    assert exploded.entries().count() == 8
    assert restored.union_rows(restored).entries().count() == 8
    unique = restored.key_rows_by("row_idx")
    assert unique.union_cols(unique).entries().count() == 8
    coordinates = restored.entries().to_matrix_table(
        ["row_idx"], ["col_idx"], row_fields=["k"]
    )
    assert coordinates.x.collect() == [7, 27]
    plain = hl.range_table(2).annotate(a=1, b=2)
    assert plain.to_matrix_table_row_major(["a", "b"], "x").count() == (2, 2)
    reopened = restored.checkpoint(tmp_path / "aligned")
    assert reopened._same(restored)
    descriptor = json.loads((tmp_path / "aligned" / "_vail_matrix.json").read_text())
    generation = tmp_path / "aligned" / descriptor["generation"]
    assert descriptor["version"] == 3
    assert (generation / "core").is_dir()
    assert not (generation / "entries").exists()
    assert not (generation / "rows").exists()


def test_duplicate_row_keys_preserve_entry_ownership():
    m = matrix(2, 2)
    m = m.key_rows_by(k=0).select_rows("row_idx")
    localized = m.localize_entries("cells", "columns").collect()
    assert [r.row_idx for r in localized] == [0, 1]
    assert [[e.x for e in r.cells] for r in localized] == [[5, 6], [15, 16]]
    keep = m.filter_rows(m.row_idx == 1)
    assert keep.x.collect() == [15, 16]
    changed = m.annotate_entries(x=hl.if_else(m.row_idx == 0, m.x + 10, m.x - 10))
    assert not changed._same(m)


def test_empty_entry_schema_grouping_uses_no_row_labels():
    import vail.matrixtable as module

    m = hl.range_matrix_table(3, 2, 2)
    annotated = m.annotate_rows(n=hl.agg.count())
    assert annotated.n.collect() == [2, 2, 2]
    grouped = m.group_rows_by(g=m.row_idx % 2).aggregate(n=hl.agg.count())
    assert grouped.n.collect() == [2, 2, 1, 1]
    assert set(grouped._mir.core._schema) == {module._ROW, module._CELLS}


def test_row_aligned_entry_lookup_filters_preserve_presence():
    m = matrix(2, 2)
    lookup = m[m.row_key, m.col_key]
    filtered = m.filter_entries(lookup.x > 10)
    assert filtered.count() == (2, 2)
    assert filtered.x.collect() == [15, 16]
    assert filtered.unfilter_entries().x.collect() == [None, None, 15, 16]


def test_column_sum_preserves_positions_missing_entries_and_empty_rows():
    m = hl.range_matrix_table(4, 5, 3)
    m = m.annotate_entries(x=hl.or_missing(m.row_idx != 1, m.row_idx * 10 + m.col_idx))
    m = m.key_cols_by().choose_cols([4, 1, 4])
    m = m.filter_entries(m.col_idx != 1)
    result = m.annotate_cols(total=hl.agg.sum(m.x))
    assert [(row.col_idx, row.total) for row in result.cols().collect()] == [
        (4, 62), (1, 0), (4, 62),
    ]
    empty = hl.range_matrix_table(0, 3, 2)
    empty = empty.annotate_entries(x=hl.int32(1))
    assert empty.annotate_cols(total=hl.agg.sum(empty.x)).total.collect() == [0, 0, 0]


def test_column_sum_preserves_many_partitions_and_missing_values():
    m = hl.range_matrix_table(80, 3, 80).choose_cols([2, 0, 2])
    m = m.annotate_entries(x=hl.if_else(
        m.row_idx % 3 == 0, hl.missing(hl.tint32), m.row_idx * 10 + m.col_idx,
    ))
    m = m.filter_entries(m.row_idx % 2 == 0)
    result = m.annotate_cols(total=hl.agg.sum(m.x))
    assert result.n_partitions() == 80
    assert result.total.dtype == hl.tint64
    assert result.total.collect() == [
        sum(row * 10 + column for row in range(80) if row % 2 == 0 and row % 3 != 0)
        for column in [2, 0, 2]
    ]
    empty = m.filter_rows(False)
    assert empty.annotate_cols(total=hl.agg.sum(empty.x)).total.collect() == [0, 0, 0]


def test_row_aggregations_skip_unreferenced_column_context():
    m = hl.range_matrix_table(3, 4, 2)
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    result = m.annotate_rows(total=hl.agg.sum(m.x), n=hl.agg.count())
    assert result.total.collect() == [6, 46, 86]
    assert result.n.collect() == [4, 4, 4]


def test_localization_preserves_indexed_row_order_without_resorting(tmp_path):
    m = hl.range_matrix_table(20, 4, 4)
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    path = str(tmp_path / "localized.mt")
    m.write(path)
    restored = hl.read_matrix_table(path)
    localized = restored.localize_entries("entries", "columns")
    rows = localized.head(3).collect()
    assert [row.row_idx for row in rows] == [0, 1, 2]
    assert [[entry.x for entry in row.entries] for row in rows] == [
        [0, 1, 2, 3], [10, 11, 12, 13], [20, 21, 22, 23],
    ]


def test_entry_aggregations_skip_unreferenced_column_context():

    m = hl.range_matrix_table(3, 4, 2)
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    m = m.filter_entries(m.col_idx % 2 == 0)
    result = m.aggregate_entries(hl.struct(stats=hl.agg.stats(m.x), n=hl.agg.count()))
    assert result.n == 6
    assert result.stats.sum == 66
    assert result.stats.mean == 11


def test_entry_context_drops_cell_array_before_exploding():

    m = matrix(2, 3)
    assert m.aggregate_entries(hl.agg.stats(m.x)).sum == 66
    assert m.x.take(2) == [5, 6]
    assert len(m.entries().collect()) == 6


def test_aggregations_and_grouping_have_no_coordinate_or_partition_fallback():
    import vail.matrixtable as module

    m = hl.range_matrix_table(3, 4, 2)
    m = m.annotate_entries(x=m.row_idx * 10 + m.col_idx)
    rows = m.annotate_rows(total=hl.agg.sum(m.x), n=hl.agg.count())
    assert rows.total.collect() == [6, 46, 86]
    assert rows.n.collect() == [4, 4, 4]
    filtered = m.filter_entries(m.col_idx % 2 == 0)
    assert filtered.annotate_rows(total=hl.agg.sum(filtered.x)).total.collect() == [
        2,
        22,
        42,
    ]
    grouped_rows = filtered.group_rows_by(g=filtered.row_idx % 2).aggregate(
        total=hl.agg.sum(filtered.x)
    )
    assert grouped_rows.total.collect() == [20, 0, 24, 0, 10, 0, 12, 0]
    grouped_cols = filtered.group_cols_by(g=filtered.col_idx % 2).aggregate(
        total=hl.agg.sum(filtered.x)
    )
    assert grouped_cols.total.collect() == [2, 0, 22, 0, 42, 0]
    items = m.annotate_cols(items=hl.array([0, 1]))
    exploded = items.explode_cols("items")
    assert exploded.count() == (3, 8)
    lookup = m.annotate_entries(copy=m[m.row_key, m.col_key].x)
    assert lookup.copy.collect() == m.x.collect()
    for result in (rows, grouped_rows, grouped_cols, exploded, lookup):
        plan = result._mir.core._native.ir()
        assert "__vail_matrix_row_id" not in plan
        assert "TableMapPartitions" not in plan
        assert set(result._mir.core._schema) == {module._ROW, module._CELLS}
        assert module._COLUMNS in result._mir.core._global_schema
        assert module._COLUMNS not in result.globals.dtype
    assert not hasattr(module, "_index_relation")


def test_direct_list_reducers_nested_aggregators_and_missing_values():
    m = matrix(2, 3)
    m = m.annotate_entries(x=hl.if_else(m.col_idx == 1, hl.missing(hl.tint32), m.x))
    rows = m.annotate_rows(
        values=hl.agg.collect(m.x),
        count=hl.agg.count(),
        defined=hl.agg.count_where(hl.is_defined(m.x)),
        stats=hl.agg.stats(m.x),
        first=hl.agg.take(m.x, 1),
        grouped=hl.agg.group_by(m.col_idx % 2, hl.agg.sum(m.x)),
    )
    result = rows.rows().collect()
    assert result[0]["values"] == [5, None, 7]
    assert result[0].count == 3
    assert result[0].defined == 2
    assert result[0].stats.mean == 6
    assert result[0].stats.stdev == 1
    assert result[0].first == [5]
    assert result[0].grouped == {0: 12, 1: 0}


def test_group_by_reductions_use_fused_per_list_groups():
    import math

    m = hl.range_matrix_table(2, 3, 2)
    m = m.annotate_entries(
        x=hl.if_else(m.col_idx == 1, hl.missing(hl.tint32), m.row_idx * 10 + m.col_idx)
    )
    annotated_rows = m.annotate_rows(
        means=hl.agg.group_by(m.col_idx % 2, hl.agg.mean(m.x)),
        shifted=hl.agg.group_by(m.col_idx % 2, hl.agg.mean(m.x) + 2),
        counts=hl.agg.group_by(m.col_idx % 2, hl.agg.count()),
        positive=hl.agg.group_by(m.col_idx % 2, hl.agg.count_where(m.x > 0)),
        filtered=hl.agg.group_by(m.col_idx % 2, hl.agg.filter(m.x > 0, hl.agg.count())),
        fully_filtered=hl.agg.group_by(
            m.col_idx % 2,
            hl.agg.filter(m.col_idx < 0, hl.agg.sum(m.x)),
        ),
        summary=hl.agg.group_by(
            m.col_idx % 2,
            hl.struct(
                total=hl.agg.sum(m.x),
                count=hl.agg.count(),
                selected=hl.agg.filter(m.x > 0, hl.agg.sum(m.x)),
                selected_count=hl.agg.filter(m.x > 0, hl.agg.count()),
            ),
        ),
    )
    rows = annotated_rows.rows().collect()
    assert rows[0].means[0] == 1.0 and math.isnan(rows[0].means[1])
    assert rows[0].shifted[0] == 3.0 and math.isnan(rows[0].shifted[1])
    assert rows[0].counts == {0: 2, 1: 1}
    assert rows[0].positive == {0: 1, 1: 0}
    assert rows[0].filtered == {0: 1, 1: 0}
    assert rows[0].fully_filtered == {0: 0, 1: 0}
    assert rows[0].summary[0].total == 2 and rows[0].summary[0].count == 2
    assert rows[0].summary[0].selected == 2
    assert rows[0].summary[0].selected_count == 1
    assert rows[0].summary[1].total == 0 and rows[0].summary[1].count == 1
    assert rows[0].summary[1].selected == 0
    assert rows[0].summary[1].selected_count == 0
    assert rows[1].means[0] == 11.0 and math.isnan(rows[1].means[1])
    assert rows[1].shifted[0] == 13.0 and math.isnan(rows[1].shifted[1])
    assert rows[1].counts == {0: 2, 1: 1}
    assert rows[1].positive == {0: 2, 1: 0}
    assert rows[1].filtered == {0: 2, 1: 0}
    assert rows[1].fully_filtered == {0: 0, 1: 0}
    assert rows[1].summary[0].total == 22 and rows[1].summary[0].count == 2
    assert rows[1].summary[0].selected == 22
    assert rows[1].summary[0].selected_count == 2
    assert rows[1].summary[1].total == 0 and rows[1].summary[1].count == 1
    assert rows[1].summary[1].selected == 0
    assert rows[1].summary[1].selected_count == 0

    cols = m.annotate_cols(
        sums=hl.agg.group_by(m.row_idx % 2, hl.agg.sum(m.x)),
        counts=hl.agg.group_by(m.row_idx % 2, hl.agg.count()),
    ).cols().collect()
    assert cols[0].sums == {0: 0, 1: 10}
    assert cols[0].counts == {0: 1, 1: 1}
    assert cols[1].sums == {0: 0, 1: 0}
    assert cols[1].counts == {0: 1, 1: 1}
    assert cols[2].sums == {0: 2, 1: 12}
    assert cols[2].counts == {0: 1, 1: 1}

    # Identical aggregate IR under distinct filters must retain its occurrence
    # scope, and a filtered value must not be evaluated for excluded entries.
    signed = hl.range_matrix_table(1, 3)
    signed = signed.annotate_entries(x=signed.col_idx - 1)
    shared_sum = hl.agg.sum(signed.x)
    scoped = signed.annotate_rows(
        groups=hl.agg.group_by(
            signed.col_idx % 2,
            hl.struct(
                positive=hl.agg.filter(signed.x > 0, shared_sum),
                negative=hl.agg.filter(signed.x < 0, shared_sum),
                safe_division=hl.agg.filter(
                    signed.x != 0,
                    hl.agg.sum(hl.int32(1) // signed.x),
                ),
            ),
        )
    ).rows().collect()[0].groups
    assert scoped[0].positive == 1 and scoped[0].negative == -1
    assert scoped[0].safe_division == 0
    assert scoped[1].positive == 0 and scoped[1].negative == 0
    assert scoped[1].safe_division == 0

    # A filter scopes reducer values only; it cannot hide an invalid grouping
    # key on an item that the reducer predicate excludes.
    with pytest.raises(Exception):
        signed.annotate_rows(
            groups=hl.agg.group_by(
                hl.int32(1) // signed.x,
                hl.agg.filter(signed.x > 0, hl.agg.sum(signed.x)),
            )
        ).rows().collect()


def test_column_lookups_inside_entries_preserve_values_and_missingness():
    target = hl.range_table(2)
    target = target.annotate(value=target.idx + 100)
    m = matrix(2, 3).choose_cols([2, 0, 0, 1])
    result = m.annotate_entries(
        value=target[m.col_idx].value,
        row_exists=hl.is_defined(m.rows()[m.row_key]),
        col_exists=hl.is_defined(m.cols()[m.col_key]),
        entry_copy=m[m.row_key, m.col_key].x,
    )
    assert result.value.collect() == [None, 100, 100, 101] * 2
    assert result.row_exists.collect() == [True] * 8
    assert result.col_exists.collect() == [True] * 8
    assert result.entry_copy.collect() == m.x.collect()
    assert "__vail_matrix_row_id" not in repr(result._mir.core._native.ir())


def test_missing_native_list_kernels_are_reported_without_row_index_fallback():
    m = matrix(1, 2)
    with pytest.raises(NotImplementedError, match="per-list hist reducer"):
        m.annotate_rows(histogram=hl.agg.hist(hl.float64(m.x), 0, 100, 5))
    with pytest.raises(NotImplementedError, match="correlated list-lookup"):
        target = hl.range_table(2)
        target = target.annotate(v=target.idx)
        m.annotate_entries(v=target[m.x % 2].v)


def _has_native_op(node, op, **properties):
    if isinstance(node, IR_NODE_TYPES):
        if node.get('op') == op and all(node.get(k) == v for k, v in properties.items()):
            return True
        if node.get('op') == 'literal':
            return False
        return any(_has_native_op(child, op, **properties) for child in node.values())
    if isinstance(node, (list, tuple)):
        return any(_has_native_op(child, op, **properties) for child in node)
    return False


def test_show_labels_entries_in_column_slot_order():
    m = hl.range_matrix_table(1, 2)
    m = m.annotate_entries(x=m.col_idx + 10).choose_cols([1, 0])
    output = []
    m.show(handler=output.append)
    lines = [line for line in output[0].splitlines() if line.startswith('|')]
    assert [cell.strip() for cell in lines[0].split('|')[1:-1]] == ['row_idx', '1.x', '0.x']
    assert [cell.strip() for cell in lines[-1].split('|')[1:-1]] == ['0', '11', '10']


def test_filter_entries_preserves_certified_read_keys(tmp_path):
    path = tmp_path / 'entry-filter-pruning.mt'
    hl.range_matrix_table(24, 2, n_partitions=4).write(path)
    source = hl.read_matrix_table(path)
    filtered = source.filter_entries(source.col_idx == 0)
    assert filtered._mir.row_index_keys == source._mir.row_index_keys
    assert filtered._mir.core._keys == source._mir.core._keys
    selected = filtered.filter_rows(filtered.row_idx < 6)
    rows = selected.entries().collect()
    assert [(row.row_idx, row.col_idx) for row in rows] == [(i, 0) for i in range(6)]


def test_nested_matrix_array_aggregation_retains_lexical_and_lookup_types():
    target = hl.range_table(2).annotate(offset=hl.int32(7))
    m = matrix(2, 3).annotate_globals(extra=2)
    lookup = target[m.row_idx].offset
    expression = hl.agg.array_agg(
        lambda item: hl.agg.sum(item + m.x + lookup + m.extra),
        hl.array([1, 4]),
    )
    result = m.annotate_rows(nested=expression)
    assert result.nested.dtype == hl.tarray(hl.tint64)
    assert result.nested.collect() == [[48, 57], [78, 87]]


def test_row_aggregations_keep_sparse_column_alignment_and_call_fields():
    m = hl.range_matrix_table(2, 4).annotate_globals(offset=10)
    m = m.annotate_rows(alleles=['A', 'C'])
    m = m.annotate_entries(
        GT=hl.if_else(m.col_idx == 2, hl.missing(hl.tcall), hl.call(0, 1)),
        value=m.row_idx + m.col_idx,
        unused=hl.range(50),
    ).choose_cols([3, 1, 1, 2])
    m = m.filter_entries(m.col_idx != 3)
    result = m.annotate_rows(
        cs=hl.agg.call_stats(m.GT, m.alleles),
        values=hl.agg.collect(m.value),
        count=hl.agg.count(),
        weighted=hl.agg.sum(m.value + m.col_idx + m.offset),
        selected=hl.agg.filter(m.col_idx == 1, hl.agg.collect(m.value)),
    ).rows().collect()
    assert [row.cs.AC for row in result] == [[2, 2], [2, 2]]
    assert [row.cs.AN for row in result] == [4, 4]
    assert [row["values"] for row in result] == [[1, 1, 2], [2, 2, 3]]
    assert [row.count for row in result] == [3, 3]
    assert [row.weighted for row in result] == [38, 41]
    assert [row.selected for row in result] == [[1, 1], [2, 2]]


def test_column_sum_resolves_row_lookups_in_entry_context():
    target = hl.range_table(3).annotate(weight=hl.int64(2)).key_by('idx')
    m = hl.range_matrix_table(3, 4, 2).key_cols_by().choose_cols([3, 1, 3])
    m = m.annotate_entries(x=m.row_idx + 1)
    result = m.annotate_cols(total=hl.agg.sum(m.x * target[m.row_idx].weight))
    assert [(row.col_idx, row.total) for row in result.cols().collect()] == [(3, 12), (1, 12), (3, 12)]
