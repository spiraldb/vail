"""Public contracts for composite and filtered coordinate aggregations."""

from collections import Counter

import pytest
import vail as hl


@pytest.mark.parametrize("partitions", [1, 3])
def test_composite_coordinate_reducers_keep_filter_and_null_semantics(partitions):
    table = hl.Table.parallelize(
        [
            {"xs": [None, 1, None], "keep": True},
            {"xs": [2, 3, None], "keep": False},
            {"xs": [4, None, None], "keep": True},
            {"xs": None, "keep": True},
        ],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), keep=hl.tbool),
        n_partitions=partitions,
    )
    result = table.aggregate(hl.agg.array_agg(
        lambda x: hl.struct(
            total=hl.agg.sum(x),
            n=hl.agg.count(),
            positives=hl.agg.count_where(x > 2),
            product=hl.agg.product(x),
            kept=hl.agg.filter(table.keep, hl.agg.collect(x)),
            selected=hl.agg.filter(x > 2, hl.struct(
                n=hl.agg.count(), total=hl.agg.sum(x))),
            combined=hl.agg.sum(x) + hl.agg.count(),
        ),
        table.xs,
    ))
    assert [r.total for r in result] == [6, 4, 0]
    assert [r.n for r in result] == [3, 3, 3]
    assert [r.positives for r in result] == [1, 1, 0]
    assert [r.product for r in result] == [8, 3, 1]
    assert [Counter(r.kept) for r in result] == [
        Counter([None, 4]), Counter([1, None]), Counter([None, None])]
    assert [r.selected for r in result] == [
        hl.Struct(n=1, total=4), hl.Struct(n=1, total=3), hl.Struct(n=0, total=0)]
    assert [r.combined for r in result] == [9, 7, 3]


def test_coordinate_width_is_independent_for_each_table_group():
    table = hl.Table.parallelize(
        [
            {"g": 0, "xs": [1, None]},
            {"g": 1, "xs": [5]},
            {"g": 0, "xs": [3, 4]},
            {"g": 1, "xs": [7]},
        ],
        schema=hl.tstruct(g=hl.tint32, xs=hl.tarray(hl.tint32)),
        n_partitions=3,
    )
    result = table.group_by(table.g).aggregate(
        coordinates=hl.agg.array_agg(
            lambda x: hl.struct(total=hl.agg.sum(x), n=hl.agg.count()), table.xs))
    assert result.collect() == [
        hl.Struct(g=0, coordinates=[hl.Struct(total=4, n=2), hl.Struct(total=4, n=2)]),
        hl.Struct(g=1, coordinates=[hl.Struct(total=12, n=2)]),
    ]


@pytest.mark.parametrize("arrays,expected", [
    ([None, None], None),
    ([[], None], []),
    ([[None, None], None], [hl.Struct(n=0, xs=[]), hl.Struct(n=0, xs=[])]),
])
def test_composite_coordinates_preserve_shape_when_every_row_is_filtered(arrays, expected):
    table = hl.Table.parallelize(
        [{"xs": xs} for xs in arrays],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32)),
        n_partitions=2,
    )
    result = table.aggregate(hl.agg.array_agg(
        lambda x: hl.agg.filter(False, hl.struct(n=hl.agg.count(), xs=hl.agg.collect(x))),
        table.xs,
    ))
    assert result == expected


def test_grouped_matrix_coordinate_callbacks_preserve_column_alignment():
    matrix = hl.range_matrix_table(5, 3, n_partitions=3)
    matrix = matrix.annotate_entries(value=matrix.row_idx * 10 + matrix.col_idx)
    matrix = matrix.choose_cols([2, 0, 1])
    matrix = matrix.filter_entries((matrix.row_idx != 2) | (matrix.col_idx != 0))
    grouped = matrix.group_rows_by(g=matrix.row_idx % 2).aggregate_entries(
        total=hl.agg.sum(matrix.value), n=hl.agg.count()).result()
    actual = {(e.g, e.col_idx): (e.total, e.n) for e in grouped.entries().collect()}
    assert actual == {
        (0, 2): (66, 3), (0, 0): (40, 2), (0, 1): (63, 3),
        (1, 2): (44, 2), (1, 0): (40, 2), (1, 1): (42, 2),
    }
    rows_only = matrix.group_rows_by(g=matrix.row_idx % 2).aggregate_rows(
        rows=hl.agg.count()).result()
    assert [(r.g, r.rows) for r in rows_only.rows().collect()] == [(0, 3), (1, 2)]
    assert {(e.g, e.col_idx) for e in rows_only.entries().collect()} == set(actual)


def test_coordinate_take_and_filtered_empty_struct_collect():
    table = hl.range_table(6, n_partitions=3)
    table = table.annotate(xs=[table.idx, table.idx + 10])
    result = table.aggregate(hl.agg.array_agg(
        lambda x: hl.struct(
            first=hl.agg.take(x, 2),
            empty=hl.agg.filter(table.idx % 2 == 0, hl.agg.collect(hl.struct())),
        ),
        table.xs,
    ))
    assert [r.first for r in result] == [[0, 1], [10, 11]]
    assert [r.empty for r in result] == [[hl.Struct()] * 3, [hl.Struct()] * 3]


@pytest.mark.parametrize("partitions", [1, 3])
@pytest.mark.parametrize("dtype,arrays,expected", [
    (hl.tint64, [[9223372036854775807, None], [1, 4]], [-9223372036854775808, 4]),
    (hl.tfloat64, [[1.5, None], [2.25, None]], [3.75, 0.0]),
    (hl.tbool, [[True, None], [True, False]], [2, 0]),
])
def test_coordinate_sum_widening_nulls_and_partial_merges(partitions, dtype, arrays, expected):
    table = hl.Table.parallelize(
        [{"xs": xs} for xs in arrays],
        schema=hl.tstruct(xs=hl.tarray(dtype)), n_partitions=partitions)
    assert table.aggregate(hl.agg.array_sum(table.xs)) == expected
    assert table.aggregate(hl.agg.array_agg(
        lambda x: hl.agg.filter(False, hl.agg.sum(x)), table.xs)) == [0, 0]
