"""Execution boundaries and relational plans for whole-table aggregation."""

import pytest
import math
import vail as hl


def test_common_reducers_return_consistent_large_partitioned_results():
    n = 200_003
    t = hl.range_table(n, n_partitions=7)
    query = hl.struct(n=hl.agg.count(), total=hl.agg.sum(t.idx),
                      smallest=hl.agg.min(t.idx), largest=hl.agg.max(t.idx))
    assert t.aggregate(query) == hl.Struct(n=n, total=n*(n-1)//2, smallest=0, largest=n-1)


def test_mean_uses_native_state_for_group_scan_and_array_aggregation():
    table = hl.Table.parallelize(
        [
            {"group": "a", "x": 1, "xs": [1, 10]},
            {"group": "a", "x": 3, "xs": [3, 30]},
            {"group": "b", "x": None, "xs": [None, 50]},
        ],
        schema=hl.tstruct(group=hl.tstr, x=hl.tint32, xs=hl.tarray(hl.tint32)),
        n_partitions=3,
    )
    assert table.aggregate(hl.agg.mean(table.x)) == 2.0

    grouped = table.group_by(group=table.group).aggregate(value=hl.agg.mean(table.x)).collect()
    values = {row.group: row.value for row in grouped}
    assert values["a"] == 2.0
    assert math.isnan(values["b"])

    scanned = table.annotate(prefix=hl.scan.mean(table.x)).prefix.collect()
    assert math.isnan(scanned[0])
    assert scanned[1] == 1.0
    assert scanned[2] == 2.0

    array_means = table.aggregate(
        hl.agg.array_agg(lambda value: hl.agg.mean(value), table.xs)
    )
    assert array_means == [2.0, 30.0]

    boolean_table = hl.Table.parallelize(
        [{"x": True}, {"x": False}, {"x": None}],
        schema=hl.tstruct(x=hl.tbool),
        n_partitions=2,
    )
    assert boolean_table.aggregate(hl.agg.mean(boolean_table.x)) == 0.5


def test_mean_preserves_matrix_entry_coordinate_groups():
    matrix = hl.range_matrix_table(2, 3, 2)
    matrix = matrix.annotate_entries(value=hl.int32(matrix.row_idx + matrix.col_idx))
    entries = matrix.entries()
    grouped = entries.group_by(row=entries.row_idx).aggregate(
        value=hl.agg.mean(entries.value)
    ).collect()
    values = {row.row: row.value for row in grouped}
    assert values == {0: 1.0, 1: 2.0}

    row_means = matrix.annotate_rows(mean_value=hl.agg.mean(matrix.value)).rows().collect()
    col_means = matrix.annotate_cols(mean_value=hl.agg.mean(matrix.value)).cols().collect()
    assert [row.mean_value for row in row_means] == [1.0, 2.0]
    assert [column.mean_value for column in col_means] == [0.5, 1.5, 2.5]


def test_collect_uses_ordered_stream_across_files(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(23, n_partitions=4).write(path)
    t = hl.read_table(path)
    assert t.aggregate(hl.agg.collect(t.idx)) == list(range(23))
    reverse = t.order_by(hl.desc(t.idx))
    assert reverse.aggregate(hl.agg.collect(reverse.idx)) == list(reversed(range(23)))


def test_collect_multiset_semantics_across_partitions():
    t = hl.Table.parallelize(
        [{'x': 2, 'nested': [2, None]}, {'x': 2, 'nested': [9, None]},
         {'x': None, 'nested': None}, {'x': 2, 'nested': [2, None]},
         {'x': 1, 'nested': [1]}],
        schema=hl.tstruct(x=hl.tint32, nested=hl.tarray(hl.tint32)),
        n_partitions=3,
    )
    assert sorted(map(repr, t.aggregate(hl.agg.collect(t.row)))) == sorted(map(repr, [
        hl.Struct(x=2, nested=[2, None]), hl.Struct(x=None, nested=None),
        hl.Struct(x=2, nested=[9, None]), hl.Struct(x=2, nested=[2, None]),
        hl.Struct(x=1, nested=[1]),
    ]))
    grouped = t.group_by(k=t.x).aggregate(values=hl.agg.collect(t.nested)).collect()
    grouped_multisets = {
        row.k: sorted(map(repr, row["values"]))
        for row in grouped
    }
    expected_multisets = {
        2: sorted(map(repr, [[2, None], [9, None], [2, None]])),
        None: sorted(map(repr, [None])),
        1: sorted(map(repr, [[1]])),
    }
    assert grouped_multisets == expected_multisets


def test_query_backed_scalar_reduction_is_lazy_and_reusable(tmp_path):
    path = tmp_path / 'source'
    hl.range_table(6, n_partitions=2).write(path)
    rows = hl.query_table(str(path), hl.interval(0, 6))
    total = rows.aggregate(lambda row: hl.agg.sum(row.idx))
    backup = tmp_path / 'unavailable'
    path.rename(backup)
    target = hl.range_table(2)
    annotated = target.annotate(total=total + target.idx)
    assert annotated.head(0).collect() == []
    with pytest.raises(FileNotFoundError):
        hl.eval(total)
    backup.rename(path)
    assert annotated.total.collect() == [15, 16]
    path.rename(backup)
    with pytest.raises(FileNotFoundError):
        hl.eval(total)


def test_filter_and_explode_preserve_count_sum_and_collection():
    t = hl.Table.parallelize(
        [{'xs': [1, None, 2], 'keep': True}, {'xs': None, 'keep': True},
         {'xs': [], 'keep': True}, {'xs': [100], 'keep': None}],
        schema=hl.tstruct(xs=hl.tarray(hl.tint32), keep=hl.tbool), n_partitions=3)
    query = hl.agg.filter(t.keep, hl.agg.explode(
        lambda x: hl.struct(n=hl.agg.count(), total=hl.agg.sum(x), items=hl.agg.collect(x)), t.xs))
    assert t.aggregate(query) == hl.Struct(n=3, total=3, items=[1, None, 2])


def test_nested_explode_can_capture_outer_element_and_row():
    t = hl.range_table(2)
    query = hl.agg.explode(lambda x: hl.agg.explode(
        lambda y: hl.agg.collect(x + y + t.idx), [10, 20]), [1, 2])
    assert t.aggregate(query) == [11, 21, 12, 22, 12, 22, 13, 23]


def test_filtered_out_rows_do_not_evaluate_aggregate_body():
    t = hl.range_table(3)
    query = hl.agg.filter(False, hl.agg.sum(hl.array([1])[t.idx + 10]))
    assert t.aggregate(query) == 0


def test_collect_nested_values_and_sets_preserves_arrow_types():
    t = hl.range_table(3).annotate_globals(offset=7)
    result = t.aggregate(hl.agg.collect(hl.struct(i=t.idx, xs=[t.idx, None],
                                                  s=hl.literal({1, 2}), g=t.offset)))
    assert result == [hl.Struct(i=i, xs=[i, None], s={1, 2}, g=7) for i in range(3)]


def test_public_aggregate_boundaries_reject_invalid_scopes_and_argument_types():
    table = hl.range_table(2)
    with pytest.raises((ValueError, TypeError)):
        table.aggregate(table.idx)
    with pytest.raises((ValueError, TypeError)):
        table.aggregate(hl.agg.sum(hl.agg.count()))
    with pytest.raises(TypeError):
        hl.agg.filter(1, hl.agg.count())
    with pytest.raises(TypeError):
        hl.agg.explode(lambda x: hl.agg.count(), 1)
    with pytest.raises(TypeError):
        hl.agg.count(1)
