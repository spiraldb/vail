"""Lists and row-owned matrix lists execute the table reducer algorithms."""
import math

import pytest
import vail as hl


def test_matrix_call_stats_matches_tables_with_row_varying_initializers():
    mt = hl.range_matrix_table(4, 5)
    mt = mt.annotate_rows(n=mt.row_idx + 1)
    mt = mt.annotate_entries(GT=hl.if_else(
        mt.col_idx == 4, hl.missing(hl.tcall),
        hl.if_else(mt.col_idx == 3, hl.call(mt.col_idx % mt.n),
                   hl.if_else(mt.col_idx % 2 == 0,
                              hl.call(mt.col_idx % mt.n, (mt.col_idx + 1) % mt.n, phased=True),
                              hl.call(mt.col_idx % mt.n, (mt.col_idx + 1) % mt.n)))))
    result = mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, mt.n))
    for row in result.rows().collect():
        calls = mt.filter_rows(mt.row_idx == row.row_idx).entries()
        expected = calls.aggregate(hl.agg.call_stats(calls.GT, row.n))
        assert row.stats == expected


def test_empty_matrix_call_stats_retains_shared_identity_and_validates_initializer():
    mt = hl.range_matrix_table(3, 0)
    mt = mt.annotate_rows(n=mt.row_idx)
    mt = mt.annotate_entries(GT=hl.missing(hl.tcall))
    result = mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, mt.n))
    for row in result.rows().collect():
        assert row.stats.AC == [0] * row.n
        assert row.stats.homozygote_count == [0] * row.n
        assert row.stats.AN == 0
        assert row.stats.AF is None
    with pytest.raises(RuntimeError, match="nonnegative"):
        mt.annotate_rows(stats=hl.agg.call_stats(mt.GT, -1)).rows().collect()


def test_filtered_matrix_call_stats_uses_call_list_and_row_initializer():
    mt = hl.range_matrix_table(2, 4)
    mt = mt.annotate_rows(n=mt.row_idx + 2)
    mt = mt.annotate_entries(GT=hl.if_else(
        mt.row_idx == 0,
        hl.if_else(
            mt.col_idx == 0, hl.call(0, 0),
            hl.if_else(mt.col_idx == 1, hl.call(0, 1),
                       hl.if_else(mt.col_idx == 2, hl.call(1, 1), hl.missing(hl.tcall)))),
        hl.if_else(
            mt.col_idx == 0, hl.call(0, 2),
            hl.if_else(mt.col_idx == 1, hl.call(1, 2),
                       hl.if_else(mt.col_idx == 2, hl.call(2, 2), hl.missing(hl.tcall)))),
    ))
    selected = hl.agg.filter(mt.col_idx % 2 == 0, hl.agg.call_stats(mt.GT, mt.n))
    rows = mt.annotate_rows(stats=selected).rows().collect()
    assert [row.stats for row in rows] == [
        hl.Struct(AC=[2, 2], AF=[0.5, 0.5], AN=4,
                  homozygote_count=[1, 1]),
        hl.Struct(AC=[1, 0, 3], AF=[0.25, 0.0, 0.75], AN=4,
                  homozygote_count=[0, 0, 1]),
    ]


def test_collection_and_table_reducers_share_large_integer_and_empty_semantics():
    values = hl.literal([2**53 + 1, 7, None, -2], hl.tarray(hl.tint64))
    table = hl.Table.parallelize(
        [hl.Struct(x=2**53 + 1), hl.Struct(x=7), hl.Struct(x=None), hl.Struct(x=-2)],
        schema=hl.tstruct(x=hl.tint64))
    assert hl.eval(values.aggregate(lambda x: hl.agg.sum(x))) == table.aggregate(hl.agg.sum(table.x))
    empty = hl.empty_array(hl.tfloat64)
    assert math.isnan(hl.eval(empty.aggregate(lambda x: hl.agg.mean(x))))
    assert hl.eval(empty.aggregate(lambda x: hl.agg.sum(x))) == 0.0


def test_collection_call_stats_uses_native_initializer_even_without_calls():
    calls = hl.empty_array(hl.tcall)
    result = hl.eval(calls.aggregate(lambda call: hl.agg.call_stats(call, 3)))
    assert result == hl.Struct(AC=[0, 0, 0], AF=None, AN=0, homozygote_count=[0, 0, 0])


@pytest.mark.parametrize("dtype,values", [
    (hl.tint64, [2**63 - 1, 1, None]),
    (hl.tint64, [2**53 + 1, 7, -2]),
    (hl.tfloat32, [16777216.0, 1.0, -16777216.0]),
    (hl.tfloat64, [float("inf"), -float("inf"), None]),
    (hl.tfloat64, [None, None]),
    (hl.tint64, []),
])
def test_direct_matrix_entry_sum_matches_shared_table_numeric_semantics(dtype, values):
    mt = hl.range_matrix_table(1, len(values))
    literals = hl.literal(values, hl.tarray(dtype))
    mt = mt.annotate_entries(x=literals[mt.col_idx])
    actual = mt.annotate_rows(total=hl.agg.sum(mt.x)).rows().collect()[0].total
    table = hl.Table.parallelize([hl.Struct(x=x) for x in values], schema=hl.tstruct(x=dtype))
    expected = table.aggregate(hl.agg.sum(table.x))
    if isinstance(expected, float) and math.isnan(expected):
        assert math.isnan(actual)
    else:
        assert actual == expected


def test_sparse_matrix_entry_sum_does_not_count_removed_cells():
    mt = hl.range_matrix_table(1, 4)
    mt = mt.annotate_entries(x=hl.int64(mt.col_idx + 1))
    mt = mt.filter_entries(mt.col_idx % 2 == 0)
    assert mt.annotate_rows(total=hl.agg.sum(mt.x)).rows().collect()[0].total == 4


def test_ordered_take_uses_same_native_kernel_for_matrix_collection_and_table():
    mt = hl.range_matrix_table(1, 5)
    mt = mt.annotate_entries(x=hl.int64(5 - mt.col_idx))
    table = mt.entries()
    expected = table.aggregate(hl.agg.take(table.x, 3, ordering=table.x))
    actual = mt.annotate_rows(xs=hl.agg.take(mt.x, 3, ordering=mt.x)).rows().collect()[0].xs
    values = hl.literal([5, 4, 3, 2, 1])
    collected = hl.eval(values.aggregate(lambda x: hl.agg.take(x, 3, ordering=x)))
    assert actual == collected == expected == [1, 2, 3]
    assert hl.eval(values.aggregate(lambda x: hl.agg.take(x, 0, ordering=x))) == []


def test_unordered_take_keeps_missing_values_and_order():
    values = hl.literal([None, 3, 1, 5], hl.tarray(hl.tint32))
    assert hl.eval(values.aggregate(lambda x: hl.agg.take(x, 3))) == [None, 3, 1]
