"""Nested reducers share the logical input, including its random row identities."""
import pytest
import vail as hl


@pytest.mark.parametrize('partitions', [1, 3])
def test_array_aggregation_over_random_derived_rows(partitions):
    table = hl.range_table(12, n_partitions=partitions)
    table = table.annotate(unused=hl.rand_bool(.5, seed=17), xs=hl.range(3))
    assert table.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs)) == [0, 12, 24]


def test_nested_reducers_use_same_random_values_as_collect():
    table = hl.range_table(24, n_partitions=4)
    table = table.annotate(xs=hl.range(3).map(lambda _: hl.rand_int32(0, 100, seed=23)))
    rows = table.collect()
    expected = [sum(row.xs[i] for row in rows) for i in range(3)]
    expression = hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs)
    assert table.aggregate(expression) == expected
    assert table.aggregate(expression) == expected
    result = table.aggregate(hl.agg.group_by(table.idx % 2, expression))
    assert result == {g: [sum(row.xs[i] for row in rows if row.idx % 2 == g)
                          for i in range(3)] for g in range(2)}


@pytest.mark.parametrize('partitions', [1, 4])
def test_recursive_array_aggregation_over_random_derived_rows(partitions):
    table = hl.range_table(7, n_partitions=partitions)
    table = table.annotate(unused=hl.rand_bool(.5), xss=[[table.idx, 1], [table.idx + 2, 3]])
    result = table.aggregate(hl.agg.array_agg(
        lambda xs: hl.agg.array_agg(lambda x: hl.agg.sum(x), xs), table.xss))
    assert result == [[21, 7], [35, 21]]


def test_grouped_nested_reducers_use_native_coordinate_aggregate():
    table = hl.range_table(12, n_partitions=3)
    table = table.annotate(unused=hl.rand_bool(.5), xs=[table.idx, table.idx + 1])
    grouped = table.group_by(g=table.idx % 2).aggregate(
        xs=hl.agg.array_agg(lambda x: hl.agg.sum(x), table.xs),
        counts=hl.agg.group_by(table.idx % 3, hl.agg.count()),
    )
    plan = grouped._native.logical_plan()
    assert 'Aggregate:' in plan
    assert 'vail_coordinate_sum' in plan
    assert 'VailPartitionOperation: Aggregate' not in plan
    for _ in range(5):
        assert grouped.collect() == [
            hl.Struct(g=0, xs=[30, 36], counts={0: 2, 1: 2, 2: 2}),
            hl.Struct(g=1, xs=[36, 42], counts={0: 2, 1: 2, 2: 2}),
        ]


def test_fold_over_random_derived_rows_with_global_initializer():
    table = hl.range_table(12, n_partitions=3).annotate_globals(zero=0)
    table = table.annotate(x=hl.rand_int32(0, 100, seed=31))
    expected = sum(row.x for row in table.collect())
    assert table.aggregate(hl.agg.fold(table.zero, lambda a: a + table.x, lambda a, b: a + b)) == expected
    empty = table.head(0)
    assert empty.aggregate(hl.agg.fold(empty.zero, lambda a: a + empty.x, lambda a, b: a + b)) == 0


def test_nested_filter_preserves_empty_group_defaults():
    table = hl.range_table(6, n_partitions=2)
    table = table.annotate(unused=hl.rand_bool(.5), xs=[table.idx])
    grouped = table.group_by(g=table.idx % 2).aggregate(
        xs=hl.agg.array_agg(lambda x: hl.agg.filter(x < 0, hl.agg.sum(x)), table.xs),
        groups=hl.agg.filter(table.idx < 0, hl.agg.group_by(table.idx, hl.agg.count())),
    )
    assert grouped.collect() == [hl.Struct(g=g, xs=[0], groups={}) for g in range(2)]


def test_upstream_array_agg_sum_vs_sum():
    # Hail expr/Tests.test_agg_array_agg_sum_vs_sum, original assertion.
    ht = hl.utils.range_table(25).annotate(x=hl.range(0, 10).map(lambda _: hl.rand_bool(0.5)))
    assert ht.aggregate(hl.agg.array_agg(lambda x: hl.agg.sum(x), ht.x) == hl.agg.array_sum(ht.x))


def test_upstream_random_array_correlation():
    # Hail expr/Tests.test_agg_corr, with NumPy as the independent reference.
    from numpy import corrcoef

    ht = hl.utils.range_table(10)
    ht = ht.annotate(tests=hl.range(0, 10).map(lambda i: hl.struct(
        x=hl.if_else(hl.rand_bool(0.1), hl.missing(hl.tfloat64), hl.rand_unif(-10, 10)),
        y=hl.if_else(hl.rand_bool(0.1), hl.missing(hl.tfloat64), hl.rand_unif(-10, 10)),
    )))
    results = ht.aggregate(hl.agg.array_agg(
        lambda test: (hl.agg.corr(test.x, test.y), hl.agg.collect((test.x, test.y))), ht.tests))
    for corr, xy in results:
        filtered = [(x, y) for x, y in xy if x is not None and y is not None]
        reference = corrcoef([x for x, _ in filtered], [y for _, y in filtered])[0, 1]
        assert corr == pytest.approx(reference, abs=1e-7)


def test_upstream_random_table_to_matrix_round_trip():
    # Hail table/Tests.test_to_matrix_table, original dimensions and assertion.
    n, m = 50, 50
    mt = hl.utils.range_matrix_table(n, m)
    mt = mt.key_cols_by(s='Col' + hl.str(m - mt.col_idx))
    mt = mt.annotate_cols(c1=hl.rand_bool(0.5))
    mt = mt.annotate_rows(r1=hl.rand_bool(0.5))
    mt = mt.annotate_entries(e1=hl.rand_bool(0.5))
    re_mt = mt.entries().to_matrix_table(['row_idx'], ['s'], ['r1'], ['col_idx', 'c1'])
    new_col_order = re_mt.col_idx.collect()
    mapping = [t[1] for t in sorted([(old, new) for new, old in enumerate(new_col_order)])]
    assert re_mt.choose_cols(mapping).drop('col_idx')._same(mt.drop('col_idx'))
