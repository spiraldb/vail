import math

import pytest
import vail as hl


def test_grouped_scopes_preserve_null_keys_and_empty_identities():
    t = hl.Table.parallelize(
        [dict(k=None, xs=None, x=0), dict(k=1, xs=[], x=1),
         dict(k=2, xs=[None, 3], x=2), dict(k=2, xs=[4], x=3)],
        schema=hl.tstruct(k=hl.tint32, xs=hl.tarray(hl.tint32), x=hl.tint32),
        n_partitions=3,
    )
    result = t.group_by('k').aggregate(
        n=hl.agg.count(),
        filtered=hl.agg.filter(t.x < 0, hl.struct(
            n=hl.agg.count(), product=hl.agg.product(t.x),
            values=hl.agg.collect(t.x), any=hl.agg.any(t.x > 0),
            all=hl.agg.all(t.x > 0), mean=hl.agg.mean(t.x), min=hl.agg.min(t.x))),
        exploded=hl.agg.explode(lambda x: hl.struct(n=hl.agg.count(), values=hl.agg.collect(x)), t.xs),
    )
    rows = result.collect()
    assert [r.k for r in rows] == [1, 2, None]
    assert [(r.n, r.exploded.n, r.exploded["values"]) for r in rows] == [(1, 0, []), (2, 3, [None, 3, 4]), (1, 0, [])]
    for row in rows:
        f = row.filtered
        assert (f.n, f.product, f["values"], f.any, f.all, f.min) == (0, 1, [], False, True, None)
        assert math.isnan(f.mean)
    plan = result._native.physical_plan()
    assert 'AggregateExec' in plan and 'HashJoinExec' in plan
    assert 'UnnestExec' in plan and 'FilterExec' in plan


def test_grouped_multi_batch_and_multiple_files(tmp_path):
    t = hl.range_table(200_003, n_partitions=7)
    path = tmp_path / 'source'
    t.write(str(path))
    assert len(list(path.rglob('*.vortex'))) >= 2
    t = hl.read_table(str(path))
    result = t.group_by(k=t.idx % 7).aggregate(n=hl.agg.count(), total=hl.agg.sum(t.idx))
    plan = result._native.physical_plan()
    assert plan.count('AggregateExec') == 2
    assert plan.count('mode=Partial') == 1
    assert plan.count('mode=Final') == 1
    for row in result.collect():
        expected = range(row.k, 200_003, 7)
        assert row.n == len(expected)
        assert row.total == sum(expected)
    destination = tmp_path / 'result'
    result.write(str(destination))
    assert hl.read_table(str(destination))._same(result)


def test_grouped_construction_is_lazy(tmp_path):
    t = hl.range_table(12)
    path = tmp_path / 'source'
    t.write(str(path))
    t = hl.read_table(str(path))
    data = next(path.rglob('*.vortex'))
    contents = data.read_bytes()
    data.unlink()
    try:
        result = t.group_by(k=t.idx % 3).aggregate(n=hl.agg.count())
        assert result.head(0).collect() == []
        with pytest.raises(Exception):
            result.collect()
    finally:
        data.write_bytes(contents)
    assert result.collect() == [hl.Struct(k=i, n=4) for i in range(3)]


def test_grouped_empty_output_and_no_key():
    t = hl.range_table(4)
    assert t.group_by(k=t.idx % 2).aggregate().collect() == [hl.Struct(k=0), hl.Struct(k=1)]
    assert t.group_by().aggregate().collect() == [hl.Struct()]
    t = hl.range_table(0)
    assert t.group_by().aggregate(n=hl.agg.count()).collect() == []
    assert t.group_by().aggregate().collect() == []


def test_grouped_external_globals_and_chaining():
    external = hl.range_table(0).annotate_globals(offset=2).index_globals()
    t = hl.range_table(8).annotate_globals(g=10)
    result = t.group_by(k=t.idx % external.offset).aggregate(
        values=hl.agg.collect(t.idx), total=hl.agg.sum(t.idx) + t.g + external.offset)
    assert result.collect() == [hl.Struct(k=0, values=[0, 2, 4, 6], total=24),
                                hl.Struct(k=1, values=[1, 3, 5, 7], total=28)]
    assert hl.eval(result.index_globals().g) == 10
    assert result.group_by().aggregate(total=hl.agg.sum(result.total)).collect() == [hl.Struct(total=52)]


def test_grouped_filter_short_circuits_invalid_argument():
    t = hl.Table.parallelize([dict(k=None, xs=[]), dict(k=1, xs=[])],
                            schema=hl.tstruct(k=hl.tint32, xs=hl.tarray(hl.tint32)))
    result = t.group_by('k').aggregate(s=hl.agg.filter(False, hl.agg.sum(t.xs[0])))
    assert result.collect() == [hl.Struct(k=1, s=0), hl.Struct(k=None, s=0)]


def test_grouped_orders_float_and_struct_keys():
    t = hl.range_table(3)
    assert t.group_by(k=hl.float64(t.idx)).aggregate(n=hl.agg.count()).collect() == [
        hl.Struct(k=float(i), n=1) for i in range(3)]
    assert t.group_by(k=hl.struct(x=t.idx)).aggregate(n=hl.agg.count()).collect() == [
        hl.Struct(k=hl.Struct(x=i), n=1) for i in range(3)]


def test_grouped_float_missing_nan_and_signed_zero():
    rows = [{'k': -0.0}, {'k': 0.0}, {'k': float('nan')},
            {'k': float('nan')}, {'k': None}]
    t = hl.Table.parallelize(rows, schema=hl.tstruct(k=hl.tfloat64))
    actual = t.group_by('k').aggregate(n=hl.agg.count()).collect()
    assert [r.n for r in actual] == [2, 2, 1]
    assert actual[0].k == 0.0
    assert math.isnan(actual[1].k)
    assert actual[2].k is None
    scoped = t.group_by('k').aggregate(
        n=hl.agg.count(),
        none=hl.agg.filter(False, hl.agg.count()),
    ).collect()
    assert [row.none for row in scoped] == [0, 0, 0]
    assert [row.n for row in scoped] == [2, 2, 1]


@pytest.mark.parametrize('dtype, values, expected_counts', [
    (hl.tarray(hl.tint32), [[1, None], [1, None], [1, 2], None], [1, 2, 1]),
    (hl.tset(hl.tint32), [{2, 1}, {1, 2}, {3}, None], [2, 1, 1]),
    (hl.tdict(hl.tstr, hl.tint32),
     [{'a': 1, 'b': 2}, {'b': 2, 'a': 1}, {'a': 2}, None], [2, 1, 1]),
])
def test_grouped_nested_keys_follow_hail_order(dtype, values, expected_counts):
    table = hl.Table.parallelize([{'k': value} for value in values], schema=hl.tstruct(k=dtype))
    if isinstance(dtype, (hl.tset, hl.tdict)):
        with pytest.raises(NotImplementedError, match='Set/Dictionary'):
            table.group_by('k')
    else:
        assert [row.n for row in table.group_by('k').aggregate(n=hl.agg.count()).collect()] == expected_counts


def test_localized_struct_hash_matches_equality():
    a = hl.Struct(k=1, n=2)
    b = hl.Struct(n=2, k=1)
    assert a == b and hash(a) == hash(b)
    assert len({a, b}) == 1
    with pytest.raises(TypeError):
        hash(hl.Struct(xs=[]))


def test_grouped_reducers_with_unused_and_referenced_global_arrays():
    table = hl.utils.range_table(12, n_partitions=3)
    table = table.annotate_globals(payload=hl.range(1024), shift=7)
    unused = table.group_by(k=table.idx % 3).aggregate(
        n=hl.agg.count(), total=hl.agg.sum(table.idx))
    assert [(x.k, x.n, x.total) for x in unused.collect()] == [
        (0, 4, 18), (1, 4, 22), (2, 4, 26)]
    referenced = table.group_by(k=(table.idx + table.shift) % 3).aggregate(
        total=hl.agg.sum(table.idx + table.shift),
        payload=hl.agg.take(table.payload[table.idx], 1))
    assert [(x.k, x.total) for x in referenced.collect()] == [(0, 54), (1, 46), (2, 50)]


def test_composite_column_reducers_preserve_filtered_entry_positions():
    matrix = hl.utils.range_matrix_table(12, 4, n_partitions=3)
    matrix = matrix.annotate_entries(x=hl.or_missing(matrix.row_idx % 3 != 0,
        matrix.row_idx + matrix.col_idx))
    matrix = matrix.annotate_cols(result=hl.struct(
        n=hl.agg.count(), total=hl.agg.sum(matrix.x), mean=hl.agg.mean(matrix.x),
        fraction=hl.agg.fraction(matrix.x > 6)))
    rows = matrix.result.collect()
    assert [x.n for x in rows] == [12] * 4
    assert [x.total for x in rows] == [48, 56, 64, 72]
    assert [x.mean for x in rows] == [6.0, 7.0, 8.0, 9.0]
    assert [x.fraction for x in rows] == [4 / 12, 4 / 12, 5 / 12, 6 / 12]
