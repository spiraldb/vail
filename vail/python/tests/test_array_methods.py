import math

import pytest
import vail as hl


def test_array_methods_missingness_and_types():
    values = hl.literal([None, 2, 3, None], hl.tarray(hl.tint32))
    assert hl.eval(values.first()) == 2
    assert hl.eval(values.last()) is None
    assert hl.eval(values.index(3)) == 2
    assert hl.eval(values.index(lambda x: x > 2)) == 2
    assert hl.eval(values.index(9)) is None
    assert hl.eval(values.find(lambda x: x > 2)) == 3
    assert hl.eval(values.extend([4])) == [None, 2, 3, None, 4]
    assert hl.eval(values.extend(hl.missing(values.dtype))) is None
    assert hl.eval(hl.empty_array(hl.tint32).first()) is None
    assert hl.eval(hl.empty_array(hl.tint32).last()) is None
    assert hl.eval(hl.empty_array(hl.tint32).all(lambda x: x > 0)) is True
    assert hl.eval(hl.empty_array(hl.tint32).any(lambda x: x > 0)) is False
    with pytest.raises(TypeError, match='same array type'):
        values.extend(['s'])
    assert hl.eval(hl.array([(1, 2), (3, 4)]).starmap(lambda x, y: x + y)) == [3, 7]
    assert hl.eval(hl.array([hl.struct(x=1), hl.struct(x=2)]).x) == [1, 2]
    assert hl.eval(hl.group_by(lambda x: x % 2, [1, 2, 3])) == {0: [2], 1: [1, 3]}
    assert hl.eval(hl.group_by(lambda x: hl.missing(hl.tint32), [1, 2])) == {None: [1, 2]}


def test_array_fold_scan_missingness_promotion_and_row_capture():
    empty = hl.empty_array(hl.tint32)
    assert hl.eval(empty.fold(lambda a, x: a + x, 7)) == 7
    assert hl.eval(empty.scan(lambda a, x: a + x, 7)) == [7]
    missing = hl.missing(empty.dtype)
    assert hl.eval(missing.fold(lambda a, x: a + x, 7)) is None
    assert hl.eval(missing.scan(lambda a, x: a + x, 7)) is None
    assert hl.eval_typed(hl.array_scan(lambda a, x: a + x, 0, [1.0, 2.0])) == ([0.0, 1.0, 3.0], hl.tarray(hl.tfloat64))
    assert hl.eval(hl.literal([1, None, 3], empty.dtype).scan(lambda a, x: a + x, 0)) == [0, 1, None, None]
    table = hl.range_table(3)
    table = table.annotate(values=hl.range(table.idx))
    result = table.annotate(fold=table.values.fold(lambda a, x: a + x + table.idx, 0),
                            scan=table.values.scan(lambda a, x: a + x, 0)).collect()
    assert [r.fold for r in result] == [0, 1, 5]
    assert [r.scan for r in result] == [[0], [0, 0], [0, 0, 1]]
    nested = hl.array([[1, 2], [3]]).fold(lambda a, row: a + row.fold(lambda b, x: b + x, 0), 0)
    assert hl.eval(nested) == 6
    assert hl.eval(hl.array([1, 2]).fold(lambda a, x: hl.struct(total=a.total+x), hl.struct(total=0))) == hl.Struct(total=3)


def test_array_mean_median():
    mean_expr = hl.mean([1, None, 5])
    assert math.isnan(hl.eval(hl.mean(hl.empty_array(hl.tint32))))
    assert hl.eval_typed(mean_expr) == (3.0, hl.tfloat64)
    assert hl.eval(hl.mean([1, None, 5], filter_missing=False)) is None
    assert hl.eval(hl.mean(hl.missing(hl.tarray(hl.tint32)))) is None
    assert math.isnan(
        hl.eval(hl.mean(hl.literal([None, None], hl.tarray(hl.tint32))))
    )
    assert math.isnan(hl.eval(hl.mean(hl.empty_array(hl.tfloat64))))
    nan_mean = hl.eval(hl.mean([1.0, float("nan"), 3.0]))
    assert math.isnan(nan_mean)
    assert hl.eval(hl.mean(hl.set([1, 2, 3]))) == 2.0
    assert hl.eval(hl.median([1, 9, None, 3, 5])) == 3
    assert hl.eval(hl.median(hl.empty_array(hl.tint32))) is None
    assert hl.eval(hl.median(hl.set([2, 3, 1]))) == 2


def test_fold_and_scan_batch_lanes_keep_ragged_owner_context():
    table = hl.Table.parallelize(
        [{'idx': 2, 'a': [1, 2, 3]}, {'idx': 4, 'a': []},
         {'idx': 7, 'a': None}, {'idx': 9, 'a': [4]},
         {'idx': 11, 'a': [None, 6]}],
        schema=hl.tstruct(idx=hl.tint32, a=hl.tarray(hl.tint32)),
        n_partitions=1,
    )
    result = table.annotate(
        folded=table.a.fold(lambda acc, item: acc + item + table.idx, table.idx),
        scanned=table.a.scan(lambda acc, item: acc + item + table.idx, table.idx),
    ).collect()
    assert [row.folded for row in result] == [14, 4, None, 22, None]
    assert [row.scanned for row in result] == [[2, 5, 9, 14], [4], None, [9, 22], [11, None, None]]


def test_fold_ignores_large_unreferenced_nested_columns():
    table = hl.Table.parallelize(
        [{'idx': i, 'a': list(range(256)),
          'unused': [list(range(256)) for _ in range(8)]} for i in range(4)],
        schema=hl.tstruct(idx=hl.tint32, a=hl.tarray(hl.tint32),
                          unused=hl.tarray(hl.tarray(hl.tint32))),
        n_partitions=1,
    )
    result = table.annotate(
        total=table.a.fold(lambda accumulator, item: accumulator + item, 0),
        nested=table.unused.fold(lambda accumulator, items:
                                 accumulator + items.fold(lambda a, x: a + x, 0), 0),
    ).collect()
    assert [row.total for row in result] == [sum(range(256))] * 4
    assert [row.nested for row in result] == [8 * sum(range(256))] * 4


@pytest.mark.parametrize('dtype, values', [
    (hl.tint32, [2147483647, 2147483647]),
    (hl.tint64, [9223372036854775807, 9223372036854775807]),
    (hl.tfloat32, [1.0, 2.0, 3.0]),
    (hl.tfloat64, [1.0, 2.0, 3.0]),
])
def test_mean_uses_typed_kernel_without_array_cast(dtype, values):
    expression = hl.mean(hl.literal(values, hl.tarray(dtype)))
    assert expression.to_ir()['function'] == 'vail_array_mean'
    assert hl.eval(expression) == pytest.approx(sum(float(x) for x in values) / len(values))


def test_typed_range_mean_in_lazy_dataframe():
    table = hl.range_table(10, 3)
    table = table.annotate(values=hl.range(table.idx, -1, -2))
    result = table.annotate(mean=hl.mean(table.values)).collect()
    for row in result:
        expected = list(range(row.idx, -1, -2))
        assert row["values"] == expected
        assert row.mean == sum(expected) / len(expected)
    plan = table._native.logical_plan()
    assert 'vail_range_int32' in plan
    assert 'AS List(Int32)' not in plan
