"""Public evaluation contracts for local sharing inside mapped projections."""
import pytest
import vail as hl


def test_mapped_struct_arithmetic_nulls_captures_and_repeated_actions():
    table = hl.Table.parallelize([
        {'offset': 2, 'xs': [1, None, 12]},
        {'offset': 9, 'xs': []},
        {'offset': 4, 'xs': None},
    ], schema=hl.tstruct(offset=hl.tint32, xs=hl.tarray(hl.tint32)))
    result = table.annotate(values=table.xs.map(lambda x: hl.struct(
        low=(x + table.offset) / 5 < 2,
        high=(x + table.offset) / 5 >= 2,
        even=(x + table.offset) % 2 == 0,
        odd=(x + table.offset) % 2 != 0)))
    expected = [
        [hl.Struct(low=True, high=False, even=False, odd=True),
         hl.Struct(low=None, high=None, even=None, odd=None),
         hl.Struct(low=False, high=True, even=True, odd=False)], [], None]
    assert [r['values'] for r in result.collect()] == expected
    assert [r['values'] for r in result.collect()] == expected


def test_local_sharing_does_not_cross_lazy_branches_or_nested_lambda_scope():
    values = hl.array([0, 2, 4]).map(lambda x: hl.struct(
        selected=hl.if_else(x == 0, 0, 12 // x),
        nested=hl.array([1, 3]).map(lambda y: hl.struct(
            low=(x + y) / 2 < 2, high=(x + y) / 2 >= 2))))
    result = hl.eval(values)
    assert [r.selected for r in result] == [0, 6, 3]
    assert [[r.low for r in row.nested] for row in result] == [[True, True], [True, False], [False, False]]
    with pytest.raises(RuntimeError):
        hl.eval(hl.array([0]).map(lambda x: hl.struct(a=12 // x, b=12 // x)))


@pytest.mark.parametrize('partitions', [1, 3])
def test_shared_column_predicates_preserve_sparse_entries(partitions):
    matrix = hl.utils.range_matrix_table(9, 4, n_partitions=partitions)
    matrix = matrix.annotate_entries(x=matrix.row_idx + matrix.col_idx)
    matrix = matrix.filter_entries(matrix.row_idx % 3 != 0)
    matrix = matrix.annotate_cols(
        low=hl.agg.fraction(matrix.x / 5 < 1),
        high=hl.agg.fraction(matrix.x / 5 >= 1),
        even=hl.agg.count_where(matrix.x % 2 == 0),
        odd=hl.agg.count_where(matrix.x % 2 != 0))
    result = matrix.cols().collect()
    for col, value in enumerate(result):
        xs = [row + col for row in range(9) if row % 3 != 0]
        assert value.low == sum(x < 5 for x in xs) / len(xs)
        assert value.high == sum(x >= 5 for x in xs) / len(xs)
        assert value.even == sum(x % 2 == 0 for x in xs)
        assert value.odd == sum(x % 2 != 0 for x in xs)
