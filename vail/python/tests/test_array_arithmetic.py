"""Elementwise arithmetic on lazy array expressions."""
import pytest
import vail as hl


def test_array_arithmetic_is_lazy_in_rows_globals_and_aggregations():
    table = hl.range_table(3).annotate_globals(offset=hl.array([10, 20]))
    rows = table.annotate(values=hl.array([1, 2]) + table.idx)
    assert [row['values'] for row in rows.collect()] == [[1, 2], [2, 3], [3, 4]]
    assert hl.eval(table.index_globals().offset + hl.array([1, 2])) == [11, 22]

    aggregate = table.aggregate(hl.agg.collect(hl.array([1, 2]) * (table.idx + 1)))
    assert aggregate == [[1, 2], [2, 4], [3, 6]]


def test_issue_3654_array_aggregate_division():
    table = hl.range_table(10).annotate(x=hl.array([1, 2]))
    assert table.aggregate(hl.agg.array_sum(table.x) / [2, 2]) == [5.0, 10.0]


def test_list_collect_uses_empty_list_identity_and_preserves_null_items():
    values = hl.literal([[], [None, 2]], hl.tarray(hl.tarray(hl.tint32)))
    collected = values.map(lambda xs: xs.aggregate(lambda x: hl.agg.collect(x)))
    assert hl.eval(collected) == [[], [None, 2]]
    assert hl.eval(hl.literal([], hl.tarray(hl.tint32)).aggregate(
        lambda x: hl.agg.collect(x)
    )) == []


def test_array_array_and_reverse_arithmetic():
    left = hl.array([8, 9])
    right = hl.array([2, 3])
    assert hl.eval(left + right) == [10, 12]
    assert hl.eval(left - right) == [6, 6]
    assert hl.eval(left * right) == [16, 27]
    assert hl.eval(left / right) == [4.0, 3.0]
    assert hl.eval(left // right) == [4, 3]
    assert hl.eval(left % right) == [0, 0]
    assert hl.eval(hl.array([2, 3]) ** hl.array([2, 2])) == [4.0, 9.0]
    assert hl.eval(20 - right) == [18, 17]
    assert hl.eval(20 / right) == [10.0, 20 / 3]
    assert hl.eval(20 // right) == [10, 6]
    assert hl.eval(20 % right) == [0, 2]
    assert hl.eval(2 ** right) == [4.0, 8.0]
    assert hl.eval(left + hl.array([0.5, 1.5])) == [8.5, 10.5]
    assert hl.eval(-left) == [-8, -9]


def test_missing_empty_and_missing_elements_in_array_arithmetic():
    table = hl.Table.parallelize(
        [{'left': None, 'right': [1]}, {'left': [], 'right': []},
         {'left': [None, 3], 'right': [2, None]}],
        schema=hl.tstruct(left=hl.tarray(hl.tint32), right=hl.tarray(hl.tint32)))
    assert (table.left + table.right).collect() == [None, [], [None, None]]
    assert (table.left + 4).collect() == [None, [], [None, 7]]
    assert (4 - table.left).collect() == [None, [], [None, 1]]


@pytest.mark.parametrize('left,right', [([1], [1, 2]), ([1, 2], [1])])
def test_array_array_length_mismatch_fails_when_evaluated(left, right):
    table = hl.Table.parallelize(
        [{'left': left, 'right': right}],
        schema=hl.tstruct(left=hl.tarray(hl.tint32), right=hl.tarray(hl.tint32)))
    expression = table.left + table.right
    with pytest.raises(RuntimeError, match='arrays of equal length'):
        table.annotate(value=expression).collect()


def test_nested_array_element_types_are_rejected():
    nested = hl.literal([[1], [2]], hl.tarray(hl.tarray(hl.tint32)))
    with pytest.raises(TypeError, match='numeric'):
        _ = nested + nested


def test_long_array_arithmetic_and_random_inputs_are_zipped_once():
    assert hl.eval(hl.range(20000) + hl.range(20000)) == list(range(0, 40000, 2))
    values = hl.range(20).map(lambda _: hl.rand_int32(1000, seed=203))
    expected = hl.eval(values)
    assert hl.eval(values + hl.range(20)) == [value + index for index, value in enumerate(expected)]
