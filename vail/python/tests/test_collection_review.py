"""Boundary cases for collection lowering, independent of its implementation."""
import pytest
import vail as hl


def test_strided_slices_match_python_for_clamped_and_negative_bounds():
    values = [[], [1], list(range(9))]
    table = hl.Table.parallelize([{'v': value} for value in values], schema=hl.tstruct(v=hl.tarray(hl.tint32)))
    for start, stop, step in (
        (None, None, 1), (None, None, -1),
        (-30, 30, 2), (30, -30, -2), (2, 7, 3),
        (-2, None, -1), (None, -2, -1), (4, 4, 1),
    ):
        index = slice(start, stop, step)
        assert table.v[index].collect() == [value[index] for value in values]


def test_slice_missing_outer_array_is_distinct_from_empty_array():
    table = hl.Table.parallelize([{'v': None}, {'v': []}, {'v': [None, 4]}], schema=hl.tstruct(v=hl.tarray(hl.tint32)))
    assert table.v[::-1].collect() == [None, [], [4, None]]
    assert table.v.length().collect() == [None, 0, 2]


def test_index_bounds_are_errors_instead_of_silent_missing_values():
    table = hl.Table.parallelize([{'v': [10, None, 30]}], schema=hl.tstruct(v=hl.tarray(hl.tint32)))
    assert table.v[-3].collect() == [10]
    assert table.v[1].collect() == [None]
    for index in (-4, 3):
        with pytest.raises(RuntimeError, match='(?i)bound|index'):
            table.v[index].collect()


def test_array_construction_interleaves_values_from_each_row():
    table = hl.Table.parallelize([{'a': 1, 'b': 10}, {'a': 2, 'b': 20}], schema=hl.tstruct(a=hl.tint32, b=hl.tint32))
    assert table.annotate(v=[table.a, table.b]).v.collect() == [[1, 10], [2, 20]]


def test_reductions_preserve_int64_precision_and_include_first_element():
    table = hl.Table.parallelize(
        [{'v': [2**53 + 1]}, {'v': [2**53 + 1, 2]}, {'v': []}, {'v': [None]}, {'v': None}],
        schema=hl.tstruct(v=hl.tarray(hl.tint64)),
    )
    assert hl.sum(table.v).collect() == [2**53 + 1, 2**53 + 3, 0, 0, None]
    assert hl.product(table.v).collect() == [2**53 + 1, 2 * (2**53 + 1), 1, 1, None]
    assert hl.min(table.v).collect() == [2**53 + 1, 2, None, None, None]
    assert hl.sum(table.v).dtype == hl.tint64


def test_float32_minmax_preserve_signed_zero_and_propagate_nan():
    import math
    table = hl.Table.parallelize(
        [{'v': [0.0, -0.0]}, {'v': [-0.0, 0.0]}, {'v': [1.0, float('nan')]}, {'v': [float('nan'), 1.0]}],
        schema=hl.tstruct(v=hl.tarray(hl.tfloat32)),
    )
    minima, maxima = hl.min(table.v).collect(), hl.max(table.v).collect()
    assert [math.copysign(1, value) for value in minima[:2]] == [-1, -1]
    assert [math.copysign(1, value) for value in maxima[:2]] == [1, 1]
    assert all(math.isnan(value) for value in minima[2:] + maxima[2:])


def test_reverse_slice_stop_at_negative_length_matches_hail_source():
    # Hail Emit.scala uses requestedStop + length > 0, unlike Python's >= 0.
    # Thus explicit -length is clamped to -1 for a reverse slice.
    table = hl.Table.parallelize([{'v': [1]}, {'v': [1, 2, 3]}], schema=hl.tstruct(v=hl.tarray(hl.tint32)))
    assert table.v[:-1:-1].collect() == [[1], []]
    assert table.v[:-3:-1].collect() == [[1], [3, 2, 1]]


def test_array_length_method_does_not_shadow_struct_length_field():
    table = hl.Table.parallelize(
        [{'g': {'length': 7}, 'a': [1, 2]}],
        schema=hl.tstruct(g=hl.tstruct(length=hl.tint32), a=hl.tarray(hl.tint32)),
    )
    assert table.g.length.collect() == [7]
    assert table.a.length().collect() == [2]


def test_collection_helpers_accept_hail_collection_keyword():
    table = hl.Table.parallelize([{'v': [2, 3]}], schema=hl.tstruct(v=hl.tarray(hl.tint32)))
    expression = table.v
    assert hl.array(collection=expression) is expression
    assert hl.sum(collection=expression).collect() == [5]
    assert hl.product(collection=expression).collect() == [6]


@pytest.mark.parametrize('size', [0, -1, -(2**31)])
def test_grouped_requires_positive_sizes(size):
    value = hl.literal([1, 2, 3])
    with pytest.raises(ValueError, match='positive'):
        value.grouped(size)
    table = hl.Table.parallelize([{'values': [1, 2], 'size': size}],
                                schema=hl.tstruct(values=hl.tarray(hl.tint32), size=hl.tint32))
    with pytest.raises((RuntimeError, ValueError), match='positive'):
        table.values.grouped(table.size).collect()


def test_grouped_preserves_slices_empty_null_arrays_and_missing_sizes():
    import pyarrow as pa
    # Null list rows can retain hidden child spans in valid Arrow storage.
    arrays = pa.ListArray.from_arrays(
        pa.array([0, 1, 6, 9, 9, 11], type=pa.int32()),
        pa.array(['sentinel', 'é', None, '漢字', 'last', 'tail', 'hidden', 'hidden', 'hidden', 'a', 'b']),
        mask=pa.array([False, False, True, False, False]))
    table = hl.Table.parallelize(pa.table({'values': arrays.slice(1, 4),
                                         'size': pa.array([2, 2, 2, None], type=pa.int32())}))
    assert table.values.grouped(table.size).collect() == [
        [['é', None], ['漢字', 'last'], ['tail']], None, [], None]
    assert hl.eval(hl.literal([1, 2, 3]).grouped(2)) == [[1, 2], [3]]
    assert hl.eval(hl.literal([], hl.tarray(hl.tint32)).grouped(2)) == []


def test_literal_dictionary_shared_expression_duplicate_precedence():
    value = hl.int32(1)
    for _ in range(10):
        value = value + value
    dictionary = hl.dict(hl.array([hl.tuple([0, value]), hl.tuple([0, value + 1])]))
    assert dictionary.dtype == hl.tdict(hl.tint32, hl.tint32)
    assert hl.eval(dictionary) == {0: 1025}
