import pytest
import vail as hl


def test_collection_aggregate_nested_result_types():
    value = hl.array([1, None, 2])
    assert hl.eval(value.aggregate(lambda x: hl.tuple([hl.agg.sum(x), hl.agg.count()]))) == (3, 3)
    assert hl.eval(value.aggregate(lambda x: hl.array([hl.agg.sum(x), hl.agg.count()]))) == [3, 3]
    assert hl.eval(value.aggregate(lambda x: hl.struct(total=hl.agg.sum(x), xs=hl.agg.collect(x)))) == hl.Struct(total=3, xs=[1, None, 2])
    assert hl.eval(value.aggregate(lambda x: hl.int32(hl.agg.sum(x)) + 1)) == 4
    assert hl.eval(value.aggregate(lambda x: hl.agg.filter(x > 1, hl.agg.sum(x)))) == 2


def test_collection_aggregate_retains_row_globals_and_rejects_cross_source():
    table = hl.range_table(3).annotate_globals(offset=4)
    result = table.select(total=hl.array([1, 2]).aggregate(lambda x: hl.agg.sum(x + table.idx + table.offset)))
    assert result.total.collect() == [11, 13, 15]
    other = hl.range_table(4)
    with pytest.raises(hl.ExpressionException, match='source mismatch'):
        hl.array([table.idx]).aggregate(lambda x: hl.agg.sum(other.idx))


def test_collection_aggregate_matrix_scope_and_lookup_context():
    matrix = hl.range_matrix_table(3, 2)
    matrix = matrix.annotate_entries(total=hl.array([1, 2]).aggregate(lambda x: hl.agg.sum(x + matrix.row_idx + matrix.col_idx)))
    assert matrix.total.collect() == [3, 5, 5, 7, 7, 9]
    table = hl.range_table(3)
    lookup = hl.Table.parallelize([{'idx': i, 'v': i + 10} for i in range(3)]).key_by('idx')
    result = table.select(total=hl.array([1, 2]).aggregate(lambda x: hl.agg.sum(x + lookup[table.idx].v)))
    assert result.total.collect() == [23, 25, 27]


def test_nested_collection_aggregate_retains_outer_lexical_capture():
    result = hl.array([10, 20]).map(lambda outer: hl.array([1, 2]).aggregate(
        lambda inner: hl.agg.sum(inner + outer)))
    assert hl.eval(result) == [23, 43]


def test_collection_aggregate_seeded_callback_positions():
    result = hl.array([1., 2., 3.]).aggregate(lambda x: hl.agg.collect(hl.rand_unif(seed=13) + x))
    assert hl.eval(result) == [1.045788308691845, 2.1686479204948523, 3.312676810370315]


def test_collection_reducer_input_has_its_own_local_map_scope():
    result = hl.array([1, 2]).aggregate(lambda x: hl.agg.sum(
        hl.sum(hl.array([10, 20]).map(lambda y: y + x))))
    assert hl.eval(result) == 66


def test_collection_aggregate_seeded_filter_preserves_indexed_zip_context():
    result = hl.array([1, 2, 3, 4]).aggregate(lambda x: hl.agg.filter(
        hl.rand_unif(seed=13) > .5, hl.agg.collect(x)))
    assert hl.eval(result) == [2, 3, 4]


def test_collection_aggregate_promotes_integer_sum_before_reduction():
    result = hl.array([2**31 - 1, 2**31 - 1]).aggregate(lambda x: hl.agg.sum(x))
    assert result.dtype == hl.tint64
    assert hl.eval(result) == 2**32 - 2
