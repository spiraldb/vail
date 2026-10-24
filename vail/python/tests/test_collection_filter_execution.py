"""Filtered collection reductions retain values, scopes and effects."""
from collections import Counter

import pytest

import vail as hl


@pytest.mark.parametrize('dtype', [hl.tint32, hl.tint64, hl.tfloat32, hl.tfloat64])
def test_filtered_collection_numeric_bundle_nulls_and_shape(dtype):
    for values, expected in [
        (None, hl.Struct(n=None, total=None, xs=None)),
        ([], hl.Struct(n=0, total=0, xs=[])),
        ([None, -1, 0, 2, 3], hl.Struct(n=2, total=5, xs=[2, 3])),
    ]:
        result = hl.literal(values, hl.tarray(dtype)).aggregate(lambda x: hl.agg.filter(
            x > 0, hl.struct(n=hl.agg.count(), total=hl.agg.sum(x), xs=hl.agg.collect(x))))
        assert hl.eval(result) == expected
    values = hl.literal([None, 1, 1, 3], hl.tarray(dtype))
    result = hl.eval(values.aggregate(lambda x: hl.agg.filter(
        True, hl.struct(n=hl.agg.count(), xs=hl.agg.collect(x)))))
    assert result.n == 4
    assert Counter(result.xs) == Counter([None, 1, 1, 3])


def test_filtered_collection_row_global_and_outer_lexical_captures():
    table = hl.Table.parallelize([
        {'limit': 0, 'xs': [None, 1, 4]}, {'limit': 3, 'xs': []},
        {'limit': None, 'xs': [1, 2]},
    ], schema=hl.tstruct(limit=hl.tint32, xs=hl.tarray(hl.tint32))).annotate_globals(offset=10)
    rows = table.select(metric=table.xs.aggregate(lambda x: hl.agg.filter(
        x > table.limit, hl.struct(n=hl.agg.count(), total=hl.agg.sum(x + table.offset))))).collect()
    assert rows == [hl.Struct(metric=hl.Struct(n=2, total=25)),
                    hl.Struct(metric=hl.Struct(n=0, total=0)),
                    hl.Struct(metric=hl.Struct(n=0, total=0))]
    nested = hl.array([0, 2]).map(lambda lower: hl.array([None, 1, 3]).aggregate(
        lambda x: hl.agg.filter(x > lower, hl.agg.sum(x + lower))))
    assert hl.eval(nested) == [4, 5]


def test_nested_collection_filters_missing_predicates_and_null_elements():
    values = hl.literal([None, -1, 1, 2, 3], hl.tarray(hl.tint32))
    assert hl.eval(values.aggregate(lambda x: hl.agg.filter(x > 0, hl.agg.filter(
        x < 3, hl.struct(n=hl.agg.count(), total=hl.agg.sum(x)))))) == hl.Struct(n=2, total=3)
    assert hl.eval(values.aggregate(lambda x: hl.agg.filter(
        hl.missing(hl.tbool), hl.struct(n=hl.agg.count(), total=hl.agg.sum(x))))) == hl.Struct(n=0, total=0)


def test_filtered_collection_preserves_selected_and_unselected_errors():
    values = hl.array([0, 1])
    assert hl.eval(values.aggregate(lambda x: hl.agg.filter(x > 0, hl.agg.sum(1 // x)))) == 1
    with pytest.raises((ValueError, RuntimeError), match='zero'):
        hl.eval(values.aggregate(lambda x: hl.agg.filter((1 // x) > 0, hl.agg.count())))
    with pytest.raises((ValueError, RuntimeError), match='zero'):
        hl.eval(values.map(lambda x: 1 // x).aggregate(lambda x: hl.agg.filter(False, hl.agg.count())))


def test_filtered_collection_seeded_random_positions_remain_stable():
    # Seeded callback positions are a public reproducibility contract.
    body = hl.array([1, 2, 3, 4]).aggregate(lambda x: hl.agg.filter(
        x > 1, hl.agg.collect(hl.rand_unif(seed=13) + x)))
    assert hl.eval(body) == [2.045788308691845, 3.1686479204948523, 4.312676810370315]
    predicate = hl.array([1, 2, 3, 4]).aggregate(lambda x: hl.agg.filter(
        hl.rand_unif(seed=13) > .5, hl.agg.collect(x)))
    assert hl.eval(predicate) == [2, 3, 4]
    items = hl.range(4).map(lambda x: hl.rand_unif(seed=13)).aggregate(
        lambda x: hl.agg.filter(x > .2, hl.agg.collect(x)))
    assert hl.eval(items) == [0.2578718497119425, 0.9064616491146612,
                             0.5291855418022392, 0.5260567498511101]


def test_native_matrix_per_entry_array_summary_preserves_absence(tmp_path):
    mt = hl.range_matrix_table(5, 4, n_partitions=2)
    mt = mt.annotate_entries(AD=hl.or_missing((mt.row_idx + mt.col_idx) % 4 != 0,
                                            hl.array([mt.row_idx, hl.missing(hl.tint32), mt.col_idx])))
    mt = mt.filter_entries((mt.row_idx + mt.col_idx) % 3 != 0)
    path = str(tmp_path / 'array-entries.mt')
    mt.write(path)
    expected = {}
    for r in range(5):
        for c in range(4):
            if (r + c) % 3 == 0:
                continue
            values = None if (r + c) % 4 == 0 else [x for x in [r, c] if x > 1]
            expected[r, c] = hl.Struct(n=None if values is None else len(values),
                                      total=None if values is None else sum(values))
    for matrix in [mt, hl.read_matrix_table(path)]:
        result = matrix.annotate_entries(metric=matrix.AD.aggregate(lambda x: hl.agg.filter(
            x > 1, hl.struct(n=hl.agg.count(), total=hl.agg.sum(x))))).entries()
        assert {(e.row_idx, e.col_idx): e.metric for e in result.collect()} == expected
        captured = matrix.annotate_entries(metric=matrix.AD.aggregate(lambda x: hl.agg.filter(
            x > matrix.row_idx, hl.struct(n=hl.agg.count(), total=hl.agg.sum(x + matrix.col_idx))))).entries()
        expected_captured = {}
        for r, c in expected:
            if (r + c) % 4 == 0:
                value = hl.Struct(n=None, total=None)
            else:
                value = hl.Struct(n=int(c > r), total=2 * c if c > r else 0)
            expected_captured[r, c] = value
        assert {(e.row_idx, e.col_idx): e.metric for e in captured.collect()} == expected_captured


def test_filtered_nullable_structs_and_zero_field_structs():
    dtype = hl.tarray(hl.tstruct(keep=hl.tbool, value=hl.tint32))
    values = hl.literal([None, {'keep': True, 'value': None}, {'keep': False, 'value': 9},
                         {'keep': None, 'value': 10}, {'keep': True, 'value': 3}], dtype)
    result = values.aggregate(lambda item: hl.agg.filter(item.keep, hl.struct(
        n=hl.agg.count(), xs=hl.agg.collect(item.value))))
    assert hl.eval(result) == hl.Struct(n=2, xs=[None, 3])
    empty = hl.literal([{}, None, {}], hl.tarray(hl.tstruct()))
    assert hl.eval(empty.aggregate(lambda item: hl.agg.filter(hl.is_defined(item), hl.agg.count()))) == 2
    assert hl.eval(empty.aggregate(lambda item: hl.agg.filter(True, hl.agg.count()))) == 3


def test_filtered_collection_call_predicate_keeps_value_domain_errors():
    calls = hl.literal([hl.Call([0]), hl.Call([0, 1])], hl.tarray(hl.tcall))
    with pytest.raises((ValueError, RuntimeError), match='ploidy'):
        hl.eval(calls.aggregate(lambda c: hl.agg.filter(
            c.unphased_diploid_gt_index() > 0, hl.agg.count())))
    guarded = calls.aggregate(lambda c: hl.agg.filter(
        hl.if_else(c.is_diploid(), c.unphased_diploid_gt_index() > 0, False), hl.agg.count()))
    assert hl.eval(guarded) == 1
