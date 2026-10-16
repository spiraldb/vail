import math
import pytest
import vail as hl


def test_zip_lengths_missingness_and_types():
    assert hl.eval(hl.zip([1, 2])) == [(1,), (2,)]
    assert hl.eval(hl.zip([1, 2], ['a'])) == [(1, 'a')]
    assert hl.eval(hl.zip([1, 2], ['a'], fill_missing=True)) == [(1, 'a'), (2, None)]
    assert hl.eval(hl.zip(hl.empty_array(hl.tint32), ['a'])) == []
    assert hl.eval(hl.zip(hl.missing('array<int32>'), ['a'], fill_missing=True)) is None
    with pytest.raises(TypeError): hl.zip({1}, [2])
    with pytest.raises(ValueError): hl.zip()


def test_collection_mapping_filtering_search():
    assert hl.eval(hl.map(lambda a, b: a + b, [1, 2], [3])) == [4]
    assert hl.eval(hl.starmap(lambda a, b: a + b, [(1, 2), (3, 4)])) == [3, 7]
    assert hl.eval(hl.map(lambda a: a + 1, {1, 2})) == {2, 3}
    assert hl.eval(hl.filter(lambda a: a > 1, [1, None, 2])) == [2]
    assert hl.eval(hl.find(lambda a: a > 1, [1, None, 3, 2])) == 3
    assert hl.eval(hl.find(lambda a: a > 9, [1, 2])) is None
    assert hl.eval(hl.flatten(hl.literal([[1, 2], [], [3]], 'array<array<int32>>'))) == [1, 2, 3]
    assert hl.eval(hl.flatmap(lambda a: hl.range(a), [1, 2])) == [0, 0, 1]
    assert hl.eval(hl.enumerate(['a', 'b'], start=3, index_first=False)) == [('a', 3), ('b', 4)]
    with pytest.raises(TypeError): hl.starmap(lambda a: a, [1, 2])


@pytest.mark.parametrize('name, identity, decisive', [('any', False, True), ('all', True, False)])
def test_boolean_overloads(name, identity, decisive):
    f = getattr(hl, name)
    assert hl.eval(f()) is identity
    assert hl.eval(f(hl.empty_array(hl.tbool))) is identity
    assert hl.eval(f([identity, None])) is None
    assert hl.eval(f([decisive, None])) is decisive
    assert hl.eval(f(identity, hl.missing(hl.tbool))) is None
    assert hl.eval(f(lambda x: x > 0, {1, 2})) is True
    assert hl.eval(f(hl.missing('array<bool>'))) is None
    with pytest.raises(TypeError): f([1, 2])


def test_corr_and_or_else():
    assert hl.eval(hl.corr([1, 2, 4], [2, 3, 1])) == pytest.approx(-0.6546536707079772)
    assert hl.eval(hl.corr([1, None, 3], [2, 7, 6])) == pytest.approx(1)
    assert hl.eval(hl.corr(hl.empty_array(hl.tfloat64), hl.empty_array(hl.tfloat64))) is None
    assert math.isnan(hl.eval(hl.corr([1, 1], [2, 3])))
    assert math.isnan(hl.eval(hl.corr(hl.literal([None], 'array<float64>'), [1])))
    assert hl.eval(hl.corr(hl.missing('array<float64>'), [1])) is None
    with pytest.raises(RuntimeError, match='equal length'): hl.eval(hl.corr([1], [1, 2]))
    assert hl.eval(hl.or_else(hl.missing(hl.tint32), 2.5)) == 2.5
    assert hl.eval(hl.or_else(3, 2.5)) == 3.0
    with pytest.raises(TypeError): hl.or_else('a', 1)


def test_sources_and_existing_table_builtin_calls():
    t = hl.range_table(3)
    assert t.annotate(xs=hl.map(lambda x: x + t.idx, [1, 2])).xs.collect() == [[1, 2], [2, 3], [3, 4]]
    other = hl.range_table(3)
    with pytest.raises(hl.ExpressionException): hl.zip(hl.array([t.idx]), hl.array([other.idx]))
    assert t.count() == 3


def test_zip_three_arrays_and_offset_enumeration_use_scalar_callbacks():
    values = hl.eval(hl.zip([1, 2], ['a'], [True, False, True], fill_missing=True))
    assert values == [(1, 'a', True), (2, None, False), (None, None, True)]
    assert hl.eval(hl.zip([1, 2], ['a'], [True, False])) == [(1, 'a', True)]
    assert hl.eval(hl.enumerate([7, 8], start=-3)) == [(-3, 7), (-2, 8)]
    assert hl.eval(hl.enumerate([7, 8], start=-3, index_first=False)) == [(7, -3), (8, -2)]
    assert hl.eval(hl.enumerate(hl.missing('array<int32>'))) is None


def test_zip_random_arrays_are_evaluated_before_tuple_projection():
    values = hl.range(12).map(lambda _: hl.rand_int64(seed=94))
    expected = hl.eval(values)
    assert hl.eval(hl.zip(values, hl.range(12))) == list(zip(expected, range(12)))
    assert hl.eval(hl.enumerate(values, start=3)) == list(zip(range(3, 15), expected))


def test_corr_long_arrays_use_shared_constant_size_reducer_state():
    # The old expression embedded whole-array means in per-element callbacks,
    # exceeding a gigabyte at only 4000 elements.
    assert hl.eval(hl.corr(hl.range(20000), hl.range(20000))) == pytest.approx(1.0)
    assert hl.eval(hl.corr([1e12, 1e12 + 1, 1e12 + 2], [3, 2, 1])) == pytest.approx(-1.0)


def test_random_array_composition_preserves_input_stream_values():
    def input(): return hl.range(3).map(lambda x:hl.rand_unif(seed=13))
    expected = hl.eval(input())
    assert hl.eval(hl.zip(input(),[1.,2.,3.])) == list(zip(expected,[1.,2.,3.]))
    assert hl.eval(hl.enumerate(input())) == list(enumerate(expected))
    assert hl.eval(input()+[1.,2.,3.]) == [x + y for x, y in zip(expected, [1.,2.,3.])]
    from statistics import correlation
    assert hl.eval(hl.corr(input(),[1.,2.,3.])) == pytest.approx(correlation(expected, [1.,2.,3.]))


def test_zip_missing_input_does_not_evaluate_unused_other_array():
    invalid = hl.array([hl.array([1])[10]])
    assert hl.eval(hl.zip(hl.missing(hl.tarray(hl.tint32)),invalid)) is None


def test_variadic_zip_truncation_padding_and_missing_parents():
    values = ([1,None,3],['a'],[True,False])
    assert hl.eval(hl.zip(*values)) == [(1,'a',True)]
    assert hl.eval(hl.zip(*values,fill_missing=True)) == [(1,'a',True),(None,None,False),(3,None,None)]
    assert hl.eval(hl.zip(hl.empty_array(hl.tint32),['a'],[True])) == []
    assert hl.eval(hl.zip([1],hl.missing(hl.tarray(hl.tstr)),[True],fill_missing=True)) is None


def test_random_collection_composition_in_rows_globals_and_nested_callbacks():
    table = hl.range_table(5, n_partitions=3)
    table = table.annotate_globals(global_draws=hl.range(4).map(lambda _: hl.rand_unif(seed=333)))
    values = hl.range(table.idx + 2).map(lambda _: hl.rand_int32(1000, seed=334))
    result = table.annotate(
        draws=values,
        paired=hl.zip(values, hl.range(table.idx + 2)),
        indexed=hl.enumerate(values, start=-2),
        shifted=values + hl.range(table.idx + 2),
        globals_paired=hl.zip(table.index_globals().global_draws, [0, 1, 2, 3]),
    )
    expected_globals = hl.eval(table.index_globals().global_draws)
    for _ in range(2):
        for row in result.collect():
            assert row.paired == list(zip(row.draws, range(row.idx + 2)))
            assert row.indexed == list(zip(range(-2, row.idx), row.draws))
            assert row.shifted == [value + index for index, value in enumerate(row.draws)]
            assert row.globals_paired == list(zip(expected_globals, range(4)))

    def nested(_):
        draws = hl.range(4).map(lambda _: hl.rand_unif(seed=335))
        return hl.struct(draws=draws, paired=hl.zip(draws, [0, 1, 2, 3]))
    for row in hl.eval(hl.range(3).map(nested)):
        assert row.paired == list(zip(row.draws, range(4)))


def test_zip_evaluates_defined_array_inputs_before_truncation():
    # Inputs are materialized arrays, even when the zip emits fewer elements.
    values = hl.range(3).map(lambda x: hl.if_else(x < 2, x, hl.int32('bad')))
    with pytest.raises(RuntimeError, match='bad'):
        hl.eval(hl.zip(values, [0, 1]))
