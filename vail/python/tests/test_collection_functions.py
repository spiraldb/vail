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


def test_random_array_inputs_retain_seeded_composition_goldens():
    def input(): return hl.range(3).map(lambda x:hl.rand_unif(seed=13))
    expected = [.2578718497119425,.9064616491146612,.5291855418022392]
    assert hl.eval(hl.zip(input(),[1.,2.,3.])) == list(zip(expected,[1.,2.,3.]))
    assert hl.eval(hl.enumerate(input())) == list(enumerate(expected))
    assert hl.eval(input()+[1.,2.,3.]) == [1.2578718497119425,2.9064616491146613,3.529185541802239]
    assert hl.eval(hl.corr(input(),[1.,2.,3.])) == .1591118698427001


def test_zip_missing_input_does_not_evaluate_unused_other_array():
    invalid = hl.array([hl.array([1])[10]])
    assert hl.eval(hl.zip(hl.missing(hl.tarray(hl.tint32)),invalid)) is None


def test_variadic_zip_truncation_padding_and_missing_parents():
    values = ([1,None,3],['a'],[True,False])
    assert hl.eval(hl.zip(*values)) == [(1,'a',True)]
    assert hl.eval(hl.zip(*values,fill_missing=True)) == [(1,'a',True),(None,None,False),(3,None,None)]
    assert hl.eval(hl.zip(hl.empty_array(hl.tint32),['a'],[True])) == []
    assert hl.eval(hl.zip([1],hl.missing(hl.tarray(hl.tstr)),[True],fill_missing=True)) is None
