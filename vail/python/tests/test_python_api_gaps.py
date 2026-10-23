"""Public Python compatibility wrappers; execution stays in native expressions."""
import pickle

import numpy as np
import pytest
import vail as hl


def test_struct_values_fields_and_missing_rename():
    s = hl.struct(a=1, b=2, c=3)
    assert [hl.eval(v) for v in s.values()] == [1, 2, 3]
    assert hl.eval(s.rename({'a': 'b', 'b': 'a'})) == hl.Struct(c=3, b=1, a=2)
    missing = hl.missing(s.dtype)
    assert hl.eval(missing.rename({'a': 'renamed'})) is None
    t = hl.range_table(3)
    row = t.select(s=hl.struct(a=t.idx, b=t.idx + 1))
    assert row.select(v=hl.sum(hl.array(list(row.s.values())))).collect() == [
        hl.Struct(idx=i, v=2 * i + 1) for i in range(3)]
    for mapping in ({'a': 'b'}, {'a': 'x', 'b': 'x'}, {'missing': 'x'}):
        with pytest.raises(ValueError):
            s.rename(mapping)
    with pytest.raises(TypeError):
        s.rename({'a': 1})


@pytest.mark.parametrize('phased', [False, True])
@pytest.mark.parametrize('alleles', [[], [0], [1], [0, 0], [0, 1], [1, 0], [1, 1], [1, 2]])
def test_python_calls_match_expression_predicates(alleles, phased):
    c = hl.Call(alleles, phased)
    e = hl.literal(c)
    for method in ('is_haploid', 'is_diploid', 'is_hom_ref', 'is_het',
                   'is_hom_var', 'is_non_ref', 'is_het_ref', 'is_het_non_ref', 'n_alt_alleles'):
        assert getattr(c, method)() == hl.eval(getattr(e, method)())
    assert c.one_hot_alleles(3) == [c.alleles.count(i) for i in range(3)]
    if c.is_diploid() and not phased:
        assert c.unphased_diploid_gt_index() == hl.eval(e.unphased_diploid_gt_index())
    else:
        with pytest.raises(hl.utils.FatalError):
            c.unphased_diploid_gt_index()


def test_frozendict_pickle_and_literal():
    d = {'a': 4, 'b': 8}
    frozen = hl.utils.frozendict(d)
    d['a'] = 99
    assert frozen['a'] == 4
    assert pickle.loads(pickle.dumps({frozen: 3})) == {frozen: 3}
    expression = hl.literal(frozen, hl.tdict(hl.tstr, hl.tint32))
    assert expression.dtype == hl.tdict(hl.tstr, hl.tint32)
    assert hl.eval(expression) == frozen
    assert hl.eval(expression.values()) == [4, 8]


def test_array_windows_duplicate_bounds_and_types():
    starts, stops = hl.linalg.utils.array_windows(np.array([1, 2, 4, 4, 6, 8]), 2)
    np.testing.assert_equal(starts, [0, 0, 1, 1, 2, 4])
    np.testing.assert_equal(stops, [2, 4, 5, 5, 6, 6])
    for values, radius in [(np.array([2, 1]), 1), (np.array([1]), -1),
                           (np.array([np.nan]), 1), (np.array([[1]]), 1),
                           (np.array([True]), 1)]:
        with pytest.raises(ValueError):
            hl.linalg.utils.array_windows(values, radius)
    with pytest.raises(TypeError):
        hl.linalg.utils.array_windows([1], 1)


def test_python_call_display_and_errors():
    calls = [hl.Call([]), hl.Call([], True), hl.Call([2]), hl.Call([2], True),
             hl.Call([1, 0]), hl.Call([1, 0], True)]
    assert list(map(str, calls)) == ['-', '|-', '2', '|2', '0/1', '1|0']
    with pytest.raises(NotImplementedError, match='greater than 2'):
        hl.Call([0, 1, 2])
    with pytest.raises(ValueError):
        hl.Call([-1])
    with pytest.raises(TypeError):
        hl.Call([0]).one_hot_alleles(2.0)


@pytest.mark.parametrize('values', [[], [1, 2]])
def test_array_windows_rejects_nan_radius(values):
    with pytest.raises(ValueError, match='radius.*nan'):
        hl.linalg.utils.array_windows(np.array(values, dtype=np.int64), float('nan'))


@pytest.mark.parametrize('radius,starts,stops', [
    (0.0, [0, 1, 2], [1, 2, 3]),
    (0.75, [0, 1, 2], [1, 2, 3]),
    (1.0, [0, 0, 1], [2, 3, 3]),
])
def test_array_windows_exact_int64_float_radius(radius, starts, stops):
    actual = hl.linalg.utils.array_windows(np.array([2**53, 2**53 + 1, 2**53 + 2], dtype=np.int64), radius)
    np.testing.assert_equal(actual[0], starts)
    np.testing.assert_equal(actual[1], stops)


def test_array_windows_integer_domain_and_unbounded_radius():
    bounds = np.iinfo(np.int64)
    for value in (bounds.min, bounds.max):
        a = np.array([value], dtype=np.int64)
        np.testing.assert_equal(hl.linalg.utils.array_windows(a, 0.0), ([0], [1]))
        with pytest.raises(ValueError, match='underflow|overflow'):
            hl.linalg.utils.array_windows(a, 1.0)
    np.testing.assert_equal(hl.linalg.utils.array_windows(np.array([bounds.min, bounds.max]), float('inf')), ([0, 0], [2, 2]))


def test_python_call_genotype_index_uses_unbounded_python_integer():
    # This localized-value contract matches upstream Hail, rather than wrapping
    # to the native expression API's Int32 result type.
    value = hl.Call([65535, 65535]).unphased_diploid_gt_index()
    assert type(value) is int
    assert value == 2147516415
