"""Public per-list set semantics through table, matrix and lexical scopes."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('element,values,expected', [
    (hl.tint32, [1, 1, None, 2], {1, None, 2}),
    (hl.tint64, [2**40, None, 2**40], {2**40, None}),
    (hl.tbool, [True, False, None, True], {True, False, None}),
    (hl.tstr, ['x', 'x', None, '', 'λ'], {'x', None, '', 'λ'}),
])
def test_list_sets_keep_empty_missing_and_element_types(element, values, expected):
    table = hl.Table.parallelize(
        [{'i': 0, 'xs': values}, {'i': 1, 'xs': []}, {'i': 2, 'xs': None}],
        schema=hl.tstruct(i=hl.tint32, xs=hl.tarray(element)), n_partitions=2)
    out = table.annotate(s=table.xs.aggregate(lambda x: hl.agg.collect_as_set(x)))
    assert out.s.dtype == hl.tset(element)
    assert {row.i: row.s for row in out.select('i', 's').collect()} == {
        0: expected, 1: set(), 2: None}


@pytest.mark.parametrize('partitions', [1, 3])
@pytest.mark.parametrize('cols', [0, 7])
def test_matrix_row_sets_include_missing_values_and_exclude_filtered_entries(partitions, cols):
    m = hl.utils.range_matrix_table(5, cols, n_partitions=partitions)
    m = m.annotate_entries(x=hl.or_missing(m.col_idx != 2, m.col_idx % 3))
    m = m.filter_entries((m.row_idx != 3) & (m.col_idx != 5))
    m = m.annotate_rows(s=hl.agg.collect_as_set(m.x))
    got = {row.row_idx: row.s for row in m.rows().select('s').collect()}
    expected = {None if col == 2 else col % 3 for col in range(cols) if col != 5}
    assert got == {row: set() if row == 3 else expected for row in range(5)}


def test_list_sets_nested_structs_tuples_arrays_and_empty_structs():
    records = hl.literal([{'a': 1, 'b': 'x'}, {'a': 1, 'b': 'x'},
                          {'a': None, 'b': 'x'}, None],
                         hl.tarray(hl.tstruct(a=hl.tint32, b=hl.tstr)))
    assert hl.eval(records.aggregate(lambda x: hl.agg.collect_as_set(x))) == {
        hl.Struct(a=1, b='x'), hl.Struct(a=None, b='x'), None}
    pairs = hl.literal([(1, 'x'), (1, 'x'), (None, 'x'), None],
                       hl.tarray(hl.ttuple(hl.tint32, hl.tstr)))
    assert hl.eval(pairs.aggregate(lambda x: hl.agg.collect_as_set(x))) == {
        (1, 'x'), (None, 'x'), None}
    arrays = hl.literal([[1, None], [1, None], [2], None], hl.tarray(hl.tarray(hl.tint32)))
    result = arrays.aggregate(lambda x: hl.agg.collect_as_set(x))
    assert hl.eval(hl.len(result)) == 3
    assert hl.eval(hl.sorted(hl.array(result).filter(hl.is_defined).map(hl.sum))) == [1, 2]
    empty = hl.literal([{}, {}, None], hl.tarray(hl.tstruct()))
    assert hl.eval(empty.aggregate(lambda x: hl.agg.collect_as_set(x))) == {hl.Struct(), None}


def test_list_sets_nested_lexical_and_row_captures():
    nested = hl.literal([[1, 1, 2], [], None, [3, 3]], hl.tarray(hl.tarray(hl.tint32)))
    result = hl.bind(lambda offset: nested.map(
        lambda xs: xs.aggregate(lambda x: hl.agg.collect_as_set(x + offset))), 10)
    assert hl.eval(result) == [{11, 12}, set(), None, {13}]
    table = hl.Table.parallelize([{'k': 10, 'xs': [1, 1, 2]}, {'k': 20, 'xs': [2, None]}],
                                schema=hl.tstruct(k=hl.tint32, xs=hl.tarray(hl.tint32)))
    out = table.annotate(s=table.xs.aggregate(lambda x: hl.agg.collect_as_set(x + table.k)))
    assert {row.k: row.s for row in out.select('k', 's').collect()} == {10: {11, 12}, 20: {22, None}}


@pytest.mark.parametrize('element', [hl.tfloat32, hl.tfloat64])
def test_list_set_float_and_nested_float_fallback_keeps_signed_zero_keys(element):
    values = hl.literal([-0.0, 0.0, math.nan, math.nan, None], hl.tarray(element))
    # Count inside the engine: Python's set conversion collapses the signed zeros.
    assert hl.eval(hl.len(values.aggregate(lambda x: hl.agg.collect_as_set(x)))) == 4
    nested = hl.literal([[-0.0], [0.0], [math.nan], [math.nan], None],
                        hl.tarray(hl.tarray(element)))
    assert hl.eval(hl.len(nested.aggregate(lambda x: hl.agg.collect_as_set(x)))) == 4


def test_list_set_opaque_and_unordered_fallbacks():
    calls = hl.literal([hl.Call([0, 1]), hl.Call([0, 1]), hl.Call([0, 1], phased=True), None],
                       hl.tarray(hl.tcall))
    # Existing per-list Call support loses its descriptor in generic coercion;
    # this conservative rewrite must retain that fallback/capability boundary.
    with pytest.raises((ValueError, RuntimeError), match='UInt16'):
        hl.eval(hl.len(calls.aggregate(lambda x: hl.agg.collect_as_set(x))))
    dictionaries = hl.literal([{'a': 1}, {'a': 1}, None], hl.tarray(hl.tdict(hl.tstr, hl.tint32)))
    assert hl.eval(hl.len(dictionaries.aggregate(lambda x: hl.agg.collect_as_set(x)))) == 2
    sets = hl.literal([{1, 2}, {1, 2}, None], hl.tarray(hl.tset(hl.tint32)))
    assert hl.eval(hl.len(sets.aggregate(lambda x: hl.agg.collect_as_set(x)))) == 2


def test_list_sets_keep_error_branch_and_seeded_input_evaluation():
    excluded = hl.array([0, 0]).aggregate(lambda x: hl.agg.collect_as_set(
        hl.if_else(x == 0, 7, hl.int32('set value failure'))))
    assert hl.eval(excluded) == {7}
    selected = hl.array([1]).aggregate(lambda x: hl.agg.collect_as_set(
        hl.if_else(x == 0, 7, hl.int32('set value failure'))))
    with pytest.raises((ValueError, RuntimeError), match='failure|parse|cast|Cast|Parse'):
        hl.eval(selected)
    values = hl.range(32).map(lambda x: hl.rand_int32(1000000, seed=19))
    out = hl.eval(hl.bind(lambda xs: hl.struct(
        reduced=xs.aggregate(lambda x: hl.agg.collect_as_set(x)), direct=hl.set(xs)), values))
    assert out.reduced == out.direct


def test_list_set_logical_type_survives_native_write_read(tmp_path):
    table = hl.Table.parallelize([{'i': 0, 'xs': [1, 1, None]}, {'i': 1, 'xs': [2, 3]}],
                                schema=hl.tstruct(i=hl.tint32, xs=hl.tarray(hl.tint32)))
    table = table.annotate(s=table.xs.aggregate(lambda x: hl.agg.collect_as_set(x)))
    path = str(tmp_path / 'sets.ht')
    table.write(path)
    restored = hl.read_table(path)
    assert restored.s.dtype == hl.tset(hl.tint32)
    assert {r.i: r.s for r in restored.select('i', 's').collect()} == {0: {1, None}, 1: {2, 3}}
