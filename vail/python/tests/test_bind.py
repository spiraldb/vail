"""Lexical binding must preserve values across nested random contexts."""
import ast
import copy
from pathlib import Path
import unittest

import pytest
import vail as hl


@pytest.mark.parametrize('name', [
    'test_bind_multiple', 'test_bind_placement',
    'test_rbind_multiple', 'test_rbind_placement',
])
def test_upstream_scalar_bind(name):
    source = Path(__file__).resolve().parents[3] / 'hail/hail/python/test/hail/expr/test_expr.py'
    node = copy.deepcopy(next(n for n in ast.walk(ast.parse(source.read_text()))
                              if isinstance(n, ast.FunctionDef) and n.name == name))
    node.decorator_list = []
    scope = {'hl': hl}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[])),
                 str(source), 'exec'), scope)
    scope[name](unittest.TestCase())


def test_random_argument_is_bound_before_repeated_body():
    expected = hl.eval(hl.rand_unif(seed=17))
    actual = hl.eval(hl.bind(lambda x: hl.range(6).map(lambda _: x), hl.rand_unif(seed=17)))
    assert actual == [expected] * 6


def test_bind_keeps_enclosing_random_context_without_adding_a_new_one():
    expected = hl.eval(hl.range(5).map(lambda _: hl.rand_unif(seed=23)))
    actual = hl.eval(hl.range(5).map(lambda _: hl.bind(
        lambda x: hl.range(3).map(lambda _: x), hl.rand_unif(seed=23)
    )))
    assert actual == [[x] * 3 for x in expected]
    assert len(set(expected)) == 5


def test_nested_binders_preserve_outer_values():
    expression = hl.bind(lambda x: hl.range(4).map(lambda i: hl.bind(
        lambda y: hl.struct(outer=x, inner=y, repeated=hl.range(3).map(lambda _: y)),
        hl.rand_unif(seed=2),
    )), hl.rand_unif(seed=1))
    actual = hl.eval(expression)
    assert [x.outer for x in actual] == [hl.eval(hl.rand_unif(seed=1))] * 4
    assert all(x.repeated == [x.inner] * 3 for x in actual)
    assert len({x.inner for x in actual}) == 4


def test_bind_preserves_row_scope_and_global_references():
    table = hl.utils.range_table(5, 3).annotate_globals(offset=7)
    table = table.annotate(values=hl.bind(
        lambda x: hl.range(3).map(lambda i: x + i), table.idx + table.offset
    ))
    assert [r['values'] for r in table.collect()] == [list(range(i + 7, i + 10)) for i in range(5)]


def test_bind_preserves_keyed_lookup_references():
    right = hl.Table.parallelize([hl.Struct(k=1, v=11), hl.Struct(k=2, v=22)], key='k')
    left = hl.utils.range_table(3)
    result = left.annotate(v=hl.bind(lambda x: x + x, right[left.idx].v))
    assert [r.v for r in result.collect()] == [None, 22, 44]


def test_bind_missing_values_and_multiple_types():
    assert hl.eval(hl.bind(lambda x: x + 1, hl.missing(hl.tint32))) is None
    assert hl.eval(hl.bind(lambda x, y: hl.struct(x=x, y=y), 3, 'a')) == hl.Struct(x=3, y='a')
    assert hl.eval(hl.bind(lambda: 9)) == 9


def test_binding_inside_and_after_aggregation():
    table = hl.utils.range_table(5)
    assert table.aggregate(hl.agg.sum(hl.bind(lambda x: x + 1, table.idx))) == 15
    assert table.aggregate(hl.bind(lambda total: total * 2, hl.agg.sum(table.idx))) == 20


@pytest.mark.parametrize('scan', [False, True])
def test_outer_binding_cannot_cross_an_aggregation_scope(scan):
    table = hl.utils.range_table(5)
    reducer = hl.scan.sum if scan else hl.agg.sum
    with pytest.raises(hl.ExpressionException, match='outer bind variable'):
        hl.bind(lambda offset: reducer(table.idx + offset), 1)
