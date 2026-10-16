"""Verify backend adaptation executes assertions and exposes unsupported APIs."""
import ast
import importlib
from pathlib import Path

import pytest
import vail as hl


@pytest.fixture
def adapter(monkeypatch):
    monkeypatch.syspath_prepend(str(Path(__file__).parent))
    return importlib.import_module('upstream_backend')


def test_backend_identity_and_native_execution(adapter):
    from vail.utils.java import choose_backend
    assert choose_backend() == choose_backend('vail') == 'vail'
    with pytest.raises(ValueError, match='cannot select'):
        choose_backend('spark')
    with adapter.VailTestBackend() as backend:
        directory = backend.directory
        assert directory.is_dir()
        assert hl.current_backend().supports('randomness')
        assert not hl.current_backend().supports('jvm')
        assert hl.range_table(4).count() == 4
    assert not directory.exists()


def test_class_configuration_keeps_test_assertions_and_defers_missing_apis(adapter):
    source = '''
class Tests:
    functions = [hl.range_table, hl.nonexistent_test_api]
    def test_supported(self):
        assert hl.range_table(3).count() == 3
    def test_unsupported(self):
        self.functions[1]()
'''
    original = ast.parse(source)
    tree = adapter.adapt_class_configuration(ast.parse(source))
    for before, after in zip(original.body[0].body[1:], tree.body[0].body[1:]):
        assert ast.dump(before) == ast.dump(after)
    scope = {'hl': hl, '_vail_backend_api': adapter.backend_api}
    exec(compile(tree, '<upstream class>', 'exec'), scope)
    tests = scope['Tests']()
    tests.test_supported()
    with pytest.raises(NotImplementedError, match='nonexistent_test_api'):
        tests.test_unsupported()


def test_failures_are_distinct_from_unsupported_and_harness_errors(adapter):
    classify = adapter.VailTestBackend.classify_failure
    assert classify('body', AssertionError('wrong result')) == 'behavior_failure'
    assert classify('body', NotImplementedError('missing API')) == 'unsupported'
    assert classify('fixture', RuntimeError('fixture failed')) == 'harness_error'
    assert classify('body', KeyError('missing result field')) == 'behavior_failure'
    assert classify('body', NameError('undefined implementation variable')) == 'behavior_failure'


def test_structural_ir_adapter_preserves_executable_value_assertions(adapter):
    source = '''
def run(expected):
    def assert_contains_node(expr, node):
        assert expr._ir.base_search(lambda x: isinstance(x, node))
    value = 7
    assert_contains_node(value, ir.NA)
    assert value == expected
    return value
'''
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'expr/test_expr.py')
    scope = {}
    exec(compile(tree, '<adapted upstream>', 'exec'), scope)
    assert scope['run'](7) == 7  # Neither ir nor a Hail IR class exists here.
    with pytest.raises(AssertionError):
        scope['run'](8)
    assert len(manifest) == 1
    assert manifest[0]['module'] == 'expr/test_expr.py'
    assert manifest[0]['line'] == manifest[0]['end_line'] == 6
    assert 'structural IR' in manifest[0]['reason']


def test_structural_ir_adapter_keeps_unrelated_helpers_and_argument_evaluation(adapter):
    source = '''
def run():
    def assert_contains_node(expr, node):
        assert expr == node
    assert_contains_node(7, ir.NA)
'''
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'unrelated.py')
    assert manifest == []
    scope = {}
    exec(compile(tree, '<unrelated upstream>', 'exec'), scope)
    with pytest.raises(NameError, match='ir'):
        scope['run']()
    scope['ir'] = type('Values', (), {'NA': 8})
    with pytest.raises(AssertionError):
        scope['run']()
    scope['ir'] = type('Values', (), {'NA': 7})
    scope['run']()


def test_structural_ir_adapter_preserves_shadowed_calls(adapter):
    source = '''
def run():
    def assert_contains_node(expr, node):
        assert expr._ir.base_search(lambda x: isinstance(x, node))
    def nested(assert_contains_node):
        assert_contains_node(1, 2)
    nested(lambda a, b: 1 / 0)
'''
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'shadowed.py')
    assert manifest == []
    scope = {}
    exec(compile(tree, '<shadowed upstream>', 'exec'), scope)
    with pytest.raises(ZeroDivisionError):
        scope['run']()


@pytest.mark.parametrize('attribute', ['_ir', '_mir', '_tir'])
def test_module_structural_helper_keeps_behavior_and_nested_shadowing(adapter, attribute):
    source = f'''
def assert_contains_node(expr, node):
    assert expr.{attribute}.base_search(lambda x: isinstance(x, node))
def run(expected):
    assert_contains_node(7, ir.NA)
    assert 7 == expected
    def nested(assert_contains_node):
        assert_contains_node(1, 2)
    nested(lambda a, b: a / (b - 2))
'''
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'module.py')
    assert len(manifest) == 1
    scope = {}
    exec(compile(tree, '<module structural upstream>', 'exec'), scope)
    with pytest.raises(AssertionError):
        scope['run'](8)
    with pytest.raises(ZeroDivisionError):
        scope['run'](7)


def test_structural_helper_assignment_retains_argument_evaluation(adapter):
    source = '''
def assert_contains_node(expr, node):
    assert expr._tir.base_search(lambda x: isinstance(x, node))
def run():
    assert_contains_node = lambda a, b: a == b
    assert_contains_node(7, ir.NA)
'''
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'assignment.py')
    assert manifest == []
    scope = {}
    exec(compile(tree, '<rebound structural helper>', 'exec'), scope)
    with pytest.raises(NameError, match='ir'):
        scope['run']()
