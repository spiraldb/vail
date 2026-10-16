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
