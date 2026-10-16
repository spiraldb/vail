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


@pytest.mark.parametrize('binding,error', [
    ('''try:
    raise RuntimeError('handler')
except RuntimeError as assert_contains_node:
    pass''', UnboundLocalError),
    ('''match lambda a, b: 1 / 0:
    case assert_contains_node:
        pass''', ZeroDivisionError),
    ('''match [1]:
    case [*assert_contains_node]:
        pass''', TypeError),
    ('''match {'value': 1}:
    case {**assert_contains_node}:
        pass''', TypeError),
    ('del assert_contains_node', UnboundLocalError),
    ('unrelated = lambda value=(assert_contains_node := lambda a, b: 1 / 0): None', ZeroDivisionError),
    ('''def unrelated(value=(assert_contains_node := lambda a, b: 1 / 0)):
    pass''', ZeroDivisionError),
    ('''class Unrelated((assert_contains_node := lambda a, b: 1 / 0) and object):
    pass''', ZeroDivisionError),
])
def test_structural_helper_capture_and_unbinding_preserve_runtime_errors(adapter, binding, error):
    import textwrap
    source = '''
def run():
    def assert_contains_node(expr, node):
        assert expr._ir.base_search(lambda x: isinstance(x, node))
''' + textwrap.indent(binding, '    ') + '\n    assert_contains_node(1, 2)\n'
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'capture.py')
    assert manifest == []
    scope = {}
    exec(compile(tree, '<captured or unbound helper>', 'exec'), scope)
    with pytest.raises(error):
        scope['run']()


def test_structural_helper_late_wildcard_import_preserves_argument_evaluation(adapter):
    source = """
def assert_contains_node(expr, node):
    assert expr._ir.base_search(lambda x: isinstance(x, node))
from math import *
assert_contains_node(7, ir.NA)
"""
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'wildcard.py')
    assert manifest == []
    with pytest.raises(NameError, match='ir'):
        exec(compile(tree, '<wildcard after helper>', 'exec'), {})


def test_structural_helper_call_before_definition_preserves_unbound_error(adapter):
    source = """
def run():
    assert_contains_node(1, 2)
    def assert_contains_node(expr, node):
        assert expr._ir.base_search(lambda x: isinstance(x, node))
"""
    tree, manifest = adapter.adapt_structural_ir_assertions(ast.parse(source), 'early.py')
    assert manifest == []
    scope = {}
    exec(compile(tree, '<helper not yet defined>', 'exec'), scope)
    with pytest.raises(UnboundLocalError):
        scope['run']()


@pytest.mark.parametrize('reverse', [False, True])
def test_upstream_pytest_fixture_resets_default_randomness_between_tests(adapter, tmp_path, reverse):
    import json
    import os
    import subprocess
    import sys
    adapter.reset_test_randomness()
    expected = hl.eval(hl.rand_int64())
    suite = tmp_path / 'test' / 'hail'
    suite.mkdir(parents=True)
    (suite / 'helpers.py').write_text('')
    (suite / 'conftest.py').write_text('''import pytest
@pytest.fixture(autouse=True)
def reset_global_randomness():
    Env.reset_global_randomness()
''')
    tests = [f'''def test_{name}():
    assert hl.eval(hl.rand_int64()) == {expected!r}
    for _ in range({count}):
        hl.eval(hl.rand_int64())
''' for name, count in [('first', 13), ('second', 29)]]
    (suite / 'test_rng.py').write_text('import hail as hl\n' + '\n'.join(tests[::-1] if reverse else tests))
    output = tmp_path / 'results'
    runner = Path(__file__).with_name('full_hail_pytest.py')
    result = subprocess.run([sys.executable, str(runner), '--source', str(suite),
                             '--output', str(output), '--module', 'test_rng.py',
                             '--module-timeout', '30', '--per-test-timeout', '10'],
                            env=dict(os.environ), capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stdout + result.stderr
    modules = json.loads((output / 'modules.json').read_text())
    assert modules[0].get('exit_code') == 0, (output / 'test_rng.py.log').read_text()
    events = [json.loads(line) for line in (output / 'test_rng.py.jsonl').read_text().splitlines()]
    calls = [event for event in events if event['kind'] == 'test' and event['when'] == 'call']
    assert len(calls) == 2 and all(event['outcome'] == 'passed' for event in calls)
    fixture = json.loads((output / 'fixture-adaptations.json').read_text())['randomness_reset']
    assert fixture['replacement'] == 'vail.reset_global_randomness'
    assert fixture['scope'] == 'function' and fixture['autouse'] is True
    assert fixture['upstream_fixture'] == 'conftest.py::reset_global_randomness'


def test_original_body_runner_resets_default_randomness_between_parameter_variants(adapter, tmp_path):
    import json
    adapter.reset_test_randomness()
    expected = hl.eval(hl.rand_int64())
    expected_second = hl.eval(hl.rand_int64())
    suite = tmp_path / 'test' / 'hail'
    suite.mkdir(parents=True)
    (suite / 'helpers.py').write_text('')
    (suite / 'conftest.py').write_text('''import pytest
@pytest.fixture(autouse=True)
def reset_global_randomness():
    Env.reset_global_randomness()
@pytest.fixture
def seeded_capture():
    return hl.eval(hl.rand_int64())
''')
    source = suite / 'test_rng.py'
    source.write_text(f'''import pytest
@pytest.mark.parametrize('draw_count', [3, 7])
def test_variants(draw_count, seeded_capture, reset_global_randomness):
    assert seeded_capture == {expected!r}
    assert hl.eval(hl.rand_int64()) == {expected_second!r}
    for _ in range(draw_count):
        hl.eval(hl.rand_int64())
''')
    bodies = importlib.import_module('full_hail_bodies')
    output = tmp_path / 'body.json'
    bodies.worker(source, 'test_variants', output, suite)
    report = json.loads(output.read_text())
    assert report['status'] == 'passed', report
    assert len(report['variants']) == 2
    assert all(variant['status'] == 'passed' for variant in report['variants'])
    assert report['randomness_fixture']['replacement'] == 'vail.reset_global_randomness'
