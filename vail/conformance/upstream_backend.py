"""Explicit Vail backend lifecycle and reporting for upstream test adapters.

This adapter runs Vail's native backend. It does not emulate Spark, a JVM, or
Hail compiler internals, and unsupported operations never become passes.
"""
import ast
from pathlib import Path
import tempfile


class VailTestBackend:
    name = 'vail'

    def __init__(self):
        self._temporary = None
        self.options = {}

    def initialize(self, **options):
        from vail.backend import choose_backend, current_backend
        choose_backend(options.get('backend'))
        self.options = dict(options)
        if self._temporary is None:
            self._temporary = tempfile.TemporaryDirectory(prefix='vail-upstream-')
        # Establish the real native session, rather than a no-op initialization.
        current_backend().session
        return current_backend()

    @property
    def directory(self):
        if self._temporary is None:
            self.initialize()
        return Path(self._temporary.name)

    def stop(self):
        if self._temporary is not None:
            self._temporary.cleanup()
            self._temporary = None
        self.options = {}

    def __enter__(self):
        self.initialize()
        return self

    def __exit__(self, *exc):
        self.stop()

    @staticmethod
    def classify_failure(phase, error):
        if phase != 'body':
            return 'harness_error'
        if isinstance(error, NotImplementedError):
            return 'unsupported'
        if isinstance(error, AttributeError) and ("module 'vail" in str(error)
                                                 or "Hail Scala/backend dependency" in str(error)):
            return 'unsupported'
        if type(error).__name__ == 'BackendDependencyUnavailable':
            return 'unsupported'
        return 'behavior_failure'


def test_timeout(overall=None, *, batch=None, local=None, spark=None, vail=None):
    import pytest
    timeout = vail if vail is not None else local if local is not None else overall
    return pytest.mark.timeout(timeout) if timeout is not None else lambda f: f


def backend_api(path):
    """Resolve class-level API lists without blocking unrelated test collection."""
    import vail
    value = vail
    try:
        for name in path.split('.'):
            value = getattr(value, name)
        return value
    except (AttributeError, NotImplementedError):
        def unsupported(*args, **kwargs):
            raise NotImplementedError(f'Vail backend does not support hl.{path}')
        unsupported.__name__ = path
        return unsupported


def adapt_class_configuration(tree):
    """Only class-level configuration expressions change, never test bodies."""
    class APIs(ast.NodeTransformer):
        def visit_Attribute(self, node):
            parts = []
            value = node
            while isinstance(value, ast.Attribute):
                parts.append(value.attr)
                value = value.value
            if isinstance(value, ast.Name) and value.id in ('hl', 'hail'):
                return ast.copy_location(ast.Call(func=ast.Name(id='_vail_backend_api', ctx=ast.Load()),
                    args=[ast.Constant('.'.join(reversed(parts)))], keywords=[]), node)
            return self.generic_visit(node)
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            for item in node.body:
                if isinstance(item, (ast.Assign, ast.AnnAssign)) and item.value is not None:
                    item.value = APIs().visit(item.value)
    return ast.fix_missing_locations(tree)


def adapt_helpers(code):
    """Adapt infrastructure functions; preserve upstream assertion helpers."""
    tree = ast.parse(code)
    replacements = {
        'hl_init_for_test': 'return _vail_test_backend.initialize(**kwargs)',
        'hl_stop_for_test': 'return _vail_test_backend.stop()',
        'test_timeout': 'return _vail_test_timeout(overall, batch=batch, local=local, spark=spark)',
        'with_flags': 'return lambda f: f',
    }
    for node in tree.body:
        if isinstance(node, ast.ImportFrom) and node.module in ('hail.utils.java', 'vail.utils.java'):
            node.module = 'vail.utils.java'
        if isinstance(node, ast.FunctionDef) and node.name in replacements:
            node.body = ast.parse(replacements[node.name]).body
        if isinstance(node, ast.Assign) and any(isinstance(target, ast.Name) and target.id == 'qobtest' for target in node.targets):
            node.value = ast.parse("pytest.mark.backend('vail')", mode='eval').body
    prefix = ('from upstream_backend import VailTestBackend, test_timeout as _vail_test_timeout\n'
              '_vail_test_backend = VailTestBackend()\n')
    return prefix + ast.unparse(ast.fix_missing_locations(tree))
