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


def adapt_structural_ir_assertions(tree, module):
    """Omit calls to a proven local Hail IR-shape helper before arguments run.

    Only the standalone ``assert_contains_node`` helper with an exact upstream
    _ir, _mir, or _tir base_search assertion body qualifies. Value assertions remain executable.
    Return an explicit source-span manifest alongside the adapted tree.
    """
    removed = []

    def structural_helper(node):
        if not isinstance(node, ast.FunctionDef) or node.name != 'assert_contains_node':
            return False
        args = node.args
        if (len(args.args) != 2 or args.posonlyargs or args.kwonlyargs
                or args.vararg or args.kwarg or args.defaults or node.decorator_list):
            return False
        expr, kind = (arg.arg for arg in args.args)
        actual = ast.dump(ast.Module(body=node.body, type_ignores=[]))
        return any(actual == ast.dump(ast.Module(body=ast.parse(
            f'assert {expr}.{attribute}.base_search(lambda x: isinstance(x, {kind}))').body,
            type_ignores=[])) for attribute in ('_ir', '_mir', '_tir'))

    class StructuralCalls(ast.NodeTransformer):
        enabled = False
        helper_line = 0

        def visit_scope(self, node):
            previous = self.enabled, self.helper_line
            declarations = [child for child in node.body
                            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef))
                            and child.name == 'assert_contains_node']
            if declarations:
                self.enabled = len(declarations) == 1 and structural_helper(declarations[0])
                self.helper_line = declarations[0].lineno

            class Rebinding(ast.NodeVisitor):
                found = False

                def visit_Name(self, child):
                    if child.id == 'assert_contains_node' and isinstance(child.ctx, (ast.Store, ast.Del)):
                        self.found = True

                def visit_Import(self, child):
                    if any((alias.asname or alias.name.split('.')[0]) == 'assert_contains_node'
                           for alias in child.names):
                        self.found = True

                def visit_ImportFrom(self, child):
                    # A wildcard import after the proven declaration can
                    # replace its binding without a Name(Store) node.
                    if any((alias.asname or alias.name) == 'assert_contains_node'
                           or (alias.name == '*' and (not declarations
                               or child.lineno > declarations[0].lineno))
                           for alias in child.names):
                        self.found = True

                def visit_ExceptHandler(self, child):
                    # Python also deletes this binding when the handler exits.
                    if child.name == 'assert_contains_node':
                        self.found = True
                    self.generic_visit(child)

                def visit_MatchAs(self, child):
                    if child.name == 'assert_contains_node':
                        self.found = True
                    self.generic_visit(child)

                def visit_MatchStar(self, child):
                    if child.name == 'assert_contains_node':
                        self.found = True

                def visit_MatchMapping(self, child):
                    if child.rest == 'assert_contains_node':
                        self.found = True
                    self.generic_visit(child)

                def visit_FunctionDef(self, child):
                    # Defaults/decorators execute in the enclosing scope;
                    # their assignment expressions can replace its helper.
                    for value in [*child.decorator_list, *child.args.defaults,
                                  *(v for v in child.args.kw_defaults if v is not None)]:
                        self.visit(value)
                    annotations = [argument.annotation for argument in
                                   [*child.args.posonlyargs, *child.args.args,
                                    *child.args.kwonlyargs]
                                   if argument.annotation is not None]
                    annotations += [argument.annotation for argument in
                                    [child.args.vararg, child.args.kwarg]
                                    if argument is not None and argument.annotation is not None]
                    for value in [*annotations, *([child.returns] if child.returns else [])]:
                        self.visit(value)
                    # Function bodies have their own lexical binding checks.
                    if child.name == 'assert_contains_node' and child not in declarations:
                        self.found = True

                visit_AsyncFunctionDef = visit_FunctionDef

                def visit_ClassDef(self, child):
                    for value in [*child.decorator_list, *child.bases,
                                  *(keyword.value for keyword in child.keywords)]:
                        self.visit(value)
                    if child.name == 'assert_contains_node':
                        self.found = True

                def visit_Global(self, child):
                    if 'assert_contains_node' in child.names:
                        self.found = True

                visit_Nonlocal = visit_Global

                def visit_Lambda(self, child):
                    # Lambda defaults, unlike their bodies, execute here.
                    for value in [*child.args.defaults,
                                  *(v for v in child.args.kw_defaults if v is not None)]:
                        self.visit(value)

            rebinding = Rebinding()
            for child in node.body:
                rebinding.visit(child)
            if hasattr(node, 'args'):
                bound_names = {arg.arg for arg in [*node.args.posonlyargs, *node.args.args,
                                                   *node.args.kwonlyargs]}
                if node.args.vararg:
                    bound_names.add(node.args.vararg.arg)
                if node.args.kwarg:
                    bound_names.add(node.args.kwarg.arg)
                if 'assert_contains_node' in bound_names:
                    rebinding.found = True
            if any(getattr(parameter, 'name', None) == 'assert_contains_node'
                   for parameter in getattr(node, 'type_params', ())):
                rebinding.found = True
            if rebinding.found:
                self.enabled = False
            self.generic_visit(node)
            self.enabled, self.helper_line = previous
            return node

        visit_Module = visit_scope
        visit_ClassDef = visit_scope
        visit_FunctionDef = visit_scope
        visit_AsyncFunctionDef = visit_scope

        def visit_Expr(self, node):
            call = node.value
            if (self.enabled and node.lineno > self.helper_line
                    and isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == 'assert_contains_node'
                    and len(call.args) == 2 and not call.keywords
                    and not any(isinstance(arg, ast.Starred) for arg in call.args)):
                removed.append(dict(module=str(module), line=node.lineno,
                                    end_line=node.end_lineno, column=node.col_offset,
                                    end_column=node.end_col_offset,
                                    reason='Hail structural IR assertion; Vail IR topology intentionally differs'))
                return ast.copy_location(ast.Pass(), node)
            return self.generic_visit(node)

    return ast.fix_missing_locations(StructuralCalls().visit(tree)), removed


def reset_test_randomness():
    """Preserve upstream's function-autouse seed reset through Vail's public API."""
    import vail as hl
    hl.reset_global_randomness()
