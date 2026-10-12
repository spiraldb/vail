"""Audit the public aggregator surface against the checked-out Hail source."""
import ast
from pathlib import Path

import vail as hl


def test_every_public_hail_aggregator_is_exported():
    path = Path(__file__).resolve().parents[3] / 'hail/hail/python/hail/expr/aggregators/__init__.py'
    module = ast.parse(path.read_text())
    exported = next(ast.literal_eval(node.value) for node in module.body
                    if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == '__all__' for t in node.targets))
    public = {name for name in exported if not name.startswith('_')}
    assert len(public) == 33
    assert set(hl.agg.__all__) == public
    assert all(callable(getattr(hl.agg, name)) for name in public)


def test_public_parameter_names_match_hail():
    import inspect

    path = Path(__file__).resolve().parents[3] / 'hail/hail/python/hail/expr/aggregators/aggregators.py'
    functions = {node.name: node for node in ast.parse(path.read_text()).body
                 if isinstance(node, ast.FunctionDef)}
    for name in hl.agg.__all__:
        args = functions[name].args
        expected = [arg.arg for arg in args.args + args.kwonlyargs]
        signature = inspect.signature(getattr(hl.agg, name))
        assert list(signature.parameters) == expected, name
        for arg in args.kwonlyargs:
            assert signature.parameters[arg.arg].kind == inspect.Parameter.KEYWORD_ONLY, name
