"""Public expression types supported by the native frontend."""
from ..expressions import Expression, ExpressionException, Float32Expression, Float64Expression
from . import expressions
from .. import types, agg as aggregators
import importlib as _importlib
import sys as _sys

# Reuse the implementing modules so classes, functions and aggregation state
# have the same identity through both import paths.
_sys.modules[__name__ + '.types'] = types
_sys.modules[__name__ + '.aggregators'] = aggregators


def __getattr__(name):
    if name == 'functions':
        return _importlib.import_module(__name__ + '.functions')
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')

__all__ = ["Expression", "ExpressionException", "Float32Expression", "Float64Expression",
           "expressions", "types", "aggregators", "functions"]


def construct_expr(*args, **kwargs):
    """Import shim for Hail IR-oriented tests; Vail has no Hail IR constructor."""
    raise NotImplementedError('vail.expr.construct_expr requires Hail Python IR support')


__all__.append('construct_expr')
