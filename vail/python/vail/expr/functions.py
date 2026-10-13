"""Hail-compatible import path for implemented public frontend functions."""
import types as _types
import vail as _vail

__all__ = [name for name, value in vars(_vail).items()
           if not name.startswith('_') and isinstance(value, _types.FunctionType)]


def __getattr__(name):
    value = getattr(_vail, name, None)
    if not name.startswith('_') and isinstance(value, _types.FunctionType):
        return value
    raise AttributeError(f'module {__name__!r} has no attribute {name!r}')


def __dir__():
    return sorted(set(globals()) | set(__all__))
