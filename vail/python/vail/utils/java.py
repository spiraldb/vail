"""JVM compatibility names needed to import selected upstream Hail tests.

Frontend helpers and backend selection use Vail's native implementation.
Actual JVM accessors raise explicitly when a test tries to use them.
"""

from . import Env, FatalError, HailUserError
from ..backend import choose_backend


def scala_object(*args, **kwargs):
    raise NotImplementedError('scala_object requires Hail JVM support')


def __getattr__(name):
    if name.startswith('__'):
        raise AttributeError(name)
    raise NotImplementedError(f'vail.utils.java.{name} requires Hail JVM support')


__all__ = ['Env', 'FatalError', 'HailUserError', 'scala_object', 'choose_backend']
