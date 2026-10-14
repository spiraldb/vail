"""The in-process native backend and its local filesystem interface."""
import builtins
from .vcf import _local_path


class LocalFilesystem:
    def open(self, path, mode='r', **kwargs):
        return builtins.open(_local_path(path), mode, **kwargs)


class NativeBackend:
    name = 'vail'
    requires_lowering = False
    capabilities = frozenset({'table', 'matrix_table', 'randomness', 'local_filesystem'})
    fs = LocalFilesystem()

    def supports(self, capability):
        return capability in self.capabilities

    @property
    def session(self):
        from . import _native_session
        return _native_session()


_backend = NativeBackend()


def current_backend():
    return _backend


def choose_backend(backend=None):
    """Identify Vail explicitly; a Hail backend cannot execute Vail queries."""
    if backend not in (None, 'vail'):
        raise ValueError(f'Vail cannot select backend {backend!r}; use "vail"')
    return current_backend().name
