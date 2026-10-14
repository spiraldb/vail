"""The in-process native backend and its local filesystem interface."""
import builtins
from .vcf import _local_path


class LocalFilesystem:
    def open(self, path, mode='r', **kwargs):
        return builtins.open(_local_path(path), mode, **kwargs)


class NativeBackend:
    requires_lowering = False
    fs = LocalFilesystem()

    @property
    def session(self):
        from . import _native_session
        return _native_session()


_backend = NativeBackend()


def current_backend():
    return _backend
