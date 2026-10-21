"""The in-process native backend and its local filesystem interface."""
import builtins
import gzip
import os
import shutil
import glob
from dataclasses import dataclass
from .vcf import _local_path


@dataclass(frozen=True)
class LocalFileInfo:
    path: str
    size: int
    _directory: bool

    def is_dir(self):
        return self._directory

    def is_file(self):
        return not self._directory


class LocalFilesystem:
    def open(self, path, mode='r', **kwargs):
        path = _local_path(path)
        # Binary handles expose the stored bytes, including compression headers;
        # import/export workflows use them to concatenate BGZF members.
        if 'b' not in mode and path.endswith(('.gz', '.bgz')):
            if path.endswith('.bgz') and any(flag in mode for flag in 'wax+'):
                raise NotImplementedError('use a native export action to write BGZF')
            kwargs.pop('buffering', None)
            return gzip.open(path, mode if 'b' in mode or 't' in mode else mode + 't', **kwargs)
        return builtins.open(path, mode, **kwargs)

    def copy(self, source, destination):
        shutil.copyfile(_local_path(source), _local_path(destination))

    def remove(self, path):
        os.remove(_local_path(path))

    def rmtree(self, path):
        shutil.rmtree(_local_path(path))

    def mkdir(self, path):
        os.makedirs(_local_path(path), exist_ok=True)

    def exists(self, path):
        return os.path.exists(_local_path(path))

    def is_file(self, path):
        return os.path.isfile(_local_path(path))

    def is_dir(self, path):
        return os.path.isdir(_local_path(path))

    def stat(self, path):
        path = _local_path(path)
        info = os.stat(path)
        return LocalFileInfo(path, info.st_size, os.path.isdir(path))

    def ls(self, path):
        path = _local_path(path)
        if glob.has_magic(path):
            paths = glob.glob(path)
        elif os.path.isdir(path):
            paths = [os.path.join(path, name) for name in os.listdir(path)]
        else:
            paths = [path]
        return [self.stat(name) for name in sorted(paths)]


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
