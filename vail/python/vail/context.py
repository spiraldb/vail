"""Local temporary paths with deterministic context-manager cleanup."""
import os
import secrets
import shutil
import tempfile
from .vcf import _local_path


class TemporaryFilename:
    """Produce an uncreated unique filename and remove it on context exit."""
    def __init__(self, *, prefix='', suffix='', dir=None):
        directory = tempfile.gettempdir() if dir is None else _local_path(dir)
        self.name = os.path.join(directory, prefix + secrets.token_hex(16) + suffix)

    def __enter__(self):
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            os.remove(self.name)
        except FileNotFoundError:
            pass

    async def __aenter__(self):
        return self.__enter__()

    async def __aexit__(self, exc_type, exc_value, traceback):
        return self.__exit__(exc_type, exc_value, traceback)


class TemporaryDirectory(TemporaryFilename):
    """Produce a unique local directory and recursively remove it on exit."""
    def __init__(self, *, prefix='', suffix='', dir=None, ensure_exists=True):
        super().__init__(prefix=prefix, suffix=suffix, dir=dir)
        self.ensure_exists = ensure_exists

    def __enter__(self):
        if self.ensure_exists:
            os.mkdir(self.name)
        return self.name

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            shutil.rmtree(self.name)
        except FileNotFoundError:
            pass
