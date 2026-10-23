"""Small Python utilities exposed by the native Vail frontend.

The compatibility exceptions and temporary-path helper intentionally provide
only the import surface needed by upstream Hail tests. They do not emulate a
Hail JVM or backend.
"""

from itertools import count
from pathlib import Path
import secrets
import string
import tempfile
from urllib.parse import urlparse

from .. import range_table
from ..types import Interval, Struct


class HailUserError(RuntimeError):
    """A user-facing query error (compatibility type for Hail-shaped callers)."""


class FatalError(HailUserError):
    """A fatal query error (compatibility type for Hail-shaped callers)."""

    def __init__(self, msg, error_id=-1):
        super().__init__(msg)
        self._error_id = error_id


class Env:
    """Frontend utilities that do not require a JVM."""

    _uids = count(1)

    @staticmethod
    def get_uid(base=None):
        return f"__uid_{base or ''}{next(Env._uids)}"


def new_temp_file(prefix=None, extension=None):
    """Return a unique, currently nonexistent local path for a test or caller."""
    alphabet = string.ascii_letters + string.digits
    token = ''.join(secrets.choice(alphabet) for _ in range(22))
    name = f'{prefix}-{token}' if prefix is not None else token
    if extension is not None:
        name = f'{name}.{str(extension).lstrip(".")}'
    return str(Path(tempfile.gettempdir()) / name)


def new_local_temp_file(filename='temp'):
    """Return a unique local file path; file creation is left to the caller."""
    return new_temp_file(prefix=Path(filename).stem, extension=Path(filename).suffix.lstrip('.') or None)


def uri_path(uri):
    """Extract the path component from a URI."""
    return urlparse(uri).path


def run_command(command, *args, **kwargs):
    """Run a local process for compatibility callers."""
    import subprocess

    return subprocess.run(command, *args, check=True, **kwargs)


__all__ = [
    'frozendict', 'range_table', 'Struct', 'Interval', 'Env', 'HailUserError', 'FatalError',
    'new_temp_file', 'new_local_temp_file', 'uri_path', 'run_command',
]


from .frozendict import frozendict
