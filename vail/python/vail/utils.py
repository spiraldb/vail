"""Utility constructors supported by the native Table bridge."""

from itertools import count

from . import range_table
from .types import Interval, Struct


class Env:
    """Frontend utilities independent of a JVM or execution backend."""

    _uids = count(1)

    @staticmethod
    def get_uid(base=None):
        """Return a process-local unique field name with Hail's spelling."""
        return f"__uid_{base or ''}{next(Env._uids)}"


__all__ = ["range_table", "Struct", "Interval", "Env"]
