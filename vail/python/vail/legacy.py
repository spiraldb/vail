"""Thin Python entry points for the native Hail storage scans."""
import os


def read_table_legacy(path):
    from . import Table, _native_session, _schema_from_native, _keys_from_native
    native = _native_session().read_legacy(os.fspath(path))
    return Table(native, _schema_from_native(native), _keys_from_native(native))


def read_matrix_legacy(path):
    from . import Table, _native_session, _schema_from_native
    from .matrixtable import MatrixTable, MatrixIR
    native, row_keys, col_keys, row_key_unique = _native_session().read_matrix_legacy(os.fspath(path))
    table = Table(native, _schema_from_native(native))
    return MatrixTable(MatrixIR(table, row_key=row_keys, col_key=col_keys, _aligned=True, _row_key_unique=row_key_unique))
