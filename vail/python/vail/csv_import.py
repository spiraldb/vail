"""Hail CSV imports use the delimited-text reader with comma and quote defaults.

Contract: Hail ``methods/impex.py:import_csv``; format reference: RFC 4180.
Like Hail's reader, records must fit on individual physical lines.
"""

from .text import import_table


def import_csv(paths, *, key=None, min_partitions=None, impute=False,
               no_header=False, comment=(), missing='NA', types=None,
               quote='"', skip_blank_lines=False, force_bgz=False,
               filter=None, find_replace=None, force=False,
               source_file_field=None):
    """Import comma-delimited text as a Table, with string fields by default.

    Use ``impute=True`` to infer primitive types or ``types`` to specify them.
    Quotes protect commas within fields. As in Hail, embedded quotes and
    multiline records are unsupported. Other options have the same meaning
    as :func:`import_table`.
    """
    return import_table(
        paths, key=key, min_partitions=min_partitions, impute=impute,
        no_header=no_header, comment=comment, delimiter=',', missing=missing,
        types=types, quote=quote, skip_blank_lines=skip_blank_lines,
        force_bgz=force_bgz, filter=filter, find_replace=find_replace,
        force=force, source_file_field=source_file_field,
    )
