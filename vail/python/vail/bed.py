"""UCSC BED import (https://genome.ucsc.edu/FAQ/FAQformat.html#format1).

BED coordinates are zero-based and half-open. Hail exposes one-based locus
intervals and retains the fourth field as ``target`` when present.
"""
from .expressions import literal, int32, is_defined
from .interval_import import _checked_interval
from .text import import_table
from .types import tdict, tstr
from .utils import FatalError


def import_bed(path, reference_genome='default', skip_invalid_intervals=False,
               contig_recoding=None, **kwargs):
    """Import BED3 or BED4+ records as a lazy interval-keyed table."""
    if type(skip_invalid_intervals) is not bool:
        raise TypeError('skip_invalid_intervals must be bool')
    for reserved in ('no_header', 'delimiter', 'impute', 'skip_blank_lines', 'types', 'comment'):
        if reserved in kwargs:
            raise TypeError(f'import_bed controls {reserved}')
    table = import_table(path, no_header=True, delimiter=r'\s+', impute=False,
                         skip_blank_lines=True,
                         comment=[r'^browser.*', r'^track.*', r'^\w+=("[\w\d ]+"|\d+).*'],
                         **kwargs)
    n_fields = len(table.row.dtype)
    if n_fields < 3:
        raise FatalError(f'too few fields for BED file: expected 3 or more, but found {n_fields}')
    contig = table.f0
    if contig_recoding is not None:
        if not isinstance(contig_recoding, dict) or any(
                not isinstance(k, str) or not isinstance(v, str)
                for k, v in contig_recoding.items()):
            raise TypeError('contig_recoding must be a mapping from strings to strings')
        contig = literal(contig_recoding, tdict(tstr, tstr)).get(contig, contig)
    result = _checked_interval(contig, int32(table.f1) + 1, int32(table.f2) + 1,
                               True, False, reference_genome, skip_invalid_intervals)
    extra = {'target': table.f3} if n_fields >= 4 else {}
    table = table.select(interval=result, **extra)
    if skip_invalid_intervals and reference_genome is not None:
        table = table.filter(is_defined(table.interval))
    return table.key_by('interval')
