"""Lazy interval-list imports.

Picard's interval_list specification uses one-based inclusive coordinates.
Reference-backed one-column inputs follow Hail's parse_locus_interval branch,
including right-exclusive shorthand and its unmodified contig validation;
recoding and invalid-row skipping apply to the other input branches.
"""
from .expressions import (as_expr, literal, int32, float64, interval, locus,
                          struct, is_valid_locus, if_else, case, or_missing,
                          is_defined)
from .collection_functions import or_else
from .reference_genome import resolve_reference
from .text import import_table
from .types import tstr, tint32, tdict


def _checked_interval(contig, start, end, includes_start, includes_end,
                      reference_genome, invalid_missing):
    if reference_genome is None:
        return interval(struct(contig=contig, position=start),
                        struct(contig=contig, position=end), includes_start, includes_end)
    reference = resolve_reference(reference_genome)
    contig, start, end = as_expr(contig), as_expr(start), as_expr(end)
    includes_start, includes_end = as_expr(includes_start), as_expr(includes_end)
    lengths = literal(reference.lengths, tdict(tstr, tint32))
    length = lengths.get(contig, 0)
    # Hail normalizes exclusive sentinel endpoints to valid inclusive loci.
    zero_start = (start == 0) & ~includes_start
    after_end = (end == length + 1) & ~includes_end
    start = if_else(zero_start, 1, start)
    end = if_else(after_end, length, end)
    includes_start = includes_start | zero_start
    includes_end = includes_end | after_end
    # Hail canonicalizes the empty [p,p) interval to (p-1,p), which
    # preserves an empty interval while keeping both endpoints valid loci.
    empty_half_open = (start == end) & includes_start & ~includes_end & (start > 1)
    start = if_else(empty_half_open, start - 1, start)
    includes_start = includes_start & ~empty_half_open
    valid = (is_valid_locus(contig, start, reference) & is_valid_locus(contig, end, reference)
             & (start <= end))
    value = interval(locus(contig, start, reference), locus(contig, end, reference),
                     includes_start, includes_end)
    if type(invalid_missing) is bool and invalid_missing:
        return or_missing(valid, value)
    return case().when(valid, value).or_error('Invalid locus interval')


def locus_interval(contig, start, end, includes_start=True, includes_end=False,
                   reference_genome='default', invalid_missing=False):
    """Construct a validated locus interval, normalizing exclusive sentinels."""
    return _checked_interval(contig, start, end, includes_start, includes_end,
                             reference_genome, invalid_missing)


def parse_locus_interval(value, reference_genome='default', invalid_missing=False):
    """Parse Hail's chromosome/range notation with optional explicit bounds."""
    value = as_expr(value)
    reference = resolve_reference(reference_genome)
    lengths = literal(reference.lengths, tdict(tstr, tint32))
    text = value.replace(r'^[\[(]', '').replace(r'[\])]$', '')
    # Grammar alternatives are chosen once per row and all coordinate arithmetic
    # remains in the native expression engine, including k/M abbreviations.
    parts = text.first_match_in(r'^([^:]+):([^-]+)-(?:(.*):)?([^-:]+)$')
    same_or_cross = is_defined(parts)
    chrom_range = text.first_match_in(r'^([^:-]+)-([^:-]+)$')
    c1 = if_else(same_or_cross, parts[0], if_else(is_defined(chrom_range), chrom_range[0], text))
    c2 = if_else(same_or_cross, or_else(parts[2], parts[0]),
                 if_else(is_defined(chrom_range), chrom_range[1], text))
    def position(token, contig, default):
        lower = token.lower()
        numeric = lower.replace(r'[km]$', '')
        factor = if_else(lower.endswith('k'), 1000.0, if_else(lower.endswith('m'), 1000000.0, 1.0))
        return (case().when(~same_or_cross, default).when(lower == 'start', 1)
                .when(lower == 'end', lengths.get(contig, 0))
                .default(int32(float64(numeric) * factor)))
    start = position(parts[1], c1, 1)
    end = position(parts[3], c2, lengths.get(c2, 0))
    inc_start = ~value.startswith('(')
    # Whole-contig shorthand includes its final base; numeric ranges default
    # to right-exclusive, matching parse_locus_interval rather than Picard.
    inc_end = value.endswith(']') | (~same_or_cross & ~value.endswith(')'))
    if_same = _checked_interval(c1, start, end, inc_start, inc_end, reference, invalid_missing)
    ranks = literal({c: i for i, c in enumerate(reference.contigs)}, tdict(tstr, tint32))
    valid_cross = (is_valid_locus(c1, start, reference) & is_valid_locus(c2, end, reference)
                   & (ranks.get(c1, -1) < ranks.get(c2, -1)))
    cross = interval(locus(c1, start, reference), locus(c2, end, reference), inc_start, inc_end)
    cross = (or_missing(valid_cross, cross) if invalid_missing else
             case().when(valid_cross, cross).or_error('Invalid locus interval'))
    return if_else(c1 == c2, if_same, cross)


def import_locus_intervals(path, reference_genome='default', skip_invalid_intervals=False,
                           contig_recoding=None, **kwargs):
    """Import one-, three-, or five-column genomic interval records."""
    if type(skip_invalid_intervals) is not bool:
        raise TypeError('skip_invalid_intervals must be bool')
    for reserved in ('no_header', 'comment', 'impute', 'types'):
        if reserved in kwargs:
            raise TypeError(f'import_locus_intervals controls {reserved}')
    table = import_table(path, comment='@', no_header=True, impute=False, **kwargs)
    fields = list(table.row.dtype)
    recoding = None if contig_recoding is None else literal(contig_recoding, tdict(tstr, tstr))
    def recode(contig):
        return contig if recoding is None else recoding.get(contig, contig)
    if len(fields) == 1:
        if reference_genome is not None:
            result = parse_locus_interval(table.f0, reference_genome)
        else:
            match = table.f0.first_match_in(r'([^:]*):(\d+)\-(\d+)')
            result = _checked_interval(recode(match[0]), int32(match[1]), int32(match[2]),
                                       True, True, None, skip_invalid_intervals)
        table = table.select(interval=result)
    elif len(fields) in (3, 5):
        result = _checked_interval(recode(table.f0), int32(table.f1), int32(table.f2),
                                   True, True, reference_genome, skip_invalid_intervals)
        extra = {'target': table.f4} if len(fields) == 5 else {}
        table = table.select(interval=result, **extra)
    else:
        raise ValueError('invalid interval format: expected one, three, or five tab-separated fields')
    if skip_invalid_intervals and reference_genome is not None:
        table = table.filter(is_defined(table.interval))
    return table.key_by('interval')
