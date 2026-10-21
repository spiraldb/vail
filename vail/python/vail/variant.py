"""Biallelic HTS splitting used by VCF import workflows.

This MatrixTable implementation always sorts the resulting public variant key.
Table input and VEP consequence splitting are not yet supported.
"""
from uuid import uuid4


def _kernel(name, dtype, *args):
    from .expressions import Expression, _merge_sources
    source = None
    for arg in args:
        source = _merge_sources(source, arg.source)
    return Expression(dtype, source, {'op': 'string_function', 'function': name,
                                     'args': [arg.to_ir() for arg in args]})


def _downcode(value, allele):
    from . import struct, int32, if_else, is_defined, missing, tcall
    from .expressions import Expression
    alleles = value.alleles
    phased = value.phased
    coded = alleles.map(lambda original: int32(original == allele))
    from . import sorted as sort
    coded = if_else(phased, coded, sort(coded))
    packed = struct(__vail_call_alleles=coded, __vail_call_phased=phased)
    result = Expression(tcall, packed.source, {'op': 'cast', 'dtype': 'call', 'value': packed.to_ir()})
    return if_else(is_defined(value), result, missing(tcall))


def _min_rep(locus, reference, alternate):
    import vail as hl
    def normalized(trimmed):
        position = hl.int64(locus.position) + hl.int64(trimmed.shift)
        position = hl.case().when(position <= 2**31 - 1, hl.int32(position)).or_error(
            'split_multi_hts normalized position exceeds int32')
        return hl.struct(locus=hl.locus(locus.contig, position, locus.dtype.reference_genome),
                         alleles=trimmed.alleles)

    return hl.bind(normalized, _kernel(
        'variant_min_rep', hl.tstruct(shift=hl.tint32, alleles=hl.tarray(hl.tstr)),
        reference, alternate))


def split_multi_hts(dataset, keep_star=False, left_aligned=False, vep_root='vep', *, permit_shuffle=False):
    """Split variant rows, sorting by the resulting variant key.

    Alleles must be defined, nonempty strings and calls haploid/diploid with
    indices in range. Defined AD/PL must match allele count and GT ploidy.
    Missing GT yields missing PL; absent GT assumes diploidy. Missing PL
    elements are ignored when reducing buckets. Filtered entries stay filtered.
    ``permit_shuffle`` is accepted for compatibility; output is always sorted.
    """
    import vail as hl
    from .types import tlocus
    if not isinstance(dataset, hl.MatrixTable):
        raise NotImplementedError('split_multi_hts currently supports MatrixTable input')
    if (tuple(dataset.row_key) != ('locus', 'alleles')
            or not isinstance(dataset.locus.dtype, tlocus)
            or dataset.alleles.dtype != hl.tarray(hl.tstr)):
        raise ValueError('split_multi_hts requires locus and alleles variant keys')
    if vep_root in dataset.row.dtype:
        raise NotImplementedError('VEP consequence splitting is not implemented')
    expected = {'GT': hl.tcall, 'PGT': hl.tcall, 'AD': hl.tarray(hl.tint32),
                'PL': hl.tarray(hl.tint32), 'DP': hl.tint32, 'GQ': hl.tint32, 'PID': hl.tstr}
    for name, dtype in expected.items():
        if name in dataset.entry.dtype and dataset.entry.dtype[name] != dtype:
            raise TypeError(f'split_multi_hts: invalid type for {name}: {dataset.entry.dtype[name]}')
    # Validate before explode, including rows with no alternate (which otherwise
    # disappear). Entry reductions are materialized before duplicating rows.
    valid = (hl.is_defined(dataset.locus) & hl.is_defined(dataset.alleles)
             & (hl.len(dataset.alleles) >= 1)
             & hl.all(lambda a: hl.is_defined(a) & (hl.len(a) > 0), dataset.alleles))
    name = '__split_allele_' + uuid4().hex
    prepared = '__split_entry_' + uuid4().hex
    indices = hl.case().when(valid, hl.range(1, hl.len(dataset.alleles))).or_error(
        'split_multi_hts requires a defined locus and defined nonempty alleles')
    if not keep_star:
        indices = indices.filter(lambda i: dataset.alleles[i] != '*')
    dataset = dataset.annotate_rows(**{name: indices})
    fields = dataset.entry.dtype
    ints = hl.tarray(hl.tint32)
    gt = dataset.GT.alleles if 'GT' in fields else hl.literal([0, 0])
    pgt = dataset.PGT.alleles if 'PGT' in fields else hl.missing(ints)
    ad = dataset.AD if 'AD' in fields else hl.missing(ints)
    pl = dataset.PL if 'PL' in fields else hl.missing(ints)
    entry = _kernel('variant_entry', hl.tstruct(ad=hl.tarray(ints), pl=hl.tarray(ints)),
                    hl.len(dataset.alleles), gt, pgt, ad, pl)
    split = dataset.annotate_entries(**{prepared: entry})
    split = split.drop(*[f for f in ('AD', 'PL') if f in fields])
    split = split.key_rows_by().explode_rows(name)
    split = split.annotate_rows(a_index=split[name], was_split=hl.len(split.alleles) > 2,
                                old_locus=split.locus, old_alleles=split.alleles)
    variant = _min_rep(split.locus, split.alleles[0], split.alleles[split.a_index])
    if left_aligned:
        variant = hl.bind(lambda v: hl.case().when(v.locus == split.locus, v).or_error(
            'Found non-left-aligned variant in split_multi_hts'), variant)
    split = split.annotate_rows(locus=variant.locus, alleles=variant.alleles).drop(name)
    updates = {}
    for field in ('GT', 'PGT'):
        if field in fields:
            updates[field] = _downcode(split[field], split.a_index)
    if 'AD' in fields:
        updates['AD'] = split[prepared].ad[split.a_index - 1]
    if 'PL' in fields:
        pl = split[prepared].pl[split.a_index - 1]
        updates['PL'] = pl
        if 'GQ' in fields:
            quality = hl.gq_from_pl(pl)
            updates['GQ'] = hl.coalesce(quality, split.GQ)
    return (split.annotate_entries(**updates)
            .drop('old_locus', 'old_alleles', prepared)
            .key_rows_by('locus', 'alleles'))


def parse_variant(value, reference_genome='default'):
    """Parse ``contig:position:reference:alternate[,alternate...]`` lazily."""
    import vail as hl
    value = hl.as_expr(value)
    if value.dtype != hl.tstr:
        raise TypeError('parse_variant requires a string expression')

    def parse(text):
        fields = text.split(':')
        fields = hl.case().when(hl.len(fields) == 4, fields).or_error(
            'parse_variant expects contig:position:reference:alternate')
        return hl.or_missing(hl.is_defined(text), hl.struct(
            locus=hl.parse_locus(fields[0] + ':' + fields[1], reference_genome),
            alleles=hl.array([fields[2]]).extend(fields[3].split(',')),
        ))

    return hl.bind(parse, value)
