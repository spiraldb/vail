"""Oxford GEN input built as lazy, native text and expression plans.

Specification: https://www.chg.ox.ac.uk/~gav/snptest/ (GEN/SAMPLE).
Only the SAMPLE identifiers, which define matrix columns, are read in Python.
"""
from __future__ import annotations


def import_gen(path, sample_file=None, tolerance=0.2, min_partitions=None,
               chromosome=None, reference_genome='default', contig_recoding=None,
               skip_invalid_loci=False):
    import builtins
    import math
    import gzip
    from . import (import_lines, literal, tarray, tstruct, tstr, tfloat64,
                   struct, array, int32, float64, len, case, locus, is_valid_locus,
                   missing, if_else, sum, call, max, tcall, is_defined, bind, tdict)
    from .text import _local_path
    from .reference_genome import resolve_reference

    def absolute(value):
        return if_else(value < 0, -value, value)

    if sample_file is None:
        raise ValueError('import_gen requires sample_file')
    if not isinstance(tolerance, (int, float)) or not math.isfinite(tolerance) or tolerance < 0:
        raise ValueError('tolerance must be finite and nonnegative')
    if chromosome is not None and not isinstance(chromosome, str):
        raise TypeError('chromosome must be a string')
    if type(skip_invalid_loci) is not bool:
        raise TypeError('skip_invalid_loci must be bool')
    recoding = contig_recoding or {}
    if any(not isinstance(k, str) or not isinstance(v, str) for k, v in recoding.items()):
        raise TypeError('contig_recoding must map strings to strings')
    sample_path = _local_path(sample_file, allow_compressed=True)
    opener = gzip.open if sample_path.endswith(('.gz', '.bgz')) else open
    with opener(sample_path, 'rt') as stream:
        header, types = stream.readline().split(), stream.readline().split()
        if not header or builtins.len(header) != builtins.len(types) or types[0] != '0':
            raise ValueError('invalid Oxford SAMPLE header')
        samples = []
        for line in stream:
            fields = line.split()
            if builtins.len(fields) != builtins.len(header):
                raise ValueError('Oxford SAMPLE row has incorrect field count')
            samples.append({'s': fields[0]})
    reference = None if reference_genome is None else resolve_reference(reference_genome)
    table = import_lines(path, min_partitions=min_partitions, force=True)
    table = table.select(data=table.text.strip().split(r'\s+'))
    offset = 1 if chromosome is None else 0
    expected = 5 + offset + 3 * builtins.len(samples)
    table = table.annotate(data=case().when(len(table.data) == expected, table.data)
                           .or_error('GEN row has incorrect number of probability fields'))
    contig = table.data[0] if chromosome is None else literal(chromosome)
    if recoding:
        contig = literal(recoding, tdict(tstr, tstr)).get(contig, contig)
    position = int32(table.data[offset + 2])
    if reference is None:
        location = struct(contig=contig, position=position)
    else:
        if skip_invalid_loci:
            table = table.filter(is_valid_locus(contig, position, reference.name))
            contig = table.data[0] if chromosome is None else literal(chromosome)
            if recoding:
                contig = literal(recoding, tdict(tstr, tstr)).get(contig, contig)
            position = int32(table.data[offset + 2])
        location = case().when(is_valid_locus(contig, position, reference.name),
                               locus(contig, position, reference.name)).or_error('Invalid locus in GEN input')
    table = table.select(locus=location, alleles=array([table.data[offset + 3], table.data[offset + 4]]),
                         rsid=table.data[offset + 1], varid=table.data[offset],
                         entries=table.data[5 + offset:].map(lambda value: float64(value)).grouped(3)
                         .map(lambda gp: struct(GP=gp)))
    table = table.key_by('locus', 'alleles')
    table = table.annotate_globals(cols=literal(samples, tarray(tstruct(s=tstr))))
    matrix = table._unlocalize_entries('entries', 'cols', ['s'], _aligned=True)
    def normalize(gp):
        return bind(lambda total: if_else(
            (absolute(total - 1.0) <= tolerance) & (total != 0),
            gp.map(lambda p: absolute(p / total)), missing(tarray(tfloat64))), sum(gp))

    def hard_call(gp):
        return bind(lambda maximum: if_else(
            len(gp.filter(lambda p: p == maximum)) == 1,
            case().when(gp[0] == maximum, call(0, 0))
            .when(gp[1] == maximum, call(0, 1)).default(call(1, 1)),
            missing(tcall)), max(gp))

    matrix = matrix.annotate_entries(GP=normalize(matrix.GP))
    matrix = matrix.annotate_entries(GT=hard_call(matrix.GP))
    return matrix.filter_entries(is_defined(matrix.GP)).select_entries('GT', 'GP')
