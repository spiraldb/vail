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


def export_gen(dataset, output, precision=4, gp=None, id1=None, id2=None,
               missing=None, varid=None, rsid=None):
    """Stream Oxford GEN rows and SAMPLE metadata to ``output`` files.

    Contract: https://hail.is/docs/0.2/methods/impex.html#hail.methods.export_gen
    """
    import builtins
    import math
    from . import (MatrixTable, tarray, tfloat64, tstr, int64, str as text,
                   array, delimit, coalesce, literal, case, len, bind)
    from .expressions import as_expr, Expression
    from .text import _local_path
    if not isinstance(dataset, MatrixTable):
        raise TypeError('export_gen requires a MatrixTable')
    if type(precision) is not int or not 0 <= precision <= 15:
        raise ValueError('precision must be an integer between 0 and 15')
    if 'locus' not in dataset.row.dtype or 'alleles' not in dataset.row.dtype:
        raise ValueError('export_gen requires locus and alleles row fields')
    if gp is None:
        if 'GP' not in dataset.entry.dtype or dataset.GP.dtype != tarray(tfloat64):
            raise ValueError('exporting to GEN requires a GP array<float64> field in the entry')
        gp = dataset.GP
    gp = as_expr(gp)
    if gp.dtype != tarray(tfloat64):
        raise TypeError('gp must have type array<float64>')
    if id1 is None or id2 is None:
        if builtins.len(dataset.col_key.dtype) != 1 or next(iter(dataset.col_key.dtype.values())) != tstr:
            raise ValueError('export_gen requires one string column key or explicit id1 and id2')
        sample_key = dataset[next(iter(dataset.col_key.dtype))]
        id1 = sample_key if id1 is None else id1
        id2 = sample_key if id2 is None else id2
    id1, id2 = as_expr(id1), as_expr(id2)
    if id1.dtype != tstr or id2.dtype != tstr:
        raise TypeError('id1 and id2 must be string expressions')
    missing = as_expr(0.0 if missing is None else missing)
    if varid is None and dataset.row.dtype.get('varid') == tstr:
        varid = dataset.varid
    if rsid is None and dataset.row.dtype.get('rsid') == tstr:
        rsid = dataset.rsid
    default_id = delimit(array([dataset.locus.contig, text(dataset.locus.position),
                               dataset.alleles[0], dataset.alleles[1]]), ':')
    varid = default_id if varid is None else coalesce(as_expr(varid), default_id)
    rsid = literal('.') if rsid is None else coalesce(as_expr(rsid), '.')
    if varid.dtype != tstr or rsid.dtype != tstr:
        raise TypeError('varid and rsid must be string expressions')
    for value, axis in [(gp, 'entry'), (varid, 'row'), (rsid, 'row'),
                        (id1, 'col'), (id2, 'col'), (missing, 'col')]:
        dataset._validate(value, axis)
    samples = dataset.select_cols(__gen_id1=id1, __gen_id2=id2, __gen_missing=missing)
    # Use physical column order, shared by localization, including choose_cols.
    samples = samples._public_axis('col', key=False).collect()
    for sample in samples:
        for field in ('__gen_id1', '__gen_id2'):
            value = sample[field]
            if value is None or not value or any(c.isspace() for c in value):
                raise ValueError('GEN sample identifiers must be nonempty and contain no whitespace')
        rate = sample.__gen_missing
        if rate is None or not math.isfinite(rate) or not 0 <= rate <= 1:
            raise ValueError('GEN sample missing rate must be between 0 and 1')
    matrix = dataset.annotate_rows(__gen_varid=varid, __gen_rsid=rsid)
    # Annotation retains existing field meanings; bind the validated original GP
    # expression to the new matrix source without rebuilding the user's IR.
    gp = Expression(gp.dtype, matrix if gp.source is not None else None, gp.to_ir())
    matrix = matrix.select_entries(__gen_gp=gp)
    rows = matrix.localize_entries('__gen_entries', '__gen_cols')
    factor = 10 ** precision

    def probability(p):
        # Integer decimal rendering gives a fixed number of fractional digits.
        def render(scaled):
            whole = text(scaled // factor)
            if precision == 0:
                return whole
            fraction = (literal('0' * precision) + text(scaled % factor))[-precision:]
            return whole + '.' + fraction
        return bind(render, int64(coalesce(p, 0.0) * factor + 0.5))

    def triple(entry):
        value = coalesce(entry.__gen_gp, literal([0.0, 0.0, 0.0]))
        value = case().when(len(value) == 3, value).or_error('GEN GP must have length 3')
        return value.map(probability)

    rows = rows.key_by()
    alleles = case().when(len(rows.alleles) == 2, rows.alleles).or_error('export_gen requires biallelic variants')
    fields = array([rows.locus.contig, rows.__gen_varid, rows.__gen_rsid,
                    text(rows.locus.position), alleles[0], alleles[1]])
    fields = fields.map(lambda value: case()
                        .when(~value.matches(r'.*\s.*', full_match=True), value)
                        .or_error('GEN variant fields must contain no whitespace'))
    fields = fields.extend(rows.__gen_entries.flatmap(triple))
    rows = rows.select(line=delimit(fields, ' '))
    output = _local_path(output, allow_compressed=True)
    rows.export(output + '.gen', header=False)
    with open(output + '.sample', 'w') as stream:
        stream.write('ID_1 ID_2 missing\n0 0 0\n')
        for sample in samples:
            stream.write(f'{sample.__gen_id1} {sample.__gen_id2} {sample.__gen_missing}\n')
