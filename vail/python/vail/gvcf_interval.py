"""gVCF interval arrays, composed from the native VCF source.

Contract: Hail's GVCFPartitionReader in the pinned upstream impex.py/ir.py;
VCF 4.2: https://samtools.github.io/hts-specs/VCFv4.2.pdf.
"""
from __future__ import annotations

import json
import os
import tempfile
from collections.abc import Mapping


def _header_text(header):
    """Serialize the small supplied header, never genomic data rows."""
    if not isinstance(header, Mapping):
        raise TypeError("header_info must be a mapping")
    samples = header['sampleIDs']
    if not all(isinstance(s, str) and not any(c in s for c in '\t\r\n') for s in samples):
        raise ValueError("header_info sampleIDs must contain valid sample names")
    lines = ['##fileformat=VCFv4.2']
    for category in ('info', 'format'):
        attrs_by_name = header.get(category + 'Attrs', {})
        for name, typ in header[category + 'Fields']:
            # VCF Number metadata preserves scalar versus array cardinality.
            attrs = dict(attrs_by_name.get(name, {}))
            array = typ.startswith('Array[')
            scalar = typ[6:-1] if array else typ
            kind = {'Int32': 'Integer', 'Float64': 'Float', 'String': 'String', 'Boolean': 'Flag'}.get(scalar)
            if kind is None:
                raise ValueError(f"unsupported VCF header field type {typ!r}")
            number = attrs.get('Number', '.' if array else '0' if kind == 'Flag' else '1')
            description = json.dumps(attrs.get('Description', ''))
            lines.append(f'##{category.upper()}=<ID={name},Number={number},Type={kind},Description={description}>')
    for name, attrs in header.get('filterAttrs', {}).items():
        lines.append(f'##FILTER=<ID={name},Description={json.dumps(attrs.get("Description", ""))}>')
    columns = '#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO'
    if samples:
        columns += '\tFORMAT\t' + '\t'.join(samples)
    return '\n'.join(lines + [columns]) + '\n'


def import_gvcf_interval(
    path, file_num, contig, start, end, header_info, call_fields=('PGT',),
    entry_float_type='float64', array_elements_required=True,
    reference_genome='default', contig_recoding=None, skip_invalid_loci=False,
    filter=None, find=None, replace=None,
):
    """Return a lazy array of VCF records with POS in [start, end].

    Records retain reference blocks and symbolic alleles, with samples in the
    supplied header order under ``__entries``. This currently scans the source;
    tabix seeking and row-dependent path expressions are not yet supported.
    ``file_num`` supplies internal reader identity only; it adds no public field.
    """
    from . import eval as evaluate
    from .expressions import Expression, as_expr
    from .types import dtype, tint32, tstr
    from .vcf import import_vcf

    if isinstance(path, Expression):
        if path.dtype != tstr:
            raise TypeError('path must be a string expression')
        if path.source is not None or path._lexical_types:
            raise NotImplementedError('import_gvcf_interval requires a scalar path; row-dependent paths are not supported')
        path = evaluate(path)
    if not isinstance(path, (str, os.PathLike)):
        raise TypeError('path must be a string')
    file_num, contig, start, end = map(as_expr, (file_num, contig, start, end))
    for name, value, expected in (('file_num', file_num, tint32), ('contig', contig, tstr),
                                  ('start', start, tint32), ('end', end, tint32)):
        if value.dtype != expected:
            raise TypeError(f'{name} must have type {expected}')
    if (find is None) != (replace is None):
        raise ValueError('find and replace must be specified together')
    if isinstance(header_info, Expression):
        if header_info.source is not None or header_info._lexical_types:
            raise NotImplementedError('header_info must be a scalar expression')
        header_info = evaluate(header_info)
    entry_float_type = dtype(entry_float_type) if isinstance(entry_float_type, str) else entry_float_type
    # read_vcf captures the parsed header in its native source configuration,
    # so the temporary header can be removed as soon as construction finishes.
    with tempfile.NamedTemporaryFile(mode='w', suffix='.vcf') as header_file:
        header_file.write(_header_text(header_info))
        header_file.flush()
        mt = import_vcf(path, header_file=header_file.name, call_fields=call_fields,
                        entry_float_type=entry_float_type, array_elements_required=array_elements_required,
                        reference_genome=reference_genome, contig_recoding=contig_recoding,
                        skip_invalid_loci=skip_invalid_loci, filter=filter,
                        find_replace=None if find is None else (find, replace))
    rows = mt.localize_entries('__entries', '__cols')
    if all(value.source is None and not value._lexical_types for value in (contig, start, end)):
        rows = rows.filter((rows.locus.contig == contig) &
                           (rows.locus.position >= start) & (rows.locus.position <= end))
        return rows.collect(_localize=False)
    records = rows.collect(_localize=False)
    return records.filter(lambda row: (row.locus.contig == contig) &
                          (row.locus.position >= start) & (row.locus.position <= end))


def get_vcf_header_info(path, filter=None, find=None, replace=None):
    """Read the small VCF header and return Hail's typed header expression."""
    from . import _native_session, _schema_from_native, literal
    from .expressions import Expression
    from .matrixtable import _ROW, _CELLS, _VALUE
    from .types import tarray, tdict, tstr, tstruct, ttuple
    from .vcf import _local_path

    if isinstance(path, Expression):
        from . import eval as evaluate
        if path.dtype != tstr or path.source is not None or path._lexical_types:
            raise NotImplementedError('get_vcf_header_info requires a scalar string path')
        path = evaluate(path)
    if (find is None) != (replace is None):
        raise ValueError('find and replace must be specified together')
    options = dict(reference=None, contig_recoding={}, call_fields=['GT'],
                   entry_float32=False, array_elements_required=True,
                   skip_invalid_loci=False, drop_samples=False, filter=filter,
                   find_replace=None if find is None else (find, replace))
    native, samples, metadata = _native_session().read_vcf([_local_path(path)], None, options)
    schema = _schema_from_native(native)
    info_names = list(schema[_ROW]['info'])
    format_names = list(schema[_CELLS].element_type[_VALUE])

    def field_type(attrs):
        kind = attrs['Type']
        scalar = {'Integer': 'Int32', 'Float': 'Float64', 'String': 'String',
                  'Character': 'String', 'Flag': 'Boolean'}[kind]
        return scalar if attrs['Number'] == '1' or kind == 'Flag' else f'Array[{scalar}]'

    header = dict(sampleIDs=samples,
                  infoFields=[(name, field_type(metadata['info'][name])) for name in info_names],
                  formatFields=[(name, field_type(metadata['format'][name])) for name in format_names],
                  filterAttrs=metadata['filter'], infoAttrs=metadata['info'], formatAttrs=metadata['format'],
                  infoFlagFields=[name for name in info_names if metadata['info'][name]['Type'] == 'Flag'])
    attrs_type = tdict(tstr, tdict(tstr, tstr))
    fields_type = tarray(ttuple(tstr, tstr))
    return literal(header, tstruct(sampleIDs=tarray(tstr), infoFields=fields_type,
                                  formatFields=fields_type, filterAttrs=attrs_type,
                                  infoAttrs=attrs_type, formatAttrs=attrs_type,
                                  infoFlagFields=tarray(tstr)))
