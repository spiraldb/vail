"""Local VCF export with a bounded native Arrow batch sink.

Parallel export, tabix indexing, and BGZF output are deliberately unsupported.
"""
import json
from uuid import uuid4


def export_vcf(dataset, output, *, metadata=None, parallel=None, tabix=False, append_to_header=None):
    from . import MatrixTable
    from .types import tarray, tstr, tcall, tint32, tfloat32, tfloat64, tbool, tstruct, tset, tlocus
    from .vcf import _local_path
    if not isinstance(dataset, MatrixTable):
        raise TypeError('export_vcf requires a MatrixTable')
    if parallel is not None or tabix or append_to_header is not None:
        raise NotImplementedError('parallel VCF export, tabix, and appended headers are not implemented')
    output = _local_path(output)
    if output.endswith(('.gz','.bgz')):
        raise NotImplementedError('compressed VCF export is not implemented')
    if ('locus' not in dataset.row.dtype or 'alleles' not in dataset.row.dtype
            or dataset.alleles.dtype != tarray(tstr)
            or not (isinstance(dataset.locus.dtype, tlocus)
                    or dataset.locus.dtype == tstruct(contig=tstr, position=tint32))):
        raise ValueError('VCF export requires locus and alleles row fields')
    if len(dataset.col_key)!=1 or next(iter(dataset.col_key.dtype.values()))!=tstr:
        raise ValueError('VCF export requires one string sample key')
    sample_key=next(iter(dataset.col_key))
    entries_name = '__export_entries_' + uuid4().hex
    columns_name = '__export_columns_' + uuid4().hex
    localized = dataset.localize_entries(entries_name, columns_name)
    from . import eval as evaluate
    # The localized global column array and each row's entry array use the same
    # physical order. Public cols() may sort by key and cannot supply this header.
    samples = [column[sample_key] for column in evaluate(localized.index_globals()[columns_name])]
    if (any(sample is None or not sample or any(c in sample for c in '\t\r\n') for sample in samples)
            or len(set(samples)) != len(samples)):
        raise ValueError('invalid sample IDs')
    for name, expected in [('rsid', tstr), ('filters', tset(tstr))]:
        if name in dataset.row.dtype and dataset.row.dtype[name] != expected:
            raise TypeError(f'VCF {name} has invalid type')
    if 'qual' in dataset.row.dtype and dataset.qual.dtype not in (tfloat32, tfloat64):
        raise TypeError('VCF qual must be floating point')
    entry_names=list(dataset.entry.dtype)
    if 'GT' in entry_names:
        entry_names.remove('GT');entry_names.insert(0,'GT')
    info_schema=dataset.row.dtype.get('info')
    if info_schema is not None and not isinstance(info_schema, tstruct):
        raise TypeError('VCF info must be a struct')
    info_fields=list(info_schema.items()) if info_schema is not None else []
    metadata=metadata or {}
    def identifier(value):
        if not value or any(c.isspace() or c in ',:;=<>"' for c in value):
            raise ValueError('invalid VCF field identifier')
        return value

    def declaration(group,name,dtype):
        identifier(name)
        element=dtype.element_type if isinstance(dtype,tarray) else dtype
        kind={tstr:'String',tcall:'String',tint32:'Integer',tfloat32:'Float',tfloat64:'Float',tbool:'Flag'}.get(element)
        if kind is None or (group=='format' and kind=='Flag') or (isinstance(dtype,tarray) and kind=='Flag'):
            raise TypeError(f'VCF does not support {group} field {name!r} with type {dtype}')
        attrs=dict(metadata.get(group,{}).get(name,{}))
        number='0' if kind=='Flag' else '.' if isinstance(dtype,tarray) else '1'
        if attrs.get('Type', kind) != kind:
            raise ValueError(f'VCF metadata Type disagrees with {name!r}')
        declared = str(attrs.get('Number', number))
        if ((not isinstance(dtype, tarray) and declared != number)
                or (isinstance(dtype, tarray) and declared == '1')):
            raise ValueError(f'VCF metadata Number disagrees with {name!r}')
        attrs={'Number':attrs.pop('Number',number),'Type':attrs.pop('Type',kind),'Description':attrs.pop('Description',''),**attrs}
        values=[f'ID={name}']
        for key,value in attrs.items():
            value=str(value)
            if any(c in value for c in '\t\r\n'):
                raise ValueError('VCF header values must not contain tabs or newlines')
            if key=='Description' or key not in ('Number','Type'):
                value='"'+value.replace('\\','\\\\').replace('"','\\"')+'"'
            values.append(f'{key}={value}')
        return f'##{group.upper()}=<{",".join(values)}>'
    header=['##fileformat=VCFv4.2']
    header.extend(declaration('info',name,dtype) for name,dtype in info_fields)
    header.extend(declaration('format',name,dataset.entry.dtype[name]) for name in entry_names)
    for name,attrs in metadata.get('filter',{}).items():
        identifier(name)
        if any(c in str(attrs.get('Description', '')) for c in '\t\r\n'):
            raise ValueError('invalid VCF filter description')
        header.append(f'##FILTER=<ID={name},Description={json.dumps(attrs.get("Description",""))}>')
    fields=['#CHROM','POS','ID','REF','ALT','QUAL','FILTER','INFO']
    if samples:fields+=['FORMAT',*samples]
    header.append('\t'.join(fields))
    localized._native.export_vcf(output, header, entries_name, entry_names, len(samples))
