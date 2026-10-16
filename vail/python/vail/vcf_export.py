"""Local VCF export with a bounded native Arrow batch sink.

Parallel export, tabix indexing, and BGZF output are deliberately unsupported.
"""
import json
from uuid import uuid4


def export_vcf(dataset, output, *, metadata=None, parallel=None, tabix=False, append_to_header=None):
    from . import MatrixTable, eval as evaluate
    from .matrixtable import _COL
    from .types import tarray, tstr, tcall, tint32, tfloat32, tfloat64, tbool
    from .vcf import _local_path
    if not isinstance(dataset, MatrixTable):
        raise TypeError('export_vcf requires a MatrixTable')
    if parallel is not None or tabix or append_to_header is not None:
        raise NotImplementedError('parallel VCF export, tabix, and appended headers are not implemented')
    output = _local_path(output)
    if output.endswith(('.gz','.bgz')):
        raise NotImplementedError('compressed VCF export is not implemented')
    if 'locus' not in dataset.row.dtype or 'alleles' not in dataset.row.dtype:
        raise ValueError('VCF export requires locus and alleles row fields')
    if len(dataset.col_key)!=1 or next(iter(dataset.col_key.dtype.values()))!=tstr:
        raise ValueError('VCF export requires one string sample key')
    sample_key=next(iter(dataset.col_key))
    # cols() returns a keyed Table and can sort the public column keys. VCF
    # sample headers must use the same positional order as the entry arrays.
    samples = evaluate(dataset._mir.column_records.map(lambda record: record[_COL][sample_key]))
    if any(sample is None or any(c in sample for c in '\t\r\n') for sample in samples):
        raise ValueError('invalid sample IDs')
    entry_names=list(dataset.entry.dtype)
    if 'GT' in entry_names:
        entry_names.remove('GT');entry_names.insert(0,'GT')
    info_schema=dataset.row.dtype.get('info')
    info_fields=list(info_schema.items()) if info_schema is not None else []
    metadata=metadata or {}
    def declaration(group,name,dtype):
        element=dtype.element_type if isinstance(dtype,tarray) else dtype
        kind={tstr:'String',tcall:'String',tint32:'Integer',tfloat32:'Float',tfloat64:'Float',tbool:'Flag'}.get(element)
        if kind is None or (group=='format' and kind=='Flag') or (isinstance(dtype,tarray) and kind=='Flag'):
            raise TypeError(f'VCF does not support {group} field {name!r} with type {dtype}')
        attrs=dict(metadata.get(group,{}).get(name,{}))
        number='0' if kind=='Flag' else '.' if isinstance(dtype,tarray) else '1'
        attrs={'Number':attrs.pop('Number',number),'Type':attrs.pop('Type',kind),'Description':attrs.pop('Description',''),**attrs}
        values=[f'ID={name}']
        for key,value in attrs.items():
            value=str(value)
            if key=='Description' or key not in ('Number','Type'):
                value='"'+value.replace('\\','\\\\').replace('"','\\"')+'"'
            values.append(f'{key}={value}')
        return f'##{group.upper()}=<{",".join(values)}>'
    header=['##fileformat=VCFv4.2']
    header.extend(declaration('info',name,dtype) for name,dtype in info_fields)
    header.extend(declaration('format',name,dataset.entry.dtype[name]) for name in entry_names)
    for name,attrs in metadata.get('filter',{}).items():
        header.append(f'##FILTER=<ID={name},Description={json.dumps(attrs.get("Description",""))}>')
    fields=['#CHROM','POS','ID','REF','ALT','QUAL','FILTER','INFO']
    if samples:fields+=['FORMAT',*samples]
    header.append('\t'.join(fields))
    entries_name='__export_entries_'+uuid4().hex
    columns_name='__export_columns_'+uuid4().hex
    localized = dataset.localize_entries(entries_name, columns_name)
    localized._native.export_vcf(output, header, entries_name, entry_names, len(samples))
