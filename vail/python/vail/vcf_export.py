"""Basic local VCF serialization for reader round trips and input fixtures.

Parallel export, tabix indexing, and BGZF output are deliberately unsupported.
"""
import json
from uuid import uuid4


def export_vcf(dataset, output, *, metadata=None, parallel=None, tabix=False, append_to_header=None):
    from . import MatrixTable
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
    samples=[column[sample_key] for column in dataset.cols().collect()]
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
    rows=dataset.localize_entries(entries_name,columns_name).collect()
    def value_text(value,dtype):
        if value is None:return './.' if dtype==tcall else '.'
        if dtype==tcall:return ('|' if value.phased else '/').join(str(a) for a in value.alleles) or '.'
        if isinstance(dtype,tarray):return ','.join(value_text(v,dtype.element_type) for v in value) or '.'
        text=str(value)
        if any(c in text for c in '\t\r\n'):
            raise ValueError('VCF values must not contain tabs or newlines')
        return text
    with open(output,'w',encoding='utf-8',newline='\n') as stream:
        stream.write('\n'.join(header)+'\n')
        for row in rows:
            locus=row.locus
            alleles=row.alleles
            info=[]
            for name,dtype in info_fields:
                value=row.info[name]
                if dtype==tbool:
                    if value:info.append(name)
                elif value is not None:info.append(name+'='+value_text(value,dtype))
            filters=row.get('filters')
            fixed=[locus.contig,str(locus.position),row.get('rsid') or '.',alleles[0],','.join(alleles[1:]) or '.',
                   '.' if row.get('qual') is None else str(row.qual),
                   '.' if filters is None else ';'.join(sorted(filters)) if filters else 'PASS',
                   ';'.join(info) or '.']
            if samples:
                fixed.append(':'.join(entry_names) or '.')
                for entry in row[entries_name]:
                    fixed.append(':'.join(value_text(None if entry is None else entry[name],dataset.entry.dtype[name]) for name in entry_names) or '.')
            stream.write('\t'.join(fixed)+'\n')
