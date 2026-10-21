"""Local VCF export with a bounded native Arrow batch sink.

Tabix indexing uses the htslib ``tabix`` executable when requested.
"""
import json
import os
from uuid import uuid4


def export_vcf(dataset, output, *, metadata=None, parallel=None, tabix=False, append_to_header=None):
    from . import MatrixTable, Table, empty_array
    from .types import tarray, tstr, tcall, tint32, tfloat32, tfloat64, tbool, tstruct, tset, tlocus
    from .vcf import _local_path
    from .utils import FatalError
    if isinstance(dataset, Table):
        entries = '__vcf_sites_entries_' + uuid4().hex
        columns = '__vcf_sites_columns_' + uuid4().hex
        dataset = dataset.annotate(**{entries: empty_array(tstruct())})
        dataset = dataset.annotate_globals(**{columns: empty_array(tstruct(s=tstr))})
        dataset = dataset.unlocalize_entries(entries, columns, ['s'])
    if not isinstance(dataset, MatrixTable):
        raise TypeError('export_vcf requires a MatrixTable')
    if parallel not in (None, 'separate_header', 'header_per_shard'):
        raise ValueError('invalid parallel VCF export mode')
    output = _local_path(output)
    indexer = None
    if tabix:
        import shutil
        indexer = shutil.which('tabix')
        if indexer is None:
            raise NotImplementedError('VCF tabix indexing requires the htslib tabix executable')
        if not output.endswith('.bgz'):
            raise ValueError('tabix indexing requires BGZF output ending in .bgz')
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
    for group, fields in [('INFO', info_fields), ('FORMAT', list(dataset.entry.dtype.items()))]:
        invalid = []
        for name, dtype in fields:
            element = dtype.element_type if isinstance(dtype, tarray) else dtype
            if (element not in (tstr, tcall, tint32, tfloat32, tfloat64, tbool)
                    or (element == tbool and (group == 'FORMAT' or isinstance(dtype, tarray)))):
                invalid.append(f"\t'{name}': '{dtype}'.\n")
        if invalid:
            raise FatalError(f'VCF does not support the type(s) for the following {group} field(s):\n' + ''.join(invalid))
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
    if isinstance(dataset.locus.dtype, tlocus):
        reference = dataset.locus.dtype.reference_genome
        lengths = reference.lengths
        header.extend(
            f'##contig=<ID={contig},length={lengths[contig]},assembly={reference.name}>'
            for contig in reference.contigs
        )
    header.extend(declaration('info',name,dtype) for name,dtype in info_fields)
    header.extend(declaration('format',name,dtype) for name,dtype in dataset.entry.dtype.items())
    for name,attrs in metadata.get('filter',{}).items():
        identifier(name)
        if any(c in str(attrs.get('Description', '')) for c in '\t\r\n'):
            raise ValueError('invalid VCF filter description')
        header.append(f'##FILTER=<ID={name},Description={json.dumps(attrs.get("Description",""))}>')
    if append_to_header is not None:
        with open(_local_path(append_to_header)) as extra:
            header.extend(extra.read().splitlines())
    fields=['#CHROM','POS','ID','REF','ALT','QUAL','FILTER','INFO']
    if samples:fields+=['FORMAT',*samples]
    header.append('\t'.join(fields))
    if parallel is None:
        localized._native.export_vcf(output, header, entries_name, entry_names, len(samples))
        index_paths = [output]
    else:
        os.makedirs(output, exist_ok=True)
        suffix = '.vcf.bgz' if output.endswith('.bgz') else '.vcf.gz' if output.endswith('.gz') else '.vcf'
        shards = []
        if parallel == 'separate_header':
            header_name = 'header' + suffix
            localized.head(0)._native.export_vcf(os.path.join(output, header_name), header, entries_name, entry_names, len(samples))
            shards.append(header_name)
        data_name = 'part-00000' + suffix
        localized._native.export_vcf(os.path.join(output, data_name), [] if parallel == 'separate_header' else header, entries_name, entry_names, len(samples))
        shards.append(data_name)
        with open(os.path.join(output, 'shard-manifest.txt'), 'w') as manifest:
            manifest.write(''.join(name + '\n' for name in shards))
        index_paths = [os.path.join(output, data_name)]
    if indexer is not None:
        import subprocess
        for path in index_paths:
            subprocess.run([indexer, '-f', '-p', 'vcf', path], check=True, capture_output=True, text=True)
