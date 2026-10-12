"""Expression-based biallelic HTS splitting used by VCF import workflows.

This MatrixTable implementation always sorts the resulting public variant key.
Table input and VEP consequence splitting are not yet supported.
"""
from uuid import uuid4


def _call_field(value, name, dtype):
    from .expressions import Expression, _IR
    return Expression(dtype,value.source,_IR({'op':'get_field','value':value.to_ir(),'name':name},value._global_refs,value._lookup_refs))


def _downcode(value, allele):
    from . import struct,int32,if_else,is_defined,missing,tcall,tarray,tbool,tint32
    from .expressions import Expression
    alleles=_call_field(value,'__vail_call_alleles',tarray(tint32))
    phased=_call_field(value,'__vail_call_phased',tbool)
    coded=alleles.map(lambda original:int32(original==allele))
    from . import sorted as sort
    coded=if_else(phased,coded,sort(coded))
    packed=struct(__vail_call_alleles=coded,__vail_call_phased=phased)
    result=Expression(tcall,packed.source,{'op':'cast','dtype':'call','value':packed.to_ir()})
    return if_else(is_defined(value),result,missing(tcall))


def _min_rep(locus, reference, alternate):
    from . import len as length,range as array_range,if_else,locus as make_locus,struct
    symbolic=alternate.startswith('<') | alternate.contains('[') | alternate.contains(']')
    limit=if_else(length(reference)<length(alternate),length(reference),length(alternate))-1
    unequal=array_range(limit).filter(lambda i:reference[length(reference)-1-i]!=alternate[length(alternate)-1-i])
    suffix=if_else(symbolic,0,if_else(length(unequal)>0,unequal[0],limit))
    ref=reference[:length(reference)-suffix]
    alt=alternate[:length(alternate)-suffix]
    limit=if_else(length(ref)<length(alt),length(ref),length(alt))-1
    unequal=array_range(limit).filter(lambda i:ref[i]!=alt[i])
    prefix=if_else(symbolic,0,if_else(length(unequal)>0,unequal[0],limit))
    return struct(locus=make_locus(locus.contig,locus.position+prefix,locus.dtype.reference_genome),
                  alleles=[ref[prefix:],alt[prefix:]])


def split_multi_hts(dataset,keep_star=False,left_aligned=False,vep_root='vep',*,permit_shuffle=False):
    import vail as hl
    from .types import tlocus
    if not isinstance(dataset,hl.MatrixTable):
        raise NotImplementedError('split_multi_hts currently supports MatrixTable input')
    if tuple(dataset.row_key)!=('locus','alleles') or not isinstance(dataset.locus.dtype,tlocus):
        raise ValueError('split_multi_hts requires locus and alleles variant keys')
    if vep_root in dataset.row.dtype:
        raise NotImplementedError('VEP consequence splitting is not implemented')
    expected={'GT':hl.tcall,'PGT':hl.tcall,'AD':hl.tarray(hl.tint32),'PL':hl.tarray(hl.tint32),'DP':hl.tint32,'GQ':hl.tint32,'PID':hl.tstr}
    for name,dtype in expected.items():
        if name in dataset.entry.dtype and dataset.entry.dtype[name]!=dtype:
            raise TypeError(f'split_multi_hts: invalid type for {name}: {dataset.entry.dtype[name]}')
    name='__split_allele_'+uuid4().hex
    indices=hl.range(1,hl.len(dataset.alleles))
    if not keep_star:indices=indices.filter(lambda i:dataset.alleles[i]!='*')
    split=dataset.annotate_rows(**{name:indices}).explode_rows(name).key_rows_by()
    split=split.annotate_rows(a_index=split[name],was_split=hl.len(split.alleles)>2,
                              old_locus=split.locus,old_alleles=split.alleles)
    variant=_min_rep(split.locus,split.alleles[0],split.alleles[split.a_index])
    if left_aligned:
        variant=hl.case().when(variant.locus==split.locus,variant).or_error('Found non-left-aligned variant in split_multi_hts')
    split=split.annotate_rows(locus=variant.locus,alleles=variant.alleles).drop(name)
    fields=dataset.entry.dtype
    updates={}
    for field in ('GT','PGT'):
        if field in fields:updates[field]=_downcode(split[field],split.a_index)
    if 'AD' in fields:
        updates['AD']=hl.or_missing(hl.is_defined(split.AD),[hl.int32(hl.sum(split.AD))-split.AD[split.a_index],split.AD[split.a_index]])
    if 'PL' in fields:
        alleles=hl.len(split.old_alleles)
        codes=hl.range(alleles).flatmap(lambda b:hl.range(b+1).map(lambda a:hl.struct(
            index=b*(b+1)//2+a,code=hl.int32(a==split.a_index)+hl.int32(b==split.a_index))))
        diploid=hl.range(3).map(lambda code:hl.min(codes.filter(lambda x:x.code==code).map(lambda x:split.PL[x.index])))
        haploid=hl.range(2).map(lambda code:hl.min(hl.range(alleles).filter(lambda a:hl.int32(a==split.a_index)==code).map(lambda a:split.PL[a])))
        is_diploid=hl.len(_call_field(split.GT,'__vail_call_alleles',hl.tarray(hl.tint32)))==2 if 'GT' in fields else True
        pl=hl.or_missing(hl.is_defined(split.PL),hl.if_else(is_diploid,diploid,haploid))
        updates['PL']=pl
        if 'GQ' in fields:
            sorted_pl=hl.sorted(pl.filter(lambda value:hl.is_defined(value)))
            quality=hl.or_missing(hl.len(sorted_pl)>=2,sorted_pl[1]-sorted_pl[0])
            updates['GQ']=hl.coalesce(quality,split.GQ)
    return split.annotate_entries(**updates).drop('old_locus','old_alleles').key_rows_by('locus','alleles')
