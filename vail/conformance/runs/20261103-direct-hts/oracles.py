import json,hashlib,os
from collections.abc import Mapping
import vail as hl

def encode(value):
    if isinstance(value,Mapping):return {k:encode(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [encode(v) for v in value]
    if isinstance(value,hl.Call):return {'call':list(value.alleles),'phased':value.phased}
    if hasattr(value,'contig') and hasattr(value,'position'):return {'locus':[value.contig,value.position]}
    if value is None or isinstance(value,(str,int,float,bool)):return value
    raise TypeError(type(value))
results={}
for kind in ['left','moved','mixed','many']:
    mt=hl.range_matrix_table(6,4,n_partitions=3)
    alleles=['A','C','G'] if kind=='left' else ['CAA','CGA','CAT']
    if kind=='mixed':alleles=hl.if_else(mt.row_idx%2==0,['A','C','G'],['CAA','CGA','CAT'])
    if kind=='many':alleles=['A','C','G','T','CC','GG','TT']
    n=7 if kind=='many' else 3
    mt=mt.annotate_rows(locus=hl.locus('1',100+mt.row_idx*4),alleles=alleles)
    mt=mt._copy(row_key=('locus','alleles'))
    mt=mt.annotate_cols(sample='sample').annotate_globals(study='oracle')
    mt=mt.annotate_entries(GT=hl.call(0,1,phased=True),AD=hl.array([i+1 for i in range(n)]),PL=hl.array(list(range(n*(n+1)//2))),GQ=99,DP=mt.col_idx+1)
    mt=mt.filter_entries(mt.col_idx!=1)
    for flags in [{},{'permit_shuffle':True},*([{'left_aligned':True}] if kind in ['left','many'] else [])]:
        split=hl.split_multi_hts(mt,**flags)
        data={'row':str(split.row.dtype),'entry':str(split.entry.dtype),'columns':split.cols().collect(),'globals':hl.eval(split.index_globals()),'rows':split.localize_entries('entries','columns').collect()}
        label=kind+json.dumps(flags,sort_keys=True)
        results[label]=encode(data)
# Cross-action reuse must not retain an earlier successful execution as stale state.
for i in range(2):
    results['repeat'+str(i)]=encode(hl.split_multi_hts(mt).localize_entries('entries','columns').collect())
raw=json.dumps(results,sort_keys=True,allow_nan=False)
print(json.dumps({'shared':bool(os.environ.get('VAIL_EXPERIMENT_SHARED_UNION')),'sha256':hashlib.sha256(raw.encode()).hexdigest(),'cases':len(results),'results':results},sort_keys=True))
