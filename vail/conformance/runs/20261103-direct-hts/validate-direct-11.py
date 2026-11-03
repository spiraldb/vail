import os,json
import vail as hl
from collections.abc import Mapping
def plain(x):
 if isinstance(x,Mapping):return {k:plain(v) for k,v in x.items()}
 if isinstance(x,(list,tuple)):return [plain(v) for v in x]
 if isinstance(x,hl.Call):return (list(x.alleles),x.phased)
 if hasattr(x,'contig') and hasattr(x,'position'):return (x.contig,x.position)
 return x
cases=0
for n in [2,3,8]:
 for mode in ['diploid','haploid','missing-gt','no-gt','no-pl','dp-only','no-samples']:
  mt=hl.range_matrix_table(4,0 if mode=='no-samples' else 5,n_partitions=2)
  mt=mt.annotate_rows(locus=hl.locus('1',mt.row_idx*4+100),alleles=['A','C','G','T','CC','GG','TT','CCC'][:n])._copy(row_key=('locus','alleles'))
  gt=hl.call(1,phased=True) if mode=='haploid' else hl.call(0,n-1,phased=True)
  if mode=='missing-gt':gt=hl.missing(hl.tcall)
  plsize=n if mode=='haploid' else n*(n+1)//2
  mt=mt.annotate_entries(GT=gt,PGT=hl.call(1,0,phased=True),AD=hl.array([None if j==1 else j+1 for j in range(n)]),PL=hl.array([None if j%3==0 else j-4 for j in range(plsize)]),GQ=21,DP=mt.col_idx,tag='retained')
  if mode=='no-gt':mt=mt.drop('GT')
  if mode=='no-pl':mt=mt.drop('PL')
  if mode=='dp-only':mt=mt.select_entries('DP')
  mt=mt.filter_entries(mt.col_idx!=2)
  outputs=[]
  for direct in [False,True]:
   if direct:os.environ['VAIL_EXPERIMENT_DIRECT_HTS']='1'
   else:os.environ.pop('VAIL_EXPERIMENT_DIRECT_HTS',None)
   q=hl.split_multi_hts(mt)
   outputs.append(plain(q.localize_entries('entries','cols').collect()))
  assert outputs[0]==outputs[1],(n,mode,outputs)
  cases+=1
print(json.dumps({'complete_output_cases':cases,'passed':True}))
