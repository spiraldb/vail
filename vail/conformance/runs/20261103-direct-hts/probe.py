import sys,time,json,os
import vail as hl
case=sys.argv[1]
if case.startswith('split-'):
 path='/Users/danielking/.cache/vail-benchmarks/indexed-20261103/'+('profile-first-200.mt' if case=='split-real' else 'generated.mt')
 mt=hl.read_matrix_table(path)
 def query():return hl.split_multi_hts(mt)._force_count_rows()
 expected=204 if case=='split-real' else 2000
 batch=1
else:
 rows=128;cols={'empty':0,'one':1,'short':8,'wide':64}[case]
 mt=hl.range_matrix_table(rows,cols,n_partitions=4)
 mt=mt.annotate_entries(x=hl.int32(mt.row_idx+mt.col_idx),xs=hl.range(8))
 def query():
  q=mt.annotate_entries(y=hl.sum(mt.xs.map(lambda a:a+mt.x)))
  return q.aggregate_entries(hl.agg.sum(q.y))
 expected=8*(cols*rows*(rows-1)//2+rows*cols*(cols-1)//2)+28*rows*cols
 batch=5
start=time.perf_counter();warm=0
while time.perf_counter()-start<1:
 value=query();assert value==expected,(case,value,expected);warm+=1
print(json.dumps({'warmup_operations':warm,'warmup_wall':time.perf_counter()-start}),flush=True)
for sample in range(7):
 start=time.perf_counter();cpu=time.process_time()
 for _ in range(batch):
  value=query();assert value==expected,(case,value,expected)
 print(json.dumps({'case':case,'sample':sample,'operations':batch,'wall':(time.perf_counter()-start)/batch,'cpu':(time.process_time()-cpu)/batch,'value':value}),flush=True)
