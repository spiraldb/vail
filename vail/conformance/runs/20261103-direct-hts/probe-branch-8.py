import os,sys,json,time
import vail as hl
case=sys.argv[1]
root='/Users/danielking/.cache/vail-benchmarks/indexed-20261103/'
mt=hl.read_matrix_table(root+('profile-first-200.mt' if case=='real' else 'generated.mt'))
if case in ['moving','mixed']:
 mt=mt.key_rows_by().annotate_rows(alleles=['CAA','CGA','CAT'] if case=='moving' else ['CAA','CGA','TAA'])._copy(row_key=('locus','alleles'))
expected=204 if case=='real' else 2000
def query():return hl.split_multi_hts(mt)._force_count_rows()
t=time.perf_counter();warm=0
while time.perf_counter()-t<1:
 assert query()==expected;warm+=1
print(json.dumps({'warmup_operations':warm}),flush=True)
for sample in range(7):
 t=time.perf_counter();c=time.process_time();value=query();wall=time.perf_counter()-t;cpu=time.process_time()-c
 assert value==expected
 print(json.dumps({'case':case,'sample':sample,'wall':wall,'cpu':cpu,'value':value}),flush=True)
