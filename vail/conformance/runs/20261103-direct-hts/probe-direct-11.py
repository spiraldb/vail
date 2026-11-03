import sys,time,json,os
import vail as hl
case=sys.argv[1]
root='/Users/danielking/.cache/vail-benchmarks/indexed-20261103/'
mt=hl.read_matrix_table(root+('profile-first-200.mt' if case.startswith('real') else 'generated.mt'))
if case.startswith('moving') or case.startswith('mixed'):
 mt=mt.key_rows_by().annotate_rows(alleles=['CAA','CGA','CAT'] if case.startswith('moving') else ['CAA','CGA','TAA'])._copy(row_key=('locus','alleles'))
if case=='high-allele':
 mt=mt.key_rows_by().annotate_rows(alleles=['A','C','G','T','CC','GG','TT','CCC'])._copy(row_key=('locus','alleles'))
 mt=mt.annotate_entries(AD=hl.literal(list(range(8))),PL=hl.literal(list(range(36))))
if case=='absent-fields':mt=mt.select_entries('DP')
def build():return hl.split_multi_hts(mt)
def action(q):
 if case.endswith('count'):return q._force_count_rows()
 return q.aggregate_entries(hl.struct(n=hl.agg.count(),dp=hl.agg.sum(q.DP),**({} if case=='absent-fields' else dict(ad=hl.agg.sum(hl.sum(q.AD)),pl=hl.agg.sum(hl.sum(q.PL)),gq=hl.agg.sum(q.GQ),gt=hl.agg.sum(hl.sum(q.GT.alleles))))))
q=build();expected=action(q)
# Time query construction separately; action-only and whole query are both reported.
t=time.perf_counter();warm=0
while time.perf_counter()-t<1:
 assert action(build())==expected;warm+=1
print(json.dumps({'case':case,'expected':str(expected),'warmup':warm}),flush=True)
for sample in range(7):
 start=time.perf_counter();cpu=time.process_time();q=build();prepared=time.perf_counter();prepared_cpu=time.process_time();value=action(q);end=time.perf_counter();end_cpu=time.process_time();assert value==expected
 print(json.dumps(dict(case=case,sample=sample,wall=end-start,cpu=end_cpu-cpu,construction=prepared-start,action=end-prepared,action_cpu=end_cpu-prepared_cpu,value=str(value))),flush=True)
