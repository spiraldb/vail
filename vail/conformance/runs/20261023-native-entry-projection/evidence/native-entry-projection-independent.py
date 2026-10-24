import argparse, hashlib, json, pathlib, resource, time
import vail as hl
import vail._native as native
p=argparse.ArgumentParser();p.add_argument('case');a=p.parse_args()
path='/home/danielking/cloud-scan-20261023/data/vail-from-hail/profile25_mt.mt'
def query():
 m=hl.read_matrix_table(path)
 if a.case=='gq_summary':
  m=m.select_entries('GQ').select_rows();t=m.entries()
  return t.aggregate(hl.struct(total=hl.agg.sum(t.GQ), present=hl.agg.count_where(hl.is_defined(t.GQ)),high=hl.agg.count_where(t.GQ>=20)))
 if a.case=='sample_depth':
  m=m.select_entries('DP').select_rows();t=m.entries()
  g=t.group_by(s=t.s).aggregate(total=hl.agg.sum(t.DP),present=hl.agg.count())
  return g.aggregate(hl.struct(total=hl.agg.sum(g.total),samples=hl.agg.count(),entries=hl.agg.sum(g.present)))
 raise ValueError(a.case)
print(json.dumps({'case':a.case,'native':native.__file__,'sha256':hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest()}),flush=True)
for i in range(4):
 b=resource.getrusage(resource.RUSAGE_SELF);start=time.perf_counter();value=query();after=resource.getrusage(resource.RUSAGE_SELF)
 print(json.dumps({'iteration':i,'wall':time.perf_counter()-start,'cpu':after.ru_utime+after.ru_stime-b.ru_utime-b.ru_stime,'maxrss_kib':after.ru_maxrss,'result':str(value)}),flush=True)
