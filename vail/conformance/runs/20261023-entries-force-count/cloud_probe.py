import json, os, resource, sys, time
from pathlib import Path
import vail as hl
kind, case, output = sys.argv[1:]
root=Path('/home/danielking/cloud-scan-20261023')
p=root/'data/supplemental'/('hail' if kind=='legacy' else 'vail-from-hail')/'matrix_100000x10000.mt'
m=hl.MatrixTable.read_legacy(str(p)) if kind=='legacy' else hl.read_matrix_table(str(p))
if case=='rows': table=m._mir.core
elif case in ('entries', 'entries_sum_dp'): table=m.entries()
elif case=='unordered': table=m._context('entry')
elif case=='no_columns': table=m._context('entry', with_columns=False, with_rows=False)
else: raise ValueError(case)
base=Path(output);base.parent.mkdir(parents=True,exist_ok=True)
base.with_suffix('.plan.txt').write_text(table._native.physical_plan())
results={'pid':os.getpid(),'kind':kind,'case':case,'path':str(p),'row_key_unique':m._mir.row_key_unique,'column_ordering':m._mir.column_ordering,'iterations':[]}
print(json.dumps({k:v for k,v in results.items() if k!='iterations'}),flush=True)
for rep in range(int(os.environ.get("ENTRIES_ITERATIONS", "4"))):
 b=resource.getrusage(resource.RUSAGE_SELF);s=time.monotonic()
 m=hl.MatrixTable.read_legacy(str(p)) if kind=='legacy' else hl.read_matrix_table(str(p))
 if case=='rows': table=m._mir.core
 elif case in ('entries', 'entries_sum_dp'): table=m.entries()
 elif case=='unordered': table=m._context('entry')
 elif case=='no_columns': table=m._context('entry', with_columns=False, with_rows=False)
 n=table.aggregate(hl.agg.sum(table.DP)) if case=='entries_sum_dp' else table._force_count();e=time.monotonic();a=resource.getrusage(resource.RUSAGE_SELF)
 value={'rep':rep,'warmup':rep==0,'count':n,'wall_seconds':e-s,'cpu_seconds':a.ru_utime+a.ru_stime-b.ru_utime-b.ru_stime,'peak_rss_kib':a.ru_maxrss,'minor_faults':a.ru_minflt-b.ru_minflt,'major_faults':a.ru_majflt-b.ru_majflt}
 results['iterations'].append(value);base.write_text(json.dumps(results,indent=2));print(json.dumps(value),flush=True)
 if case != 'entries_sum_dp': assert n==(100000 if case=='rows' else 1000000000)
