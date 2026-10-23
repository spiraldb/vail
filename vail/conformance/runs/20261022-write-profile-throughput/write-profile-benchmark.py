import vail as hl, time, resource, sys, json, uuid, pathlib
source='/tmp/entries-profile25.mt'
out=pathlib.Path('/tmp')/('write-profile-'+uuid.uuid4().hex+'.mt')
m=hl.read_matrix_table(source)
a=resource.getrusage(resource.RUSAGE_SELF); st=time.perf_counter(); m.write(str(out)); b=resource.getrusage(resource.RUSAGE_SELF)
print(json.dumps({'wall':time.perf_counter()-st,'cpu':b.ru_utime+b.ru_stime-a.ru_utime-a.ru_stime,'peak_rss_bytes':b.ru_maxrss,'output':str(out),'source':source,'module':hl.__file__}),flush=True)

import shutil
shutil.rmtree(out)  # Reclaim only this run's unique output, after timing.
