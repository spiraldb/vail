import sys,time,resource
import vail as hl
n,p,depth=map(int,sys.argv[1:])
t=hl.Table.parallelize([{'k':i,'v0':i} for i in range(n)],key='k',n_partitions=p)
for d in range(1,depth+1):
 r=hl.Table.parallelize([{'k':i,f'v{d}':i} for i in range(n)],key='k',n_partitions=p)
 t=t.join(r).key_by('k')
start=time.monotonic();rows=t.collect()
print(n,p,depth,len(rows),round(time.monotonic()-start,3),resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,flush=True)
