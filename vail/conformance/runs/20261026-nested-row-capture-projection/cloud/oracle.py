"""Independent row-reducer reference; no aligned Struct row capture."""
import json,time,resource,sys
from pathlib import Path
import vail as h
from common import EXPECTED

def independent_reference(m):
    fields={
        '__h012_oracle_sum':h.agg.sum(m.GQ),
        '__h012_oracle_n':h.agg.count_where(h.is_defined(m.GQ)),
        '__h012_oracle_min':h.agg.min(m.GQ),'__h012_oracle_max':h.agg.max(m.GQ),
        '__h012_oracle_entries':h.agg.count(),
    }
    if any(name in m.row.dtype for name in fields):raise RuntimeError('Oracle field collision')
    m=m.annotate_rows(**fields)
    rows=m.rows()
    rows=rows.select(*[name for name in ('locus','alleles',*fields) if name not in rows.key.dtype])
    result=rows.collect()
    total=present=payload=small=entries=0
    for row in result:
        n=row['__h012_oracle_n'];count=row['__h012_oracle_entries']
        k=row.locus.position%7+len(row.alleles)
        if not 0<=k<=2**31-1:raise RuntimeError('Row constant exceeds exact Int32 reference domain; STOP')
        if n and (row['__h012_oracle_min']<0 or row['__h012_oracle_max']+k>2**31-1):
            raise RuntimeError('GQ range invalidates exact independent no-overflow formula; STOP')
        total+=row['__h012_oracle_sum']+k*n;present+=n;entries+=count
        payload+=(len(str(row.locus))+1+EXPECTED['padding_bytes'])*count;small+=k*count
    expected={'projected':dict(total=total,present=present),'scalar':dict(total=total,present=present),
              'whole_full':dict(total=total,present=present,payload=payload,small=small)}
    return expected,entries,len(result)

if __name__=='__main__':
    m=h.read_matrix_table(EXPECTED['fixture'])
    print(json.dumps(dict(phase='action_start',id=0,monotonic=time.monotonic())),flush=True)
    started=time.perf_counter();cpu=time.process_time()
    expected,entries,count=independent_reference(m)
    if count!=EXPECTED['dimensions'][0]:raise RuntimeError('Oracle row count differs')
    if expected['projected']!={'total':1536178152,'present':57513340}:
        raise RuntimeError('Independent row reference differs from recorded baseline full output')
    Path(sys.argv[1]).write_text(json.dumps(expected,indent=2)+'\n')
    print(json.dumps(dict(phase='action',id=0,result=expected,entries=entries,wall_seconds=time.perf_counter()-started,
        core_seconds=time.process_time()-cpu,maxrss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024)),flush=True)
