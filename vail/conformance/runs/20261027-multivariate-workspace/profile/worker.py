"""One fully consumed public matrix linreg; real fitted values are observations."""
import argparse
from collections.abc import Mapping
import json
import math
import os
from pathlib import Path
import resource
import time
from common import EXPECTED, digest
p=argparse.ArgumentParser()
p.add_argument('mode',choices=['confirm','sample'])
p.add_argument('--stop-launch',required=True)
p.add_argument('--reference')
a=p.parse_args()
import vail as h
import vail._native as native
if os.environ.get('PYTHONPATH') or os.cpu_count()!=32:
    raise RuntimeError('Unexpected import/CPU environment')
for name,value in EXPECTED['threads'].items():
    if os.environ.get(name)!=value:raise RuntimeError(f'Thread setting differs:{name}')
frontend=Path(h.__file__).parent.resolve()
if frontend!=Path(EXPECTED['python']).parent.parent/'lib/python3.12/site-packages/vail':
    raise RuntimeError('Frontend escaped normal environment')
if digest(native.__file__)!=EXPECTED['native_sha256']:raise RuntimeError('Native differs')
for name,sha in EXPECTED['frontend'].items():
    if digest(frontend/name)!=sha:raise RuntimeError(f'Frontend differs:{name}')
fixture=Path(EXPECTED['fixture'])
for name,sha in EXPECTED['metadata_sha256'].items():
    if digest(fixture/name)!=sha:raise RuntimeError(f'Input metadata differs:{name}')
def emit(**event):print(json.dumps(event,allow_nan=False),flush=True)
def encode(value):
    # Explicit tags keep every field, null, signed zero and nonfinite class.
    if value is None:return {'null':True}
    if isinstance(value,float):
        if math.isnan(value):return {'float':'NaN'}
        if math.isinf(value):return {'float':'+Inf' if value>0 else '-Inf'}
        return {'float':value.hex()}
    if isinstance(value,(str,int,bool)):return value
    if isinstance(value,Mapping):return {str(k):encode(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)):return [encode(v) for v in value]
    raise TypeError(f'Unsupported public result/key type:{type(value)}')
def compatible(left,right):
    if isinstance(left,dict) and set(left)=={'float'} and isinstance(right,dict) and set(right)=={'float'}:
        x,y=left['float'],right['float']
        if x in ('NaN','+Inf','-Inf') or y in ('NaN','+Inf','-Inf'):return x==y
        return math.isclose(float.fromhex(x),float.fromhex(y),rel_tol=1e-8,abs_tol=1e-10)
    if type(left)!=type(right):return False
    if isinstance(left,dict):return left.keys()==right.keys() and all(compatible(left[k],right[k]) for k in left)
    if isinstance(left,list):return len(left)==len(right) and all(compatible(x,y) for x,y in zip(left,right))
    return left==right
emit(phase='provenance',main=EXPECTED['main'],production=EXPECTED['compiled_production'],native_sha256=digest(native.__file__),pid=os.getpid(),threads=EXPECTED['threads'],metadata_dimensions=EXPECTED['dimensions'])
m=h.read_matrix_table(str(fixture))
if 'DP' not in m.entry.dtype or m.entry.dtype['DP']!=h.tint32:
    raise RuntimeError('Required original DP Int32 absent; no substitute')
if 'locus' not in m.row.dtype:raise RuntimeError('Original locus absent')
if '__h047_x' in m.row.dtype or '__h047_fit' in m.col.dtype:raise RuntimeError('Reserved field collision')
m=m.annotate_rows(__h047_x=h.float64(m.locus.position)/1000000.0)
m=m.annotate_cols(__h047_fit=h.agg.linreg(h.float64(h.or_else(m.DP,0)),[1.0,m['__h047_x']]))
c=m.cols()
c=c.select(fit=c['__h047_fit'])
key_fields=tuple(c.key.dtype)
if not key_fields:raise RuntimeError('Column keys required')
fit_fields={'beta','standard_error','t_stat','p_value','multiple_standard_error','multiple_r_squared','adjusted_r_squared','f_stat','multiple_p_value','n'}
reference=None if a.reference is None else json.loads(Path(a.reference).read_text())
first=None
for action in range(1 if a.mode=='confirm' else 12):
    if first is not None and (Path(a.stop_launch).exists() or time.monotonic()-first>=6):break
    emit(phase='action_start',id=action,monotonic=time.monotonic())
    wall=time.perf_counter();cpu=time.process_time()
    rows=c.collect()
    if len(rows)!=EXPECTED['dimensions'][1]:raise RuntimeError('Incomplete column domain')
    ordered={}
    for row in rows:
        if set(row.fit)!=fit_fields:raise RuntimeError('Incomplete fit fields')
        if not isinstance(row.fit.n,int) or not 0<=row.fit.n<=EXPECTED['dimensions'][0]:raise RuntimeError('Invalid observation count')
        for name in ('beta','standard_error','t_stat','p_value'):
            if row.fit[name] is not None and len(row.fit[name])!=2:raise RuntimeError('Invalid predictor output width')
        key=encode({name:row[name] for name in key_fields})
        token=json.dumps(key,sort_keys=True,allow_nan=False)
        if token in ordered:raise RuntimeError('Duplicate column key')
        ordered[token]={'key':key,'fit':encode(row.fit)}
    result=[ordered[token] for token in sorted(ordered)]
    if reference is not None and not compatible(result,reference):raise RuntimeError('Full real fits differ beyond declared tolerance')
    elapsed=time.perf_counter()-wall;peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024;completed=time.monotonic()
    emit(phase='action',id=action,result=result,wall_seconds=elapsed,core_seconds=time.process_time()-cpu,maxrss_bytes=peak,completed_monotonic=completed)
    if elapsed>10 or peak>6*1024**3:raise RuntimeError('Completed action resource budget')
    if first is None:first=completed
