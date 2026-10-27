"""Bounded real native DP slice and independent exclusive-prefix reference."""
import json,time,math
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
from values import encode
h=verify();out=Path(EXPECTED['generated_fixture']);lock=Path(EXPECTED['generated_lock']);original=Path(EXPECTED['original_fixture'])
if out.exists() or lock.exists():raise RuntimeError('Fresh owned paths required')
for relative,sha in EXPECTED['original_metadata'].items():
    if digest(original/relative)!=sha:raise RuntimeError('Original fixture metadata differs')
print(json.dumps(dict(phase='action_start',id=0,monotonic=time.monotonic())),flush=True)
m=h.read_matrix_table(str(original))
if 'DP' not in m.entry.dtype or m.entry.dtype['DP']!=h.tint32:raise RuntimeError('Original DP Int32 required; no substitute')
m=m.head(256).choose_cols(list(range(128)))
t=m.localize_entries('entries','columns')
columns=h.eval(t.columns)
if len(columns)!=128:raise RuntimeError('Incomplete original sample domain')
t=t.select(key=h.struct(locus=h.str(t.locus),alleles=t.alleles),values=t.entries.map(lambda e:e.DP))
t.write(str(out));t=h.read_table(str(out));rows=t.collect()
if len(rows)!=256:raise RuntimeError('Incomplete variant domain')
sums=[0]*128;counts=[0]*128;expected=[];inputs=[];seen=set()
for i,row in enumerate(rows):
    key=encode(row['key']);token=json.dumps(key,sort_keys=True)
    if token in seen:raise RuntimeError('Duplicate variant key')
    seen.add(token);a=row['values']
    if a is None or len(a)!=128:raise RuntimeError('Width/missing-array premise differs')
    prefix=None if i==0 else [sums[j]/counts[j] if counts[j] else float('nan') for j in range(128)]
    expected.append(dict(key=key,prefix=encode(prefix)));inputs.append(dict(key=key,values=a))
    for j,x in enumerate(a):
        if x is not None:
            if type(x)!=int or not -(2**31)<=x<2**31:raise RuntimeError('DP Int32 premise')
            sums[j]+=x;counts[j]+=1
            # Every sum/partial sum here is exact in Float64 (<=256Int32 terms).
            if abs(sums[j])>=2**53:raise RuntimeError('Exact Float64-sum premise')
files={str(p.relative_to(out)):dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(out.rglob('*')) if p.is_file()}
if not files or sum(v['bytes'] for v in files.values())>10*1024**3:raise RuntimeError('10GiB output cap')
record=dict(files=files,schema=str(t.row.dtype),source_inputs=inputs,columns=encode(columns),expected=expected,dimensions=[256,128],original_metadata=EXPECTED['original_metadata'])
lock.write_text(json.dumps(record,indent=2,allow_nan=False)+'\n');(Path.cwd()/'fixture-lock.json').write_bytes(lock.read_bytes())
print(json.dumps(dict(phase='action',id=0,completed_monotonic=time.monotonic(),lock_sha256=digest(lock),rows=256,samples=128)),flush=True)
