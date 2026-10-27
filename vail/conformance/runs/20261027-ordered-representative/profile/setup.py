"""Transparent duplicate table provisioning, excluded from profile actions."""
import json,time
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
h=verify();path=Path(EXPECTED['generated_fixture']);lock=Path(EXPECTED['generated_lock'])
if path.exists() or lock.exists():raise RuntimeError('Fresh owned fixture required')
print(json.dumps(dict(phase='action_start',id=0,monotonic=time.monotonic())),flush=True)
t=h.utils.range_table(262144,n_partitions=16)
t=t.annotate(source_index=t.idx,r=t.idx%4096,c=(t.idx//4096)%4,value=t.idx,payload=h.str(t.idx)+':'+'x'*64)
t=t.key_by().select('source_index','r','c','value','payload')
t.write(str(path));t=h.read_table(str(path))
if t.count()!=262144:raise RuntimeError('Source domain differs')
# Bounds/domain checks do not profile or materialize final conversion.
counts=dict(t.aggregate(h.struct(n=h.agg.count(),minimum=h.agg.min(t.source_index),maximum=h.agg.max(t.source_index),total=h.agg.sum(t.source_index))))
if counts!=dict(n=262144,minimum=0,maximum=262143,total=262144*262143//2):raise RuntimeError('Source formula checks differ')
files={str(p.relative_to(path)):dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(path.rglob('*')) if p.is_file()}
if not files or sum(x['bytes'] for x in files.values())>10*1024**3:raise RuntimeError('Output10GiB cap')
lock.write_text(json.dumps(dict(files=files,source_schema=str(t.row.dtype),counts=counts),indent=2)+'\n')
# Copy ORIGINAL lock receipt into supervised output too, avoiding prior off-tree omission.
(Path.cwd()/'fixture-lock.json').write_bytes(lock.read_bytes())
print(json.dumps(dict(phase='action',id=0,completed_monotonic=time.monotonic(),fixture_bytes=sum(x['bytes'] for x in files.values()),lock_sha256=digest(lock))),flush=True)
