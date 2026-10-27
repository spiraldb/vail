"""One consumed public nested row-capture pipeline; no production modifications."""
import argparse,json,os,resource,time
from pathlib import Path
from common import EXPECTED,digest
p=argparse.ArgumentParser();p.add_argument('mode',choices=['confirm','sample']);p.add_argument('--stop-launch',required=True);p.add_argument('--reference');a=p.parse_args()
import vail as h
import vail._native as native
if os.environ.get('PYTHONPATH') or os.cpu_count()!=32:raise RuntimeError('Unexpected import/CPU environment')
for name,value in EXPECTED['threads'].items():
    if os.environ.get(name)!=value:raise RuntimeError(f'Thread setting differs:{name}')
frontend=Path(h.__file__).parent.resolve()
if frontend!=Path(EXPECTED['python']).parent.parent/'lib/python3.12/site-packages/vail':raise RuntimeError('Frontend escaped normal environment')
if digest(native.__file__)!=EXPECTED['native_sha256']:raise RuntimeError('Native fingerprint differs')
for name,sha in EXPECTED['frontend'].items():
    if digest(frontend/name)!=sha:raise RuntimeError(f'Frontend differs:{name}')
fixture=Path(EXPECTED['fixture'])
for name,sha in EXPECTED['metadata_sha256'].items():
    if digest(fixture/name)!=sha:raise RuntimeError(f'Input metadata differs:{name}')
def emit(**event):print(json.dumps(event,allow_nan=False),flush=True)
emit(phase='provenance',main=EXPECTED['main'],production=EXPECTED['compiled_production'],native_sha256=digest(native.__file__),pid=os.getpid(),threads=EXPECTED['threads'],metadata_dimensions=EXPECTED['dimensions'])
m=h.read_matrix_table(str(fixture))
if 'GQ' not in m.entry.dtype or m.entry.dtype['GQ']!=h.tint32:raise RuntimeError('Original GQ Int32 required; no substitute')
if not all(name in m.row.dtype for name in ('locus','alleles')):raise RuntimeError('Original keyed row fields required')
if '__h012_capture' in m.row.dtype:raise RuntimeError('Reserved diagnostic field already exists')
m=m.annotate_rows(__h012_capture=h.struct(
    small=h.array([h.int32(m.locus.position%7),h.len(m.alleles)]),
    unused=h.str(m.locus)+':'+'x'*EXPECTED['padding_bytes']))
m=m.select_entries(score=m.GQ+h.sum(m['__h012_capture'].small))
q=h.struct(total=h.agg.sum(m.score),present=h.agg.count_where(h.is_defined(m.score)))
if a.mode=='confirm':
    # Private diagnostic ONLY: producer plan, not the full aggregate action,
    # not an oracle or route-forcing assertion. Actual bound CPU stacks decide.
    plan=m._mir.core._native.physical_plan()
    if len(plan)>2_000_000:raise RuntimeError('Diagnostic plan limit')
    emit(phase='diagnostic_plan',plan=plan,scope='producer-only diagnostic, not an action-path or correctness oracle')
reference=None if a.reference is None else json.loads(Path(a.reference).read_text())
first=None
for action in range(1 if a.mode=='confirm' else 12):
    if first is not None and (Path(a.stop_launch).exists() or time.monotonic()-first>=6):break
    emit(phase='action_start',id=action,monotonic=time.monotonic())
    wall=time.perf_counter();cpu=time.process_time()
    result=dict(m.aggregate_entries(q));elapsed=time.perf_counter()-wall
    if (set(result)!={'total','present'} or result['present']!=EXPECTED['present_reference']
        or not isinstance(result['total'],int)):raise RuntimeError(f'Complete result invalid:{result!r}')
    if reference is not None and result!=reference:raise RuntimeError(f'Complete result changed:{result!r}')
    peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024;completed=time.monotonic()
    emit(phase='action',id=action,result=result,wall_seconds=elapsed,core_seconds=time.process_time()-cpu,maxrss_bytes=peak,completed_monotonic=completed)
    if elapsed>10 or peak>6*1024**3:raise RuntimeError('Completed action resource budget')
    if first is None:first=completed
