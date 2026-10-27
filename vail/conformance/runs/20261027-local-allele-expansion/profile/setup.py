"""One controlled materialization, outside all profile/action measurements."""
import json,time
from pathlib import Path
from common import EXPECTED,digest
from preflight import verify
from reference import source
from query import construct
h=verify();reference=source();path=Path(EXPECTED['generated_fixture']);lock_path=Path(EXPECTED['generated_lock'])
if path.exists() or lock_path.exists():raise RuntimeError('Fresh owned fixture/lock required; no reuse/retry')
print(json.dumps(dict(phase='action_start',id=0,monotonic=time.monotonic())),flush=True)
entry_type=h.tstruct(LA=h.tarray(h.tint32),LAD=h.tarray(h.tint32))
record_type=h.tstruct(n_alleles=h.tint32,entries=h.tarray(entry_type),source_contig=h.tstr,source_position=h.tint32,source_alleles=h.tarray(h.tstr))
records=h.literal(reference['records'],h.tarray(record_type))
m=h.utils.range_matrix_table(331776,5,n_partitions=32)
m=m.annotate_rows(__h085_source=records[m.row_idx%81])
m=m.filter_entries(h.is_defined(m['__h085_source']['entries'][m.col_idx]))
e=m['__h085_source']['entries'][m.col_idx]
m=m.select_entries(LA=e.LA,LAD=e.LAD)
m=m.annotate_rows(n_alleles=m['__h085_source'].n_alleles,source_contig=m['__h085_source'].source_contig,source_position=m['__h085_source'].source_position,source_alleles=m['__h085_source'].source_alleles)
m=m.key_rows_by(replica_id=m.row_idx//81,original_row_index=m.row_idx%81)
m=m.select_rows('n_alleles','source_contig','source_position','source_alleles')
m=m.key_cols_by(s=h.literal(reference['samples'])[m.col_idx]).select_cols()
m.write(str(path))
m=h.read_matrix_table(str(path))
if m.entry.dtype!=entry_type or m.row.dtype['n_alleles']!=h.tint32:raise RuntimeError('Persisted input schema differs')
# Full public one-replica outputs, compared to independent original Python arrays.
t=m.filter_rows(m.replica_id==0).entries()
t=t.select(expanded=h.vds.local_to_global(t.LAD,t.LA,t.n_alleles,0,'R'))
actual={}
for row in t.collect():
    key=(row.original_row_index,row.s)
    if key in actual:raise RuntimeError('Duplicate original key')
    actual[key]=row.expanded
expected={(i,s):reference['source_outputs'][i][j] for i in range(81) for j,s in enumerate(reference['samples']) if reference['source_outputs'][i][j] is not None}
if actual!=expected:raise RuntimeError('Complete one-replica outputs differ')
# Complete independent aggregate/distribution verification on persisted data.
observed=construct(h,m)()
if observed!=reference['expected']:raise RuntimeError(f'Generated aggregate differs:{observed}')
counts=dict(m.aggregate_entries(h.struct(present=h.agg.count(),missing_inputs=h.agg.count_where(h.is_missing(m.LA)|h.is_missing(m.LAD)|h.is_missing(m.n_alleles)),local2=h.agg.count_where(h.len(m.LA)==2),local3=h.agg.count_where(h.len(m.LA)==3),global2=h.agg.count_where(m.n_alleles==2),global3=h.agg.count_where(m.n_alleles==3))))
if counts!=dict(present=118*4096,missing_inputs=0,local2=117*4096,local3=4096,global2=116*4096,global3=2*4096):raise RuntimeError('Persisted source distributions differ')
if m.count_rows()!=331776 or m.count_cols()!=5:raise RuntimeError('Persisted dimensions differ')
files={str(p.relative_to(path)):dict(bytes=p.stat().st_size,sha256=digest(p)) for p in sorted(path.rglob('*')) if p.is_file()}
if not files or sum(v['bytes'] for v in files.values())>10*1024**3:raise RuntimeError('10GiB output cap')
reference.update(generated_files=files,distribution=counts,generated_schema=dict(row=str(m.row.dtype),entry=str(m.entry.dtype),col=str(m.col.dtype)),replications=4096,original_sha256=EXPECTED['source_sha256'])
lock_path.write_text(json.dumps(reference,indent=2)+'\n')
print(json.dumps(dict(phase='action',id=0,completed_monotonic=time.monotonic(),fixture_bytes=sum(v['bytes'] for v in files.values()),lock_sha256=digest(lock_path),complete_one_replica=reference['source_outputs'],distribution=counts)),flush=True)
