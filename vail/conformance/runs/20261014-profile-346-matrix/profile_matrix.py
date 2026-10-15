"""Stage timings and Python call profile for the upstream 346-row matrix."""
import cProfile
import json
import os
from pathlib import Path
import time
import vail as hl
from upstream_paths import resource_path

ROOT=Path(__file__).resolve().parent
RESOURCES=ROOT.parents[3]/'hail/hail/hail/test/resources'
OUTPUT=ROOT/os.environ.get('VAIL_PROFILE_VARIANT','baseline')
OUTPUT.mkdir(exist_ok=True)
profiler=cProfile.Profile();profiler.enable()
def stage(name,fn):
    print(json.dumps(dict(event='start',stage=name,pid=os.getpid())),flush=True)
    before=time.perf_counter()
    value=fn()
    elapsed=time.perf_counter()-before
    print(json.dumps(dict(event='end',stage=name,seconds=elapsed,result=value if isinstance(value,(int,float,str,tuple)) else None)),flush=True)
    profiler.dump_stats(OUTPUT/'python.prof')
    return value

print(json.dumps(dict(pid=os.getpid(),native=str(Path(hl.__file__).parent/'_native.abi3.so'))),flush=True)
path=stage('fixture_snapshot',lambda:resource_path(RESOURCES,'sample.vcf-20-partitions.mt',Path('/private/tmp/vail-profile-346-fixtures')))
mt=stage('read_matrix',lambda:hl.read_matrix_table(path))
stage('unfiltered_partitions',mt.n_partitions)
stage('unfiltered_rows',mt.count_rows)
stage('unfiltered_cols',mt.count_cols)
predicate=stage('predicate',lambda:mt.locus.position < 17434581)
filtered=stage('filter_construct',lambda:mt.filter_rows(predicate))
stage('filtered_partitions',filtered.n_partitions)
if os.environ.get('VAIL_PROFILE_STOP_AFTER_PARTITIONS'):
    profiler.disable();profiler.dump_stats(OUTPUT/'python.prof');raise SystemExit(0)
stage('filtered_count_rows',filtered.count_rows)
stage('filtered_count_cols',filtered.count_cols)
stage('filtered_rows_view_count',lambda:filtered.rows().count())
profiler.disable();profiler.dump_stats(OUTPUT/'python.prof')
