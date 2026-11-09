from pathlib import Path
import hashlib,json,os,resource,statistics,sys,time
import vail as hl
import vail._native as native
case,storage,fixture,output=sys.argv[1:]
assert case in ('grouped','read') and storage in ('legacy','native')
assert native._benchmark_allocator=='mimalloc'
reader=hl.MatrixTable.read_legacy if storage=='legacy' else hl.read_matrix_table

def action():
    mt=reader(fixture)
    if case=='read':
        return mt._force_count_rows()
    table=mt.localize_entries('e','c')
    return table.group_by(*table.key).aggregate(value=hl.agg.collect(table.row_value))._force_count()

samples=[]
for i in range(7):
    cpu=time.process_time();wall=time.perf_counter();value=action()
    wall=time.perf_counter()-wall;cpu=time.process_time()-cpu
    if i>=2:
        samples.append({'wall_s':wall,'cpu_s':cpu,'output_count':value})
record={'case':case,'storage':storage,'fixture':fixture,'fixture_scope':'existing public profile25 subset,1000rows x2535samples,4partitions; NOT full250000x1000gnomad fixture','package':hl.__file__,'native':native.__file__,'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),'legacy_frontend_sha256':hashlib.sha256((Path(hl.__file__).parent/'legacy.py').read_bytes()).hexdigest(),'allocator':native._benchmark_allocator,'environment':{k:os.environ.get(k) for k in ('RAYON_NUM_THREADS','VAIL_EXECUTION_CONCURRENCY','OPENBLAS_NUM_THREADS','OMP_NUM_THREADS','MKL_NUM_THREADS','VECLIB_MAXIMUM_THREADS','PYTHONMALLOC','ARROW_DEFAULT_MEMORY_POOL')},'samples':samples,'wall_median':statistics.median(x['wall_s'] for x in samples),'cpu_median':statistics.median(x['cpu_s'] for x in samples),'process_peak_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,'timing_scope':'read/query construction/action; import excluded;2warm5timed','output_validation':'counterfactual and compatibility tests separate; timed output counts retained'}
Path(output).write_text(json.dumps(record,indent=2)+'\n')
