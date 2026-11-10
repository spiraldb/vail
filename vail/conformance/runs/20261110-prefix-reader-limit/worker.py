from pathlib import Path
import sys,json,os,time,resource,hashlib
import vail as hl,vail._native as native
mode,case,out=sys.argv[1:]
expected = '4031dcb7146f593be5309dcedf0972231ebc021087a7a40dff0bc82bbb7132e0' if mode == 'baseline' else json.loads((Path(__file__).parent/'ready.json').read_text())['identity']['sha256']
assert hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest() == expected
fixture='/private/tmp/vail-hail-four-20261109/inputs/native.mt'
def query():
 if case.startswith('explode_'):
  ht=hl.utils.range_table(400,n_partitions=4)
  ht=ht.annotate(items=hl.range(100)).explode('items')
  if case=='explode_prefix':return ht.head(8192).to_arrow().num_rows
  if case=='explode_prefix_all':return ht.head(40000).to_arrow().num_rows
  return ht.to_arrow().num_rows
 mt=hl.read_matrix_table(fixture)
 if case=='entries_show':
  text=[];mt.entries().show(handler=text.append);return ''.join(text)
 if case=='entries_prefix':return mt.entries().head(8192).to_arrow().num_rows
 if case=='take_entry':return repr(mt.GT.take(100))
 if case=='entries_count':return mt.entries()._force_count()
 if case=='entries_count_no_key':return mt.key_rows_by().key_cols_by().entries()._force_count()
 if case=='filter_entries':return mt.filter_entries((mt.GQ>8)&(mt.DP>2))._force_count_rows()
 if case=='split_multi_hts':return hl.split_multi_hts(mt)._force_count_rows()
 if case=='read_rows':return mt._force_count_rows()
 if case=='read_prefix_all':return mt.rows().head(1000).to_arrow().num_rows
 raise ValueError(case)
samples=[];results=[]
for i in range(6):
 cpu=time.process_time();wall=time.perf_counter();result=query();wall=time.perf_counter()-wall;cpu=time.process_time()-cpu
 results.append(hashlib.sha256(str(result).encode()).hexdigest())
 if i>=2:samples.append({'wall':wall,'cpu':cpu,'maxrss':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss})
assert len(set(results))==1
Path(out).write_text(json.dumps({'mode':mode,'case':case,'fixture':fixture,'result_sha256':results[0],'samples':samples,'native_sha256':hashlib.sha256(Path(native.__file__).read_bytes()).hexdigest(),'native':native.__file__,'allocator':native._benchmark_allocator,'python':sys.version,'environment':{k:v for k,v in os.environ.items() if k in ['RAYON_NUM_THREADS','OMP_NUM_THREADS','OPENBLAS_NUM_THREADS','MKL_NUM_THREADS','PYTHONMALLOC','ARROW_DEFAULT_MEMORY_POOL']},'scope':'Vail native small published 1000x2535 fixture, 4 partitions, Tokio32/Rayon4, same laptop; not Hail or full-suite comparison'},indent=2)+'\n')
