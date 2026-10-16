import hashlib,json,time,resource
import pyarrow as pa
import numpy as np
import vail as hl
import vail._native as native
result={'native_sha256':hashlib.file_digest(open(native.__file__,'rb'), 'sha256').hexdigest()}
arrays=[pa.array(np.arange(250000,dtype=np.int32)) for _ in range(8)]
source=pa.table({'x':pa.chunked_array(arrays)})
start=time.perf_counter();table=hl.Table.parallelize(source)
result['multibatch_import']={'rows':2000000,'batches':8,'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
assert table.count()==2000000
schema=pa.schema([('x',pa.int32())]);one=pa.record_batch([pa.array([1],type=pa.int32())],schema=schema)
large=pa.record_batch([pa.array(np.arange(4000000,dtype=np.int32))],schema=schema)
reader=pa.RecordBatchReader.from_batches(schema,[one,one,large,large,large,large])
start=time.perf_counter()
try: hl.current_backend().session.scalar_arrow(reader)
except ValueError as error: assert 'exactly one row' in str(error)
else: raise AssertionError('accepted multiple rows')
reader.close()
result['oversized_scalar_import']={'seconds':time.perf_counter()-start,'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
print(json.dumps(result))
