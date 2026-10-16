"""Public display work over large cells, timed after source construction."""
import argparse
import json
import resource
import subprocess
import sys
import time


def worker(kind, size):
    import numpy as np
    import pyarrow as pa
    import vail as hl
    hl.eval(1)
    if kind == 'wide':
        values = None
        table = hl.Table.parallelize(pa.table({f'f{i}': pa.array(range(16), type=pa.int32()) for i in range(size)}))
    elif kind == 'string':
        values = pa.array(['α\\\n' * size])
    else:
        items = pa.array(np.arange(size, dtype=np.int32))
        values = pa.ListArray.from_arrays(pa.array([0, size], type=pa.int32()), items)
        if kind == 'nested':
            values = pa.ListArray.from_arrays(pa.array([0, 1], type=pa.int32()), values)
    if kind != 'wide':
        table = hl.Table.parallelize(pa.table({'value': values}))
    start = time.perf_counter()
    output = []
    table.show(n=0 if kind == 'sentinel' else 1, width=40, truncate=16, handler=output.append)
    if kind == 'wide':
        assert len(output[0]) < size * 128
    elif kind == 'sentinel':
        assert 'showing top 0 rows' in output[0]
    else:
        assert len(output[0]) < 256 and '...' in output[0]
    print(json.dumps({'workload': kind, 'size': size, 'seconds': time.perf_counter()-start,
                      'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', choices=['array','nested','string','sentinel','wide'])
    parser.add_argument('--size', type=int)
    args=parser.parse_args()
    if args.worker:
        worker(args.worker,args.size)
    else:
        for kind in ['array','nested','string','sentinel','wide']:
            for size in ([64, 256, 1024] if kind == 'wide' else [4096, 65536, 1048576]):
                result=subprocess.run([sys.executable,__file__,'--worker',kind,'--size',str(size)],
                                      timeout=15,check=True,text=True,capture_output=True)
                print(result.stdout.strip(),flush=True)
