"""Common public-API correctness/throughput harness for aligned-map experiments.

Run each binary in a fresh process with its own PYTHONPATH. No runtime feature
switch selects an implementation: each result records the imported native hash.
"""
import argparse
import hashlib
import json
import math
from pathlib import Path
import platform
import resource
import time

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('--label', required=True)
parser.add_argument('--workload', choices=['vds', 'numeric', 'nested'], required=True)
parser.add_argument('--rows', type=int, default=2048)
parser.add_argument('--cols', type=int, default=32)
parser.add_argument('--partitions', type=int, default=4)
parser.add_argument('--repeats', type=int, default=3)
parser.add_argument('--warmups', type=int, default=1)
parser.add_argument('--vds-path', default='/tmp/vail-vds-materialized/1kg_chr22_5_samples.vds')
args = parser.parse_args()
import vail as hl
import vail._native as native
native_path = Path(native.__file__).resolve()
print(json.dumps(dict(event='environment', label=args.label, workload=args.workload,
    native_path=str(native_path), native_sha256=hashlib.sha256(native_path.read_bytes()).hexdigest(),
    rows=81 if args.workload=='vds' else args.rows, cols=5 if args.workload=='vds' else args.cols, partitions=args.partitions, python=platform.python_version())), flush=True)

def normalize(x):
    if isinstance(x, float) and math.isnan(x): return {'__nan__': True}
    if isinstance(x, hl.Call): return {'alleles': list(x.alleles), 'phased': x.phased}
    if isinstance(x, hl.Locus): return {'contig': x.contig, 'position': x.position, 'reference_genome': x.reference_genome.name}
    if hasattr(x, 'items'): return {k: normalize(v) for k,v in x.items()}
    if isinstance(x, (list, tuple)): return [normalize(v) for v in x]
    if isinstance(x, set): return sorted((normalize(v) for v in x), key=repr)
    return x

if args.workload == 'vds':
    source = hl.vds.read_vds(args.vds_path)
    start=time.perf_counter()
    out=hl.vds.to_dense_mt(source)
    result=out.localize_entries('entries','cols')
    plan_seconds=time.perf_counter()-start
    def action(): return normalize(result.collect())
    def validate(value):
        assert len(value)==81, len(value)
    total_entries=405
else:
    source=hl.utils.range_matrix_table(args.rows,args.cols,n_partitions=args.partitions)
    source=source.annotate_globals(shift=17)
    source=source.annotate_cols(coefficient=source.col_idx % 7)
    source=source.annotate_entries(x=hl.int64(source.row_idx) + hl.int64(source.col_idx))
    source=source.filter_entries((source.row_idx + source.col_idx) % 11 != 0)
    if args.workload == 'nested':
        source=source.annotate_rows(metadata=hl.struct(
            offset=source.row_idx * 3, label=hl.str(source.row_idx),
            **{f'field{i}': hl.struct(name=f'field{i}', weights=hl.array([float(i), float(i+1)])) for i in range(16)}))
        offset=source.metadata.offset
        label=source.metadata.label
    else:
        offset=source.row_idx * 3
        label=hl.str(source.row_idx)
    start=time.perf_counter()
    out=source.annotate_entries(y=source.x + offset + source.coefficient + source.shift,
                               tag=label + ':' + hl.str(source.col_idx))
    plan_seconds=time.perf_counter()-start
    def action():
        return normalize(out.aggregate_entries(hl.struct(
            n=hl.agg.count(), checksum=hl.agg.sum(out.y), tag_length=hl.agg.sum(hl.len(out.tag)))))
    expected=dict(n=0,checksum=0,tag_length=0)
    for r in range(args.rows):
        for c in range(args.cols):
            if (r+c)%11:
                expected['n']+=1
                expected['checksum']+=4*r+c+c%7+17
                expected['tag_length']+=len(str(r))+1+len(str(c))
    def validate(value): assert value==expected, (value,expected)
    total_entries=args.rows*args.cols

print(json.dumps(dict(event='plan', label=args.label, workload=args.workload, seconds=plan_seconds)),flush=True)
for i in range(args.warmups+args.repeats):
    wall=time.perf_counter(); cpu=time.process_time()
    value=action()
    wall=time.perf_counter()-wall; cpu=time.process_time()-cpu
    validate(value)
    digest=hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    maxrss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_bytes=maxrss if platform.system()=='Darwin' else maxrss*1024
    print(json.dumps(dict(event='measurement',label=args.label,workload=args.workload,
        iteration=i-args.warmups,warmup=i<args.warmups,wall_seconds=wall,cpu_seconds=cpu,
        nominal_entries=total_entries,entries_per_second=total_entries/wall,
        peak_rss_bytes=peak_bytes,result_sha256=digest,correctness='pass')),flush=True)
