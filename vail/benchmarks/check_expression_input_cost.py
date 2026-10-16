"""Bounded public input construction/action costs, isolated per workload."""
import argparse
import json
import resource
import subprocess
import sys
import time


def worker(kind, size):
    import vail as hl
    hl.eval(1)
    if kind == 'mixed':
        rows = [{'x': i, 'y': hl.int32(str(i))} for i in range(size)]
    else:
        value = hl.int64(1)
        for _ in range(size):
            value = value + value
        rows = hl.array([hl.struct(x=value)])
    started = time.perf_counter()
    table = hl.Table.parallelize(rows)
    constructed = time.perf_counter() - started
    started = time.perf_counter()
    if kind == 'mixed':
        assert table.aggregate(hl.agg.sum(table.y)) == size * (size-1) // 2
    else:
        assert table.collect() == [hl.Struct(x=2**size)]
    print(json.dumps({'workload': kind, 'size': size, 'construction_seconds': constructed,
                      'action_seconds': time.perf_counter()-started,
                      'max_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}), flush=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', choices=['mixed', 'shared'])
    parser.add_argument('--size', type=int)
    args = parser.parse_args()
    if args.worker:
        worker(args.worker, args.size)
    else:
        for kind, sizes in [('mixed', [128, 512, 2048]), ('shared', [4, 8, 12, 16])]:
            for size in sizes:
                try:
                    result = subprocess.run([sys.executable, __file__, '--worker', kind, '--size', str(size)],
                                            timeout=15, check=True, text=True, capture_output=True)
                    print(result.stdout.strip(), flush=True)
                except subprocess.TimeoutExpired:
                    print(json.dumps({'workload':kind, 'size':size, 'timeout_seconds':15}), flush=True)
