"""Bounded fresh-process public split_multi_hts workloads."""
import argparse
import importlib.util
import json
import resource
import subprocess
import sys
import time


def worker(mode, size, baseline):
    import vail as hl
    split = hl.split_multi_hts
    if baseline:
        spec = importlib.util.spec_from_file_location('vail._variant_baseline', baseline)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        split = module.split_multi_hts
    hl.eval(1)
    mt = hl.range_matrix_table(1, 1)
    alleles = ['é' * size + 'A', 'é' * size + 'T'] if mode == 'long' else ['A'] + ['C' + str(i) for i in range(size - 1)]
    mt = mt.annotate_rows(locus=hl.locus('1', 100), alleles=alleles).key_rows_by('locus', 'alleles')
    if mode == 'pl':
        mt = mt.annotate_entries(GT=hl.call(0, 1), PL=list(range(size * (size + 1) // 2)), AD=[1] * size)
    start = time.perf_counter()
    result = split(mt)
    construction = time.perf_counter() - start
    start = time.perf_counter()
    rows = result.entries().collect() if mode == 'pl' else result.rows().collect()
    if mode == 'long':
        assert rows[0].locus.position == size + 100
        assert rows[0].alleles == ['A', 'T']
    else:
        assert len(rows) == size - 1
        assert all(row.AD == [size - 1, 1] for row in rows)
        for row in rows:
            a = row.a_index
            assert row.PL == [0, a * (a + 1) // 2, a * (a + 1) // 2 + a]
    print(json.dumps(dict(mode=mode, size=size, construction_seconds=construction,
                         action_seconds=time.perf_counter()-start,
                         max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--mode', choices=['long', 'pl'])
    p.add_argument('--size', type=int)
    p.add_argument('--baseline')
    args = p.parse_args()
    if args.mode:
        worker(args.mode, args.size, args.baseline)
    else:
        for mode, sizes in [('long', [128, 1024, 8192, 65536]), ('pl', [8, 32, 128, 256])]:
            for size in sizes:
                command = [sys.executable, __file__, '--mode', mode, '--size', str(size)]
                if args.baseline:
                    command += ['--baseline', args.baseline]
                try:
                    result = subprocess.run(command, timeout=20, check=True, capture_output=True, text=True)
                    print(result.stdout.strip(), flush=True)
                except subprocess.TimeoutExpired:
                    print(json.dumps(dict(mode=mode, size=size, timeout_seconds=20)), flush=True)
