"""Capped public random construction/action and shared-parameter workloads."""
import argparse
import importlib.util
import json
import math
import resource
import subprocess
import sys
import time


def worker(mode, size, baseline):
    import vail as hl
    hl.eval(1)
    if baseline:
        spec = importlib.util.spec_from_file_location('vail.randomness', baseline)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules['vail.randomness'] = module
    table = hl.range_table(64 if mode == 'dag' else 2048)
    if mode == 'dag':
        value = hl.rand_unif(seed=7)
        for _ in range(size):
            value = (value + value) * .5
    start = time.perf_counter()
    if mode == 'dag':
        result = table.annotate(value=value)
    elif mode == 'callback':
        result = hl.range(size).map(lambda item: hl.struct(item=item, random=hl.rand_unif(seed=7)))
    elif mode in ['cat_shared', 'cat_rows']:
        weights = hl.literal([1.] * size)
        if mode == 'cat_rows':
            weights = hl.if_else(table.idx % 2 == 0, weights, hl.literal([2.] * size))
        result = table.annotate(value=hl.rand_cat(weights, seed=7))
    elif mode == 'shuffle':
        result = hl.shuffle(hl.range(size), seed=7)
    elif mode == 'ndarray':
        result = hl.rand_norm(size=(size, size), seed=7)
    elif mode == 'dirichlet':
        result = hl.rand_dirichlet([.01] * size, seed=7)
    elif mode == 'hyper_boundary':
        result = hl.rand_hyper(size, size, size, seed=7)
    else:
        result = hl.rand_multi_hyper([0, size, size], size, seed=7)
    construction = time.perf_counter() - start
    print(json.dumps(dict(phase='constructed', mode=mode, size=size, construction_seconds=construction)), flush=True)
    start = time.perf_counter()
    if mode == 'dag':
        values = result.value.collect()
        assert len(values) == 64 and all(0 <= value <= 1 for value in values)
    elif mode == 'callback':
        values = hl.eval(result)
        assert [value.item for value in values] == list(range(size))
        assert all(0 <= value.random <= 1 for value in values)
    elif mode in ['cat_shared', 'cat_rows']:
        values = result.value.collect()
        assert len(values) == 2048 and all(0 <= value < size for value in values)
    elif mode == 'shuffle':
        values = hl.eval(result)
        assert sorted(values) == list(range(size))
    elif mode == 'ndarray':
        values = hl.eval(result)
        assert values.shape == (size, size) and all(math.isfinite(value) for value in values.ravel())
    elif mode == 'dirichlet':
        values = hl.eval(result)
        assert all(math.isfinite(value) and value >= 0 for value in values)
        assert abs(sum(values) - 1) < 1e-10
    elif mode == 'hyper_boundary':
        assert hl.eval(result) == size
    else:
        values = hl.eval(result)
        assert sum(values) == size and values[0] == 0
    print(json.dumps(dict(phase='completed', mode=mode, size=size, construction_seconds=construction,
                         action_seconds=time.perf_counter()-start,
                         max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--mode')
    p.add_argument('--size', type=int)
    p.add_argument('--baseline')
    args = p.parse_args()
    if args.mode:
        worker(args.mode, args.size, args.baseline)
    else:
        cases = [('dag', n) for n in [8, 12, 16]] + [('callback', n) for n in [64, 512, 2048]]
        if not args.baseline:
            cases += [('callback', 8192)]
            cases += [(mode, n) for mode in ['cat_shared', 'cat_rows'] for n in [16, 256, 4096]]
            cases += [('shuffle', n) for n in [4096, 65536, 262144]]
            cases += [('ndarray', n) for n in [32, 256, 512]]
            cases += [('dirichlet', n) for n in [16, 256, 4096]]
            cases += [('hyper_boundary', n) for n in [1000, 2000000000]]
            cases += [('multi_hyper', n) for n in [100, 10000]]
        for mode, size in cases:
            command = [sys.executable, __file__, '--mode', mode, '--size', str(size)]
            if args.baseline:
                command += ['--baseline', args.baseline]
            try:
                result = subprocess.run(command, check=True, capture_output=True, text=True, timeout=20)
                print(result.stdout.strip(), flush=True)
            except subprocess.TimeoutExpired as exc:
                print((exc.stdout or b'').decode() if isinstance(exc.stdout, bytes) else (exc.stdout or ''), end='', flush=True)
                print(json.dumps(dict(mode=mode, size=size, timeout_seconds=20)), flush=True)
