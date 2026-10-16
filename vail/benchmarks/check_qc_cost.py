"""Capped public Matrix QC and scalar HWE resource workloads."""
import argparse
import importlib.util
import json
import resource
import subprocess
import sys
import time


def worker(mode, samples, alleles, baseline):
    import vail as hl
    qc = hl.variant_qc
    if baseline:
        for name in ['matrix_list_aggregates', 'qc']:
            spec = importlib.util.spec_from_file_location('vail.' + name, baseline + '/' + name + '.py')
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            sys.modules['vail.' + name] = module
            if name == 'qc':
                qc = module.variant_qc
    hl.eval(1)
    if mode == 'qc':
        mt = hl.range_matrix_table(1, samples)
        mt = mt.annotate_rows(alleles=['A'] + ['C' + str(i) for i in range(alleles - 1)])
        mt = mt.annotate_entries(GT=hl.call(0, 1))
        start = time.perf_counter()
        result = qc(mt)
    else:
        start = time.perf_counter()
        result = hl.hardy_weinberg_test(samples, 2 * samples, samples)
    construction = time.perf_counter() - start
    start = time.perf_counter()
    if mode == 'qc':
        value = result.rows().collect()[0].variant_qc
        assert value.n_het == samples and value.n_non_ref == samples
        assert value.AC == [samples, samples] + [0] * (alleles - 2)
    else:
        value = hl.eval(result)
        assert .49 < value.het_freq_hwe < .51 and 0 <= value.p_value <= 1
    print(json.dumps(dict(mode=mode, samples=samples, alleles=alleles,
                         construction_seconds=construction, action_seconds=time.perf_counter()-start,
                         max_rss_bytes=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)), flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--mode', choices=['qc', 'hwe'])
    p.add_argument('--samples', type=int)
    p.add_argument('--alleles', type=int, default=2)
    p.add_argument('--baseline')
    args = p.parse_args()
    if args.mode:
        worker(args.mode, args.samples, args.alleles, args.baseline)
    else:
        cases = [('qc', n, 2) for n in [128, 1024, 8192]]
        cases += [('qc', 1024, a) for a in [32, 256]]
        cases += [('hwe', n, 2) for n in [100, 10000, 1000000]]
        for mode, samples, alleles in cases:
            command = [sys.executable, __file__, '--mode', mode, '--samples', str(samples), '--alleles', str(alleles)]
            if args.baseline:
                command += ['--baseline', args.baseline]
            try:
                result = subprocess.run(command, timeout=20, check=True, text=True, capture_output=True)
                print(result.stdout.strip(), flush=True)
            except subprocess.TimeoutExpired:
                print(json.dumps(dict(mode=mode, samples=samples, alleles=alleles, timeout_seconds=20)), flush=True)
