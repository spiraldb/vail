"""Measure bounded public reference/locus queries at 64 and 4096 entries.

Run in Vail's installed Python environment. Setup is outside the measured region;
queries exercise shared/equivalently registered references, contig rank, chromosome
classification, and half-open PAR membership. No functions are replaced or spied
on. Timings are evidence for the module review, not machine-independent test limits.
"""
import argparse
import json
from pathlib import Path
import resource
import sys
import time

import vail as hl


def workload(kind, size, queries):
    names = [f'c{i}' for i in range(size)]
    name = f'reference_cost_{kind}_{size}'
    if kind == 'par':
        reference = hl.ReferenceGenome(name, ['X'], {'X': 4 * size + 1}, x_contigs=['X'],
                                       par=[('X', 4 * i + 1, 4 * i + 3) for i in range(size)])
        inside = hl.Locus('X', 4 * (size - 1) + 2, reference)
        outside = hl.Locus('X', 4 * (size - 1) + 3, reference)
        def run():
            return sum(inside.in_par and not outside.in_par for _ in range(queries))
        expected = queries
    elif kind == 'classification':
        reference = hl.ReferenceGenome(name, names, dict.fromkeys(names, 100), x_contigs=names)
        locus = hl.Locus(names[-1], 1, reference)
        def run():
            return sum(locus.is_x and not reference.is_autosomal(locus.contig) for _ in range(queries))
        expected = queries
    else:
        reference = hl.ReferenceGenome(name, names, dict.fromkeys(names, 100))
        # Construct a distinct equivalent registration as well as the shared instance.
        equivalent = hl.ReferenceGenome(name, names, dict.fromkeys(names, 100))
        first = hl.Locus(names[-1], 1, reference)
        shared = hl.Locus(names[-1], 2, reference)
        registered = hl.Locus(names[-1], 2, equivalent)
        def run():
            return sum(first.contig_idx for _ in range(queries) if first < shared and first < registered)
        expected = queries * (size - 1)
    before = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    checksum = run()
    elapsed = time.perf_counter() - started
    after = resource.getrusage(resource.RUSAGE_SELF)
    assert checksum == expected
    unit = 1 if sys.platform == 'darwin' else 1024
    return {'workload': kind, 'size': size, 'queries': queries,
            'wall_s': elapsed, 'cpu_s': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
            'incremental_peak_rss_mib': max(0, after.ru_maxrss - before.ru_maxrss) * unit / 1024**2,
            'checksum': checksum}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    results = []
    for kind in ['comparison', 'classification', 'par']:
        for size in [64, 4096]:
            result = workload(kind, size, 10_000)
            results.append(result)
            print(json.dumps(result), flush=True)
    if args.output:
        args.output.write_text(json.dumps({'python': sys.version, 'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()
