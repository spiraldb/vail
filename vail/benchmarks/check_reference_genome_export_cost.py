"""Bounded public PAR export and equivalent-registration scaling evidence.

Each case constructs its reference outside the timer, exports all PAR Interval
values through the public property, and re-registers the complete reference using
those intervals. No validation or codec functions are replaced. Timings are
machine-specific review evidence, not unit-test limits.
"""
import argparse
import json
from pathlib import Path
import resource
import sys
import time

import vail as hl


def measure(size, many_sex_contigs=False):
    contigs = [f'c{i}' for i in range(size - 1)] + ['X']
    reference = hl.ReferenceGenome(
        f'export_cost_{size}', contigs, dict.fromkeys(contigs, 4 * size + 1),
        x_contigs=contigs if many_sex_contigs else ['X'],
        par=[('X', 4 * i + 1, 4 * i + 3) for i in range(size)],
    )
    expected = reference.to_wire()
    before = resource.getrusage(resource.RUSAGE_SELF)
    started = time.perf_counter()
    exported = reference.par
    equivalent = hl.ReferenceGenome(
        reference.name, reference.contigs, reference.lengths,
        x_contigs=reference.x_contigs, y_contigs=reference.y_contigs,
        mt_contigs=reference.mt_contigs, par=exported,
    )
    elapsed = time.perf_counter() - started
    after = resource.getrusage(resource.RUSAGE_SELF)
    assert len(exported) == size
    assert exported[0].start.position == 1
    assert exported[-1].end.position == 4 * (size - 1) + 3
    assert equivalent.to_wire() == expected
    unit = 1 if sys.platform == 'darwin' else 1024
    return {'contigs': size, 'classified_contigs': len(reference.x_contigs), 'par_intervals': size, 'exported_intervals': len(exported),
            'wall_s': elapsed, 'cpu_s': after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime,
            'incremental_peak_rss_mib': max(0, after.ru_maxrss - before.ru_maxrss) * unit / 1024**2}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--many-sex-contigs', action='store_true')
    args = parser.parse_args()
    results = []
    for size in ([64, 256] if args.many_sex_contigs else [64, 256, 1024]):
        result = measure(size, args.many_sex_contigs)
        results.append(result)
        print(json.dumps(result), flush=True)
    if args.output:
        args.output.write_text(json.dumps({'python': sys.version, 'results': results}, indent=2) + '\n')


if __name__ == '__main__':
    main()
