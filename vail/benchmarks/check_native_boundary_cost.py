"""Bounded blackbox resource check for expression construction and Arrow input.

Run with the installed Vail environment's Python. Each workload runs in a fresh
process with a 30 s timeout. Acceptance: under 512 MiB incremental peak RSS and
under 20 s measured wall time. These generous regression bounds detect accidental
expression-tree expansion or per-row bridge overhead; they do not prove which
transport/representation is used. Reports include actual wall/CPU/RSS, workload
size, and Python version. No library functions are replaced or instrumented.
"""
import argparse
import json
import resource
import subprocess
import sys
import time


def worker(workload):
    import vail as hl

    # Initialize the runtime before measuring the boundary workload.
    assert hl.eval(hl.int32(1)) == 1
    before_rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    started_wall, started_cpu = time.perf_counter(), time.process_time()
    if workload.startswith('expression-'):
        depth = int(workload.split('-')[1])
        values = []
        for _ in range(100):
            value = hl.int64(1)
            for _ in range(depth):
                value = value + value
            values.append(value)
        # Construction only: execution of the expanded expression is a separate
        # property and is covered with bounded depths in the semantic tests.
        assert all(value.dtype == hl.tint64 for value in values)
        size = {'depth': depth, 'expressions': len(values)}
    elif workload.startswith('wide-'):
        from vail.arrow import rows_to_arrow, decode_rows
        width = int(workload.split('-')[1])
        count = 1_000
        nested = hl.tstruct(**{f'f{i}': hl.tint32 for i in range(width)})
        schema = hl.tstruct(x=nested)
        rows = [{'x': {f'f{i}': row + i for i in range(width)}} for row in range(count)]
        actual = decode_rows(rows_to_arrow(rows, schema), schema)
        assert len(actual) == count
        assert all(dict(result.x) == expected['x'] for result, expected in zip(actual, rows))
        size = {'rows': count, 'nested_fields': width}
    elif workload.startswith('locus-'):
        from vail.arrow import rows_to_arrow, decode_rows
        from vail.reference_genome import ReferenceGenome, Locus
        contigs = int(workload.split('-')[1])
        count = 1_000
        names = [f'c{i}' for i in range(contigs)]
        reference = ReferenceGenome('boundary-cost', names, {name: 100 for name in names})
        schema = hl.tstruct(x=hl.tlocus(reference))
        rows = [{'x': Locus(names[i % contigs], i % 100 + 1, reference)} for i in range(count)]
        actual = decode_rows(rows_to_arrow(rows, schema), schema)
        assert [(row.x.contig, row.x.position) for row in actual] == [
            (row['x'].contig, row['x'].position) for row in rows]
        size = {'rows': count, 'contigs': contigs}
    else:
        count = 10_000
        rows = [{'a': [index, index + 1]} for index in range(count)]
        table = hl.Table.parallelize(rows, schema=hl.tstruct(a=hl.tarray(hl.tint32)))
        table = table.annotate(total=hl.sum(table.a))
        assert table.aggregate(hl.agg.sum(table.total)) == count * count
        size = {'rows': count, 'array_width': 2}
    wall, cpu = time.perf_counter() - started_wall, time.process_time() - started_cpu
    # macOS reports bytes, Linux reports KiB.
    rss_unit = 1 if sys.platform == 'darwin' else 1024
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    delta_mib = max(0, rss - before_rss) * rss_unit / 1024**2
    passed = wall < 20 and delta_mib < 512
    print(json.dumps({'workload': workload, 'size': size, 'python': sys.version,
                      'wall_s': wall, 'cpu_s': cpu, 'peak_rss_mib': rss * rss_unit / 1024**2,
                      'incremental_peak_rss_mib': delta_mib,
                      'limits': {'wall_s': 20, 'incremental_peak_rss_mib': 512},
                      'passed': passed}), flush=True)
    return 0 if passed else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', choices=['expression-32', 'expression-64', 'arrow', 'wide-64', 'wide-256', 'locus-64', 'locus-4096'])
    args = parser.parse_args()
    if args.worker:
        return worker(args.worker)
    for workload in ['expression-32', 'expression-64', 'arrow', 'wide-64', 'wide-256', 'locus-64', 'locus-4096']:
        subprocess.run([sys.executable, __file__, '--worker', workload], timeout=30, check=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
