"""Compare native legacy scans with Hail on the same independently written files.

Run generate/hail with a Hail Python environment; run vail with the built Vail
extension. Times include opening the dataset and executing the action. JVM and
interpreter startup are excluded. Run engines sequentially on an idle machine.
"""
import argparse
import json
import statistics
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('engine', choices=['generate', 'hail', 'vail'])
    parser.add_argument('path')
    parser.add_argument('--rows', type=int, default=1_000_000)
    parser.add_argument('--partitions', type=int, default=16)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--warmup', type=int, default=1)
    args = parser.parse_args()
    if args.engine in ('generate', 'hail'):
        import hail as hl
        hl.init(master='local[*]', quiet=True, log=args.path + '.' + args.engine + '.log')
    else:
        import vail as hl
    if args.engine == 'generate':
        table = hl.utils.range_table(args.rows, n_partitions=args.partitions)
        table = table.annotate(x=hl.int64(table.idx) * 3, s=hl.str(table.idx),
                               a=hl.range(8).map(lambda i: table.idx + i))
        table.write(args.path)  # The caller chooses a new destination.
        hl.stop()
        return
    low, high = args.rows * 9 // 10, args.rows * 95 // 100
    expected = dict(count=args.rows, sum=3 * args.rows * (args.rows - 1) // 2,
                    filter=3 * (low + high - 1) * (high - low) // 2,
                    array=4 * args.rows * (args.rows - 1) + 28 * args.rows)
    results = {}
    for name in expected:
        times = []
        for iteration in range(args.warmup + args.runs):
            start = time.perf_counter()
            table = (hl.read_table(args.path) if args.engine == 'hail'
                     else hl.Table.read_legacy(args.path))
            if name == 'count':
                value = table._force_count()
            elif name == 'sum':
                value = table.aggregate(hl.agg.sum(table.x))
            elif name == 'filter':
                table = table.filter((table.idx >= low) & (table.idx < high))
                value = table.aggregate(hl.agg.sum(table.x))
            else:
                value = table.aggregate(hl.agg.sum(hl.sum(table.a)))
            elapsed = time.perf_counter() - start
            assert value == expected[name], (name, value, expected[name])
            if iteration >= args.warmup:
                times.append(elapsed)
        results[name] = dict(seconds=times, median=statistics.median(times), value=value)
    print(json.dumps(dict(engine=args.engine, rows=args.rows, partitions=args.partitions,
                          results=results)), flush=True)
    if args.engine == 'hail':
        hl.stop()


if __name__ == '__main__':
    main()
