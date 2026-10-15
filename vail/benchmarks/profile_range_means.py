"""Profile the exact upstream 10M-row range-means workload on macOS.

CPU time includes live descendant processes (notably Hail's JVM). Native
profiles and 10 Hz telemetry are captured separately for each implementation.
"""
from __future__ import annotations
import argparse
import csv
import hashlib
import importlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import threading
import time

from profile_annotate import counters


def worker(args):
    import psutil
    hl = importlib.import_module('hail' if args.variant == 'hail' else 'vail')
    args.output.mkdir(parents=True, exist_ok=True)
    if args.variant == 'hail':
        hl.init(master='local[14]', spark_conf={'spark.driver.memory': '16g',
            'spark.executor.memory': '16g', 'spark.ui.enabled': 'false'},
            quiet=True, log=str(args.output / 'hail.log'))
    read, tick_ns = counters()
    root = psutil.Process()
    def snapshot():
        values = {}
        for process in [root, *root.children(recursive=True)]:
            try:
                value = read(process.pid)
                values[(process.pid, process.create_time())] = value
            except (OSError, psutil.Error):
                pass
        return values
    done = threading.Event()
    def monitor():
        previous = None
        with (args.output / 'metrics.csv').open('w', newline='') as file:
            fields = ['monotonic_ns', 'wall_ns', 'interval_seconds', 'cpu_percent',
                'rss_bytes', 'instructions', 'cycles', 'disk_read_bytes', 'disk_write_bytes']
            writer = csv.DictWriter(file, fieldnames=fields)
            writer.writeheader()
            deadline = time.perf_counter()
            while not done.is_set():
                now = time.perf_counter_ns()
                sample = snapshot()
                total = lambda key: sum(v[key] for v in sample.values())
                cpu = sum(v['user_time'] + v['system_time'] for v in sample.values()) * tick_ns / 1e9
                interval = (now - previous[0]) / 1e9 if previous else 0
                writer.writerow(dict(zip(fields, [now, time.time_ns(), interval,
                    (cpu - previous[1]) / interval * 100 if previous else 0,
                    total('resident_size'), total('instructions'), total('cycles'),
                    total('diskio_bytesread'), total('diskio_byteswritten')])))
                file.flush()
                previous = now, cpu
                deadline += .1
                done.wait(max(0, deadline - time.perf_counter()))
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    events = []
    try:
        for phase, count in [('warmup', args.warmups), ('measured', args.iterations)]:
            for index in range(count):
                before = snapshot()
                start_wall = time.time_ns()
                start = time.perf_counter_ns()
                t = hl.utils.range_table(args.rows, 16)
                values = hl.range(0, t.idx % 1111)
                if args.variant == 'composed':
                    defined = values.filter(lambda x: ~hl.is_missing(x))
                    expression = defined.map(lambda x: hl.float64(x))._reduce('sum') / hl.float64(hl.len(defined))
                elif args.variant == 'array_avg':
                    from vail.collections import _function
                    expression = _function('array_avg', [values], hl.tfloat64)
                else:
                    expression = hl.mean(values)
                t = t.annotate(m=expression)
                constructed = time.perf_counter_ns()
                result = t._force_count()
                end = time.perf_counter_ns()
                end_wall = time.time_ns()
                after = snapshot()
                assert result == args.rows
                cpu = 0
                for identity, value in after.items():
                    old = before.get(identity, {})
                    cpu += sum(value[k] - old.get(k, 0) for k in ('user_time', 'system_time')) * tick_ns / 1e9
                seconds = (end - start) / 1e9
                event = dict(phase=phase, index=index, start_monotonic_ns=start,
                    end_monotonic_ns=end, start_wall_ns=start_wall, end_wall_ns=end_wall,
                    seconds=seconds, cpu_seconds=cpu, average_cores=cpu / seconds,
                    construction_seconds=(constructed-start)/1e9, action_seconds=(end-constructed)/1e9)
                events.append(event)
                (args.output / 'iterations.json').write_text(json.dumps(events, indent=2)+'\n')
                print(json.dumps(event), flush=True)
    finally:
        done.set()
        thread.join()
    measured = [e for e in events if e['phase'] == 'measured']
    summary = dict(variant=args.variant, rows=args.rows, partitions=16,
        median_wall_seconds=statistics.median(e['seconds'] for e in measured),
        median_core_seconds=statistics.median(e['cpu_seconds'] for e in measured),
        median_average_cores=statistics.median(e['average_cores'] for e in measured),
        package=hl.__file__, logical_cpus=psutil.cpu_count(), sampling_hz=10,
        cpu_scope='worker plus live descendants; warmups excluded; includes in-process monitor',
        semantic_note='raw array_avg returns null for empty arrays; composed/Hail return NaN; force_count forces evaluation without inspecting m')
    if args.variant != 'hail':
        native = Path(hl.__file__).parent / '_native.abi3.so'
        summary['native_sha256'] = hashlib.sha256(native.read_bytes()).hexdigest()
        probe = hl.range_table(3)
        from vail.collections import _function
        assert [r.m for r in probe.annotate(m=_function('array_avg', [hl.range(probe.idx)], hl.tfloat64)).collect()] == [None, 0.0, 0.5]
    (args.output / 'summary.json').write_text(json.dumps(summary, indent=2)+'\n')
    print(json.dumps(summary), flush=True)
    if args.variant == 'hail':
        hl.stop()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--variant', choices=['composed', 'array_avg', 'typed', 'hail'], required=True)
    parser.add_argument('--rows', type=int, default=10_000_000)
    parser.add_argument('--warmups', type=int, default=1)
    parser.add_argument('--iterations', type=int, default=3)
    parser.add_argument('--output', type=Path, required=True)
    worker(parser.parse_args())
