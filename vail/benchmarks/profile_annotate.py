"""Profile annotation and Matrix entry-sum benchmarks on macOS.

The monitor is a separate process. proc_pid_rusage V4 supplies cumulative process
counters; Mach CPU ticks are converted with mach_timebase_info. Metrics contain
no query data. Samply captures native stacks independently.
"""
from __future__ import annotations
import argparse
import csv
import ctypes
import hashlib
import json
import os
from pathlib import Path
import statistics
import subprocess
import time


class Usage(ctypes.Structure):
    _fields_ = [('uuid', ctypes.c_uint8 * 16)] + [(name, ctypes.c_uint64) for name in (
        'user_time', 'system_time', 'pkg_idle_wkups', 'interrupt_wkups', 'pageins',
        'wired_size', 'resident_size', 'phys_footprint', 'proc_start_abstime', 'proc_exit_abstime',
        'child_user_time', 'child_system_time', 'child_pkg_idle_wkups', 'child_interrupt_wkups',
        'child_pageins', 'child_elapsed_abstime', 'diskio_bytesread', 'diskio_byteswritten',
        'cpu_time_qos_default', 'cpu_time_qos_maintenance', 'cpu_time_qos_background',
        'cpu_time_qos_utility', 'cpu_time_qos_legacy', 'cpu_time_qos_user_initiated',
        'cpu_time_qos_user_interactive', 'billed_system_time', 'serviced_system_time',
        'logical_writes', 'lifetime_max_phys_footprint', 'instructions', 'cycles',
        'billed_energy', 'serviced_energy', 'interval_max_phys_footprint', 'runnable_time',
    )]


class Timebase(ctypes.Structure):
    _fields_ = [('numer', ctypes.c_uint32), ('denom', ctypes.c_uint32)]


def counters():
    lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(Usage)]
    lib.proc_pid_rusage.restype = ctypes.c_int
    base = Timebase()
    system = ctypes.CDLL('/usr/lib/libSystem.B.dylib')
    system.mach_timebase_info(ctypes.byref(base))
    def read(pid):
        value = Usage()
        if lib.proc_pid_rusage(pid, 4, ctypes.byref(value)) != 0:
            raise OSError(ctypes.get_errno(), 'proc_pid_rusage failed')
        return {name: getattr(value, name) for name, _ in Usage._fields_ if name != 'uuid'}
    return read, base.numer / base.denom


def worker(output, iterations, warmups, workload="annotate"):
    import vail as hl
    output.mkdir(parents=True, exist_ok=True)
    (output / 'pid').write_text(str(os.getpid()))
    read, tick_ns = counters()
    events = []
    for phase, count in [('warmup', warmups), ('measured', iterations)]:
        for index in range(count):
            before = read(os.getpid())
            start_wall = time.time_ns()
            started = time.perf_counter_ns()
            if workload == 'annotate':
                table = hl.utils.range_table(1_000_000, 14)
                table = table.annotate(**{f'x{i}': i + table.idx for i in range(100)})
                expected = 1_000_000
            else:
                table = hl.utils.range_matrix_table(100_000, 10, 14)
                table = table.annotate_entries(value=table.row_idx + table.col_idx)
                expression = hl.agg.sum(hl.int64(table.value))
                expected = 10 * 100_000 * 99_999 // 2 + 100_000 * 10 * 9 // 2
            constructed = time.perf_counter_ns()
            result = table._force_count() if workload == 'annotate' else table.aggregate_entries(expression)
            ended = time.perf_counter_ns()
            end_wall = time.time_ns()
            after = read(os.getpid())
            assert result == expected
            delta = {name: after[name] - before[name] for name in (
                'user_time', 'system_time', 'instructions', 'cycles', 'pageins',
                'diskio_bytesread', 'diskio_byteswritten')}
            cpu_seconds = (delta['user_time'] + delta['system_time']) * tick_ns / 1e9
            events.append({'phase': phase, 'index': index, 'start_monotonic_ns': started,
                'end_monotonic_ns': ended, 'start_wall_ns': start_wall, 'end_wall_ns': end_wall,
                'construction_seconds': (constructed - started) / 1e9,
                'action_seconds': (ended - constructed) / 1e9, 'seconds': (ended - started) / 1e9,
                'cpu_seconds': cpu_seconds, 'average_cores': cpu_seconds / ((ended - started) / 1e9),
                'counter_deltas': delta})
            (output / 'iterations.json').write_text(json.dumps(events, indent=2) + '\n')
            print(phase, index, events[-1]['seconds'], 'seconds', 'cores', events[-1]['average_cores'], flush=True)
    native = Path(hl.__file__).parent / '_native.abi3.so'
    (output / 'worker.json').write_text(json.dumps({'pid': os.getpid(), 'tick_nanoseconds': tick_ns,
        'package': hl.__file__, 'native_sha256': hashlib.sha256(native.read_bytes()).hexdigest(),
        'median_seconds': statistics.median(x['seconds'] for x in events if x['phase'] == 'measured'),
        'workload': workload, 'rows': 1_000_000 if workload == 'annotate' else 100_000,
        'fields': 100 if workload == 'annotate' else 1,
        'columns': None if workload == 'annotate' else 10, 'source_partitions': 14}, indent=2) + '\n')
    time.sleep(0.2)  # let the independent monitor capture final counters


def monitor(output, python, iterations, warmups, profile, workload="annotate"):
    output.mkdir(parents=True, exist_ok=True)
    (output / 'pid').unlink(missing_ok=True)
    command = [python, str(Path(__file__).resolve()), '--worker', '--output', str(output),
               '--iterations', str(iterations), '--warmups', str(warmups), '--workload', workload]
    if profile:
        command = ['samply', 'record', '--save-only', '--unstable-presymbolicate', '--rate', '500',
            '--profile-name', 'Vail ' + workload, '--output', str(output / 'profile.json.gz')] + command
    read, tick_ns = counters()
    with (output / 'run.log').open('w') as log:
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
        while not (output / 'pid').exists():
            if process.poll() is not None:
                raise RuntimeError(f'worker failed before PID: {process.returncode}; see run.log')
            time.sleep(0.01)
        pid = int((output / 'pid').read_text())
        columns = ['monotonic_ns', 'wall_ns', 'interval_seconds', 'cpu_percent', 'user_cpu_seconds',
            'system_cpu_seconds', 'rss_bytes', 'physical_footprint_bytes', 'max_physical_footprint_bytes',
            'instructions', 'cycles', 'instructions_per_second', 'cycles_per_second', 'ipc', 'pageins',
            'disk_read_bytes', 'disk_write_bytes']
        previous = None
        deadline = time.perf_counter()
        with (output / 'metrics.csv').open('w', newline='') as file:
            writer = csv.DictWriter(file, fieldnames=columns); writer.writeheader()
            while process.poll() is None:
                now = time.perf_counter_ns()
                try:
                    value = read(pid)
                except OSError:
                    break
                interval = (now - previous[0]) / 1e9 if previous else 0
                old = previous[1] if previous else value
                du = (value['user_time'] - old['user_time']) * tick_ns / 1e9
                ds = (value['system_time'] - old['system_time']) * tick_ns / 1e9
                instructions = value['instructions'] - old['instructions']; cycles = value['cycles'] - old['cycles']
                writer.writerow(dict(zip(columns, [now, time.time_ns(), interval,
                    100 * (du + ds) / interval if interval else 0,
                    value['user_time'] * tick_ns / 1e9, value['system_time'] * tick_ns / 1e9,
                    value['resident_size'], value['phys_footprint'], value['lifetime_max_phys_footprint'],
                    value['instructions'], value['cycles'], instructions / interval if interval else 0,
                    cycles / interval if interval else 0, instructions / cycles if cycles else 0,
                    value['pageins'], value['diskio_bytesread'], value['diskio_byteswritten']])))
                file.flush(); previous = now, value
                deadline += 0.1
                time.sleep(max(0, deadline - time.perf_counter()))
        code = process.wait()
        if code:
            raise RuntimeError(f'profiler/worker failed: {code}; see run.log')
    (output / 'monitor.json').write_text(json.dumps({'sampling_hz': 10, 'profile_hz': 500 if profile else None,
        'mach_tick_nanoseconds': tick_ns, 'counter_api': 'proc_pid_rusage RUSAGE_INFO_V4',
        'cpu_percent_definition': '100% = one fully utilized CPU core',
        'hardware_counters': ['instructions', 'cycles'], 'derived_counters': ['IPC'],
        'cache_miss_counters': 'not exposed by this API'}, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--worker', action='store_true')
    parser.add_argument('--workload', choices=['annotate', 'matrix-entry-sum'], default='annotate')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--python', default='/private/tmp/vail-release-benchmark/bin/python')
    parser.add_argument('--iterations', type=int, default=5)
    parser.add_argument('--warmups', type=int, default=3)
    parser.add_argument('--profile', action='store_true')
    args = parser.parse_args()
    if args.worker:
        worker(args.output, args.iterations, args.warmups, args.workload)
    else:
        monitor(args.output, args.python, args.iterations, args.warmups, args.profile, args.workload)
