"""Paired reads of identical Hail-written legacy files; no format conversion."""
import argparse
import json
import resource
import statistics
import subprocess
import threading
import time
from pathlib import Path

p = argparse.ArgumentParser(description=__doc__)
p.add_argument('engine', choices=['hail', 'vail'])
p.add_argument('--output', required=True)
p.add_argument('--warmup', type=int, default=2)
p.add_argument('--runs', type=int, default=5)
p.add_argument('--reject-compilers', action='store_true', help='Wait for Rust builds and retry measurements that overlap one.')
p.add_argument('--cases', nargs='+', default=['table_force_count', 'table_scalar_sum', 'matrix_decode_count', 'matrix_decode_count_gt', 'matrix_entries_count', 'matrix_sum_dp'])
p.add_argument('--hail-threads', type=int, default=14)
p.add_argument('--matrix', default='/Users/danielking/projects/vail/vail/conformance/runs/20261016-hail-paired/data/profile25.mt')
p.add_argument('--table', default='/private/tmp/vail-legacy-native-benchmark.ht')
a = p.parse_args()
if a.engine == 'hail':
    import hail as hl
    hl.init(master=f'local[{a.hail_threads}]', quiet=True, log=a.output + '.hail.log')
    read_table, read_matrix = hl.read_table, hl.read_matrix_table
else:
    import vail as hl
    read_table, read_matrix = hl.Table.read_legacy, hl.MatrixTable.read_legacy
# Spark local[14] runs executor threads in this exact gateway JVM.
# Count both Python and JVM CPU, excluding unrelated Hail processes.
if a.engine == 'hail':
    import psutil
    java_process = psutil.Process(hl.current_backend()._spark.sparkContext._gateway.proc.pid)
    def engine_cpu_time():
        cpu = java_process.cpu_times()
        return time.process_time() + cpu.user + cpu.system
else:
    engine_cpu_time = time.process_time
matrix, table = a.matrix, a.table
def run(case):
    if case.startswith('table'):
        t = read_table(table)
        return t._force_count() if case == 'table_force_count' else t.aggregate(hl.agg.sum(t.x))
    m = read_matrix(matrix)
    if case == 'matrix_decode_count_gt':
        return m.select_entries('GT')._force_count_rows()
    if case == 'matrix_decode_count':
        return m._force_count_rows()
    if case == 'matrix_entries_count':
        return m.entries()._force_count()
    return m.aggregate_entries(hl.agg.sum(m.DP))
# process_time includes Vail native worker threads, but excludes Hail's Java child.
# Compare this CPU metric between Vail binaries only; Hail comparison uses wall time.
results = {}
def compiler_active():
    return any(line.strip().rsplit('/', 1)[-1] == 'rustc' for line in subprocess.check_output(['ps', '-axo', 'comm'], text=True).splitlines())

def measure(case):
    if not a.reject_compilers:
        faults_start = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
        engine_cpu_start = engine_cpu_time()
        cpu_start = time.process_time()
        start = time.perf_counter()
        value = run(case)
        return value, time.perf_counter() - start, False, time.process_time() - cpu_start, engine_cpu_time() - engine_cpu_start, resource.getrusage(resource.RUSAGE_SELF).ru_minflt - faults_start
    waiting = False
    while compiler_active():
        if not waiting:
            print(case, 'waiting for Rust compilation', flush=True)
            waiting = True
        time.sleep(.5)
    stop, interference = threading.Event(), threading.Event()
    def monitor():
        while not stop.wait(.1):
            if compiler_active():
                interference.set()
    thread = threading.Thread(target=monitor, daemon=True)
    thread.start()
    try:
        faults_start = resource.getrusage(resource.RUSAGE_SELF).ru_minflt
        engine_cpu_start = engine_cpu_time()
        cpu_start = time.process_time()
        start = time.perf_counter()
        value = run(case)
        elapsed = time.perf_counter() - start
        cpu_elapsed = time.process_time() - cpu_start
        engine_cpu_elapsed = engine_cpu_time() - engine_cpu_start
        minor_faults = resource.getrusage(resource.RUSAGE_SELF).ru_minflt - faults_start
    finally:
        stop.set()
        thread.join()
    return value, elapsed, interference.is_set() or compiler_active(), cpu_elapsed, engine_cpu_elapsed, minor_faults

for case in a.cases:
    times, cpu_times, engine_cpu_times, minor_faults_values, values, attempts = [], [], [], [], [], []
    try:
        i = 0
        while i < a.warmup + a.runs:
            value, elapsed, interference, cpu_elapsed, engine_cpu_elapsed, minor_faults = measure(case)
            values.append(value)
            assert all(v == value for v in values)
            attempts.append(dict(seconds=elapsed, python_process_cpu_seconds=cpu_elapsed, engine_cpu_seconds=engine_cpu_elapsed, python_process_minor_faults=minor_faults, compiler_interference=interference, warmup=i < a.warmup))
            if interference:
                print(case, 'retrying measurement overlapping Rust compilation', elapsed, value, flush=True)
                continue
            if i >= a.warmup:
                times.append(elapsed)
                cpu_times.append(cpu_elapsed)
                engine_cpu_times.append(engine_cpu_elapsed)
                minor_faults_values.append(minor_faults)
            print(case, i, elapsed, value, flush=True)
            i += 1
        results[case] = dict(seconds=times, median=statistics.median(times), python_process_cpu_seconds=cpu_times, python_process_cpu_median=statistics.median(cpu_times), engine_cpu_seconds=engine_cpu_times, engine_cpu_median=statistics.median(engine_cpu_times), python_process_minor_faults=minor_faults_values, python_process_minor_faults_median=statistics.median(minor_faults_values), value=value, attempts=attempts)
    except Exception as e:
        results[case] = dict(error=repr(e))
        print(case, repr(e), flush=True)
    Path(a.output).write_text(json.dumps(dict(engine=a.engine, hail_threads=a.hail_threads if a.engine == "hail" else None, warmup=a.warmup, runs=a.runs, reject_compilers=a.reject_compilers, matrix=matrix, table=table, results=results), indent=2)+'\n')
if a.engine == 'hail':
    hl.stop()
