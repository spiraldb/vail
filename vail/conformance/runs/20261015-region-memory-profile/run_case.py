"""Bounded exact reproduction of Hail benchmark_table region-memory cases.

Usage: python run_case.py CASE PACKAGE OUTPUT_DIR
CASE is map_filter, head_tail, inner_join, or left_join. The caller should run
under samply and an external wall timeout; this process enforces a 60 s action
limit and 6 GiB physical-footprint guard.
"""
import ctypes
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

case, package, out = sys.argv[1:4]
package, out = Path(package), Path(out)
out.mkdir(parents=True, exist_ok=True)

class Rusage(ctypes.Structure):
    _fields_ = [('uuid', ctypes.c_uint8 * 16)] + [
        (name, ctypes.c_uint64) for name in (
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

if case == '--memory-guard':
    pid = int(str(package))
    lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
    lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(Rusage)]
    lib.proc_pid_rusage.restype = ctypes.c_int
    class Timebase(ctypes.Structure):
        _fields_ = [("numer", ctypes.c_uint32), ("denom", ctypes.c_uint32)]
    timebase = Timebase()
    system = ctypes.CDLL('/usr/lib/libSystem.B.dylib')
    if system.mach_timebase_info(ctypes.byref(timebase)) != 0:
        raise RuntimeError("cannot read Mach timebase for CPU tick conversion")
    (out / 'counter-units.json').write_text(json.dumps({
        'cpu_tick_nanoseconds_numerator': timebase.numer,
        'cpu_tick_nanoseconds_denominator': timebase.denom,
        'instructions': 'retired instructions', 'cycles': 'CPU cycles',
        'disk_io': 'process bytes, not physical device traffic',
    }, indent=2) + '\n')
    guard_started = time.perf_counter()
    with (out / 'rss.csv').open('w') as stream:
        stream.write('elapsed_seconds,rss_bytes,physical_footprint_bytes,max_footprint_bytes,user_time_ticks,system_time_ticks,instructions,cycles,disk_read_bytes,disk_write_bytes\n')
        stream.flush()
        while True:
            value = Rusage()
            if lib.proc_pid_rusage(pid, 4, ctypes.byref(value)) != 0:
                break
            stream.write(f'{time.perf_counter()-guard_started:.3f},{value.resident_size},{value.phys_footprint},{value.lifetime_max_phys_footprint},{value.user_time},{value.system_time},{value.instructions},{value.cycles},{value.diskio_bytesread},{value.diskio_byteswritten}\n')
            stream.flush()
            if value.phys_footprint > 6 * 1024**3:
                (out / 'guard.txt').write_text('physical footprint exceeded 6 GiB; process killed\n')
                os.kill(pid, signal.SIGKILL)
                break
            time.sleep(0.1)
    raise SystemExit(0)

expected_package = package / 'vail'
expected_native = expected_package / '_native.abi3.so'
if not (expected_package / '__init__.py').is_file() or not expected_native.is_file():
    raise SystemExit(f'package is missing expected vail source/native files: {package}')
sys.path.insert(0, str(package.resolve()))
import vail as hl

if Path(hl.__file__).resolve() != (expected_package / '__init__.py').resolve():
    raise SystemExit(
        f'imported vail from {hl.__file__}, expected {(expected_package / "__init__.py").resolve()}'
    )

started = time.perf_counter()
lib = ctypes.CDLL('/usr/lib/libproc.dylib', use_errno=True)
lib.proc_pid_rusage.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.POINTER(Rusage)]
lib.proc_pid_rusage.restype = ctypes.c_int

native = expected_native
print('package', hl.__file__, flush=True)
print('native_sha256', hashlib.sha256(native.read_bytes()).hexdigest(), flush=True)
print('case', case, flush=True)
memory_guard = subprocess.Popen(
    [sys.executable, __file__, '--memory-guard', str(os.getpid()), str(out)],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
)

deadline_guard = subprocess.Popen([
    sys.executable, '-c',
    "import os,signal,sys,time; time.sleep(60); open(sys.argv[2] + '/guard.txt','w').write('60-second query guard reached; process killed\\n'); os.kill(int(sys.argv[1]), signal.SIGKILL)",
    str(os.getpid()), str(out),
], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
construct_start = time.perf_counter()
if case == 'map_filter':
    table = hl.utils.range_table(30).naive_coalesce(1).annotate(big_array=hl.zeros(100_000_000))
    table = table.filter(table.idx % 2 == 0)
    expected = 15
elif case == 'head_tail':
    table = hl.utils.range_table(100).annotate(big_array=hl.zeros(100_000_000)).head(30)
    expected = 30
elif case in ('inner_join', 'left_join'):
    left = hl.utils.range_table(30).naive_coalesce(1).annotate(big_array=hl.zeros(50_000_000))
    right = hl.utils.range_table(30).naive_coalesce(1).annotate(big_array=hl.zeros(50_000_000))
    table = left.join(right, how='left') if case == 'left_join' else left.join(right)
    expected = 30
else:
    raise ValueError(f'unknown case: {case}')
construct_seconds = time.perf_counter() - construct_start
print('construct_seconds', construct_seconds, flush=True)
action_start = time.perf_counter()
try:
    count = int(table._force_count())
finally:
    deadline_guard.terminate()
    memory_guard.terminate()
summary = {'case': case, 'status': 'completed', 'count': count, 'expected': expected,
           'construct_seconds': construct_seconds, 'action_seconds': time.perf_counter() - action_start,
           'elapsed_seconds': time.perf_counter() - started, 'native_sha256': hashlib.sha256(native.read_bytes()).hexdigest()}
(out / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps(summary), flush=True)
