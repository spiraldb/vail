"""10 Hz process-tree counters for bounded macOS/Linux benchmark attempts."""
from __future__ import annotations
import csv
import json
import os
import sys
import signal
import shutil
import subprocess
from pathlib import Path
import threading
import time


def linux_counters(pid):
    import psutil

    process = psutil.Process(pid)
    with process.oneshot():
        cpu = process.cpu_times()
        memory = process.memory_info()
        io = process.io_counters()
    return dict(user_time=cpu.user, system_time=cpu.system,
                resident_size=memory.rss, diskio_bytesread=io.read_bytes,
                diskio_byteswritten=io.write_bytes, instructions=0, cycles=0)


class Measurement:
    def __init__(self, directory):
        import psutil
        self.psutil = psutil
        self.root = psutil.Process()
        self.linux = sys.platform.startswith('linux')
        if self.linux:
            self.read, self.tick_ns = linux_counters, 1e9
        else:
            from profile_annotate import counters
            self.read, self.tick_ns = counters()
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.done = threading.Event()
        self.peak_rss = 0
        self.max_rss = int(os.environ.get('VAIL_BENCHMARK_MAX_RSS_GIB', '32')) * 2**30
        self.min_free = int(os.environ.get('LOCAL_BENCHMARK_MIN_FREE_GIB', '0')) * 2**30
        self.perf = None
        self.perf_log = None
        self.max_counters = {}
        self.max_lock = threading.Lock()

    def snapshot(self):
        result = {}
        for process in [self.root, *self.root.children(recursive=True)]:
            try:
                result[(process.pid, process.create_time())] = self.read(process.pid)
            except (OSError, self.psutil.Error):
                pass
        return result

    def retain(self, values):
        # Keep monotonic CPU counters after a child exits. PID plus creation
        # time avoids conflating PID reuse with the original process.
        with self.max_lock:
            for identity, counters in values.items():
                prior = self.max_counters.setdefault(identity, {})
                for key, value in counters.items():
                    if isinstance(value, (int, float)):
                        prior[key] = max(prior.get(key, 0), value)

    def start(self):
        if self.linux:
            self.perf_log = (self.directory / 'perf.log').open('w')
            try:
                self.perf = subprocess.Popen([
                    'perf', 'stat', '-a', '-I', '100', '-x,',
                    '-e', 'instructions,cycles,cache-misses',
                    '-o', str(self.directory / 'perf.csv'),
                ], stdout=self.perf_log, stderr=self.perf_log)
            except OSError as error:
                self.perf_log.write(str(error) + '\n')
        self.before = self.snapshot()
        self.retain(self.before)
        self.start_ns = time.perf_counter_ns()
        self.thread = threading.Thread(target=self.monitor, daemon=True)
        self.thread.start()
        return self

    def monitor(self):
        previous = None
        deadline = time.perf_counter()
        with (self.directory / 'metrics.csv').open('w', newline='') as f:
            writer = csv.writer(f, lineterminator='\n')
            writer.writerow(['monotonic_ns','wall_ns','interval_seconds','cpu_percent',
                'rss_bytes','user_cpu_seconds','system_cpu_seconds','instructions','cycles',
                'disk_read_bytes','disk_write_bytes'])
            while not self.done.is_set():
                now = time.perf_counter_ns()
                values = self.snapshot()
                self.retain(values)
                total = lambda k: sum(v[k] for v in values.values())
                with self.max_lock:
                    retained = list(self.max_counters.values())
                    user = sum(v.get('user_time',0) for v in retained)*self.tick_ns/1e9
                    system = sum(v.get('system_time',0) for v in retained)*self.tick_ns/1e9
                cpu = user+system
                dt = (now-previous[0])/1e9 if previous else 0
                rss = total('resident_size')
                self.peak_rss = max(self.peak_rss, rss)
                writer.writerow([now,time.time_ns(),dt,(cpu-previous[1])/dt*100 if previous else 0,
                    rss,user,system,None if self.linux else total('instructions'),
                    None if self.linux else total('cycles'),
                    total('diskio_bytesread'),total('diskio_byteswritten')])
                f.flush()
                if self.min_free and shutil.disk_usage(self.directory).free < self.min_free:
                    (self.directory/'disk-limit.json').write_text(json.dumps({'minimum_free_bytes':self.min_free})+'\n')
                    os._exit(87)
                if rss > self.max_rss:
                    (self.directory/'resource-limit.json').write_text(json.dumps({'rss_bytes':rss,'limit_bytes':self.max_rss})+'\n')
                    # Worker already owns a process group. Stop only its own
                    # benchmark; the supervisor reaps the JVM and continues.
                    os._exit(86)
                previous = now,cpu
                deadline += .1
                self.done.wait(max(0,deadline-time.perf_counter()))

    def stop(self):
        end_ns = time.perf_counter_ns()
        after = self.snapshot()
        self.retain(after)
        self.done.set()
        self.thread.join()
        if self.perf is not None:
            self.perf.send_signal(signal.SIGINT)
            try:
                self.perf.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.perf.kill()
                self.perf.wait()
        if self.perf_log is not None:
            self.perf_log.close()
        keys = ['user_time','system_time','instructions','cycles','diskio_bytesread','diskio_byteswritten']
        with self.max_lock:
            retained = {identity: dict(values) for identity, values in self.max_counters.items()}
        delta = {key:sum(v.get(key, 0)-self.before.get(identity,{}).get(key,0) for identity,v in retained.items()) for key in keys}
        if self.linux:
            delta.update(instructions=None, cycles=None)
        cpu = (delta['user_time']+delta['system_time'])*self.tick_ns/1e9
        result = {'cpu_seconds':cpu,'average_cores':cpu/((end_ns-self.start_ns)/1e9),
            'peak_rss_gib':self.peak_rss/2**30,'sampling_hz':10,'counter_deltas':delta,
            'cpu_scope':'worker and sampled descendants, including JVM; exited-process counters retained by (pid, creation_time)',
            'cpu_sampling_note':'10 Hz process sampling may miss descendants that start and exit between samples; reported process-tree CPU is a lower bound.',
            'directory':str(self.directory)}
        if self.linux:
            result['hardware_counters'] = {
                'scope': 'whole otherwise idle VM; separate 100 ms perf intervals',
                'path': str(self.directory / 'perf.csv'),
                'returncode': self.perf.returncode if self.perf is not None else None,
                'note': 'Inspect perf.csv and perf.log for unavailable events; blank process counters mean unavailable.',
            }
        (self.directory/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
