"""10 Hz macOS process-tree counters for bounded upstream benchmark attempts."""
from __future__ import annotations
import csv
import json
import os
from pathlib import Path
import threading
import time


class Measurement:
    def __init__(self, directory):
        import psutil
        from profile_annotate import counters
        self.psutil = psutil
        self.root = psutil.Process()
        self.read, self.tick_ns = counters()
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.done = threading.Event()
        self.peak_rss = 0
        self.max_rss = int(os.environ.get('VAIL_BENCHMARK_MAX_RSS_GIB', '32')) * 2**30

    def snapshot(self):
        result = {}
        for process in [self.root, *self.root.children(recursive=True)]:
            try:
                result[(process.pid, process.create_time())] = self.read(process.pid)
            except (OSError, self.psutil.Error):
                pass
        return result

    def start(self):
        self.before = self.snapshot()
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
                total = lambda k: sum(v[k] for v in values.values())
                user = total('user_time')*self.tick_ns/1e9
                system = total('system_time')*self.tick_ns/1e9
                cpu = user+system
                dt = (now-previous[0])/1e9 if previous else 0
                rss = total('resident_size')
                self.peak_rss = max(self.peak_rss, rss)
                writer.writerow([now,time.time_ns(),dt,(cpu-previous[1])/dt*100 if previous else 0,
                    rss,user,system,total('instructions'),total('cycles'),
                    total('diskio_bytesread'),total('diskio_byteswritten')])
                f.flush()
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
        self.done.set()
        self.thread.join()
        keys = ['user_time','system_time','instructions','cycles','diskio_bytesread','diskio_byteswritten']
        delta = {key:sum(v[key]-self.before.get(identity,{}).get(key,0) for identity,v in after.items()) for key in keys}
        cpu = (delta['user_time']+delta['system_time'])*self.tick_ns/1e9
        result = {'cpu_seconds':cpu,'average_cores':cpu/((end_ns-self.start_ns)/1e9),
            'peak_rss_gib':self.peak_rss/2**30,'sampling_hz':10,'counter_deltas':delta,
            'cpu_scope':'worker and live descendants, including JVM and telemetry thread',
            'directory':str(self.directory)}
        (self.directory/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
        return result
