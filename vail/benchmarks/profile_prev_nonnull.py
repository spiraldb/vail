"""Run the unmodified upstream previous-nonnull scan case with process telemetry."""

import argparse
import ast
import functools
import hashlib
import json
import os
from pathlib import Path
import resource
import sys
import threading
import time

import psutil


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--upstream", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--max-seconds", type=float, default=300)
    parser.add_argument("--max-rss-gib", type=float, default=8)
    args = parser.parse_args()
    sys.path.insert(0, str(args.package.resolve()))
    import vail as hl

    args.output.mkdir(parents=True, exist_ok=True)
    source = args.upstream.read_text()
    tree = ast.parse(source)
    function = next(node for node in tree.body if isinstance(node, ast.FunctionDef)
                    and node.name == "test_table_scan_prev_non_null")
    namespace = {"hl": hl}
    exec(compile(ast.fix_missing_locations(ast.Module(body=[function], type_ignores=[])),
                 str(args.upstream), "exec"), namespace)
    native = Path(hl.__file__).parent / "_native.abi3.so"
    identity = {"package": hl.__file__, "native_sha256": hashlib.sha256(native.read_bytes()).hexdigest(),
                "upstream_sha256": hashlib.sha256(source.encode()).hexdigest()}
    print(json.dumps(identity), flush=True)
    finished = threading.Event()
    process = psutil.Process()
    start = time.perf_counter()
    before = resource.getrusage(resource.RUSAGE_SELF)
    phases = []

    def timed(owner, name):
        original = getattr(owner, name)

        @functools.wraps(original)
        def call(*values, **named):
            began = time.perf_counter()
            result = None
            try:
                result = original(*values, **named)
                return result
            finally:
                phase = {"method": name, "start_seconds": began-start,
                         "elapsed_seconds": time.perf_counter()-began}
                if name == "_force_count":
                    phase["rows"] = result
                phases.append(phase)
                print(json.dumps(phase), flush=True)

        setattr(owner, name, call)

    for owner, name in [(hl.Table, "annotate"), (hl.Table, "_force_count")]:
        timed(owner, name)

    def sample():
        with (args.output / "telemetry.csv").open("w") as stream:
            stream.write("elapsed_seconds,user_seconds,system_seconds,rss_bytes\n")
            while not finished.is_set():
                cpu = process.cpu_times()
                elapsed = time.perf_counter()-start
                rss = process.memory_info().rss
                stream.write(f"{elapsed:.6f},{cpu.user},{cpu.system},{rss}\n")
                stream.flush()
                if elapsed > args.max_seconds or rss > args.max_rss_gib * 1024**3:
                    (args.output / "guard.json").write_text(json.dumps({"elapsed_seconds": elapsed,
                                                                          "rss_bytes": rss}))
                    os._exit(124)
                finished.wait(0.1)

    worker = threading.Thread(target=sample, daemon=True)
    worker.start()
    status = "failed"
    try:
        namespace[function.name]()
        status = "completed"
    finally:
        elapsed = time.perf_counter() - start
        after = resource.getrusage(resource.RUSAGE_SELF)
        finished.set()
        worker.join()
        summary = dict(identity, status=status, elapsed_seconds=elapsed,
                       cpu_seconds=after.ru_utime+after.ru_stime-before.ru_utime-before.ru_stime,
                       peak_rss_bytes=after.ru_maxrss, phases=phases)
        (args.output / "summary.json").write_text(json.dumps(summary, indent=2)+"\n")
        print(json.dumps(summary), flush=True)


if __name__ == "__main__":
    main()
