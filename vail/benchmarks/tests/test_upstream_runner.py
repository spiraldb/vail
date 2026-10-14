"""Completed benchmark results survive slow engine shutdown."""
import importlib.util
import multiprocessing as mp
import os
from pathlib import Path
import time

import pytest


MODULE_PATH = Path(__file__).resolve().parents[1] / "upstream_benchmark_coverage.py"
spec = importlib.util.spec_from_file_location("upstream_coverage", MODULE_PATH)
coverage = importlib.util.module_from_spec(spec)
spec.loader.exec_module(coverage)


def slow_shutdown_worker(case, engine, smoke, data_dir, queue):
    os.setsid()
    queue.put({"status": "ready"})
    queue.put(case["result"])
    time.sleep(60)


@pytest.mark.parametrize("result", [
    {"status": "passed", "benchmark_seconds": 1.25},
    {"status": "failed", "error": "query failure", "benchmark_seconds": 0.5},
])
def test_completed_result_survives_slow_shutdown(monkeypatch, result):
    context = mp.get_context("spawn")
    workers = []

    class SlowShutdownContext:
        Queue = staticmethod(context.Queue)

        @staticmethod
        def Process(*, target, args):
            worker = context.Process(target=slow_shutdown_worker, args=args)
            workers.append(worker)
            return worker

    monkeypatch.setattr(coverage.mp, "get_context", lambda method: SlowShutdownContext())
    actual = coverage.run_one({"upstream_marks": [], "result": result}, "hail", 5, 10, False, ".")
    assert actual.items() >= result.items()
    assert actual["worker_cleanup_forced"] is True
    assert not workers[0].is_alive()
