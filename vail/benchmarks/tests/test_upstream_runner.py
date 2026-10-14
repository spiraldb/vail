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


def test_multi_argument_parameter_rows_are_bound_by_name():
    import ast

    node = ast.parse('''
@pytest.mark.parametrize("n,m", [(10, 2), (20, 3)])
@pytest.mark.parametrize("flag", [True, False])
def test_case(n, m, flag): pass
''').body[0]
    assert coverage._parameters(node) == [(10, 2, True), (10, 2, False), (20, 3, True), (20, 3, False)]


def test_inventory_binds_every_upstream_argument():
    cases = coverage.inventory()
    assert len(cases) == 108
    for case in cases:
        supplied = set(case["arguments"]) | set(case["fixtures"]) | {"tmp_path"}
        assert set(case["args"]) <= supplied, case["id"]
    joins = [case for case in cases if case["name"] == "test_table_range_join"]
    assert [case["arguments"] for case in joins] == [
        {"m": 1_000_000_000, "n": 1000},
        {"m": 1_000_000_000, "n": 1_000_000_000},
    ]
