#!/usr/bin/env python3
"""Re-run Hail's exact local many_ints linear-regression benchmark against Vail."""
import json
import os
from pathlib import Path
import sys
import threading

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "benchmarks"))
import upstream_benchmark_coverage as runner  # noqa: E402


class Queue:
    def put(self, value):
        print(json.dumps(value), flush=True)
        if value.get("status") == "ready":
            timer = threading.Timer(120, lambda: os._exit(124))
            timer.daemon = True
            timer.start()


case = next(case for case in runner.inventory() if case["name"] == "test_table_aggregate_linreg")
data_dir = ROOT / "local-data" / "upstream-benchmark-20261015"
runner._worker(case, "vail", False, str(data_dir), Queue())
