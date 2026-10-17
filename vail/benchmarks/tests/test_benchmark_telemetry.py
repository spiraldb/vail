"""The benchmark sampler records real process counters and artifacts."""
import csv
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


spec_path = Path(__file__).resolve().parents[1] / 'benchmark_telemetry.py'
spec_name = 'benchmark_telemetry_test_module'


def test_linux_counter_units_from_current_process():
    if not sys.platform.startswith('linux'):
        pytest.skip('Linux process counters are Linux-specific')

    import importlib.util

    spec = importlib.util.spec_from_file_location(spec_name, spec_path)
    telemetry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(telemetry)
    values = telemetry.linux_counters(os.getpid())

    assert values['user_time'] >= 0
    assert values['system_time'] >= 0
    assert values['resident_size'] > 0
    assert values['diskio_bytesread'] >= 0
    assert values['diskio_byteswritten'] >= 0


def test_measurement_writes_real_process_tree_samples(tmp_path):
    if not sys.platform.startswith('linux'):
        pytest.skip('This integration check validates Linux telemetry columns')

    script = r"""
import importlib.util
import sys
import time
from pathlib import Path
spec = importlib.util.spec_from_file_location('benchmark_telemetry', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
measurement = module.Measurement(sys.argv[2]).start()
deadline = time.perf_counter() + 0.45
value = 0
while time.perf_counter() < deadline:
    value = (value * 33 + 7) % 10000019
print(measurement.stop()['cpu_seconds'])
"""
    completed = subprocess.run(
        [sys.executable, '-c', script, str(spec_path), str(tmp_path)],
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert float(completed.stdout.strip()) > 0

    summary = json.loads((tmp_path / 'summary.json').read_text())
    assert summary['sampling_hz'] == 10
    assert summary['cpu_scope'].startswith('worker and live descendants')
    assert summary['cpu_seconds'] > 0
    with (tmp_path / 'metrics.csv').open(newline='') as stream:
        rows = list(csv.DictReader(stream))
    assert rows
    assert all(float(row['rss_bytes']) > 0 for row in rows)
    assert any(float(row['user_cpu_seconds']) > 0 for row in rows)
    # Linux does not expose per-process hardware event counters in this sampler.
    assert all(row['instructions'] == '' and row['cycles'] == '' for row in rows)
