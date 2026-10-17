"""Linux counters retain their units and tolerate unavailable hardware PMUs."""
import contextlib
import csv
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import psutil


spec = importlib.util.spec_from_file_location(
    'benchmark_telemetry', Path(__file__).resolve().parents[1] / 'benchmark_telemetry.py',
)
telemetry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telemetry)


class Process:
    pid = 123

    def oneshot(self):
        return contextlib.nullcontext()

    def cpu_times(self):
        return SimpleNamespace(user=10.5, system=2.5)

    def memory_info(self):
        return SimpleNamespace(rss=1024)

    def io_counters(self):
        return SimpleNamespace(read_bytes=4096, write_bytes=8192)

    def children(self, recursive):
        assert recursive
        return []

    def create_time(self):
        return 100


def test_linux_counter_units(monkeypatch):
    monkeypatch.setattr(psutil, 'Process', lambda *args: Process())
    values = telemetry.linux_counters(123)
    assert values['user_time'] == 10.5
    assert values['system_time'] == 2.5
    assert values['resident_size'] == 1024
    assert values['diskio_bytesread'] == 4096
    assert values['diskio_byteswritten'] == 8192


def test_linux_measurement_reports_unavailable_hardware_without_fake_zeros(monkeypatch, tmp_path):
    monkeypatch.setattr(psutil, 'Process', lambda *args: Process())
    monkeypatch.setattr(telemetry.sys, 'platform', 'linux')
    def unavailable(*args, **kwargs):
        raise FileNotFoundError('perf unavailable')
    monkeypatch.setattr(telemetry.subprocess, 'Popen', unavailable)
    measurement = telemetry.Measurement(tmp_path).start()
    measurement.done.wait(0.12)
    result = measurement.stop()
    assert result['cpu_seconds'] == 0
    assert result['counter_deltas']['instructions'] is None
    assert result['counter_deltas']['cycles'] is None
    assert result['hardware_counters']['returncode'] is None
    assert 'perf unavailable' in (tmp_path / 'perf.log').read_text()
    rows = list(csv.DictReader((tmp_path / 'metrics.csv').open()))
    assert rows
    assert rows[0]['user_cpu_seconds'] == '10.5'
    assert rows[0]['instructions'] == ''
    assert rows[0]['cycles'] == ''
