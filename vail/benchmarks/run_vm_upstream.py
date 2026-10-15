"""Run every upstream case for both engines serially on one otherwise idle VM."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import subprocess
import sys

import psutil
import upstream_benchmark_coverage as coverage


def write(path, value):
    coverage._write_report(path, value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--vail-python', required=True)
    parser.add_argument('--hail-python', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--data-dir', type=Path, required=True)
    parser.add_argument('--commit', required=True)
    parser.add_argument('--timeout', type=float, default=250)
    parser.add_argument('--fixture-timeout', type=float, default=1800)
    parser.add_argument('--rss-gib', type=int, default=96)
    parser.add_argument('--resume', action='store_true')
    args = parser.parse_args()
    if not sys.platform.startswith('linux'):
        parser.error('this runner is intended for the Linux benchmark VM')
    args.output = args.output.resolve()
    args.output.mkdir(parents=True, exist_ok=args.resume)
    cases = coverage.inventory()
    scripts = Path(__file__).resolve().parent
    hashes = {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in (
        Path(__file__), scripts/'upstream_benchmark_coverage.py', scripts/'benchmark_telemetry.py',
    )}
    settings = dict(commit=args.commit, runner_hashes=hashes,
                    timeout=args.timeout, fixture_timeout=args.fixture_timeout,
                    rss_gib=args.rss_gib, data_dir=str(args.data_dir))
    previous_path = args.output/'provenance.json'
    if args.resume:
        previous = json.loads(previous_path.read_text())
        if previous['settings'] != settings:
            raise ValueError('resume source/configuration differs from the original run')
    else:
        builds = [p.info['name'] for p in psutil.process_iter(['name'])
                  if p.info['name'] in {'cargo', 'rustc', 'clang', 'clang++', 'ld.lld'}]
        if builds:
            raise RuntimeError(f'finish compilation before benchmarking: {builds}')
        versions = {}
        for engine in ('vail', 'hail'):
            versions[engine] = subprocess.check_output([
                getattr(args, f'{engine}_python'), '-c',
                f'import importlib.metadata; print(importlib.metadata.version({engine!r}))',
            ], text=True).strip()
        write(previous_path, dict(settings=settings, platform=platform.platform(),
                                 cpus=os.cpu_count(), memory_bytes=psutil.virtual_memory().total,
                                 versions=versions, lscpu=subprocess.check_output(['lscpu'], text=True)))
        write(args.output/'selection.json', cases)
        write(args.output/'settings.json', {
            'title': 'Full upstream Hail and Vail comparison on one VM',
            'comparison_note': f'Fresh exact-input runs of all {len(cases)} upstream cases for BOTH engines '
                f'on the same otherwise idle Linux VM ({os.cpu_count()} vCPUs). Engine order alternates '
                'between cases. Separate fresh Python workers; query time excludes engine initialization '
                'and fixture preparation. Stock PyPI Hail uses Spark local[32] and a 64 GiB heap. '
                f'Both engines have a {args.rss_gib} GiB process-tree RSS guard; default query timeout '
                f'{args.timeout:g} seconds and fixture timeout {args.fixture_timeout:g} seconds. '
                'One attempt per case, no smoke scaling. Upstream skips are respected. CPU/RSS/process '
                'I/O sampled at 10 Hz; guest-wide hardware perf intervals are separate and may be unavailable. '
                'Inputs are cached; this is not a cold-cache I/O comparison.',
        })
    reports = {engine: dict(engine=engine, discovered=len(cases), cases=[])
               for engine in ('vail', 'hail')}
    for index, case in enumerate(cases):
        for engine in (('hail', 'vail') if index % 2 == 0 else ('vail', 'hail')):
            folder = args.output/'attempts'/engine
            folder.mkdir(parents=True, exist_ok=True)
            path = folder/f'{index:03d}.json'
            if not (args.resume and path.exists()):
                print(f'START {index+1}/{len(cases)} {engine} {case["id"]}', flush=True)
                env = os.environ.copy()
                env.pop('PYTHONPATH', None)
                env.update(HAIL_BENCHMARK_CORES='32', HAIL_BENCHMARK_MEMORY='64g',
                           VAIL_BENCHMARK_MAX_RSS_GIB=str(args.rss_gib),
                           VAIL_BENCHMARK_TELEMETRY_DIR=str(args.output/'telemetry'))
                command = [getattr(args, f'{engine}_python'), str(scripts/'upstream_benchmark_coverage.py'),
                           '--engine', engine, '--case', case['id'], '--timeout', str(args.timeout),
                           '--fixture-timeout', str(args.fixture_timeout), '--data-dir', str(args.data_dir),
                           '--output', str(path)]
                with path.with_suffix('.log').open('w') as log:
                    subprocess.run(command, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
            result = json.loads(path.read_text())['cases'][0]
            result.update(source_commit=args.commit, comparable=True,
                          baseline_hardware='same 32-vCPU Linux VM')
            reports[engine]['cases'].append(result)
            reports[engine]['totals'] = {
                status: sum(c['status'] == status for c in reports[engine]['cases'])
                for status in {c['status'] for c in reports[engine]['cases']}
            }
            write(args.output/f'{engine}.json', reports[engine])
            print(f'DONE {index+1}/{len(cases)} {engine}: {result["status"]}', flush=True)
    subprocess.run([sys.executable, str(scripts/'render_affected_benchmarks.py'), str(args.output)], check=True)


if __name__ == '__main__':
    main()
