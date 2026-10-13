"""Attempt original bodies that pytest could not collect; never rerun assertions."""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import os
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('selection', type=Path)
    parser.add_argument('pytest_directory', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--jobs', type=int, default=2)
    parser.add_argument('--timeout', type=int, default=120)
    parser.add_argument('--prior', type=Path, action='append', default=[])
    args = parser.parse_args()
    manifest = json.loads(args.selection.read_text())
    source = Path(manifest['source'])
    observed = set()
    for path in args.pytest_directory.glob('*.jsonl'):
        module = path.name.removesuffix('.jsonl').replace('__', '/')
        for line in path.read_text().splitlines():
            event = json.loads(line)
            if event['kind'] != 'test':
                continue
            parts = event['nodeid'].split('::')[1:]
            parts[-1] = parts[-1].split('[')[0]
            observed.add((module, '.'.join(parts)))
    for folder in args.prior:
        for path in folder.glob('[0-9]*.json'):
            result = json.loads(path.read_text())
            observed.add((result['module'], result['name']))
    cases = [row for row in manifest['included'] if (row['module'], row['name']) not in observed]
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / 'selection.json').write_text(json.dumps(cases, indent=2))
    runner = Path(__file__).with_name('full_hail_bodies.py').resolve()
    def run(pair):
        index, case = pair
        result_path = args.output / f'{index:04d}.json'
        command = [sys.executable, str(runner), '--suite', str(source), '--output', str(result_path),
                   '--worker-source', str(source / case['module']), '--worker-name', case['name']]
        with result_path.with_suffix('.log').open('w') as log:
            try:
                process = subprocess.run(command, stdout=log, stderr=log, timeout=args.timeout)
                problem = f'worker exited {process.returncode}' if process.returncode else None
            except subprocess.TimeoutExpired:
                problem = f'worker exceeded {args.timeout} seconds'
        result = json.loads(result_path.read_text()) if result_path.exists() else dict(module=case['module'], name=case['name'])
        if problem:
            result.update(status='worker_blocked', worker_error=problem)
        result_path.write_text(json.dumps(result, indent=2) + '\n')
        return index, result
    results = {}
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = [pool.submit(run, pair) for pair in enumerate(cases)]
        for future in as_completed(futures):
            index, result = future.result()
            results[index] = result
            (args.output / 'results.json').write_text(json.dumps([results[i] for i in sorted(results)], indent=2))
            print(f"{len(results)}/{len(cases)} {result.get('status')} {result['module']}::{result['name']}", flush=True)


if __name__ == '__main__':
    main()
