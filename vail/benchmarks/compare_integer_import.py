"""Paired actual-workload comparison of generic and typed integer parsing.

Both modes use one native build and identical input/scanner configuration. Only
Python's integer import expression constructors change. Generic constructors
come from the explicitly selected baseline source revision. No .so is copied.
"""
import argparse
import ast
import hashlib
import functools
import inspect
import json
from pathlib import Path
import statistics
import subprocess
import time

import vail as hl
import vail.text as text


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--baseline-revision', required=True)
    parser.add_argument('--pairs', type=int, default=3)
    parser.add_argument('--serial-scan', action='store_true',
                        help='Use the public file_per_partition option for both modes')
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    scan = text.import_lines
    if args.serial_scan:
        signature = inspect.signature(scan)

        @functools.wraps(scan)
        def serial_scan(*values, **options):
            bound = signature.bind(*values, **options)
            bound.arguments['file_per_partition'] = True
            return scan(*bound.args, **bound.kwargs)

        text.import_lines = serial_scan
    raw_source_partitions = text.import_lines(args.input).n_partitions()
    original = subprocess.check_output(
        ['git', 'show', args.baseline_revision + ':vail/python/vail/text.py'], cwd=root, text=True)
    constructors = ['_parse_integer_array', '_parse_primitive']
    generic_namespace = dict(vars(text))
    functions = [node for node in ast.parse(original).body
                 if isinstance(node, ast.FunctionDef) and node.name in constructors]
    assert len(functions) == 2
    exec(compile(ast.Module(body=functions, type_ignores=[]), '<generic-baseline>', 'exec'), generic_namespace)
    generic = {name: generic_namespace[name] for name in constructors}
    typed = {name: getattr(text, name) for name in constructors}
    source = root / 'hail/hail/python/benchmark/hail/benchmark_table.py'
    function = next(node for node in ast.parse(source.read_text()).body
                    if isinstance(node, ast.FunctionDef) and node.name == 'test_table_import_ints')
    namespace = {'hl': hl}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
    native = Path(hl.__file__).parent / '_native.abi3.so'
    report = {'input': str(args.input.resolve()), 'baseline_revision': args.baseline_revision,
              'serial_scan': args.serial_scan, 'raw_source_partitions': raw_source_partitions,
              'native_sha256': hashlib.sha256(native.read_bytes()).hexdigest(), 'runs': []}
    for pair in range(args.pairs):
        for mode in (('generic', 'typed') if pair % 2 == 0 else ('typed', 'generic')):
            for name, implementation in (generic if mode == 'generic' else typed).items():
                setattr(text, name, implementation)
            start, cpu = time.perf_counter(), time.process_time()
            namespace['test_table_import_ints'](args.input)
            result = {'pair': pair, 'mode': mode, 'wall_seconds': time.perf_counter() - start,
                      'cpu_seconds': time.process_time() - cpu}
            report['runs'].append(result)
            args.output.write_text(json.dumps(report, indent=2) + '\n')
            print(json.dumps(result), flush=True)
    report['median'] = {mode: {metric: statistics.median(
        row[metric] for row in report['runs'] if row['mode'] == mode)
        for metric in ('wall_seconds', 'cpu_seconds')} for mode in ('generic', 'typed')}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report['median']), flush=True)


if __name__ == '__main__':
    main()
