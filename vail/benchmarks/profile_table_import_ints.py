"""Time the unchanged upstream table_import_ints body against a local fixture.

Run under samply record --save-only --unstable-presymbolicate for native stacks.
Fixture preparation and Python imports are outside the reported query timings.
"""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import resource
import time

import vail as hl


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--iterations', type=int, default=3)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[2]
    source = root / 'hail/hail/python/benchmark/hail/benchmark_table.py'
    tree = ast.parse(source.read_text())
    function = next(node for node in tree.body
                    if isinstance(node, ast.FunctionDef) and node.name == 'test_table_import_ints')
    namespace = {'hl': hl}
    exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), 'exec'), namespace)
    native = Path(hl.__file__).parent / '_native.abi3.so'
    report = {'input': str(args.input.resolve()), 'input_bytes': args.input.stat().st_size,
              'native_sha256': hashlib.sha256(native.read_bytes()).hexdigest(),
              'upstream_source_sha256': hashlib.sha256(source.read_bytes()).hexdigest(),
              'iterations': []}
    for i in range(args.iterations):
        start_wall_ns = time.time_ns()
        start, cpu = time.perf_counter(), time.process_time()
        namespace['test_table_import_ints'](args.input)
        result = {'iteration': i, 'wall_seconds': time.perf_counter() - start,
                  'cpu_seconds': time.process_time() - cpu,
                  'phase': 'measured', 'start_wall_ns': start_wall_ns,
                  'end_wall_ns': time.time_ns(),
                  'process_peak_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}
        report['iterations'].append(result)
        args.output.write_text(json.dumps(report, indent=2) + '\n')
        print(json.dumps(result), flush=True)


if __name__ == '__main__':
    main()
