"""Inventory upstream API tests with explicit, reviewable purpose exclusions."""
import ast
import hashlib
import json
import subprocess
from pathlib import Path


def exclusion_reason(module, name):
    parts = Path(module).parts
    leaf = name.split('.')[-1]
    if 'backend' in parts:
        return 'backend only: backend implementation suite'
    if 'fs' in parts or module == 'utils/test_hl_hadoop_and_hail_fs.py':
        return 'filesystem helpers only'
    if module == 'test_ir.py':
        return 'IR construction/serialization only'
    if module in ('test_context.py', 'test_call_caching.py',
                  'test_exceptions_from_workers_have_stack_traces.py', 'test_hail_in_notebook.py'):
        return 'backend lifecycle, execution cache, JVM trace, or notebook infrastructure only'
    if module == 'test_randomness.py' or any(token in leaf for token in ('randomness', 'seeded', 'random_seed')):
        return 'randomness determinism/seed behavior only'
    if leaf in ('test_sample', 'test_sample_rows', 'test_random_uniform', 'test_shuffle',
                'test_key_by_random') or leaf.startswith(('test_rand_', 'test_random_')):
        return 'random sampling/generation only'
    if module == 'utils/test_utils.py' and leaf.startswith('test_hadoop_'):
        return 'filesystem helpers only'
    if (module, leaf) in {
        ('table/test_table.py', 'test_from_spark_works'),
        ('expr/test_types.py', 'test_nested_type_to_spark'),
        ('matrixtable/test_matrix_table.py', 'test_codecs_matrix'),
        ('matrixtable/test_matrix_table.py', 'test_codecs_table'),
        ('matrixtable/test_file_formats.py', 'test_write'),
    }:
        return 'Spark or JVM codec backend integration only'
    return None


def manifest(source):
    from all_upstream_table_cases import definitions
    included, excluded = [], []
    for path in sorted(source.rglob('test_*.py')):
        module = str(path.relative_to(source))
        for name, node in definitions(ast.parse(path.read_text())):
            entry = dict(module=module, name=name, line=node.lineno,
                         source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            reason = exclusion_reason(module, name)
            if reason:
                excluded.append(dict(entry, reason=reason))
            else:
                included.append(entry)
    return dict(source=str(source.resolve()),
                vail_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                scope='Upstream Hail Python API tests against Vail; excludes only randomness, backend, IR, and fs tests',
                note='Mixed tests using random inputs or backend decorators remain included. Names such as aggregate_ir and lowered_shuffle test public API behavior and remain included.',
                included=included, excluded=excluded)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    result = manifest(args.source)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(f"Included {len(result['included'])}; excluded {len(result['excluded'])} definitions")
