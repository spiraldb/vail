"""Inventory upstream API tests with explicit, reviewable purpose exclusions."""
import ast
import hashlib
import json
import subprocess
from pathlib import Path


from upstream_scope import exclusion_reason as scope_exclusion_reason


def exclusion_reason(module, name, *, include_backend=False):
    reason = scope_exclusion_reason(module, name)
    if reason:
        return reason
    if not include_backend and 'backend' in Path(module).parts:
        return 'backend implementation suite; enable explicitly with --include-backend'
    return None


def manifest(source, *, include_backend=False):
    from all_upstream_table_cases import definitions
    included, excluded = [], []
    for path in sorted(source.rglob('test_*.py')):
        module = str(path.relative_to(source))
        for name, node in definitions(ast.parse(path.read_text())):
            entry = dict(module=module, name=name, line=node.lineno,
                         source_sha256=hashlib.sha256(path.read_bytes()).hexdigest())
            reason = exclusion_reason(module, name, include_backend=include_backend)
            if reason:
                excluded.append(dict(entry, reason=reason))
            else:
                included.append(entry)
    return dict(source=str(source.resolve()),
                vail_commit=subprocess.check_output(['git', 'rev-parse', 'HEAD'], text=True).strip(),
                scope='Full upstream Hail tests against Vail; shared module/definition exclusions for typecheck and Hadoop/Hail filesystem compatibility',
                note='Randomness and IR tests remain included. Backend implementation modules are optional, matching the full runner. Exclusion reasons are recorded per definition.',
                included=included, excluded=excluded)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('source', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--include-backend', action='store_true')
    args = parser.parse_args()
    result = manifest(args.source, include_backend=args.include_backend)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(f"Included {len(result['included'])}; excluded {len(result['excluded'])} definitions")
