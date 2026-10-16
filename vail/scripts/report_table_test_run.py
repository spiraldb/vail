"""Render actual upstream-body outcomes and grouped remaining Table-only work."""
import json
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
inventory = json.loads((ROOT / 'conformance/hail-test-inventory.json').read_text())
results = json.loads((ROOT / 'conformance/upstream-table-results.json').read_text())
lines = ['# Table expression test run', '',
         f"Source-extracted upstream cases: **{results['summary']['passed']} passed, {results['summary']['failed']} failed**.", '',
         'This runs unchanged Hail assertions against Vail, without Hail initialization or decorators. It is not a full upstream pytest run or live Hail oracle comparison.', '',
         'This report counts only the supplied source-body outcome records. Separate local, Rust, storage, and oracle runs require their own recorded evidence.', '',
         '## Executed definitions', '', '| Test | Passed cases | Failed cases | First failure |', '| --- | ---: | ---: | --- |']
groups = defaultdict(list)
for row in results['outcomes']:
    groups[row['test_id']].append(row)
for name, cases in groups.items():
    failures = [r for r in cases if r['status'] != 'passed']
    error = (failures[0].get('error_type', '') + ': ' + failures[0].get('error', '')) if failures else ''
    lines.append(f"| `{name.split('::')[-1]}` | {len(cases)-len(failures)} | {len(failures)} | {error.replace('|', '/').replace(chr(10), ' ')} |")
lines += ['', 'Executed cases and their first failures are listed above. Do not infer coverage of unrecorded parameter variants or implementation status from a historical inventory.', '', '## Remaining Table-only definitions by functionality', '',
          'Groups overlap: a test may need several features. These are reviewed dependencies, not execution failures; a group can include prerequisites already implemented, so its count is not a count of wholly missing implementations. Each entry states the full functionality its test requires. Legacy `.ht` decoding is deferred. Matrix-dependent fixtures, shared expression suites and Scala backend tests remain separately classified in HAIL_TEST_INVENTORY_EXTRA.md.', '']
remaining = [r for r in inventory['tests'] if r.get('reviewed_scope') == 'table_only' and r.get('execution_evidence', {}).get('outcome') != 'passed_body']
lines += [f'**{len(remaining)} Table-only definitions lack a complete passing body**, across dedicated and additional suites.', '']
packages = defaultdict(list)
for row in remaining:
    for package in row['work_packages'] or ['Unassigned']:
        packages[package].append(row)
lines += ['| Functionality | Remaining definitions needing it |', '| --- | ---: |']
for package, rows in sorted(packages.items()):
    lines.append(f'| {package} | {len(rows)} |')
for package, rows in sorted(packages.items()):
    lines += ['', f'## {package}', '']
    for row in rows:
        source = row['source']
        link = f"https://github.com/hail-is/hail/blob/{results['hail_commit']}/{source}#L{row['line']}"
        lines.append(f"- [{row['test_id']}]({link}): {row['implementation_requirement']}")
(ROOT / 'docs/TABLE_EXPRESSION_TEST_RESULTS.md').write_text('\n'.join(lines) + '\n')
print(f'Reported {len(remaining)} remaining Table-only definitions in {len(packages)} overlapping groups')
