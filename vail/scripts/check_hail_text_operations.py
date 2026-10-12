"""Record Hail oracle behavior for Vail's text parity tests."""
import json
import tempfile
from pathlib import Path
import hail as hl

hl.init(master='local[1]', quiet=True, log='/private/tmp/vail-text-oracle-hail.log')
results = {}
with tempfile.TemporaryDirectory() as directory:
    root = Path(directory)
    def rows(name, contents, **kwargs):
        path = root / (name + '.txt')
        path.write_bytes(contents)
        try:
            results[name] = [dict(row) for row in hl.import_table(str(path), **kwargs).collect()]
        except Exception as exc:
            results[name] = {'error': type(exc).__name__}
    for name, content in [('physical', b'a,"b"\r\n\r\nlast'), ('tail_cr', b'last\r'),
                           ('cr_only', b'a\rb\r'), ('empty', b''), ('nul', b'a\x00b\n')]:
        path = root / name
        path.write_bytes(content)
        try:
            imported = hl.import_lines(str(path)).collect()
            results['lines_' + name] = [row.text for row in imported]
            if imported:
                results['file_name_' + name] = imported[0].file.replace(str(root), '<fixture>')
        except Exception as exc: results['lines_' + name] = {'error': type(exc).__name__}
    rows('unquoted', b'id\tnote\n1\t"literal"\n2\t\n3\tNA\n')
    rows('valid_quoted', b'id,note\n1,"a,b"\n2,""\n', delimiter=',', quote='"')
    rows('quoted', b'id,note\n1,"a,b"\n2,"a""b"\n3,\n', delimiter=',', quote='"')
    rows('unterminated_quote', b'id,note\n1,"abc\n', delimiter=',', quote='"')
    rows('duplicate_header', b'a\ta\ta_1\n1\t2\t3\n')
    rows('too_few', b'a\tb\n1\n')
    rows('blank_line', b'a\tb\n\n')
    rows('no_header', b'a|b\nc|d\n', no_header=True, delimiter='|', find_replace=('a', 'A'))
    output = root / 'export.tsv'
    hl.Table.parallelize([{'i': 1, 'f': 1.0, 'b': True, 's': None}],
                         schema=hl.tstruct(i=hl.tint32, f=hl.tfloat64, b=hl.tbool, s=hl.tstr)).export(str(output))
    results['export'] = output.read_text()
print('VAIL_TEXT_ORACLE=' + json.dumps(results, ensure_ascii=True))
hl.stop()
