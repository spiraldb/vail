"""Public blackbox reproduction, present at baseline 3a2b941 and cycle 2.

Run with the installed Vail environment's Python. This must eventually succeed;
currently the all-null partition fails in the nested structural list writer.
"""
from pathlib import Path
from tempfile import TemporaryDirectory

import vail as hl

schema = hl.tstruct(x=hl.tdict(hl.ttuple(hl.tint32), hl.tarray(hl.ttuple(hl.tint32))))
rows = [{'x': {(1,): [(2,)]}}, {'x': None}]
table = hl.Table.parallelize(rows, schema=schema, n_partitions=2)
with TemporaryDirectory(prefix='vail-nested-list-review-') as temporary:
    path = Path(temporary) / 'table'
    table.write(path)
    assert hl.read_table(path).collect() == [hl.Struct(**row) for row in rows]
