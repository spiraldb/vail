"""Exact test fixture conversion from the independently extracted Hail snapshot.

The extraction provenance and digest live beside the golden payload. This is
not a reader for the historical Hail storage format.
"""

import gzip
import json
from pathlib import Path


def materialize(target):
    import vail as hl

    fixture = Path(__file__).with_name("fixtures") / "split-multi-sample.oracle.json.gz"
    with gzip.open(fixture, "rt") as f:
        record = json.load(f)
    row_type = hl.dtype(record["row_type"])
    col_type = hl.dtype(record["col_type"])
    entry_type = hl.dtype(record["entry_type"])

    def restore(value, dtype):
        if value is None:
            return None
        if isinstance(dtype, hl.tlocus):
            return hl.Locus(value["contig"], value["position"], dtype.reference_genome)
        if dtype == hl.tcall:
            return hl.Call(value["alleles"], value["phased"])
        if isinstance(dtype, hl.tstruct):
            return {k: restore(value[k], ty) for k, ty in dtype.items()}
        if isinstance(dtype, hl.tarray):
            return [restore(v, dtype.element_type) for v in value]
        if isinstance(dtype, hl.tset):
            return {restore(v, dtype.element_type) for v in value}
        return value

    schema = hl.tstruct(**dict(row_type.items()), entries=hl.tarray(entry_type))
    table = hl.Table.parallelize(
        [restore(row, schema) for row in record["rows"]], schema=schema
    )
    table = table.key_by("locus", "alleles").annotate_globals(
        cols=hl.literal(
            [restore(c, col_type) for c in record["cols"]], hl.tarray(col_type)
        )
    )
    matrix = table.unlocalize_entries("entries", "cols", ["s"])
    matrix.write(str(target), overwrite=True)
