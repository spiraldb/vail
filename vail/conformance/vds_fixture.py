"""Test-only reconstruction from independently extracted Hail VDS values.

This is not a Hail storage reader. Production VDS reads never call this module.
"""

import gzip, json
from pathlib import Path
import vail as hl


def restore(value, dtype):
    if value is None:
        return None
    if isinstance(dtype, hl.tlocus):
        return hl.Locus(value["contig"], value["position"], dtype.reference_genome)
    if dtype == hl.tcall:
        return hl.Call(value["alleles"], value["phased"])
    if isinstance(dtype, hl.tstruct):
        return {k: restore(value[k], t) for k, t in dtype.items()}
    if isinstance(dtype, hl.tarray):
        return [restore(v, dtype.element_type) for v in value]
    if isinstance(dtype, hl.tset):
        return {restore(v, dtype.element_type) for v in value}
    if isinstance(dtype, hl.ttuple):
        return tuple(restore(v, t) for v, t in zip(value, dtype.types, strict=True))
    return value


def matrix(name, kind):
    with gzip.open(
        (Path(__file__).with_name("fixtures") / "vds")
        / (name + "." + kind + ".json.gz"),
        "rt",
    ) as f:
        r = json.load(f)
    rt, ct, et, gt = map(
        hl.dtype, [r["row_type"], r["col_type"], r["entry_type"], r["global_type"]]
    )
    schema = hl.tstruct(**dict(rt.items()), entries=hl.tarray(et))
    t = hl.Table.parallelize(
        [restore(x, schema) for x in r["rows"]], schema=schema, n_partitions=4
    ).key_by(*r["row_key"])
    t = t.annotate_globals(
        **{k: hl.literal(restore(r["globals"][k], ty), ty) for k, ty in gt.items()},
        cols=hl.literal([restore(c, ct) for c in r["cols"]], hl.tarray(ct)),
    )
    return t.unlocalize_entries("entries", "cols", r["col_key"])


def vds(name):
    return hl.vds.VariantDataset(
        matrix(name, "reference_data"), matrix(name, "variant_data")
    )
