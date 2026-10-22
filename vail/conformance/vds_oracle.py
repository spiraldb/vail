"""Extract VDS fixture values with pinned Hail; never used by production reads."""

import argparse
import gzip
import json
from pathlib import Path
from vcf_golden import plain


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import hail as hl

    args.output.mkdir(parents=True, exist_ok=True)
    hl.init(master="local[2]", quiet=True, log=str(args.output / "hail.log"))
    try:
        name = "1kg_chr22_5_samples.vds"
        vds = hl.vds.read_vds(str(args.resources / name))
        for kind, matrix in [
            ("reference_data", vds.reference_data),
            ("variant_data", vds.variant_data),
            ("dense", hl.vds.to_dense_mt(vds)),
        ]:
            record = dict(
                row_type=str(matrix.row.dtype),
                col_type=str(matrix.col.dtype),
                entry_type=str(matrix.entry.dtype),
                global_type=str(matrix.globals.dtype),
                row_key=list(matrix.row_key),
                col_key=list(matrix.col_key),
                rows=plain(hl, matrix.localize_entries("entries", "cols").collect()),
                cols=plain(hl, matrix.cols().collect()),
                globals=plain(hl, hl.eval(matrix.index_globals())),
            )
            with gzip.open(
                args.output / (name + "." + kind + ".json.gz"), "wt"
            ) as file:
                json.dump(record, file)
    finally:
        hl.stop()


if __name__ == "__main__":
    main()
