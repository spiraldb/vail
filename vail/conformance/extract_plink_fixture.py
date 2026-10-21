"""Independently extract the original PLINK test fixture using pinned Hail."""

import argparse, gzip, json
from pathlib import Path
from vcf_golden import plain


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resources", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import hail as hl

    hl.init(master="local[2]", quiet=True, log=str(args.output) + ".hail.log")
    try:
        matrix = hl.read_matrix_table(
            str(args.resources / "split-multi-sample.vcf.mt")
        ).select_globals()
        candidate = hl.split_multi_hts(
            hl.import_vcf(
                str(args.resources / "sample.vcf"), array_elements_required=False
            )
        ).select_globals()
        assert matrix._same(candidate, tolerance=0)
        record = dict(
            rows=plain(hl, matrix.localize_entries("entries", "cols").collect()),
            cols=plain(hl, matrix.cols().collect()),
            row_type=str(matrix.row.dtype),
            col_type=str(matrix.col.dtype),
            entry_type=str(matrix.entry.dtype),
            global_type=str(matrix.globals.dtype),
        )
        with gzip.open(args.output, "wt") as file:
            json.dump(record, file, sort_keys=True)
        print(f"exact Hail fixture equality; {len(record['rows'])} rows")
    finally:
        hl.stop()


if __name__ == "__main__":
    main()
