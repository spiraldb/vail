"""Native matrix transformations, sparse cells, and local snapshot round trip."""

from pathlib import Path
from tempfile import TemporaryDirectory

import vail as hl


def main():
    matrix = hl.utils.range_matrix_table(3, 4, n_partitions=2)
    matrix = matrix.annotate_entries(value=matrix.row_idx * 10 + matrix.col_idx)
    matrix = matrix.filter_entries(matrix.value % 2 == 0)
    assert matrix.count() == (3, 4)
    assert matrix.aggregate_entries(hl.agg.count()) == 6
    rows = matrix.annotate_rows(total=hl.agg.sum(matrix.value))
    assert rows.total.collect() == [2, 22, 42]
    localized = matrix.localize_entries("cells", "samples")
    assert len(hl.eval(localized.samples)) == 4
    assert localized.count() == 3
    restored = localized._unlocalize_entries("cells", "samples", ["col_idx"])
    assert restored._same(matrix)
    with TemporaryDirectory(prefix="vail-matrix-") as folder:
        reopened = matrix.checkpoint(Path(folder) / "matrix")
        assert reopened._same(matrix)
        print(reopened.entries().collect())


if __name__ == "__main__":
    main()
