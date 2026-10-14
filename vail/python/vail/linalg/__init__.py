"""Lazy constant block-matrix source for entry-table interoperability."""


class BlockMatrix:
    def __init__(self, n_rows, n_cols, value):
        self.n_rows, self.n_cols, self.value = n_rows, n_cols, value

    @classmethod
    def fill(cls, n_rows, n_cols, value, block_size=None):
        from .. import _validate_n

        _validate_n(n_rows, "n_rows")
        _validate_n(n_cols, "n_cols")
        return cls(n_rows, n_cols, value)

    def entries(self):
        from .. import range_table, int64, float64

        table = range_table(self.n_rows * self.n_cols)
        return table.key_by(
            i=int64(table.idx // self.n_cols), j=int64(table.idx % self.n_cols)
        ).select(entry=float64(self.value))
