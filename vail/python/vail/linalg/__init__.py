"""Lazy constant block-matrix source for entry-table interoperability."""
from dataclasses import dataclass


@dataclass(frozen=True)
class BlockMatrix:
    n_rows: int
    n_cols: int
    value: object

    def __post_init__(self):
        from .. import _validate_n
        _validate_n(self.n_rows, "n_rows")
        _validate_n(self.n_cols, "n_cols")
        if self.n_rows > 2**31 - 1 or self.n_cols > 2**31 - 1 or self.n_rows * self.n_cols > 2**31 - 1:
            raise ValueError("BlockMatrix dimensions and entry count must fit int32")

    @classmethod
    def fill(cls, n_rows, n_cols, value, block_size=None):
        """Construct a constant matrix; physical block sizing is unsupported."""
        if block_size is not None:
            raise NotImplementedError("BlockMatrix block_size is not supported")
        return cls(n_rows, n_cols, value)

    def entries(self):
        from .. import range_table, int64, float64
        table = range_table(self.n_rows * self.n_cols).key_by()
        # Validated dimensions make idx -> (i, j) strictly row-major.
        columns = max(self.n_cols, 1)
        result = table.select(
            i=int64(table.idx // columns), j=int64(table.idx % columns),
            entry=float64(self.value),
        )
        return result._with_keys('i', 'j')


from . import utils
