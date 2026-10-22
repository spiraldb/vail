"""Sparse split reference/variant datasets; see README.md for supported scope."""

from .variant_dataset import VariantDataset, read_vds, store_ref_block_max_length
from .functions import lgt_to_gt, local_to_global
from .methods import (
    to_dense_mt,
    to_merged_sparse_mt,
    filter_variants,
    filter_samples,
    filter_chromosomes,
)
from . import combiner

__all__ = [
    "VariantDataset",
    "read_vds",
    "store_ref_block_max_length",
    "lgt_to_gt",
    "local_to_global",
    "to_dense_mt",
    "to_merged_sparse_mt",
    "filter_variants",
    "filter_samples",
    "filter_chromosomes",
    "combiner",
]
