"""Lazy native VCF input. Python handles options and column metadata only."""
from __future__ import annotations

import json
import glob
import os
from urllib.parse import urlparse, unquote


def _local_path(path):
    path = os.fspath(path)
    if path.startswith("file:"):
        uri = urlparse(path)
        if uri.netloc not in ("", "localhost"):
            raise NotImplementedError("VCF input currently supports local paths")
        path = unquote(uri.path)
    if "://" in path:
        raise NotImplementedError("VCF input currently supports local paths")
    return os.path.abspath(path)


def _paths(paths):
    paths = [paths] if isinstance(paths, (str, os.PathLike)) else list(paths)
    result = []
    for path in paths:
        path = _local_path(path)
        matches = sorted(glob.glob(path)) if glob.has_magic(path) else [path]
        if not matches:
            raise FileNotFoundError(path)
        result.extend(matches)
    if not result:
        raise ValueError("at least one input path is required")
    return result


def get_vcf_metadata(path):
    from . import _native_session
    return _native_session().vcf_metadata(_local_path(path))


def import_vcf(
    path, force=False, force_bgz=False, header_file=None, min_partitions=None,
    drop_samples=False, call_fields=("PGT",), reference_genome="default",
    contig_recoding=None, array_elements_required=True, skip_invalid_loci=False,
    entry_float_type=None, filter=None, find_replace=None, n_partitions=None,
    block_size=None, _create_row_uids=False, _create_col_uids=False,
):
    from . import Table, _native_session, _schema_from_native, _validate_n
    from .types import tfloat32, tfloat64, tint64, tstr, tstruct
    from .reference_genome import resolve_reference
    from .matrixtable import MatrixIR, MatrixTable, _CID, _COL

    entry_float_type = tfloat64 if entry_float_type is None else entry_float_type
    if entry_float_type not in (tfloat32, tfloat64):
        raise TypeError("entry_float_type must be float32 or float64")
    for name, value in (("force", force), ("force_bgz", force_bgz), ("drop_samples", drop_samples),
                        ("array_elements_required", array_elements_required), ("skip_invalid_loci", skip_invalid_loci)):
        if type(value) is not bool:
            raise TypeError(f"{name} must be bool")
    for name, value in (("n_partitions", n_partitions), ("min_partitions", min_partitions), ("block_size", block_size)):
        if value is not None:
            _validate_n(value, name, positive=True)
    if isinstance(call_fields, str):
        raise TypeError("call_fields must be a sequence of strings")
    call_fields = tuple(call_fields)
    if not all(isinstance(name, str) for name in call_fields):
        raise TypeError("call_fields must be a sequence of strings")
    if find_replace is not None and (len(find_replace) != 2 or not all(isinstance(v, str) for v in find_replace)):
        raise TypeError("find_replace must be a pair of strings")
    reference = None if reference_genome is None else resolve_reference(reference_genome)
    options = dict(
        reference=None if reference is None else reference.to_wire(),
        contig_recoding=contig_recoding or {}, call_fields=sorted(set(call_fields) | {"GT"}),
        entry_float32=entry_float_type == tfloat32,
        array_elements_required=array_elements_required, skip_invalid_loci=skip_invalid_loci,
        drop_samples=drop_samples, filter=filter, find_replace=None if find_replace is None else tuple(find_replace),
    )
    paths = _paths(path)
    header_path = None if header_file is None else _local_path(header_file)
    try:
        native, samples, _ = _native_session().read_vcf(paths, header_path, options)
    except ValueError as error:
        from .utils import FatalError
        raise FatalError(str(error)) from error
    core = Table(native, _schema_from_native(native), ())
    cols = Table.parallelize(
        [{_CID: i, _COL: {"s": sample}} for i, sample in enumerate(samples)],
        schema=tstruct(**{_CID: tint64, _COL: tstruct(s=tstr)}),
    ).key_by()
    # The native parser emits exactly one ordered cell per header sample and
    # checks sample cardinality when entries are decoded. Avoid adding a generic
    # whole-entry alignment predicate that defeats source field projection.
    # Samples arrive in VCF header order. Preserve that order, and only attach
    # an ordering proof when the header itself establishes the key order.
    column_ordering = ("s",) if all(a <= b for a, b in zip(samples, samples[1:])) else None
    matrix = MatrixTable(MatrixIR(
        core, cols, row_key=("locus", "alleles"), col_key=("s",), _aligned=True,
        _column_ordering=column_ordering,
    ))
    core = matrix._sort_rows(matrix._mir.core, matrix._mir.row_key)
    matrix = matrix._copy(rows=core)
    partitions = n_partitions if n_partitions is not None else min_partitions
    if partitions is not None:
        matrix = matrix.repartition(partitions)
    return matrix
