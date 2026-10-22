import pytest
import vail as hl


def fixture():
    r = hl.utils.range_matrix_table(2, 2, n_partitions=2)
    r = (
        r.annotate_rows(locus=hl.locus("1", hl.array([1, 4])[r.row_idx]))
        .key_rows_by("locus")
        .select_rows()
    )
    r = r.annotate_cols(s=hl.str(r.col_idx)).key_cols_by("s").select_cols()
    r = r.annotate_entries(LEN=5, GT=hl.call(0, 0), DP=10)
    r = r.filter_entries(
        (r.locus.position == 1) & (r.s == "0") | (r.locus.position == 4) & (r.s == "1")
    )
    v = hl.utils.range_matrix_table(5, 2, n_partitions=3)
    v = v.annotate_rows(
        locus=hl.locus(
            hl.if_else(v.row_idx == 4, "2", "1"), hl.array([2, 3, 3, 6, 1])[v.row_idx]
        ),
        alleles=hl.array(["A", "C"]),
    )
    v = v.key_rows_by("locus", "alleles").select_rows()
    v = v.annotate_cols(s=hl.str(v.col_idx)).key_cols_by("s").select_cols()
    v = v.annotate_entries(GT=hl.call(0, 1), DP=99).filter_entries(False)
    return hl.vds.VariantDataset(r, v)


@pytest.mark.parametrize("without_ref_gt", [False, True])
def test_dense(without_ref_gt):
    v = fixture()
    if without_ref_gt:
        v = hl.vds.VariantDataset(v.reference_data.drop("GT"), v.variant_data)
    d = hl.vds.to_dense_mt(v)
    rows = d.localize_entries("entries", "cols").collect()
    assert len(rows) == 5
    assert [[None if e is None else e.DP for e in r.entries] for r in rows] == [
        [10, None],
        [10, None],
        [10, None],
        [None, 10],
        [None, None],
    ]


def test_roundtrip(tmp_path):
    v = fixture()
    v.validate()
    v.write(str(tmp_path / "vds"))
    w = hl.vds.read_vds(str(tmp_path / "vds"))
    w.validate()
    assert w.n_samples() == 2
    assert w.reference_data.count_rows() == 2


def test_sparse_roundtrip():
    v = fixture()
    merged = hl.vds.to_merged_sparse_mt(v, ref_allele_function=lambda row: hl.str("A"))
    out = hl.vds.VariantDataset.from_merged_representation(
        merged,
        ref_block_indicator_field="LEN",
        is_split=True,
        infer_ref_block_fields=False,
        ref_block_fields=["GT", "DP"],
    )
    assert out.reference_data.count_rows() == 2
    assert out.variant_data.count_rows() == 0


def test_filter_chromosomes():
    v = fixture()
    w = hl.vds.filter_chromosomes(v, keep="1")
    assert w.variant_data.count_rows() == 4
    assert w.reference_data.count_rows() == 2


def test_haploid_lpl_import():
    mt = hl.utils.range_matrix_table(1, 1)
    mt = mt.annotate_rows(
        alleles=["A", "T", "<NON_REF>"], info=hl.struct(END=hl.missing(hl.tint32))
    )
    mt = mt.annotate_entries(GT=hl.call(1), PL=[10, 0, 1000])
    mt = mt.transmute_rows(
        locus=hl.locus("chrX", mt.row_idx + 1000, reference_genome="GRCh38")
    )
    mt = mt.transmute_cols(s=hl.str(mt.col_idx))
    mt = mt.key_rows_by("locus", "alleles").key_cols_by("s")
    vds = hl.vds.combiner.transform_gvcf(mt, reference_entry_fields_to_keep=[])
    vd = vds.variant_data
    lpl = vd.aggregate_entries(hl.agg.collect(vd.LPL))
    lpl = lpl[0]
    assert lpl == [10, 0]


def single(sample, alt):
    mt = hl.utils.range_matrix_table(2, 1)
    mt = mt.annotate_rows(
        locus=hl.locus("1", mt.row_idx * 2 + 1),
        alleles=hl.if_else(
            mt.row_idx == 0, ["A", "<NON_REF>"], ["A", alt, "<NON_REF>"]
        ),
        info=hl.struct(END=hl.or_missing(mt.row_idx == 0, 5)),
    )
    mt = mt.key_rows_by("locus", "alleles").select_rows("info")
    mt = mt.annotate_cols(s=sample).key_cols_by("s").select_cols()
    mt = mt.annotate_entries(
        GT=hl.if_else(mt.locus.position == 1, hl.call(0, 0), hl.call(0, 1)), DP=10
    )
    return hl.vds.combiner.transform_gvcf(mt, ["DP", "GT"])


def test_combine():
    combined = hl.vds.combiner.combine_variant_datasets(
        [single("a", "C"), single("b", "G")]
    )
    assert combined.n_samples() == 2
    assert combined.variant_data.count_rows() == 1
    rows = combined.variant_data.localize_entries("entries", "cols").collect()
    row = rows[0]
    assert row.alleles[0] == "A" and set(row.alleles[1:]) == {"C", "G"}
    assert [row.alleles[e.LA[1]] for e in row.entries] == ["C", "G"]
    assert [e.LGT for e in row.entries] == [hl.Call([0, 1]), hl.Call([0, 1])]
    combined.validate()


def test_filter_samples_and_variants():
    v = fixture()
    selected = hl.vds.filter_samples(v, ["0"])
    assert selected.n_samples() == 1
    assert selected.reference_data.cols().collect() == [hl.Struct(s="0")]
    selected.validate()
    variants = v.variant_data.rows()
    variants = variants.filter(variants.locus.position == 3)
    filtered = hl.vds.filter_variants(v, variants)
    assert filtered.variant_data.count_rows() == 2
    assert filtered.reference_data.count_rows() == 2


def test_read_partition_planning_is_explicitly_unsupported():
    with pytest.raises(NotImplementedError, match="partition planning"):
        hl.vds.read_vds("unused", n_partitions=3)


def test_fresh_hail_dense_golden():
    # The fixture is extracted using Hail itself, not reconstructed by Vail.
    # Struct field order is not stable across Hail processes (its implementation
    # iterates a Python set); compare entry field names/types and named values.
    import runpy
    from pathlib import Path
    import math

    adapter = runpy.run_path(
        str(Path(__file__).parents[2] / "conformance" / "vds_fixture.py")
    )
    name = "1kg_chr22_5_samples.vds"
    expected = adapter["matrix"](name, "dense")
    actual = hl.vds.to_dense_mt(adapter["vds"](name))
    assert actual.row.dtype == expected.row.dtype
    assert dict(actual.entry.dtype.items()) == dict(expected.entry.dtype.items())
    actual_table = actual.localize_entries("entries", "cols")
    expected_table = expected.localize_entries("entries", "cols")
    assert hl.eval(actual_table.index_globals()) == hl.eval(
        expected_table.index_globals()
    )

    def equivalent(a, b):
        if (
            isinstance(a, float)
            and isinstance(b, float)
            and math.isnan(a)
            and math.isnan(b)
        ):
            return True
        if hasattr(a, "items") and hasattr(b, "items"):
            return set(a) == set(b) and all(equivalent(a[k], b[k]) for k in a)
        if isinstance(a, (list, tuple)) and isinstance(b, (list, tuple)):
            return len(a) == len(b) and all(
                equivalent(x, y) for x, y in zip(a, b, strict=True)
            )
        return a == b

    assert equivalent(actual_table.collect(), expected_table.collect())
