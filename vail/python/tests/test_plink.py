from pathlib import Path
import pytest
import vail as hl
from vail.plink import import_plink, export_plink


def files(tmp_path, data=bytes([0x6C, 0x1B, 1, 0xE4, 0x03]), bim="1 rs1 2.5 10 A G\n"):
    p = tmp_path / "data"
    p.with_suffix(".bed").write_bytes(data)
    p.with_suffix(".bim").write_text(bim)
    p.with_suffix(".fam").write_text(
        "f z 0 0 1 1\nf a 0 0 2 2\nf y 0 0 0 -9\nf b 0 0 0 0\nf x 0 0 0 0\n"
    )
    return tuple(str(p.with_suffix(s)) for s in (".bed", ".bim", ".fam"))


def test_specification_codes_and_sample_order(tmp_path):
    mt = import_plink(*files(tmp_path))
    rows = mt.localize_entries("e", "c").collect()
    columns = hl.eval(mt.localize_entries("e", "c").index_globals().c)
    assert [c.s for c in columns] == ["z", "a", "y", "b", "x"]
    assert [e.GT for e in rows[0].e] == [
        hl.Call([1, 1]),
        None,
        hl.Call([0, 1]),
        hl.Call([0, 0]),
        hl.Call([0, 0]),
    ]
    assert rows[0].alleles == ["G", "A"]
    assert rows[0].cm_position == 2.5
    flipped = import_plink(*files(tmp_path), a2_reference=False)
    assert flipped.rows().collect()[0].alleles == ["A", "G"]


def test_roundtrip_and_projection(tmp_path):
    paths = files(tmp_path)
    mt = import_plink(*paths)
    output = str(tmp_path / "out")
    export_plink(
        mt,
        output,
        fam_id=mt.fam_id,
        ind_id=mt.s,
        is_female=mt.is_female,
        pheno=mt.is_case,
        varid=mt.rsid,
        cm_position=mt.cm_position,
    )
    rt = import_plink(*(output + s for s in (".bed", ".bim", ".fam")))
    assert mt._same(rt)
    assert rt.rows().count() == 1
    assert rt.count_cols() == 5


def test_invalid_loci_and_bad_dimensions(tmp_path):
    paths = files(tmp_path, bim="bad rs1 0 10 A G\n")
    assert import_plink(*paths, skip_invalid_loci=True).count_rows() == 0
    with pytest.raises(Exception, match="Invalid locus"):
        import_plink(*paths).count_rows()
    paths = files(tmp_path, data=bytes([0x6C, 0x1B, 1, 0]))
    with pytest.raises(Exception, match="PLINK|truncated"):
        import_plink(*paths).count_rows()


def test_export_preserves_float_precision(tmp_path):
    mt = import_plink(*files(tmp_path))
    output = str(tmp_path / "precision")
    value = 1.2345678910111213
    export_plink(mt, output, cm_position=value, pheno=value)
    assert float(Path(output + ".bim").read_text().split()[2]) == value
    assert all(
        float(line.split()[5]) == value
        for line in Path(output + ".fam").read_text().splitlines()
    )
