import importlib.util
import json
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "prepare_reference_manifest.py"
spec = importlib.util.spec_from_file_location("prepare_reference_manifest", SCRIPT)
manifest_module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(manifest_module)

pytest.importorskip("pyfaidx")


def write_reference(tmp_path, *, fasta_name="tiny.fa"):
    tmp_path.mkdir(parents=True, exist_ok=True)
    fasta = tmp_path / fasta_name
    fasta.write_text(">chrX\nACGTACGTAA\n>chrY\nTTTTCCCC\n>chrM\nACGT\n", encoding="ascii")
    metadata = tmp_path / "reference.json"
    metadata.write_text(json.dumps({
        "name": "tiny",
        "contigs": [
            {"name": "chrX", "length": 10},
            {"name": "chrY", "length": 8},
            {"name": "chrM", "length": 4},
        ],
        "xContigs": ["chrX"],
        "yContigs": ["chrY"],
        "mtContigs": ["chrM"],
        "par": [
            {"start": {"contig": "chrX", "position": 2}, "end": {"contig": "chrX", "position": 5}},
        ],
    }), encoding="utf-8")
    return fasta, metadata


def test_manifest_uses_hail_order_and_physical_half_open_par(tmp_path):
    fasta, metadata = write_reference(tmp_path)
    output = tmp_path / "out" / "reference.json"
    result = manifest_module.prepare_manifest(fasta, output, "Example", metadata)

    assert [c["name"] for c in result["contigs"]] == ["chrX", "chrY", "chrM"]
    assert result["par_intervals"] == [{"contig": "chrX", "start": 1, "end": 4}]
    assert result["coordinate_conventions"]["points"] == "1-based-closed"
    assert result["coordinate_conventions"]["sequence_intervals"] == "0-based-half-open"
    assert json.loads(output.read_text()) == result
    assert Path(str(fasta) + ".fai").is_file()


def test_identity_ignores_reference_name_and_input_path(tmp_path):
    fasta, metadata = write_reference(tmp_path / "one")
    first = manifest_module.prepare_manifest(fasta, tmp_path / "first.json", "Name A", metadata)
    fasta2, metadata2 = write_reference(tmp_path / "two")
    second = manifest_module.prepare_manifest(fasta2, tmp_path / "second.json", "Name B", metadata2)
    assert first["identity_sha256"] == second["identity_sha256"]


def test_metadata_must_match_all_fasta_contigs(tmp_path):
    fasta, metadata = write_reference(tmp_path)
    value = json.loads(metadata.read_text())
    value["contigs"].pop()
    metadata.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="exactly match"):
        manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny", metadata)


def test_unknown_par_contig_is_rejected(tmp_path):
    fasta, metadata = write_reference(tmp_path)
    value = json.loads(metadata.read_text())
    value["par"][0]["start"]["contig"] = "chrZ"
    value["par"][0]["end"]["contig"] = "chrZ"
    metadata.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="unknown contig"):
        manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny", metadata)


def test_out_of_bounds_par_is_rejected(tmp_path):
    fasta, metadata = write_reference(tmp_path)
    value = json.loads(metadata.read_text())
    value["par"][0]["end"]["position"] = 12
    metadata.write_text(json.dumps(value))
    with pytest.raises(ValueError, match="outside"):
        manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny", metadata)


def test_existing_fai_is_checked_against_fasta(tmp_path):
    fasta, _ = write_reference(tmp_path)
    fai = Path(str(fasta) + ".fai")
    # Build a valid index first, then corrupt its declared sequence length.
    manifest_module.prepare_manifest(fasta, tmp_path / "initial.json", "tiny")
    fields = fai.read_text().splitlines()
    first = fields[0].split("\t")
    first[1] = str(int(first[1]) + 1)
    fields[0] = "\t".join(first)
    fai.write_text("\n".join(fields) + "\n")
    with pytest.raises(ValueError, match="does not match"):
        manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny")


def test_par_end_at_contig_length_plus_one_is_valid(tmp_path):
    fasta, metadata = write_reference(tmp_path)
    value = json.loads(metadata.read_text())
    value["par"][0]["start"]["position"] = 9
    value["par"][0]["end"]["position"] = 11
    metadata.write_text(json.dumps(value))
    result = manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny", metadata)
    assert result["par_intervals"] == [{"contig": "chrX", "start": 8, "end": 10}]


def test_existing_fai_layout_fields_are_checked(tmp_path):
    fasta, _ = write_reference(tmp_path)
    fai = Path(str(fasta) + ".fai")
    manifest_module.prepare_manifest(fasta, tmp_path / "initial.json", "tiny")
    fields = fai.read_text().splitlines()
    first = fields[0].split("\t")
    first[2] = str(int(first[2]) + 1)  # Offset is the third standard FAI column.
    fields[0] = "\t".join(first)
    fai.write_text("\n".join(fields) + "\n")
    with pytest.raises(ValueError, match="does not match"):
        manifest_module.prepare_manifest(fasta, tmp_path / "out.json", "tiny")
