#!/usr/bin/env python3
"""Prepare a deterministic, metadata-only genome reference manifest.

This records the contigs and coordinate metadata needed by future native
reference tables. It does not register a runtime reference or load sequences
into Vail.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any


def _pyfaidx():
    try:
        from pyfaidx import Fasta
    except ImportError as exc:
        raise RuntimeError(
            "pyfaidx is required; run with `uv run --with pyfaidx "
            "vail/scripts/prepare_reference_manifest.py ...`"
        ) from exc
    return Fasta


def _read_fai(path: Path) -> list[dict[str, Any]]:
    contigs = []
    seen = set()
    try:
        with path.open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                fields = line.rstrip("\n").split("\t")
                if len(fields) < 5:
                    raise ValueError(f"{path}:{line_number}: expected at least 5 tab-separated FAI columns")
                name = fields[0]
                try:
                    length = int(fields[1])
                    offset, line_bases, line_width = map(int, fields[2:5])
                except ValueError as exc:
                    raise ValueError(f"{path}:{line_number}: invalid integer in FAI row") from exc
                if not name or name in seen:
                    raise ValueError(f"{path}:{line_number}: empty or duplicate contig name {name!r}")
                if length <= 0 or offset < 0 or line_bases <= 0 or line_width < line_bases:
                    raise ValueError(f"{path}:{line_number}: invalid length or FASTA index values for {name!r}")
                seen.add(name)
                contigs.append({
                    "name": name,
                    "length": length,
                    "offset": offset,
                    "line_bases": line_bases,
                    "line_width": line_width,
                })
    except OSError as exc:
        raise ValueError(f"cannot read FASTA index {path}: {exc}") from exc
    if not contigs:
        raise ValueError(f"FASTA index {path} has no contigs")
    return contigs


def _load_metadata(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"cannot read reference metadata {path}: {exc}") from exc
    if not isinstance(value, dict) or not isinstance(value.get("contigs"), list):
        raise ValueError("reference metadata must be a Hail reference JSON object with a contigs array")
    return value


def _par_intervals(metadata: dict[str, Any], contigs: dict[str, int]) -> list[dict[str, Any]]:
    raw_par = metadata.get("par", [])
    if not isinstance(raw_par, list):
        raise ValueError("reference metadata 'par' must be an array")
    intervals = []
    for index, par in enumerate(raw_par):
        try:
            start = par["start"]
            end = par["end"]
            contig = start["contig"]
            end_contig = end["contig"]
            # Hail reference JSON stores PAR intervals as 1-based half-open ranges.
            start_pos = int(start["position"])
            end_pos = int(end["position"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"invalid PAR entry at index {index}; expected start/end contig and position") from exc
        if contig != end_contig:
            raise ValueError(f"PAR entry {index} must stay on one physical contig")
        if contig not in contigs:
            raise ValueError(f"PAR entry {index} refers to unknown contig {contig!r}")
        if start_pos < 1 or start_pos >= end_pos or end_pos > contigs[contig] + 1:
            raise ValueError(f"PAR entry {index} is outside {contig!r} bounds")
        intervals.append({"contig": contig, "start": start_pos - 1, "end": end_pos - 1})
    return intervals


def _name_list(metadata: dict[str, Any], key: str, contigs: dict[str, int]) -> list[str]:
    value = metadata.get(key, [])
    if not isinstance(value, list) or any(not isinstance(name, str) for name in value):
        raise ValueError(f"reference metadata {key!r} must be an array of contig names")
    if len(set(value)) != len(value):
        raise ValueError(f"reference metadata {key!r} contains duplicates")
    unknown = [name for name in value if name not in contigs]
    if unknown:
        raise ValueError(f"reference metadata {key!r} refers to unknown contigs: {unknown}")
    return value


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def prepare_manifest(
    fasta_path: str | Path,
    output_path: str | Path,
    name: str,
    metadata_path: str | Path | None = None,
    index_path: str | Path | None = None,
    *,
    hash_source: bool = False,
) -> dict[str, Any]:
    """Validate a FASTA/index and atomically write its metadata manifest.

    FASTA indexing and validation use pyfaidx, which keeps memory bounded by
    its index rather than loading the reference sequence. If metadata_path is
    supplied it must exactly describe all FASTA contigs and their lengths.
    """
    fasta = Path(fasta_path).expanduser().resolve()
    output = Path(output_path).expanduser().resolve()
    if not name or not isinstance(name, str):
        raise ValueError("reference name must be a non-empty string")
    if not fasta.is_file():
        raise ValueError(f"FASTA does not exist or is not a file: {fasta}")
    fai = Path(index_path).expanduser().resolve() if index_path else Path(str(fasta) + ".fai")

    Fasta = _pyfaidx()
    # pyfaidx creates the standard .fai when missing. It also validates that
    # the index can seek into this FASTA; the independent parse below validates
    # its contig names and declared lengths.
    try:
        if fai.exists():
            # Build an independent index from the FASTA bytes, then compare the
            # supplied index against it. This catches stale or hand-edited .fai
            # lengths while keeping sequence data out of memory.
            with tempfile.TemporaryDirectory(prefix="vail-fai-check-") as check_dir:
                actual_fasta = Fasta(
                    str(fasta), indexname=str(Path(check_dir) / "actual.fai"),
                    build_index=True, rebuild=True,
                )
                actual_fasta.close()
                actual_fai = _read_fai(Path(check_dir) / "actual.fai")
            supplied_fai = _read_fai(fai)
            if supplied_fai != actual_fai:
                raise ValueError(f"FASTA index {fai} does not match contigs or layout in {fasta}")
            fai_contigs = supplied_fai
        else:
            indexed_fasta = Fasta(str(fasta), indexname=str(fai), build_index=True, rebuild=False)
            indexed_fasta.close()
            fai_contigs = _read_fai(fai)
    except Exception as exc:
        if isinstance(exc, ValueError):
            raise
        raise ValueError(f"pyfaidx could not index or read {fasta}: {exc}") from exc

    metadata = _load_metadata(Path(metadata_path).expanduser().resolve() if metadata_path else None)
    contigs = [{"name": c["name"], "length": c["length"]} for c in fai_contigs]
    roles: dict[str, list[str]] = {"x_contigs": [], "y_contigs": [], "mt_contigs": []}
    par = []
    if metadata is not None:
        expected = []
        seen = set()
        for i, entry in enumerate(metadata["contigs"]):
            if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
                raise ValueError(f"metadata contig at index {i} must have a string name")
            contig_name = entry["name"]
            if contig_name in seen:
                raise ValueError(f"duplicate contig {contig_name!r} in reference metadata")
            seen.add(contig_name)
            try:
                length = int(entry["length"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ValueError(f"metadata contig {contig_name!r} has invalid length") from exc
            if length <= 0:
                raise ValueError(f"metadata contig {contig_name!r} must have positive length")
            expected.append({"name": contig_name, "length": length})
        if {c["name"]: c["length"] for c in expected} != {c["name"]: c["length"] for c in fai_contigs}:
            raise ValueError("reference metadata contig names/lengths do not exactly match the FASTA index")
        # The Hail metadata order is authoritative and is preserved in output.
        contigs = expected
        by_name = {c["name"]: c["length"] for c in contigs}
        for output_key, metadata_key in (("x_contigs", "xContigs"), ("y_contigs", "yContigs"), ("mt_contigs", "mtContigs")):
            roles[output_key] = _name_list(metadata, metadata_key, by_name)
        par = _par_intervals(metadata, by_name)

    identity = {
        "contigs": contigs,
        "x_contigs": roles["x_contigs"],
        "y_contigs": roles["y_contigs"],
        "mt_contigs": roles["mt_contigs"],
        "par_intervals": par,
        "coordinate_conventions": {
            "points": "1-based-closed",
            "sequence_intervals": "0-based-half-open",
            "par_input": "Hail reference JSON 1-based half-open positions converted to 0-based-half-open",
        },
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "name": name,
        "identity_sha256": hashlib.sha256(canonical).hexdigest(),
        "source": {"fasta_path": str(fasta), "fai_path": str(fai)},
        **identity,
    }
    if metadata_path is not None:
        manifest["source"]["metadata_path"] = str(Path(metadata_path).expanduser().resolve())
    if hash_source:
        manifest["source"]["fasta_sha256"] = _sha256_file(fasta)

    output.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=output.parent, prefix=f".{output.name}.", delete=False) as stream:
            temp_name = stream.name
            json.dump(manifest, stream, indent=2, ensure_ascii=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp_name, output)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
    return manifest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("fasta", type=Path, help="reference FASTA; missing .fai is built with pyfaidx")
    parser.add_argument("output", type=Path, help="manifest JSON output path")
    parser.add_argument("--name", required=True, help="human-readable reference name")
    parser.add_argument("--metadata", type=Path, help="optional Hail reference JSON metadata")
    parser.add_argument("--index", type=Path, help="optional FASTA index path (default FASTA.fai)")
    parser.add_argument("--hash-source", action="store_true", help="also hash the FASTA bytes (may take time)")
    args = parser.parse_args(argv)
    try:
        result = prepare_manifest(args.fasta, args.output, args.name, args.metadata, args.index, hash_source=args.hash_source)
    except (ValueError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(f"wrote {args.output} ({len(result['contigs'])} contigs; identity sha256 {result['identity_sha256']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
