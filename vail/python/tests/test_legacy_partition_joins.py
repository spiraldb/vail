"""Public joins over genuine Hail-written partitions and Hail result oracles."""
from collections import Counter
import hashlib
import json
from pathlib import Path
import tarfile

import pytest
import vail as hl


@pytest.fixture(scope="module")
def hail_join_data(tmp_path_factory):
    root = tmp_path_factory.mktemp("hail-joins")
    archive = Path(__file__).parent / "data/legacy-join/hail-join-fixtures.tar.gz"
    with tarfile.open(archive) as source:
        source.extractall(root, filter="data")
    manifest = json.loads((root / "manifest.json").read_text())
    for relative, expected in manifest["files"].items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == expected
    return root


@pytest.mark.parametrize("name", ["range", "duplicates", "int64", "empty"])
@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
def test_hail_partition_join_matches_hail(hail_join_data, name, how):
    root = hail_join_data
    left = hl.Table.read_legacy(str(root / f"{name}-left.ht"))
    right = hl.Table.read_legacy(str(root / f"{name}-right.ht"))
    result = left.join(right, how)
    expected = json.loads((root / "expected.json").read_text())[name][how]
    assert result._force_count() == len(expected)
    assert Counter((row.k, row.lv, row.rv) for row in result.collect()) == Counter(
        (row["k"], row["lv"], row["rv"]) for row in expected
    )


def test_hail_partition_join_after_value_projection_and_filter(hail_join_data):
    root = hail_join_data
    left = hl.Table.read_legacy(str(root / "range-left.ht"))
    right = hl.Table.read_legacy(str(root / "range-right.ht"))
    left = left.annotate(lv=left.lv + 100)
    right = right.filter(right.k % 2 == 0)
    result = left.join(right)
    assert [(row.k, row.lv, row.rv) for row in result.collect()] == [
        (key, key + 100, key - 13) for key in range(14, 80, 2)
    ]


def test_hail_partition_membership_preserves_left_multiplicity(hail_join_data):
    root = hail_join_data
    left = hl.Table.read_legacy(str(root / "duplicates-left.ht"))
    right = hl.Table.read_legacy(str(root / "duplicates-right.ht"))
    assert [(row.k, row.lv) for row in left.semi_join(right).collect()] == [
        (4, 2), (4, 3), (7, 4),
    ]
    assert [(row.k, row.lv) for row in left.anti_join(right).collect()] == [
        (0, 0), (2, 1), (10, 5), (14, 6), (None, 7),
    ]
