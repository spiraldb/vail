"""Public resource and execution-error contracts used by import workflows."""
import asyncio
from pathlib import Path

import pytest
import vail as hl


def test_temporary_paths_clean_up_after_failure(tmp_path):
    with pytest.raises(ValueError, match='body failed'):
        with hl.TemporaryFilename(prefix='sample-', suffix='.txt', dir=tmp_path) as name:
            path = Path(name)
            assert not path.exists()
            path.write_text('sample')
            raise ValueError('body failed')
    assert not path.exists()
    with hl.TemporaryDirectory(dir=tmp_path, ensure_exists=False) as name:
        directory = Path(name)
        assert not directory.exists()
        directory.mkdir()
        (directory / 'nested').mkdir()
        (directory / 'nested' / 'file').write_text('data')
    assert not directory.exists()


def test_async_temporary_file_and_import_error(tmp_path):
    async def use_file():
        async with hl.TemporaryFilename(dir=tmp_path) as name:
            path = Path(name)
            path.write_text('id\tvalue\na\tbad\n')
            table = hl.import_table(name, types={'value': hl.tint32})
            with pytest.raises(hl.utils.FatalError, match='cannot parse'):
                table.collect()
        assert not path.exists()
    asyncio.run(use_file())


def test_genotype_probability_and_quality_helpers():
    assert hl.eval(hl.gp_dosage([0., .5, .5])) == 1.5
    for values, expected in [([0, 1352, 1569], 99), ([100, 101], 0), ([], 0), ([None, 5], 94)]:
        assert hl.eval(hl.gq_from_pl(hl.literal(values, hl.tarray(hl.tint32)))) == expected
    assert hl.eval(hl.gq_from_pl(hl.missing(hl.tarray(hl.tint32)))) is None
