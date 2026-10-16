"""Scope policy for full upstream audits (requested by Daniel, 2026-10-15)."""
from pathlib import PurePosixPath


def exclusion_reason(module, name=None):
    path = PurePosixPath(str(module))
    if "typecheck" in path.parts or path.name == "test_typecheck.py":
        return "Python typecheck decorator tests are out of scope"
    if "fs" in path.parts or path.name == "test_hl_hadoop_and_hail_fs.py":
        return "Hadoop open and Hail filesystem compatibility are out of scope"
    if name is not None and name.rsplit('.', 1)[-1].split('[', 1)[0].startswith('test_hadoop_'):
        return "Hadoop open and Hail filesystem compatibility are out of scope"
    return None
