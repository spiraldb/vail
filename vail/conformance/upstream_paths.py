"""Paths shared by the upstream test adapters."""
import os
from pathlib import Path


def doctest_data_dir(suite):
    """Resolve doctest data independently of the worker's current directory."""
    default = Path(suite).resolve().parents[1] / 'hail' / 'docs' / 'data'
    return str(Path(os.environ.get('HAIL_DOCTEST_DATA_DIR', default)).expanduser().resolve())
