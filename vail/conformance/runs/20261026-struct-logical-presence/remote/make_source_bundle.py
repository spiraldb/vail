#!/usr/bin/env python3
"""Export tracked build/test source only; no Git history or compiled artifacts."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile

REMOTE = 'vail/conformance/runs/20261026-struct-logical-presence/remote'
EXTRA = 'vail/conformance/runs/20261026-list-element-reader-main-integration/public-tests'
PATHS = ['AGENTS.md', 'LICENSE', 'vail/Cargo.toml', 'vail/Cargo.lock',
         'vail/rust-toolchain.toml', 'vail/pyproject.toml', 'vail/uv.lock',
         'vail/LICENSE', 'vail/licenses', 'vail/crates', 'vail/patches',
         'vail/python', REMOTE, EXTRA]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', type=Path, required=True)
    parser.add_argument('--ref', required=True)
    parser.add_argument('--out', type=Path, required=True)
    args = parser.parse_args()
    repo = args.repo.resolve()
    revision = subprocess.check_output(['git', 'rev-parse', args.ref], cwd=repo, text=True).strip()
    out = args.out.resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    if out.exists():
        raise RuntimeError('Refuse to replace an existing source export')
    list_bytes = subprocess.check_output(['git', 'show', f'{revision}:{REMOTE}/public-tests.json'], cwd=repo)
    tests = json.loads(list_bytes)
    required = ['Cargo.toml', 'Cargo.lock', 'rust-toolchain.toml', 'pyproject.toml',
                'patches/vortex-layout/src/layouts/struct_/reader.rs',
                'crates/vail-table/tests/encoded_projection.rs',
                'crates/vail-table/tests/struct_field_projection_contract.rs',
                'crates/vail-table/tests/support/struct_presence_demand.rs',
                'crates/vail-table/tests/support/element_validity_reader_io.rs',
                'crates/vail-storage/tests/native_list_evolution.rs', *tests]
    files = {}
    for relative in required:
        data = subprocess.check_output(['git', 'show', f'{revision}:vail/{relative}'], cwd=repo)
        files[relative] = hashlib.sha256(data).hexdigest()
    manifest = {'source_revision': revision, 'files': files,
                'python_test_files': len(tests), 'minimum_python_cases': 431,
                'minimum_rust_cases': {'encoded_projection': 34,
                                      'struct_field_projection_contract': 4,
                                      'native_list_evolution': 2},
                'scope': 'final correctness gate; tiny timings remain limited observations'}
    with out.open('wb') as stream:
        subprocess.run(['git', 'archive', '--format=tar', revision, '--', *PATHS],
                       cwd=repo, stdout=stream, check=True)
    with tarfile.open(out, 'a') as archive:
        data = (json.dumps(manifest, indent=2) + '\n').encode()
        entry = tarfile.TarInfo(REMOTE + '/source-manifest.json')
        entry.size = len(data)
        entry.mode = 0o644
        archive.addfile(entry, io.BytesIO(data))
    with out.open('rb') as stream:
        checksum = hashlib.file_digest(stream, 'sha256').hexdigest()
    metadata = {'source_revision': revision, 'tar_path': str(out),
                'tar_bytes': out.stat().st_size, 'tar_sha256': checksum,
                'paths': PATHS, 'manifest': manifest}
    out.with_suffix(out.suffix + '.json').write_text(json.dumps(metadata, indent=2) + '\n')
    out.with_suffix(out.suffix + '.sha256').write_text(f'{checksum}  {out.name}\n')
    print(json.dumps({k: metadata[k] for k in ['source_revision', 'tar_bytes', 'tar_sha256']}))


if __name__ == '__main__':
    main()
