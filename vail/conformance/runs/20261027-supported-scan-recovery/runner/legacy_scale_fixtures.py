"""Generate independently Hail-written legacy fixtures for row/column scaling.

Run with Hail 0.2.139 and its supported Java environment. Nothing is collected
from the table or matrix. --only accepts fixture IDs printed by --list.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import platform
import time
import uuid


GENERATOR_VERSION = 1


def atomic_json(path, value):
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    os.replace(temporary, path)


def progress(**fields):
    print(json.dumps(dict(timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                          **fields)), flush=True)


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('dimensions must be positive')
    return number


def shape(value):
    try:
        rows, cols = value.lower().split('x')
        return positive(rows), positive(cols)
    except (ValueError, argparse.ArgumentTypeError) as error:
        raise argparse.ArgumentTypeError('expected positive ROWSxCOLS') from error


def matrix_fixture(hl, rows, cols, partitions):
    matrix = hl.utils.range_matrix_table(rows, cols, n_partitions=partitions)
    # Murmur-style avalanche over independent row and column inputs. Intermediate
    # arithmetic is Int64; masking implements unsigned 32-bit multiplication.
    def mix(value):
        mask = hl.int64(0xffffffff)
        return hl.bind(lambda a: hl.bind(
            lambda b: hl.bind(lambda c: hl.bit_xor(c, hl.bit_rshift(c, 16)),
                             hl.bit_and(hl.bit_xor(b, hl.bit_rshift(b, 13)) * 0xc2b2ae35, mask)),
            hl.bit_and(hl.bit_xor(a, hl.bit_rshift(a, 16)) * 0x85ebca6b, mask)),
            hl.bit_and(value, mask))

    hashed = mix(hl.bit_xor(hl.int64(matrix.row_idx) * 0x9e3779b1,
                           hl.int64(matrix.col_idx) * 0x85ebca6b))
    # Bind once to avoid duplicating the hash expression for every annotation.
    # Small read depths dominate; a minority of PL/AD values cross the LEB byte
    # boundary. Independent prime moduli supply deterministic field missingness.
    entries = hl.bind(lambda h: hl.struct(
        GT=hl.or_missing(h % 97 != 0,
                         hl.call(hl.int32((h // 7) % 2), hl.int32((h // 19) % 2))),
        AD=hl.or_missing(h % 101 != 0,
                         hl.array([hl.int32((h // 3) % 80),
                                   hl.int32(hl.if_else(h % 29 == 0, 128 + h % 193,
                                                      (h // 83) % 80))])),
        DP=hl.or_missing(h % 103 != 0, hl.int32(1 + (h // 13) % 200)),
        GQ=hl.or_missing(h % 107 != 0, hl.int32((h // 23) % 100)),
        PL=hl.or_missing(h % 109 != 0,
                         hl.array([hl.int32(0), hl.int32(1 + (h // 31) % 180),
                                   hl.int32(1 + (h // 181) % 256)])),
    ), hashed)
    # Structs are defined; missingness is per field. Whole-entry filtering would
    # change the dense cell count and introduce a second scaling variable.
    return matrix.annotate_entries(**entries)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--table-rows', nargs='+', type=positive,
                        default=[1_000_000, 10_000_000, 100_000_000])
    parser.add_argument('--matrix-shapes', nargs='+', type=shape,
                        default=[(25_000, 2_500), (100_000, 2_500),
                                 (25_000, 10_000), (100_000, 10_000)])
    parser.add_argument('--partitions', type=positive, default=16)
    parser.add_argument('--only', nargs='+', help='fixture IDs, e.g. matrix_25000x2500')
    parser.add_argument('--skip-existing', action='store_true')
    parser.add_argument('--list', action='store_true', help='print selected fixture IDs without starting Hail')
    args = parser.parse_args()
    fixtures = [dict(id=f'table_{rows}', kind='table', rows=rows, columns=None,
                     cells=None, suffix='.ht') for rows in args.table_rows]
    fixtures += [dict(id=f'matrix_{rows}x{cols}', kind='matrix', rows=rows,
                      columns=cols, cells=rows * cols, suffix='.mt')
                 for rows, cols in args.matrix_shapes]
    fixtures = list({fixture['id']: fixture for fixture in fixtures}.values())
    if args.only:
        unknown = set(args.only) - {fixture['id'] for fixture in fixtures}
        if unknown:
            parser.error('unknown fixture IDs: ' + ', '.join(sorted(unknown)))
        fixtures = [fixture for fixture in fixtures if fixture['id'] in args.only]
    if args.list:
        for fixture in fixtures:
            progress(event='selected', **fixture)
        return
    args.output_dir = args.output_dir.expanduser().resolve()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    import hail as hl
    if not hl.__version__.startswith('0.2.139-') and hl.__version__ != '0.2.139':
        raise RuntimeError(f'expected Hail 0.2.139, found {hl.__version__}')
    hl.init(master='local[14]', quiet=True,
            log=str(args.output_dir / ('generation-' + uuid.uuid4().hex + '.log')))
    try:
        spark = hl.current_backend()._spark
        versions = dict(hail=hl.__version__, python=platform.python_version(),
                        spark=spark.version,
                        java=spark.sparkContext._jvm.java.lang.System.getProperty('java.version'))
        manifest_path = args.output_dir / 'manifest.json'
        manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else dict(fixtures={})
        for fixture in fixtures:
            identity = dict(generator_version=GENERATOR_VERSION, partitions=args.partitions,
                            **{key: value for key, value in fixture.items() if key != 'suffix'})
            path = args.output_dir / (fixture['id'] + fixture['suffix'])
            marker = args.output_dir / (fixture['id'] + '.complete.json')
            if marker.exists():
                completed = json.loads(marker.read_text())
                if completed['identity'] != identity or not (path / '_SUCCESS').is_file():
                    raise RuntimeError(f'completion record does not match requested fixture: {path}')
                if not args.skip_existing:
                    raise FileExistsError(f'completed fixture exists: {path}; use --skip-existing')
                manifest['fixtures'][fixture['id']] = completed
                atomic_json(manifest_path, manifest)
                progress(event='skipped', **completed)
                continue
            if path.exists():
                raise FileExistsError(f'unmarked fixture exists; inspect it without overwriting: {path}')
            temporary = args.output_dir / ('.' + fixture['id'] + '.building-' + uuid.uuid4().hex + fixture['suffix'])
            progress(event='building', path=str(path), **identity)
            start = time.perf_counter()
            if fixture['kind'] == 'table':
                data = hl.utils.range_table(fixture['rows'], n_partitions=args.partitions)
                data = data.annotate(x=hl.int64(data.idx) * 3, s=hl.str(data.idx),
                                     a=hl.range(8).map(lambda i: data.idx + i))
                schemas = dict(row=str(data.row.dtype), key=str(data.key.dtype),
                               globals=str(data.globals.dtype))
            else:
                data = matrix_fixture(hl, fixture['rows'], fixture['columns'], args.partitions)
                schemas = dict(row=str(data.row.dtype), row_key=str(data.row_key.dtype),
                               column=str(data.col.dtype), column_key=str(data.col_key.dtype),
                               entry=str(data.entry.dtype), globals=str(data.globals.dtype))
            data.write(str(temporary), overwrite=False)
            if not (temporary / '_SUCCESS').is_file():
                raise RuntimeError(f'Hail write has no success marker: {temporary}')
            build_seconds = time.perf_counter() - start
            # A fresh final directory is published only after Hail completes.
            if path.exists():
                raise FileExistsError(f'fixture appeared during generation: {path}')
            temporary.rename(path)
            files = [file for file in path.rglob('*') if file.is_file()]
            completed = dict(identity=identity, path=str(path), schemas=schemas,
                             file_bytes=sum(file.stat().st_size for file in files),
                             file_count=len(files), build_seconds=build_seconds,
                             versions=versions,
                             completed_at=datetime.datetime.now(datetime.timezone.utc).isoformat())
            atomic_json(marker, completed)
            manifest['fixtures'][fixture['id']] = completed
            atomic_json(manifest_path, manifest)
            progress(event='complete', **completed)
    finally:
        hl.stop()


if __name__ == '__main__':
    main()
