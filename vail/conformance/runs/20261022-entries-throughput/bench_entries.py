"""Existing exact profile25 fixture; no fixture generation; fresh process per action."""
import argparse
import contextlib
import io
import json
import resource
import time
import vail as hl

parser = argparse.ArgumentParser()
parser.add_argument('action', choices=('count', 'show'))
parser.add_argument('--fixture', default='/tmp/entries-profile25.mt')
parser.add_argument('--output', required=True)
args = parser.parse_args()
start_cpu = time.process_time()
start = time.perf_counter()
mt = hl.read_matrix_table(args.fixture)
entries = mt.entries()
if args.action == 'count':
    result = entries._force_count()
    assert result == 63_083_475, result
else:
    # Match upstream .show(); Vail chooses its normal terminal-sized prefix. Capture rendering, not terminal I/O.
    with contextlib.redirect_stdout(io.StringIO()) as output:
        entries.show()
    result = len(output.getvalue())
wall = time.perf_counter() - start
cpu = time.process_time() - start_cpu
with open(args.output, 'w') as stream:
    json.dump({'action': args.action, 'wall_seconds': wall, 'core_seconds': cpu,
               'max_rss_bytes': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
               'result': result, 'site': hl.__file__, 'fixture': args.fixture}, stream, indent=2)
print(wall, cpu, result)
