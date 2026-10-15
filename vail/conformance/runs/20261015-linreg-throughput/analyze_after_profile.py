"""Resolve and summarize the samply profile's Rust symbol sidecar."""
import argparse
import bisect
import collections
import gzip
import json
from pathlib import Path

HERE = Path(__file__).parent
parser = argparse.ArgumentParser()
parser.add_argument('--profile', type=Path, default=HERE / 'after-profile.json.gz')
parser.add_argument('--output', type=Path, default=HERE / 'after-hotspots.json')
args = parser.parse_args()
profile = json.load(gzip.open(args.profile, 'rt'))
sidecar = json.loads(Path(str(args.profile).replace('.json.gz', '.json.syms.json')).read_text())
symbols_by_code = {}
for lib in sidecar['data']:
    entries = sorted(lib.get('symbol_table', []), key=lambda item: item['rva'])
    symbols_by_code[lib['code_id']] = (entries, [item['rva'] for item in entries])

def analyze_thread(thread):
    frame_table = thread['frameTable']
    function_table = thread['funcTable']
    resource_table = thread['resourceTable']
    strings = thread['stringArray']
    cache = {}
    def name_for_frame(frame):
        if frame in cache:
            return cache[frame]
        func = frame_table['func'][frame]
        name = strings[function_table['name'][func]] if func is not None else '<no function>'
        if name.startswith('0x'):
            resource = function_table['resource'][func]
            if resource is not None:
                lib_index = resource_table['lib'][resource]
                lib = profile['libs'][lib_index]
                match = symbols_by_code.get(lib['codeId'])
                if match:
                    entries, starts = match
                    address = frame_table['address'][frame]
                    pos = bisect.bisect_right(starts, address) - 1
                    if pos >= 0 and address < entries[pos]['rva'] + entries[pos]['size']:
                        name = sidecar['string_table'][entries[pos]['symbol']]
        cache[frame] = name
        return name

    leaves = collections.Counter()
    inclusive = collections.Counter()
    sample_has = collections.Counter()
    leaf_categories = collections.Counter()
    total_depth = 0
    samples = thread['samples']['stack']
    for sample in samples:
        node = sample
        names = []
        while node is not None:
            names.append(name_for_frame(thread['stackTable']['frame'][node]))
            node = thread['stackTable']['prefix'][node]
        total_depth += len(names)
        stack_text = '\n'.join(names)
        inclusive.update(set(names))
        leaves[names[0]] += 1
        for label, needle in (
            ('malloc', 'malloc'), ('free', 'free'), ('rust_alloc', 'alloc::'),
        ):
            if needle in names[0]:
                leaf_categories[label] += 1
        for label, needle in (
            ('ensure_requirements', 'datafusion_physical_optimizer::ensure_requirements'),
            ('physical_optimizer', 'datafusion_physical_optimizer::'),
            ('logical_optimizer', 'datafusion_optimizer::'),
            ('execution_plan', 'datafusion_physical_plan::'),
            ('array_expression', 'arrow_arith::'),
            ('linreg', 'LinregAccumulator'),
            ('coordinate', 'array_aggregate::'),
            ('lambda', 'higher_order_function'),
            ('take', 'arrow_select::take'),
        ):
            if needle in stack_text:
                sample_has[label] += 1
    return {
        'thread': thread['name'], 'samples': len(samples),
        'duration_ms': thread['samples']['time'][-1] - thread['samples']['time'][0] if samples else 0,
        'mean_stack_depth': total_depth / len(samples) if samples else 0,
        'sample_has': dict(sample_has),
        'leaf_categories': dict(leaf_categories),
        'top_inclusive_symbols': [{'name':n,'samples':c,'percent':round(100*c/len(samples),2)} for n,c in inclusive.most_common(100)],
        'top_leaf_symbols': [
            {'name': name, 'samples': count, 'percent': round(100 * count / len(samples), 2)}
            for name, count in leaves.most_common(30)
        ],
    }

threads = [analyze_thread(thread) for thread in profile['threads'] if thread['samples']['stack']]
threads.sort(key=lambda entry: entry['samples'], reverse=True)

args.output.write_text(json.dumps(threads,indent=2)+'\n')
for t in threads[:4]:
 print(t['thread'],t['samples'],t['sample_has']);print(*t['top_leaf_symbols'][:8],sep='\n')
