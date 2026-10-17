"""Summarize sampled native stacks without rendering logical plans or values."""
from bisect import bisect_right
from collections import Counter
from functools import lru_cache
import gzip
import json
from pathlib import Path

root = Path(__file__).parent
summaries = {}
for path in sorted(root.glob('raw-seed-*-profile.json.gz')):
    with gzip.open(path) as stream:
        profile = json.load(stream)
    symbols = json.loads(path.with_suffix('').with_suffix('.json.syms.json').read_text())
    libraries = {item['debug_name']: item['symbol_table'] for item in symbols['data']}
    addresses = {name: [item['rva'] for item in table] for name, table in libraries.items()}
    threads = []
    for thread in profile['threads']:
        leaves, categories = Counter(), Counter()
        stacks = thread['stackTable']
        frames, functions, strings = thread['frameTable'], thread['funcTable'], thread['stringArray']
        @lru_cache(None)
        def name(index):
            frame = stacks['frame'][index]
            function = frames['func'][frame]
            fallback = strings[functions['name'][function]]
            resource = functions['resource'][function]
            if resource is None or resource < 0:
                return fallback
            library = thread['resourceTable']['lib'][resource]
            if library is None:
                return fallback
            label = profile['libs'][library]['debugName']
            table = libraries.get(label)
            if not table:
                return fallback
            address = frames['address'][frame]
            position = bisect_right(addresses[label], address) - 1
            if position < 0:
                return fallback
            item = table[position]
            if address >= item['rva'] + item['size']:
                return fallback
            return symbols['string_table'][item['symbol']]
        samples = [stack for stack in thread['samples']['stack'] if stack is not None]
        for stack in samples:
            leaves[name(stack)] += 1
            names = []
            while stack is not None:
                names.append(name(stack))
                stack = stacks['prefix'][stack]
            joined = '\n'.join(names)
            for category, text in [('optimizer','datafusion_optimizer'),('list_hash','hash_nested_array'),('formatting','ArrayFormatter'),('arrow_take','arrow_select::take'),('array_repeat','array_repeat')]:
                if text in joined:
                    categories[category] += 1
        if samples:
            threads.append({'thread':thread['name'], 'samples':len(samples), 'inclusive_categories':dict(categories), 'top_leaves':leaves.most_common(12)})
    summaries[path.name] = threads
(root / 'raw-seed-hotspots.json').write_text(json.dumps(summaries, indent=2) + '\n')
for label, threads in summaries.items():
    print(label)
    for thread in threads:
        print(thread['thread'],thread['samples'],thread['inclusive_categories'],thread['top_leaves'][:3])
