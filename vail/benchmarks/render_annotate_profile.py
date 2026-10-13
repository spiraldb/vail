"""Render measured-interval, CPU-weighted samply stacks and 10-Hz telemetry."""
from __future__ import annotations
import argparse
import bisect
from collections import Counter
import csv
import gzip
import hashlib
import html
import json
from pathlib import Path
import statistics


def stacks(folder):
    profile = json.loads(gzip.decompress((folder / 'profile.json.gz').read_bytes()))
    symbols = json.loads((folder / 'profile.json.syms.json').read_text())
    events = json.loads((folder / 'iterations.json').read_text())
    intervals = [(e['start_wall_ns']/1e6 - profile['meta']['startTime'],
                  e['end_wall_ns']/1e6 - profile['meta']['startTime']) for e in events if e['phase'] == 'measured']
    # Index by debug name and UUID so two different binaries cannot be conflated.
    def identity(name, identifier):
        return name, identifier.replace('-', '').upper()[:32]
    libraries = {identity(l['debug_name'], l['debug_id']):
        (l['symbol_table'], [s['rva'] for s in l['symbol_table']]) for l in symbols['data']}
    folded = Counter(); leaf = Counter(); inclusive = Counter(); threads = Counter()
    for thread in profile['threads']:
        frames = thread['frameTable']; functions = thread['funcTable']; resources = thread['resourceTable']
        names = []
        for index, address in enumerate(frames['address']):
            function = frames['func'][index]
            name = thread['stringArray'][functions['name'][function]]
            resource = functions['resource'][function]
            library = resources['lib'][resource] if resource is not None else None
            if library is not None:
                lib = profile['libs'][library]
                table, addresses = libraries.get(identity(lib['debugName'], lib['breakpadId']), ([], []))
                position = bisect.bisect_right(addresses, address) - 1
                if position >= 0 and address < table[position]['rva'] + table[position]['size']:
                    name = symbols['string_table'][table[position]['symbol']]
            names.append(name)
        cache = {None: ()}
        def stack(index):
            if index not in cache:
                cache[index] = stack(thread['stackTable']['prefix'][index]) + (names[thread['stackTable']['frame'][index]],)
            return cache[index]
        samples = thread['samples']
        for index, timestamp in enumerate(samples['time']):
            cpu = samples['threadCPUDelta'][index]
            if not cpu or not any(a <= timestamp <= b for a, b in intervals):
                continue
            trace = stack(samples['stack'][index])
            folded[trace] += cpu; leaf[trace[-1]] += cpu; threads[thread['name']] += cpu
            for name in set(trace):
                inclusive[name] += cpu
    total = sum(folded.values())
    result = {'sampled_cpu_seconds': total / 1e6, 'thread_cpu_microseconds': dict(threads),
        'leaf': [{'name': n, 'percent': w / total * 100} for n, w in leaf.most_common(30)],
        'inclusive': [{'name': n, 'percent': w / total * 100} for n, w in inclusive.most_common(50)],
        'stacks': [{'frames': list(trace), 'cpu_microseconds': weight} for trace, weight in folded.items()]}
    (folder / 'hotspots.json').write_text(json.dumps(result, indent=2) + '\n')
    (folder / 'collapsed.folded').write_text(''.join(';'.join(name.replace(';', ',').replace('\n', ' ') for name in trace) + f' {int(weight)}\n' for trace, weight in folded.items()))
    return folded, total


def label(name):
    if name.startswith('<datafusion_physical_expr::expressions::binary::BinaryExpr as '):
        return 'DataFusion BinaryExpr::evaluate'
    if 'projection::Projector>::project_batch' in name:
        return 'DataFusion projection: evaluate columns'
    if name.startswith('datafusion_physical_expr_common::datum::apply'):
        return 'DataFusion: Arrow arithmetic kernel'
    if name.startswith('<vail_execution::expressions::HailPhysicalExpr as '):
        return 'HailPhysicalExpr::evaluate' + (' closure' if 'closure' in name else '')
    if 'ScalarValue>::try_from_array' in name:
        return 'ScalarValue::try_from_array'
    if 'ScalarValue>::iter_to_array' in name:
        return 'ScalarValue::iter_to_array / array builder'
    if 'drop_glue' in name and 'ScalarValue' in name:
        return 'drop ScalarValue'
    if 'downcast_array_helper' in name:
        return 'Arrow: downcast array to Int32'
    if 'PrimitiveArray<' in name:
        return 'Arrow PrimitiveArray::' + name.split('>::')[-1]
    if 'Arc<dyn arrow_array' in name:
        return 'Arrow Arc<Array>::' + name.split('>::')[-1]
    if len(name) > 100:
        return name.split('>::')[-1][:100]
    return name


def workload_title(folder):
    worker = json.loads((folder / 'worker.json').read_text())
    return 'Matrix entry sum / 100K rows × 10 columns' if worker.get('workload') == 'matrix-entry-sum' else 'Annotate 100 fields / 1M rows'


def flame(folder, folded, total):
    title = workload_title(folder)
    # Remove Python/async wrapper frames in the overview; full stacks remain in
    # collapsed.folded and the native profile. Widths use sampled CPU, not idle time.
    root = {'weight': total, 'children': {}}
    for trace, weight in folded.items():
        index = next((i for i, name in enumerate(trace) if (name.startswith('<vail_execution::expressions::HailPhysicalExpr as ') and '::evaluate' in name) or 'projection::Projector>::project_batch' in name), None)
        view = trace[index:] if index is not None else ('Other native / planning', trace[-1])
        node = root
        for name in view:
            node = node['children'].setdefault(name, {'weight': 0, 'children': {}})
            node['weight'] += weight
    width = 1700; bar_height = 25; rectangles = []
    def layout(node, x, depth):
        offset = x
        for name, child in sorted(node['children'].items(), key=lambda x: (-x[1]['weight'], x[0])):
            w = (width - 40) * child['weight'] / total
            if w >= 0.5:
                rectangles.append((offset, depth, w, name, child['weight']))
                layout(child, offset, depth + 1)
            offset += w
    layout(root, 20, 0)
    levels = max(depth for _, depth, _, _, _ in rectangles) + 1
    height = 150 + levels * bar_height
    parts = [f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="#fff"/>',
        f'<text x="20" y="32" font-family="sans-serif" font-size="24" font-weight="bold">Vail: {title}</text>',
        '<text x="20" y="60" font-family="sans-serif" font-size="16">CPU flamegraph · measured runs only · 500 Hz native stack samples</text>',
        '<text x="20" y="85" font-family="sans-serif" font-size="14">Widths = CPU time; warmups and idle threads excluded. Python/async wrappers omitted from this overview.</text>']
    for x, depth, w, name, weight in rectangles:
        y = height - 45 - (depth + 1) * bar_height
        shade = int(hashlib.sha256(name.encode()).hexdigest()[:4], 16)
        color = f'rgb(255,{130 + shade % 85},{60 + shade % 65})'
        tooltip = html.escape(f'{name}\n{weight/total*100:.2f}% of measured CPU; {weight/1e6:.3f} sampled CPU seconds')
        short = label(name)
        available = max(0, int((w-8)/7))
        text = short if len(short) <= available else short[:max(0, available-1)] + '…'
        parts.append(f'<g><title>{tooltip}</title><rect x="{x:.2f}" y="{y}" width="{w:.2f}" height="24" fill="{color}" stroke="#fff" stroke-width="0.7"/>')
        if w > 30:
            parts.append(f'<text x="{x+4:.2f}" y="{y+17}" font-family="monospace" font-size="12">{html.escape(text)}</text>')
        parts.append('</g>')
    parts += ['<text x="20" y="'+str(height-12)+'" font-family="sans-serif" font-size="13">Read bottom to top; hover an SVG frame for its full symbol and CPU weight.</text>', '</svg>']
    (folder / 'flamegraph.svg').write_text('\n'.join(parts))
    # The raster preview uses the same tree/weights as the SVG.
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    from matplotlib.patches import Rectangle
    fig, ax = plt.subplots(figsize=(17, max(6, levels * .29 + 1.5)), dpi=140)
    for x, depth, w, name, weight in rectangles:
        shade = int(hashlib.sha256(name.encode()).hexdigest()[:4], 16)
        ax.add_patch(Rectangle((x, depth), w, .96, color=(1, (130+shade%85)/255, (60+shade%65)/255), linewidth=.5, ec='white'))
        text = label(name); n = max(0, int((w-8)/8))
        if w > 45:
            ax.text(x+4, depth+.48, text if len(text)<=n else text[:max(0,n-1)]+'…', va='center', fontsize=8, clip_on=True)
    ax.set_xlim(0,width);ax.set_ylim(-.2,levels+.2);ax.axis('off')
    fig.suptitle('Vail ' + title + ': CPU flamegraph', x=.04, y=.98, ha='left', fontsize=18, fontweight='bold')
    fig.text(.04,.90,title + ' · measured runs only · native samples at 500 Hz\nWidths represent CPU time; Python/async wrapper frames omitted.',fontsize=11,va='top')
    fig.subplots_adjust(top=.79,bottom=.04,left=.015,right=.995)
    fig.savefig(folder/'flamegraph.png',facecolor='white');plt.close(fig)


def telemetry(folder):
    import matplotlib.pyplot as plt
    events = json.loads((folder/'iterations.json').read_text())
    metrics = list(csv.DictReader((folder/'metrics.csv').open()))
    base = int(metrics[0]['monotonic_ns'])
    t = [(int(r['monotonic_ns'])-base)/1e9 for r in metrics]
    fig, axes = plt.subplots(4,1,figsize=(14,10),sharex=True)
    axes[0].plot(t,[float(r['cpu_percent']) for r in metrics],color='#2558a5');axes[0].set_ylabel('CPU %\n100% = 1 core')
    axes[0].axhline(100,color='gray',lw=.7,ls='--')
    axes[1].plot(t,[float(r['rss_bytes'])/2**20 for r in metrics],label='RSS',color='#2558a5')
    axes[1].plot(t,[float(r['physical_footprint_bytes'])/2**20 for r in metrics],label='Physical footprint',color='#ba5624');axes[1].legend();axes[1].set_ylabel('RAM (MiB)')
    axes[2].plot(t,[float(r['instructions_per_second'])/1e9 for r in metrics],label='Instructions/s',color='#2558a5')
    axes[2].plot(t,[float(r['cycles_per_second'])/1e9 for r in metrics],label='Cycles/s',color='#ba5624');axes[2].legend();axes[2].set_ylabel('Billions/s')
    axes[3].plot(t,[float(r['ipc']) for r in metrics],color='#2558a5');axes[3].set_ylabel('Instructions/cycle');axes[3].set_xlabel('Seconds since first telemetry sample')
    for ax in axes:
        for event in events:
            a=(event['start_monotonic_ns']-base)/1e9;b=(event['end_monotonic_ns']-base)/1e9
            if event['phase']=='warmup':ax.axvspan(a,b,color='gray',alpha=.12)
            else:ax.axvline(a,color='black',lw=.6,alpha=.35)
        ax.grid(alpha=.2)
        ax.set_xlim(0,(events[-1]['end_monotonic_ns']-base)/1e9)
    fig.suptitle('Vail ' + workload_title(folder) + ': 10-Hz telemetry\nGray = warmups; vertical lines = measured iteration boundaries',fontsize=15)
    fig.tight_layout(rect=(0,0,1,.94));fig.savefig(folder/'telemetry.png',dpi=150);fig.savefig(folder/'telemetry.svg');plt.close(fig)
    selected=[r for r in metrics if any(e['start_monotonic_ns'] <= int(r['monotonic_ns']) <= e['end_monotonic_ns'] for e in events if e['phase']=='measured')]
    measured=[e for e in events if e['phase']=='measured']
    summary={'median_wall_seconds':statistics.median(e['seconds'] for e in measured),
        'median_construction_seconds':statistics.median(e['construction_seconds'] for e in measured),
        'median_action_seconds':statistics.median(e['action_seconds'] for e in measured),
        'mean_average_cores':statistics.mean(e['average_cores'] for e in measured),
        'peak_rss_mib':max(float(r['rss_bytes']) for r in selected)/2**20,
        'peak_physical_footprint_mib':max(float(r['physical_footprint_bytes']) for r in selected)/2**20,
        'median_instructions_per_second':statistics.median(float(r['instructions_per_second']) for r in selected),
        'median_cycles_per_second':statistics.median(float(r['cycles_per_second']) for r in selected),
        'median_ipc':statistics.median(float(r['ipc']) for r in selected),
        'measured_samples':len(selected), 'median_sample_interval_seconds':statistics.median(float(r['interval_seconds']) for r in selected),
        'maximum_sample_interval_seconds':max(float(r['interval_seconds']) for r in selected)}
    (folder/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    return summary


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('folder',type=Path);args=parser.parse_args()
    folded,total=stacks(args.folder);flame(args.folder,folded,total);print(json.dumps(telemetry(args.folder),indent=2))
