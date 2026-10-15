"""Render range-mean native profiles and process-tree telemetry."""
import argparse
import csv
import json
from pathlib import Path
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import render_annotate_profile as render

parser=argparse.ArgumentParser()
parser.add_argument('folder',type=Path)
args=parser.parse_args()
folder=args.folder
summary=json.loads((folder/'summary.json').read_text())
render.workload_title=lambda _: f"Range means / {summary['rows']:,} rows / {summary['variant']}"
folded,total=render.stacks(folder)
render.flame(folder,folded,total)
events=json.loads((folder/'iterations.json').read_text())
with (folder/'metrics.csv').open() as f: rows=list(csv.DictReader(f))
base=int(rows[0]['monotonic_ns'])
x=[(int(r['monotonic_ns'])-base)/1e9 for r in rows]
fig,axes=plt.subplots(3,1,figsize=(12,8),sharex=True)
axes[0].plot(x,[float(r['cpu_percent'])/100 for r in rows]);axes[0].set_ylabel('CPU cores')
axes[1].plot(x,[float(r['rss_bytes'])/2**30 for r in rows]);axes[1].set_ylabel('Process-tree RSS (GiB)')
axes[2].plot(x,[float(r['disk_read_bytes'])/2**20 for r in rows],label='read')
axes[2].plot(x,[float(r['disk_write_bytes'])/2**20 for r in rows],label='written');axes[2].set_ylabel('Cumulative disk MiB');axes[2].legend()
for ax in axes:
    for e in events:
        a=(e['start_monotonic_ns']-base)/1e9;b=(e['end_monotonic_ns']-base)/1e9
        if e['phase']=='warmup': ax.axvspan(a,b,color='gray',alpha=.2)
        else: ax.axvline(a,color='black',alpha=.25)
    ax.grid(alpha=.2)
axes[-1].set_xlabel('Seconds since monitor start')
fig.suptitle(render.workload_title(folder)+' — 10 Hz telemetry; gray = warmup')
fig.tight_layout();fig.savefig(folder/'telemetry.png',dpi=140);plt.close(fig)
print(json.dumps(summary,indent=2))
