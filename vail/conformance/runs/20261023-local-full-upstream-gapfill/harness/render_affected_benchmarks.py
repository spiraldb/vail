"""Summarize paired affected benchmarks and embed their 10 Hz telemetry."""
from __future__ import annotations
import argparse,csv,html,json
from pathlib import Path


def render(root):
    selection=json.loads((root/'selection.json').read_text())
    settings=json.loads((root/'settings.json').read_text())
    reports={e:json.loads((root/f'{e}.json').read_text()) if (root/f'{e}.json').exists() else {'cases':[]} for e in ['vail','hail']}
    by_engine={e:{c['id']:c for c in r['cases']} for e,r in reports.items()}
    telemetry={};paired=[]
    for case in selection:
        identifier=case['id']; row={'id':identifier}
        for engine in ['vail','hail']:
            result=dict(by_engine[engine].get(identifier,{'status':'pending'}))
            label=identifier.replace('::','--').replace('/','_')
            directory=Path((result.get('telemetry') or {}).get('directory',root/'telemetry'/engine/label))
            if result.get('worker_exitcode') == 86 or (directory/'resource-limit.json').exists():
                result['status']='memory_limited'
            result['telemetry_path']=str(directory)
            row[engine]=result
            if (directory/'metrics.csv').exists():
                with (directory/'metrics.csv').open() as f:samples=list(csv.DictReader(f))
                if samples:
                    first=samples[0];start=int(first['monotonic_ns'])
                    telemetry.setdefault(identifier,{})[engine]=[
                        [(int(r['monotonic_ns'])-start)/1e9,float(r['cpu_percent'])/100,
                         float(r['rss_bytes'])/2**30,
                         (int(r['disk_read_bytes'])-int(first['disk_read_bytes']))/2**20,
                         (int(r['disk_write_bytes'])-int(first['disk_write_bytes']))/2**20]
                        for r in samples]
        if all(row[e]['status']=='passed' for e in ['vail','hail']) and row['hail'].get('comparable',True):
            row['hail_over_vail']=row['hail']['benchmark_seconds']/row['vail']['benchmark_seconds']
        paired.append(row)
    common=[r for r in paired if 'hail_over_vail' in r]
    summary={'selected':len(selection),'completed':{e:len(by_engine[e]) for e in by_engine},
        'common_passes':len(common),'vail_wins':sum(r['hail_over_vail']>1 for r in common),
        'hail_wins':sum(r['hail_over_vail']<1 for r in common),
        'common_totals_seconds':{e:sum(r[e]['benchmark_seconds'] for r in common) for e in ['vail','hail']},
        'status_counts':{e:{s:sum(r[e]['status']==s for r in paired) for s in sorted({r[e]['status'] for r in paired})} for e in ['vail','hail']}}
    (root/'comparison.json').write_text(json.dumps({'summary':summary,'cases':paired},indent=2)+'\n')
    def cell(r,key):
        if r['status']!='passed':return html.escape(r['status'])
        value=(r.get('telemetry') or {}).get('cpu_seconds') if key=='cpu_seconds' else r.get(key)
        return f'{value:.3f}' if value is not None else '—'
    rows=[]
    for r in paired:
        ratio=f"{r['hail_over_vail']:.2f}×" if 'hail_over_vail' in r else '—'
        error='; '.join(f"{e}: {r[e].get('error',r[e].get('reason',''))}" for e in ['vail','hail'] if r[e].get('error') or r[e].get('reason'))
        baseline=html.escape(r['hail'].get('baseline_hardware','local 14 cores'))
        rows.append('<tr><td>'+html.escape(r['id'])+'</td>'+''.join('<td>'+cell(r[e],k)+'</td>' for e,k in [('vail','benchmark_seconds'),('hail','benchmark_seconds'),('vail','cpu_seconds'),('hail','cpu_seconds')])+f'<td>{ratio}</td><td>{baseline}</td><td>{html.escape(error)}</td></tr>')
    options=''.join(f'<option value="{html.escape(c["id"])}">{html.escape(c["id"])}</option>' for c in selection)
    body='''<!doctype html><meta charset="utf-8"><title>Affected throughput benchmarks</title><style>body{font:15px system-ui;max-width:1450px;margin:32px auto;padding:0 20px}table{border-collapse:collapse;width:100%;font-size:13px}td,th{padding:9px;border-bottom:1px solid #ddd;text-align:left}td:first-child{max-width:380px;overflow-wrap:anywhere}td:last-child{max-width:330px;overflow-wrap:anywhere}canvas{width:100%;height:220px}select{max-width:100%;padding:8px}.legend{color:#555}</style><h1>Affected throughput benchmarks</h1><p>Full upstream inputs and dimensions; one fresh-process attempt per case and engine. Query time excludes engine initialization and fixture preparation. Same local 14-core / 96 GiB machine; Hail uses local[14] with a 16 GiB heap. Both benchmark process trees have a 32 GiB RSS limit and bounded runtimes. Totals cover only cases that passed in both engines. Fresh Vail fixtures use decomposed Vortex lists. Table-read fixture inputs are decompressed during preparation for Vail; no rows are changed.</p><pre>'''+html.escape(json.dumps(summary,indent=2))+'''</pre><h2>10 Hz process-tree telemetry</h2><select id="case">'''+options+'''</select><p class="legend">Blue: Vail · Orange: Hail. Each trace starts at its own query measurement; failures and timeouts show only the captured portion. Disk counters are process-tree physical bytes, not logical array traffic.</p><canvas id="cpu" width="1400" height="220"></canvas><canvas id="ram" width="1400" height="220"></canvas><canvas id="read" width="1400" height="220"></canvas><canvas id="write" width="1400" height="220"></canvas><h2>Every selected case</h2><p>Hail/Vail ratio above 1 means Vail is faster. Failed cases are excluded from speedups.</p><table><thead><tr><th>Benchmark</th><th>Vail seconds</th><th>Hail seconds</th><th>Vail core-seconds</th><th>Hail core-seconds</th><th>Hail/Vail</th><th>Failure details</th></tr></thead><tbody>'''+''.join(rows)+'''</tbody></table><script>
const data=PAYLOAD;
function draw(id,index,label){const c=document.getElementById(id),ctx=c.getContext('2d');ctx.clearRect(0,0,c.width,c.height);let series=data[document.getElementById('case').value]||{},points=Object.values(series).flat(),xmax=Math.max(1,...points.map(p=>p[0])),ymax=Math.max(1,...points.map(p=>p[index]));ctx.fillStyle='#333';ctx.font='14px system-ui';ctx.fillText(label,55,20);ctx.strokeStyle='#ddd';ctx.strokeRect(55,30,1310,155);for(let i=0;i<=4;i++){ctx.fillText((ymax*i/4).toFixed(1),5,185-155*i/4);ctx.fillText((xmax*i/4).toFixed(1)+'s',55+1310*i/4,210);}for(let [name,ps] of Object.entries(series)){ctx.strokeStyle=name==='vail'?'#087ec4':'#de7418';ctx.lineWidth=1.6;ctx.beginPath();ps.forEach((p,i)=>{let x=55+p[0]/xmax*1310,y=185-Math.max(0,p[index])/ymax*155;i?ctx.lineTo(x,y):ctx.moveTo(x,y)});ctx.stroke()}}
function update(){draw('cpu',1,'CPU cores (100% = one core)');draw('ram',2,'Process-tree RSS (GiB)');draw('read',3,'Physical disk reads (MiB)');draw('write',4,'Physical disk writes (MiB)')};document.getElementById('case').onchange=update;update();</script>'''
    body=body.replace('PAYLOAD',json.dumps(telemetry,separators=(',',':')).replace('<','\\u003c'))
    body=body.replace('<th>Failure details</th>','<th>Hail baseline</th><th>Failure details</th>')
    if settings.get('comparison_note'):
        start=body.index('<p>');end=body.index('</p>',start)+4
        body=body[:start]+'<p>'+html.escape(settings['comparison_note'])+'</p>'+body[end:]
    title = settings.get('title', 'Affected throughput benchmarks')
    body = body.replace('Affected throughput benchmarks', html.escape(title))
    (root/'report.html').write_text(body)
    print(json.dumps(summary,indent=2))

if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('root',type=Path);render(parser.parse_args().root)
