"""Bounded duration-driven one-sample monitor, with decision and cleanup receipts."""
import argparse,json,os,shutil,signal,subprocess,time
from pathlib import Path
from common import stop_owned,JsonEvents

def observe(query_command,sample_command,out,env,*,sample_limit=5,hard_limit=16,process_limit=60,action_limit=10,output_limit=64*1024**2):
    out=Path(out);out.mkdir(parents=True,exist_ok=False)
    query=sampler=None;record={'error':None,'owned_groups_clean':False,'gates':{},'completed_actions':0};events=JsonEvents();active={};t0=None;sample_started=None;sample_finished=None;closed=None
    log=err=None;start=time.monotonic()
    def fail(reason):record['gates'][reason]=False;raise RuntimeError(reason)
    try:
        log=(out/'stdout').open('wb');err=(out/'stderr').open('wb')
        query=subprocess.Popen(query_command,cwd=out,env=env,stdout=log,stderr=err,start_new_session=True)
        while True:
            now=time.monotonic();size=(out/'stdout').stat().st_size
            if size>output_limit:fail('full_output_cap')
            if now-start>process_limit:fail('process_deadline')
            if shutil.disk_usage(out).free<10*1024**3:fail('disk_floor')
            try:status=Path(f'/proc/{query.pid}/status').read_text()
            except FileNotFoundError:status=''
            rss=next((int(s.split()[1])*1024 for s in status.splitlines() if s.startswith('VmRSS:')),0)
            record['monitor_maxrss_bytes']=max(record.get('monitor_maxrss_bytes',0),rss)
            if rss>6*1024**3:fail('rss_cap')
            for event in events.read(out/'stdout',output_limit):
                if event['phase']=='action_start':
                    if t0 is not None and event['monotonic']>t0+sample_limit:fail('launch_after_close_deadline')
                    active[event['id']]=event['monotonic']
                elif event['phase']=='action':
                    if event['id'] not in active:fail('unmatched_action')
                    active.pop(event['id']);record['completed_actions']+=1
                    if t0 is None:t0=event['completed_monotonic'];record['t0']=t0
            if active and now-min(active.values())>action_limit:fail('inflight_deadline')
            if t0 is not None:
                if sampler is None:
                    command=[str(arg).replace('{pid}',str(query.pid)).replace('{out}',str(out)) for arg in sample_command]
                    record['sampler_command']=command;sample_started=time.monotonic();record['sampler_popen_before_observed']=sample_started
                    sampler=subprocess.Popen(command,cwd=out,env=env,stdout=err,stderr=err,start_new_session=True)
                sampler_status=sampler.poll()
                if sampler_status is not None and sample_finished is None:
                    sample_finished=time.monotonic();record['sampler_decision_status']=sampler_status;record['sampler_exit_poll_observed']=sample_finished
                    if sampler_status!=0:fail('sampler_exit')
                    if sample_finished>t0+sample_limit:fail('sample_completion_deadline')
                    closed=time.monotonic();(out/'stop-launch').touch();record['launch_closed']=closed;record['gates']['sampler_success_in_window']=True
                if now>=t0+sample_limit:
                    if closed is None:closed=now;(out/'stop-launch').touch();record['launch_closed']=closed
                    if sample_finished is None:fail('sample_completion_deadline')
                if now>t0+hard_limit:fail('postwarm_hard_deadline')
            query_status=query.poll()
            if query_status is not None:
                record['query_decision_status']=query_status;record['decision_time']=time.monotonic();record['stdout_bytes_at_decision']=size
                if (out/'stdout').stat().st_size>output_limit:fail('full_output_cap')
                if (out/'stdout').stat().st_size>events.offset:continue
                if query_status!=0:fail('query_exit')
                if sample_finished is None:fail('query_ended_before_sample')
                if active or events.buffer:fail('incomplete_action_or_line')
                record['gates']['complete_action_stream']=True
                break
            time.sleep(.02)
    except BaseException as e:record['error']=repr(e)
    finally:
        for sig in (signal.SIGTERM,signal.SIGINT,signal.SIGHUP):signal.signal(sig,signal.SIG_IGN)
        # Capture decision statuses before signals/cleanup; never overwrite them.
        record.setdefault('decision_time',time.monotonic());record.setdefault('query_decision_status',None if query is None else query.poll());record.setdefault('sampler_decision_status',None if sampler is None else sampler.poll())
        clean=True
        for proc in (query,sampler):
            try:stop_owned(proc)
            except BaseException as e:clean=False;record['error']=f"{record['error']}; cleanup:{e!r}"
        record['owned_groups_clean']=clean
        record['query_final_status']=None if query is None else query.poll();record['sampler_final_status']=None if sampler is None else sampler.poll()
        if log:log.close()
        if err:err.close()
        (out/'result.json').write_text(json.dumps(record,indent=2)+'\n')
    return record

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--query',required=True);p.add_argument('--sampler',required=True);p.add_argument('--out',required=True);a=p.parse_args()
    r=observe(json.loads(a.query),json.loads(a.sampler),a.out,os.environ.copy())
    print(json.dumps(r));raise SystemExit(bool(r['error']))
