"""This recovery's bounded host-command ownership; no remote workload code."""
import json,os,signal,subprocess,time
from pathlib import Path

def members(group):
    # Fixed, read-only OS utility; no shell, args/env dump, or new dependency.
    ps=subprocess.Popen(['/bin/ps','-axo','pid=,ppid=,pgid=,uid=,lstart='],stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True)
    try:stdout,stderr=ps.communicate(timeout=2)
    except BaseException:
        if ps.poll() is None:ps.kill()
        ps.wait(timeout=1);raise
    if ps.returncode or len(stdout)>2*1024**2:raise RuntimeError('Bounded ps identity inventory failed')
    result=[]
    for line in stdout.decode().splitlines():
        fields=line.split(maxsplit=4)
        if len(fields)!=5:raise RuntimeError('OS identity row malformed')
        pid,ppid,pgid,uid=map(int,fields[:4])
        if pgid==group:
            try:sid=os.getsid(pid)
            except ProcessLookupError:continue
            result.append(dict(pid=pid,ppid=ppid,pgid=pgid,uid=uid,sid=sid,start=fields[4]))
    return result

def capture(proc):
    info=dict(pid=proc.pid,host_uid=os.getuid(),host_euid=os.geteuid(),popen_after_monotonic=time.monotonic(),retired=False)
    try:info.update(pgid=os.getpgid(proc.pid),sid=os.getsid(proc.pid))
    except ProcessLookupError:
        info.update(pgid=None,sid=None,fast_exit_identity_unavailable=True)
    group=members(proc.pid);leader=next((m for m in group if m['pid']==proc.pid),None)
    info['spawn_members']=group;info['leader']=leader
    if leader is None:info['fast_exit_identity_unavailable']=True
    if info['pgid'] is not None and (info['pgid']!=proc.pid or info['sid']!=proc.pid):raise RuntimeError('Fresh-session identity differs')
    return info

def cleanup(proc,info,events,deadline=None):
    if proc is None:return
    if info.get('retired'):raise RuntimeError('Refuse reuse of retired group identity')
    for sig in (signal.SIGTERM,signal.SIGKILL):
        until=time.monotonic()+3
        while True:
            exited=proc.poll() is not None;group=members(proc.pid)
            events.append(dict(observed_monotonic=time.monotonic(),leader_status=proc.returncode,members=group))
            if not group:
                info['retired']=True
                if exited:return
                raise RuntimeError('Group absent but direct child not reaped; no future group signal')
            if info.get('pgid')!=proc.pid or info.get('sid')!=proc.pid:raise RuntimeError('Surviving group lacks captured session identity')
            if any(m['uid']!=info['host_euid'] or m['sid']!=info['sid'] for m in group):raise RuntimeError('Group UID/session ownership unknown; no signal')
            leader=next((m for m in group if m['pid']==proc.pid),None)
            if leader is not None and (info.get('leader') is None or leader['start']!=info['leader']['start'] or leader['ppid']!=info['leader']['ppid']):raise RuntimeError('Leader identity changed; no signal')
            # This check is NOT atomic with killpg; EPERM/races stay failures.
            if not events[-1].get('signal_attempted'):
                events[-1]['signal_attempted']=int(sig)
                os.killpg(proc.pid,sig)
            while time.monotonic()<until:
                if deadline is not None and time.monotonic()>=deadline:raise TimeoutError('Overall command/readiness deadline during cleanup')
                time.sleep(.1)
                exited=proc.poll() is not None;group=members(proc.pid)
                events.append(dict(observed_monotonic=time.monotonic(),leader_status=proc.returncode,members=group))
                if not group:
                    info['retired']=True
                    if exited:return
                    raise RuntimeError('Group absent but direct child not reaped')
            break
    raise RuntimeError('Owned group unresolved after bounded TERM/KILL')

def execute(command,folder,seconds,*,deadline=None):
    folder=Path(folder);folder.mkdir();(folder/'command.json').write_text(json.dumps(command,indent=2)+'\n')
    proc=None;info=None;error=None;clean=False;events=[];started=time.monotonic()
    try:
        with (folder/'stdout').open('wb') as stdout,(folder/'stderr').open('wb') as stderr:
            before=time.monotonic();proc=subprocess.Popen(command,stdout=stdout,stderr=stderr,start_new_session=True)
            info=capture(proc);info['popen_before_monotonic']=before
            while proc.poll() is None:
                if time.monotonic()-started>seconds or (deadline is not None and time.monotonic()>=deadline):raise TimeoutError('Command/readiness deadline')
                if (folder/'stdout').stat().st_size+(folder/'stderr').stat().st_size>2*1024**2:raise RuntimeError('2MiB command log cap')
                time.sleep(.05)
            if (folder/'stdout').stat().st_size+(folder/'stderr').stat().st_size>2*1024**2:raise RuntimeError('2MiB command log cap')
    except BaseException as e:error=repr(e)
    finally:
        handlers={sig:signal.signal(sig,signal.SIG_IGN) for sig in (signal.SIGINT,signal.SIGTERM,signal.SIGHUP)}
        try:
            if proc is not None:
                if info is None:
                    # Missing spawn proof cannot authorize signalling a live group.
                    group=members(proc.pid)
                    if proc.poll() is None or group:raise RuntimeError('Spawn identity unavailable and child/group survives')
                else:cleanup(proc,info,events,deadline)
            clean=True
        except BaseException as e:error=f'{error}; cleanup:{e!r}'
        for sig,h in handlers.items():signal.signal(sig,h)
        result=dict(returncode=None if proc is None else proc.returncode,error=error,owned_group_clean=clean,elapsed_seconds=time.monotonic()-started,identity=info,cleanup_observations=events,identity_precision='OS ps lstart seconds; read-check/signal non-atomic, not reuse proof')
        (folder/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    return result,(folder/'stdout').read_text()
