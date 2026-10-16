"""Bounded public range-query workloads; no storage-layout inspection."""
import argparse,json,resource,subprocess,sys,tempfile,time


def worker(rows, queries, mode, partitions=4):
    import vail as hl
    hl.eval(1)
    with tempfile.TemporaryDirectory() as directory:
        path=directory+'/table'
        hl.range_table(rows,n_partitions=4).write(path)
        left=hl.range_table(queries,n_partitions=partitions)
        started=time.perf_counter()
        if mode in ('scalar_direct','scalar_reused'):
            value=hl.query_table(path,hl.interval(0,rows))
            if mode == 'scalar_reused':
                value=hl.if_else(hl.len(value)>0,value,hl.empty_array(hl.tstruct(idx=hl.tint32)))
            value=hl.len(value)
            result=None
        elif mode == 'literal':
            value=hl.query_table(path, 1)
            result=None
        elif mode == 'range':
            value=hl.query_table(path,hl.interval(left.idx%rows,left.idx%rows+1))
            result=left.annotate(matches=value)
        else:
            value=hl.query_table(path,left.idx%rows)
            result=left.annotate(matches=value)
        construction=time.perf_counter()-started
        started=time.perf_counter()
        if mode in ('scalar_direct','scalar_reused'):
            assert hl.eval(value) == rows
        elif mode == 'literal':
            assert hl.eval(value) == [hl.Struct(idx=1)]
        else:
            assert result.aggregate(hl.agg.sum(hl.len(result.matches))) == queries
        print(json.dumps({'mode':mode,'right_rows':rows,'left_queries':queries,'left_partitions':partitions,
                          'construction_seconds':construction,'action_seconds':time.perf_counter()-started,
                          'max_rss_bytes':resource.getrusage(resource.RUSAGE_SELF).ru_maxrss}),flush=True)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--rows',type=int);p.add_argument('--queries',type=int);p.add_argument('--mode',choices=['range','point','literal','scalar_direct','scalar_reused']);p.add_argument('--parts',type=int,default=4);args=p.parse_args()
    if args.rows:worker(args.rows,args.queries,args.mode,args.parts)
    else:
        for mode in ['range','point']:
            for rows,queries in [(4096,16),(4096,256),(16384,256),(16384,1024)]:
                try:
                    result=subprocess.run([sys.executable,__file__,'--rows',str(rows),'--queries',str(queries),'--mode',mode],timeout=20,check=True,text=True,capture_output=True)
                    print(result.stdout.strip(),flush=True)
                except subprocess.TimeoutExpired:
                    print(json.dumps({'mode':mode,'right_rows':rows,'left_queries':queries,'left_partitions':4,'timeout_seconds':20}),flush=True)

        for mode,rows,queries,parts in [('range',65536,256,p) for p in [1,4,16,64]] + [('literal',n,1,1) for n in [4096,16384,65536]]:
            result=subprocess.run([sys.executable,__file__,'--rows',str(rows),'--queries',str(queries),'--mode',mode,'--parts',str(parts)],timeout=20,check=True,text=True,capture_output=True)
            print(result.stdout.strip(),flush=True)

        for mode in ['scalar_direct','scalar_reused']:
            for rows in [4096,65536]:
                result=subprocess.run([sys.executable,__file__,'--rows',str(rows),'--queries','1','--mode',mode,'--parts','1'],timeout=20,check=True,text=True,capture_output=True)
                print(result.stdout.strip(),flush=True)
