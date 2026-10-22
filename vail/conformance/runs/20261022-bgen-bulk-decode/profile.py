import time, resource
import vail as hl
p='/private/tmp/bgen-throughput-data/sim_ukb'
t=time.perf_counter(); c=time.process_time()
mt=hl.import_bgen(p+'.bgen',sample_file=p+'.sample',entry_fields=['GT','GP','dosage'],n_partitions=8)
print('rows',mt._force_count_rows(), 'wall',time.perf_counter()-t,'cpu',time.process_time()-c,'process_peak_rss_bytes',resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,flush=True)
