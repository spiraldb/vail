"""Render the reviewed complete-run reduction into repository documentation."""
import csv,json,re,statistics as stats
from pathlib import Path
B=Path(__file__).parent
W=Path('/private/tmp/vail-full-native-20261112')
E=B/'evidence-attempt2'
R=W/'vail/conformance/runs/20261112-full-native-benchmark'
R.mkdir(parents=True,exist_ok=True)
summary=json.loads((B/'summary.json').read_text());rows=summary['rows'];s=summary['all']
enrollment=json.loads((E/'results/enrollment.json').read_text())
vail=json.loads((E/'vail-ready.json').read_text());hail=json.loads((E/'hail-ready.json').read_text())
assert json.loads((E/'source-lock.json').read_text())['commit']=='8ed78f4fb2363fe83295363438cdf143f6a4b8a1'
hardware=(E/'hardware.txt').read_text()
assert 'AMD EPYC 7B13'in hardware and re.search(r'^CPU\(s\):\s+32\s*$',hardware,re.M)
assert vail['allocator']=='mimalloc'
fixture_observations=json.loads((B/'evidence/results/fixtures-hail/observations.json').read_text())
counts={Path(path).name:row['count']for path,row in fixture_observations.items()}
for name,count in {'profile25_mt.mt':[24885,2535],'gnomad_dp_sim.mt':[250000,1000],
                   'random_doubles_mt.mt':[60000,4000],'many_ints_ht.ht':7500000,
                   'many_strings_ht.ht':5000000}.items():assert counts[name]==count
LINK='vail/conformance/runs/20261112-full-native-benchmark'
def num(x):return '—' if x is None else f'{x:.4g}'
def ratio(x):return '—' if x is None else f'{x:.3f}×'
def label(case):return '`'+case+'`'
def metric(r,version,name):
 v=r['versions'][version]
 return num(v[name+'_median']) if v['eligible'] else '—'
def category(name,key):
 x=summary[key]
 return f"| {name} | {x['pairs']} | {ratio(x['median_wall_speedup']) if x['pairs'] else '—'} | {ratio(x['median_cpu_speedup']) if x['pairs'] else '—'} | {x['vail_wall_slower']} | {x['vail_cpu_higher']} |"
def adverse(which):
 result=['| Case | Vail median | Hail median | Vail excess |','|---|---:|---:|---:|']
 for r in sorted((r for r in rows if r['ratios'] and r['ratios'][which]<1),key=lambda r:r['ratios'][which]):
  result.append(f"| {label(r['case_id'])} | {metric(r,'current',which)} | {metric(r,'hail',which)} | {(1/r['ratios'][which]-1)*100:.1f}% |")
 if len(result)==2:result.append('| None in the complete paired set | — | — | — |')
 return '\n'.join(result)
table=['| Case | Vail wall (s) | Hail wall (s) | Wall H/V | Vail CPU (core-s) | Hail CPU (core-s) | CPU H/V |', '|---|---:|---:|---:|---:|---:|---:|']
for r in rows:
 q=r['ratios'];table.append(f"| {label(r['case_id'])} | {metric(r,'current','wall')} | {metric(r,'hail','wall')} | {ratio(q['wall']) if q else '—'} | {metric(r,'current','cpu')} | {metric(r,'hail','cpu')} | {ratio(q['cpu']) if q else '—'} |")
unpaired=['| Case | Evidence tier | Current preflight | Vail timed statuses | Hail timed statuses |','|---|---|---|---|---|']
for r in rows:
 if r['ratios'] is None:unpaired.append(f"| {label(r['case_id'])} | {r['tier']} | {r['preflight']} | {', '.join(r['versions']['current']['statuses'])} | {', '.join(r['versions']['hail']['statuses'])} |")
with (R/'samples.csv').open('w')as f:
 writer=csv.writer(f,lineterminator="\n");writer.writerow(['case_id','tier','version','repetition','status','eligible_execution_sample','planning_sample','wall_seconds','observed_cpu_seconds','sampled_peak_rss_gib','reused_nonpass'])
 for rep in range(1,4):
  for r in json.loads((E/f'results/rep{rep}/results.json').read_text()):
   telemetry=r.get('telemetry')or{}
   writer.writerow([r['case_id'],r['evidence_tier']['evidence_tier']if isinstance(r.get('evidence_tier'),dict)else'',r['version'],rep,r['status'],r.get('sample_eligible_for_timed_ratio',False),r.get('planning_sample',False),r.get('benchmark_seconds',''),telemetry.get('cpu_seconds',''),telemetry.get('peak_rss_gib',''),r.get('reused_nonpass_status',False)])
(R/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
text=f'''# Hail and Vail benchmark results

Fresh full supported native-format run, started **2026-11-12**. Vail source: `{ '8ed78f4fb2363fe83295363438cdf143f6a4b8a1' }`; Hail: `{hail['version']}`.

This compares **Vail reading Vail-native datasets with Hail reading Hail-native datasets**, plus format-independent operations on common inputs. It measures end-to-end first actions in fresh processes, rather than warmed kernel throughput. It does not measure Vail reading Hail-format storage.

## Overview

Of 112 scheduled cases, **{summary['eligible_pairs']} have complete three-process execution measurements for both engines**. One case is intentionally planning-only; all other unpaired results, if any, remain listed below. Among complete execution pairs, the median per-case Hail/Vail ratio is **{ratio(s['median_wall_speedup'])} for wall time** and **{ratio(s['median_cpu_speedup'])} for observed core-seconds**. Vail takes longer in **{s['vail_wall_slower']}** cases and uses more core-seconds in **{s['vail_cpu_higher']}**. These are descriptive measurements, not significance tests or a claim about all Hail workloads.

| Workload group | Complete execution pairs | Median wall H/V | Median CPU H/V | Vail wall slower | Vail CPU higher |
|---|---:|---:|---:|---:|---:|
{category('All native / format-independent','all')}
{category('Persisted native storage','storage')}
{category('Format-independent','format_independent')}

Each case contributes equally to the median above. A ratio greater than 1 favors Vail; a ratio below 1 favors Hail. Each case ratio divides the Hail median of three fresh processes by the corresponding Vail median. Wall and CPU are summarized separately: parallel execution can improve wall time while increasing CPU use.

The equal-case geometric means are {ratio(s['geomean_wall_speedup'])} wall and {ratio(s['geomean_cpu_speedup'])} CPU. Sums of case medians are Vail {num(s['sum_case_median_wall']['current'])} versus Hail {num(s['sum_case_median_wall']['hail'])} wall-seconds, and Vail {num(s['sum_case_median_cpu']['current'])} versus Hail {num(s['sum_case_median_cpu']['hail'])} core-seconds. These sums are neither elapsed suite duration nor a representative workload weighting. {s['vail_wall_at_least_5x']} paired cases reach 5× wall and {s['vail_wall_at_least_10x']} reach 10×; the complete distribution and adverse cases matter more than those thresholds.

## Every case where Hail has lower wall time

Values are seconds; excess is `(Vail / Hail − 1) × 100%`. No small adverse result is silently classified as noise. This run does not identify causes.

{adverse('wall')}

## Every case where Hail uses fewer core-seconds

These CPU measurements include the worker and observed descendants, including the JVM. They are a lower bound for very short-lived children between samples.

{adverse('cpu')}

## What was measured

- **Machine:** one Google Cloud `n2d-standard-32`, AMD EPYC 7B13, 32 vCPUs (16 physical cores with SMT), 128 GiB RAM, 300 GB balanced persistent disk, Ubuntu 22.04, `us-east1-b`. One benchmark process at a time; no overlapping build or profiling workload during timings.
- **Runtime:** separate Python 3.12 environments; Hail Spark `local[32]`, 16 GiB driver/executor settings; Vail default mimalloc and Rayon 32; BLAS/OMP 1 for both. These settings do not cap every runtime thread pool identically. Exact packages, Java/glibc versions and hashes are retained in the evidence.
- **Vail artifact:** normal release wheel built from the frozen source with Rust 1.98.0, kache 2.0.0, maturin 1.15.0 and `-C target-cpu=native`. Native SHA256: `{vail['native_sha256']}`. No held inner-scan prototype or dirty workspace changes are included.
- **Lifecycle:** one original upstream action per fresh process, three processes per engine/case, alternating engine order across cases/repetitions. Import and `Hail.init` are outside the action timer; query construction, planning, compilation and the original action are inside. No extra warmup or forcing action was inserted. Fresh processes do **not** imply cold filesystem caches: fixture preparation and hashing read the files beforehand, and OS cache state was not controlled.
- **Inputs:** freshly generated original-size public upstream fixtures, seeded 20261112 during Hail fixture generation, then converted once through Vail's public legacy-reader/write APIs before timing. Vail-native inputs are derived from the same Hail fixtures, with count/prefix checks; full conversion equality was not tested. Storage layout, compression and partitioning may differ. Public raw import files are common. Random values generated inside timed bodies follow engine defaults; the fixture seed does not establish identical timed random streams. Major datasets include profile25 (24,885 variants ×2,535 samples), simulated coverage (250,000×1,000), random doubles (60,000×4,000), 7.5 million integer rows, 5 million string rows and 10 million-row partition variants. Input count/prefix checks cover all 14 persisted datasets; these are not full output-equivalence tests.
- **Input integrity:** {len(enrollment['inputs']['files']):,} files, {enrollment['inputs']['bytes']:,} bytes enrolled with complete SHA256 and exact canonical file sets, including required BGEN indexes and present VCF tabix companions. Each phase checks set/stat identity; a final complete rehash checks content. Fresh generated data and the loader configuration mean these ratios should not be treated as isolated code-change speedups against older reports.
- **Loader configuration:** the normal wheel initially failed import with insufficient static TLS space. Without rebuilding it, both engines and descendants were then run with `GLIBC_TUNABLES=glibc.rtld.optional_static_tls=16384`. This reserves additional static TLS per thread and may affect TLS access; it is a recorded, nonneutral runtime configuration. The initial failed import and aborted 65,536-byte TLS attempt (Hail JVM startup failed) are retained separately. The final 16,384-byte setting passed full Hail initialization/shutdown and repeated Vail preflight before timing; no observations from the failed setting are pooled. See the [GNU libc documentation](https://sourceware.org/glibc/manual/latest/html_node/Dynamic-Linking-Tunables.html).
- **Measurements:** elapsed action time and 10 Hz process-tree CPU/RSS counters. CPU includes the measurement thread and observed JVM/other descendants; children born and exited between samples can be missed. RSS is a sampled sum, so it can miss peaks and double-count shared pages. The 96 GiB RSS guard covers fixture/action intervals after engine initialization, not total process lifetime or setup. It is not a memory-ownership or hardware-bandwidth measurement.

## Coverage and correctness limits

The frozen pool contains 68 storage and 44 format-independent cases from the upstream Hail benchmark bodies. It retains the previous 39 policy exclusions and omits 85 separately recorded supplemental cases. “Full” means the full frozen supported pool, not all Hail functionality or all benchmarks ever recorded. The pinned original bodies, parameters and assertions are preserved; the established import/decorator adapter is documented in the protocol.

The evidence tiers are 110 execution-only cases, one case with an existing assertion that its count equals 15, and one planning-only case. Most original benchmark actions discard their return value. Successful completion therefore establishes execution of that action, **not** complete cross-engine value, ordering or schema equivalence. Setup compares full counts and small logical prefixes; no new timed output validation or conformance workload was added.

Current-Vail preflight gates matching Hail timings. A nonpass is retained and reused as a non-sample in later repetitions rather than retried or removed. Ratios require three eligible, finite-positive execution samples for **both** engines. Planning measurements remain separate.

### Planning and other unpaired cases

{chr(10).join(unpaired)}

## Complete case table

All values below are medians of three independent processes. `—` means no complete paired execution comparison; consult the status table and raw observations rather than inferring a speedup. Units are seconds and observed core-seconds.

{chr(10).join(table)}

## Reproduction and evidence

The [run directory]({LINK}/) contains the frozen protocol, source manifest, exact engine/runtime/input identities, raw phase receipts, per-repetition telemetry, complete-input rehash, shutdown receipts and review record. The [sample CSV]({LINK}/samples.csv) preserves each scheduled timed observation; [summary JSON]({LINK}/summary.json) includes all sample values, min/max ranges and derivation. Original failures and superseded setup inventories remain part of the record. Native binary hashes come from guest measurements of the normally installed artifacts; the evidence packet does not retain those binaries for an independent local binary rehash.

To repeat this run, build the stated source normally with its Cargo lockfile, recreate the recorded Python/Java environments, prepare the exact fixtures with the saved protocol, enroll their hashes, run the current preflight followed by all three saved schedules, then rehash inputs. The protocol uses absolute paths for its isolated VM workspace; adapt those paths explicitly and record any environment differences. Timing results alone do not demonstrate broad Hail semantic compatibility.
'''
(W/'BENCHMARKS.md').write_text(text)
readme=W/'README.md';base=readme.read_text();heading='\n## Benchmark results\n'
by_id={r['case_id']:r for r in rows}
gzip_excess=100*(1/by_id['benchmark_sentinel::test_sentinel_read_gunzip']['ratios']['wall']-1)
hash_excess=100*(1/by_id['benchmark_sentinel::test_sentinel_cpu_hash_1']['ratios']['wall']-1)
section=f'''
## Benchmark results

A comparison started on **2026-11-12** measured Vail `8ed78f4f` and Hail `{hail['version']}` on one 32-vCPU AMD EPYC VM. Each engine read its own native format; format-independent operations used common inputs. Across **{summary['eligible_pairs']} paired execution cases**, the median per-case Hail/Vail ratio was **{s['median_wall_speedup']:.2f}× for wall time** and **{s['median_cpu_speedup']:.2f}× for observed core-seconds**. Each case has equal weight in those medians.

The total of per-case medians for this fixed case bundle is:

| Sum of per-case medians | Vail | Hail |
|---|---:|---:|
| Wall-seconds | {s['sum_case_median_wall']['current']:.1f} | {s['sum_case_median_wall']['hail']:.1f} |
| Observed core-seconds | {s['sum_case_median_cpu']['current']:.1f} | {s['sum_case_median_cpu']['hail']:.1f} |

These sums describe that case bundle, rather than elapsed suite duration. Vail took longer in **{s['vail_wall_slower']}** cases: gzip reading (+{gzip_excess:.1f}%) and the CPU-hash sentinel (+{hash_excess:.1f}%). It used more CPU in **{s['vail_cpu_higher']}** cases, including the billion-row range join, integer import with imputation, and entry filtering/unfiltering. All losses remain in the report; three repetitions do not establish statistical significance.

Results are medians of three fresh-process first actions, with query planning/compilation included and engine initialization excluded. They describe this supported pool, rather than warmed steady-state throughput or full Hail compatibility. There are 39 policy exclusions and one separately reported planning-only case. Native layouts/compression differ, caches were not forced cold, and the Linux wheel required the documented loader setting. See [the complete results and method](BENCHMARKS.md), including all 112 scheduled cases and raw samples.
'''
if heading in base:
 base=re.sub(r'\n## Benchmark results\n.*?(?=\n## |\Z)',lambda _:section,base,flags=re.S)
else:base+=section
readme.write_text(base)
project_readme=W/'vail/README.md'
project_text=project_readme.read_text()
note='Fresh native-format comparisons with Hail, including all scheduled cases, CPU costs and limitations, are recorded in [the benchmark report](../BENCHMARKS.md).\n'
if note not in project_text:
 first,rest=project_text.split('\n\n',1)
 project_readme.write_text(first+'\n\n'+note+'\n'+rest)
print('Rendered BENCHMARKS.md, root README.md, samples.csv and summary.json')
