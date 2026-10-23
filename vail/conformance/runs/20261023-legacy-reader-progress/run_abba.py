import hashlib,json,os,pathlib,shutil,subprocess,time
root=pathlib.Path.cwd()
variants={"A":"/private/tmp/legacy-reference-6d5e.abi3.so","B":"/private/tmp/legacy-outlined-candidate.abi3.so"}
summary={"A_commit":"6d5e5403","B_commit":"fcad6095","runs":[]}
try:
 for label in ["A1","B1","B2","A2"]:
  native=pathlib.Path(variants[label[0]])
  shutil.copyfile(native,root/"python/vail/_native.abi3.so")
  output="/private/tmp/legacy-abba-"+label+".json"
  with open("/private/tmp/legacy-abba-"+label+".log","w") as log:
   proc=subprocess.Popen(["/private/tmp/vail-legacy-review-env/bin/python","benchmarks/legacy_paired.py","vail","--cases","matrix_decode_count","matrix_decode_count_gt","matrix_entries_count","matrix_sum_dp","--output",output],stdout=log,stderr=subprocess.STDOUT,env=dict(os.environ,PYTHONPATH="python"))
   interference=[]
   while proc.poll() is None:
    state=subprocess.check_output(["ps","-axo","pid,comm"],text=True)
    interference.extend(line.strip() for line in state.splitlines() if line.rstrip().endswith("/rustc"))
    time.sleep(.5)
  data=json.load(open(output))
  item=dict(label=label,native_sha256=hashlib.sha256(native.read_bytes()).hexdigest(),compiler_interference=sorted(set(interference)),exit_code=proc.returncode,results=data["results"])
  summary["runs"].append(item)
  pathlib.Path("/private/tmp/legacy-abba-summary.json").write_text(json.dumps(summary,indent=2)+"\n")
  print(label,{k:round(v["median"]*1000,1) for k,v in data["results"].items()},"compiler_interference",bool(interference),flush=True)
finally:
 shutil.copyfile(variants["B"],root/"python/vail/_native.abi3.so")
