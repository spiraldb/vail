import hashlib,json,pathlib,platform,subprocess,sys
import vail, vail._native as native
root=pathlib.Path(__file__).resolve().parents[3]
files=['python/vail/matrixtable.py','crates/vail-execution/src/expression_identity.rs','crates/vail-execution/src/logical_expr.rs','Cargo.lock']
print(json.dumps(dict(python=sys.version,module=vail.__file__,native=native.__file__,native_sha256=hashlib.sha256(pathlib.Path(native.__file__).read_bytes()).hexdigest(),source_sha256={f:hashlib.sha256((pathlib.Path(vail.__file__).resolve().parents[2]/f).read_bytes()).hexdigest() for f in files},machine=platform.uname()._asdict()),indent=2))
