import ast,hashlib,io,json,sys,tempfile
from pathlib import Path
source=Path(sys.argv[1]);tree=ast.parse(source.read_text(),feature_version=(3,9))
helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='sha256_stream')
namespace={'hashlib':hashlib};exec(compile(ast.Module(body=[helper],type_ignores=[]),str(source),'exec'),namespace)
sha=namespace['sha256_stream']
assert sha(io.BytesIO(b''))=='e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855'
assert sha(io.BytesIO(b'abc'))=='ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad'
data=bytes(range(256))*8192+b'tail'
with tempfile.TemporaryFile() as stream:
 stream.write(data);stream.seek(0);assert sha(stream)==hashlib.sha256(data).hexdigest()
print(json.dumps({'passed':['empty','known_abc','file_stream_multiple_chunks_tail'],'python':sys.version,'guest_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'syntax_parse_floor':'3.9','no_engine_import':True}))
