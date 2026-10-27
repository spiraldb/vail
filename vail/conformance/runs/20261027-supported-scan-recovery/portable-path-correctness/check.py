import ast,hashlib,json,sys
from pathlib import Path
source=Path(sys.argv[1]);tree=ast.parse(source.read_text(),feature_version=(3,8))
helper=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='is_relative_to')
namespace={};exec(compile(ast.Module(body=[helper],type_ignores=[]),str(source),'exec'),namespace)
inside=namespace['is_relative_to']
assert inside(Path('/home/danielking/a'),'/home/danielking')
assert inside(Path('/home/danielking'),'/home/danielking')
assert not inside(Path('/home/danielking-other/a'),'/home/danielking')
assert not inside(Path('/tmp/a'),'/home/danielking')
assert inside(Path('a/b'),'a')
assert not inside(Path('a/b'),'/a')
print(json.dumps({'passed':['descendant','same_path','prefix_sibling_rejected','outside_rejected','relative','mixed_relative_absolute_rejected'],'syntax_parse_floor':'3.8','python':sys.version,'guest_source_sha256':hashlib.sha256(source.read_bytes()).hexdigest()}))
