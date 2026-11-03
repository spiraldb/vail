import os,inspect
import vail.variant as module
if os.environ.get('VAIL_ABLATE_EAGER_BRANCH'):
 source=inspect.getsource(module._direct_split)
 source=source.replace('if moved is not None:', 'if False:')
 needle="        variant = rows.__direct.variant"
 source=source.replace(needle,"        if moved is not None:\n            rows = rows.filter((rows.__direct.variant.locus != rows[_ROW].locus) if moved else (rows.__direct.variant.locus == rows[_ROW].locus))\n"+needle)
 exec(source,module.__dict__)
exec(open('/private/tmp/vail-indexed-batch-20261103/probe-direct-11.py').read())
