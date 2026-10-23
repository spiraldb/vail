import importlib.abc,importlib.util,sys
NATIVE='/Users/danielking/.codex/worktrees/shared-annotation-aggregates/vail/vail/python/vail/_native.abi3.so'
class NativeLoader(importlib.abc.MetaPathFinder):
 def find_spec(self,fullname,path=None,target=None):
  if fullname=='vail._native':return importlib.util.spec_from_file_location(fullname,NATIVE)
sys.meta_path.insert(0,NativeLoader())
sys.path.insert(0,'/Users/danielking/.codex/worktrees/ordered-prefetch-cores/vail/vail/python')
