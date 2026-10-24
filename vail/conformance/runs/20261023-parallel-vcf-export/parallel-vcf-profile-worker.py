"""Profile only the public read/export operation; full validation is measured separately."""
import pathlib,sys
import vail as hl
b=pathlib.Path('/home/danielking/parallel-vcf-export');storage=sys.argv[1]
path=b/'data'/('profile.mt' if storage=='legacy' else 'profile-native.mt')
m=hl.MatrixTable.read_legacy(path) if storage=='legacy' else hl.read_matrix_table(str(path))
hl.export_vcf(m,str(b/'results'/f'query-profile-{storage}.vcf.bgz'))
