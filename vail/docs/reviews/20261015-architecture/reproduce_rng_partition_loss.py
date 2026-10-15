"""Review reproducer: RNG annotation must preserve logical partition membership."""
import json
import tempfile
import vail as hl

with tempfile.TemporaryDirectory() as directory:
    path = directory + '/table'
    hl.range_table(12, n_partitions=4).write(path)
    table = hl.read_table(path)
    table = table.filter(table.idx < 8)
    annotated = table.annotate(random_value=hl.rand_unif(seed=7))
    result = {}
    for name, value in [('filtered', table), ('random_annotated', annotated)]:
        result[name] = dict(partitions=value.n_partitions(), rows=value.count(),
                            partition_zero_rows=value._filter_partitions([0]).count())
    print(json.dumps(result, indent=2))
    assert result['filtered'] == result['random_annotated'], result
