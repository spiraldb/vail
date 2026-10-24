"""Native projection keeps required semantics without demanding unused keys."""
import pytest
import vail as hl

@pytest.fixture
def keyed_native(tmp_path):
    rows=[hl.Struct(k=k,s=s,value=k*10+s) for k in [3,1,2,0] for s in [2,0,1]]
    table=hl.Table.parallelize(rows,n_partitions=3).key_by('k','s')
    path=str(tmp_path/'keyed.ht');table.write(path)
    return hl.read_table(path)


def test_nonkey_aggregate_and_zero_column_cardinality(keyed_native):
    table=keyed_native
    assert table.aggregate(hl.agg.sum(table.value))==192
    assert table.key_by().select()._force_count()==12
    projected=table.key_by().select('value')
    assert projected.aggregate(hl.agg.sum(projected.value))==192


def test_key_predicate_remains_when_only_payload_is_selected(keyed_native):
    table=keyed_native
    filtered=table.filter((table.k>=1)&(table.k<3)&(table.s!=1))
    projected=filtered.key_by().select('value')
    assert sorted(r.value for r in projected.collect())==[10,12,20,22]
    assert projected.count()==4


def test_ordered_selection_scan_and_aliases_keep_key_requirements(keyed_native):
    table=keyed_native
    assert [(r.k,r.s) for r in table.head(4).collect()]==[(0,0),(0,1),(0,2),(1,0)]
    scanned=table.annotate(n=hl.scan.count()).key_by().select('value','n')
    assert [(r.value,r.n) for r in scanned.collect()]==[(k*10+s,k*3+s) for k in range(4) for s in range(3)]
    renamed=table.rename({'k':'first','s':'second'})
    assert [(r.first,r.second) for r in renamed.head(4).collect()]==[(0,0),(0,1),(0,2),(1,0)]


def test_join_and_partition_selection_survive_narrow_reads(keyed_native):
    table=keyed_native
    left=table.select(left=table.value)
    right=table.select(right=-table.value)
    joined=left.join(right,how='inner')
    assert [(r.k,r.s,r.left,r.right) for r in joined.collect()]==[(k,s,k*10+s,-(k*10+s)) for k in range(4) for s in range(3)]
    assert table._filter_partitions([0],keep=True).key_by().select()._force_count()>0
