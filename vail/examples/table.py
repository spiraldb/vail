"""Run after `uv run maturin develop` in the project environment."""
import vail as hl

table = hl.Table.parallelize(
    [{"position": 30}, {"position": None}, {"position": 10}, {"position": 20}],
    schema=hl.tstruct(position=hl.tint32),
    n_partitions=3,
).key_by("position")

print(table.take(3))
print(table.order_by(hl.desc("position")).collect())
