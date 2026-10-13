"""Arithmetic stays in DataFusion/Arrow kernels, with Hail numeric semantics."""
import math
import pytest
import vail as hl


@pytest.mark.parametrize('dtype,bits', [(hl.tint32,32), (hl.tint64,64)])
def test_wrapping_arithmetic_with_arrays_scalars_missing_and_boundaries(dtype,bits):
    high = (1 << (bits-1))-1
    low = -(1 << (bits-1))
    table = hl.Table.parallelize(
        [{'x': high, 'y': 1}, {'x': low, 'y': -1}, {'x': None, 'y': 0}],
        schema=hl.tstruct(x=dtype, y=dtype),
    )
    result = table.annotate(add=table.x+table.y, sub=table.x-table.y,
                            mul=table.x*table.y, scalar=table.x+hl.literal(1,dtype),
                            neg=-table.x)
    rows = result.collect()
    assert [r.add for r in rows] == [low, high, None]
    assert [r.sub for r in rows] == [high-1, low+1, None]
    assert [r.mul for r in rows] == [high, low, None]
    assert [r.scalar for r in rows] == [low, low+1, None]
    assert [r.neg for r in rows] == [-high, low, None]
    plan = result._native.logical_plan()
    assert 'semantic_kernel_integer_add' not in plan
    assert 'semantic_kernel_integer_subtract' not in plan
    assert 'semantic_kernel_integer_multiply' not in plan
    assert hl.eval(hl.literal(high,dtype)+hl.literal(1,dtype)) == low


@pytest.mark.parametrize('dtype,bits', [(hl.tint32,32), (hl.tint64,64)])
def test_columnar_floor_division_modulo_missing_and_minimum(dtype,bits):
    low = -(1 << (bits-1))
    table = hl.Table.parallelize(
        [{'x': -5, 'y': 3}, {'x': 5, 'y': -3}, {'x': low, 'y': -1}, {'x': None, 'y': 0}],
        schema=hl.tstruct(x=dtype, y=dtype),
    )
    rows=table.annotate(q=table.x//table.y, r=table.x%table.y).collect()
    assert [r.q for r in rows] == [-2,-2,low,None]
    assert [r.r for r in rows] == [1,-1,0,None]
    zeros=hl.Table.parallelize([{'x':1,'y':0}],schema=hl.tstruct(x=dtype,y=dtype))
    with pytest.raises(RuntimeError,match='zero'):
        zeros.annotate(q=zeros.x//zeros.y).collect()


def test_float_arithmetic_promotion_nan_signed_zero_and_power():
    table=hl.Table.parallelize([{'x':-0.0},{'x':float('nan')},{'x':None}],schema=hl.tstruct(x=hl.tfloat64))
    rows=table.annotate(product=table.x*2.0, quotient=table.x/2.0,
                        floored=table.x//2.0, power=table.x**2.0).collect()
    assert math.copysign(1,rows[0].product) == -1
    assert math.copysign(1,rows[0].quotient) == -1
    assert all(math.isnan(rows[1][name]) for name in ['product','quotient','floored','power'])
    assert all(rows[2][name] is None for name in ['product','quotient','floored','power'])
    assert hl.eval(hl.float32(5.5)//hl.float32(2)) == 2.0
    assert hl.eval(hl.int32(2)**hl.int32(3)) == 8.0
    assert (hl.int32(1)+hl.float64(0.5)).dtype == hl.tfloat64
    assert hl.eval(hl.int32(1)+hl.float64(0.5)) == 1.5


@pytest.mark.parametrize('dtype,values,threshold', [
    (hl.tint32, [-2, 0, 5, None], 0),
    (hl.tint64, [-(1 << 63), 1 << 53, (1 << 53) + 1, None], 1 << 53),
    (hl.tstr, ['A', 'a', 'β', None], 'a'),
    (hl.tbool, [False, True, None], False),
])
def test_primitive_comparisons_in_collection_lambdas(dtype, values, threshold):
    table = hl.Table.parallelize(
        [{'values': values, 'threshold': threshold}],
        schema=hl.tstruct(values=hl.tarray(dtype), threshold=dtype),
    )
    result = table.annotate(
        comparisons=table.values.map(lambda value: hl.struct(
            eq=value == table.threshold, ne=value != table.threshold,
            lt=value < table.threshold, le=value <= table.threshold,
            gt=value > table.threshold, ge=value >= table.threshold,
        )),
        filtered=table.values.filter(lambda value: value > table.threshold),
    ).collect()[0]
    for value, comparison in zip(values, result.comparisons):
        expected = dict.fromkeys(['eq', 'ne', 'lt', 'le', 'gt', 'ge']) if value is None else {
            'eq': value == threshold, 'ne': value != threshold,
            'lt': value < threshold, 'le': value <= threshold,
            'gt': value > threshold, 'ge': value >= threshold,
        }
        assert dict(comparison) == expected
    assert result.filtered == [v for v in values if v is not None and v > threshold]


def test_collection_float_comparisons_preserve_total_order():
    table = hl.Table.parallelize(
        [{'values': [-0.0, 0.0, float('nan'), None]}],
        schema=hl.tstruct(values=hl.tarray(hl.tfloat64)),
    )
    result = table.annotate(
        equality=table.values.map(lambda x: x == -0.0),
        greater=table.values.map(lambda x: x > -0.0),
    ).collect()[0]
    assert result.equality == [True, False, False, None]
    assert result.greater == [False, True, True, None]


def test_integer_sum_batch_reduction_overflow_nulls_and_partition_merge():
    high, low = (1 << 63) - 1, -(1 << 63)
    table = hl.Table.parallelize(
        [{'x': x} for x in [high, 1, low, -1, None]],
        schema=hl.tstruct(x=hl.tint64), n_partitions=3,
    )
    assert table.aggregate(hl.agg.sum(table.x)) == -1
    for values in [[], [{'x': None}]]:
        empty = hl.Table.parallelize(values, schema=hl.tstruct(x=hl.tint64))
        assert empty.aggregate(hl.agg.sum(empty.x)) == 0
    # Promote before reducing, rather than wrapping the input's int32 width.
    narrow = hl.Table.parallelize(
        [{'x': (1 << 31) - 1}, {'x': (1 << 31) - 1}, {'x': None}],
        schema=hl.tstruct(x=hl.tint32), n_partitions=3,
    )
    assert narrow.aggregate(hl.agg.sum(narrow.x)) == 2 * ((1 << 31) - 1)
