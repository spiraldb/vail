# Python Vail API / Arrow bridge review

Compared the working tree (including new `python/vail/arrow.py` and related additions) against outer HEAD `d87680c`. Read-only review; no edits. Ran focused repros with `PYTHONPATH=python uv run --no-sync python`; did not build or run the full suite.

## Findings

### [P1] Tuple-width mismatch silently changes row values

`python/vail/__init__.py:161` (`_check_value`) has no tuple validation branch, and `python/vail/arrow.py:74-77` serializes a tuple with `zip(value, dtype.types)`, which silently truncates either side. Repro:

```python
schema = hl.tstruct(x=hl.ttuple(hl.tint32, hl.tint32))
t = hl.Table.parallelize([{'x': (1,)}], schema=schema)
print(t.collect())  # [Struct(x=(1, None))]
```

The caller supplied one tuple element, but Vail silently changes it to a two-element tuple and inserts a missing value. This is data corruption at the Python-to-Arrow source boundary. Validate exact tuple type/arity and recursively validate members in `_check_value` (also reject non-tuples), and make `_physical` defensively reject arity mismatch before zipping.

### [P2] Empty row-scoped expressions bypass `eval`'s row-scope guard

`python/vail/__init__.py:1479-1487` rejects `eval` input only when `_referenced_columns` finds a column name. But the frontend explicitly records row scope on row expressions (`Table.row` at `__init__.py:378-380`, and `Expression._row_indexed`), which matters for an empty row struct because its IR has no column references. Repro:

```python
t = hl.Table.parallelize([{}], schema=hl.tstruct())
print(hl.eval(t.row))  # Struct()
```

It returns a scalar despite being a row-indexed expression; behavior is independent of table rows and violates the normal `eval` restriction. Guard using `expr._row_indexed` (with the existing IR check retained for constructed IR) and add an empty-row regression test.

### [P2] Generic Arrow structs that resemble tuple sentinels are mis-typed

`python/vail/arrow.py:226-228` recognizes any struct whose fields are `__vail_tuple_0`, `__vail_tuple_1`, ... as Vail tuple storage, without checking representation metadata. This consumes valid user Arrow structs. Repro:

```python
schema = hl.tstruct(x=hl.tstruct(__vail_tuple_0=hl.tint32))
a = pa.table({'x': [{'__vail_tuple_0': 1}]})
hl.Table.parallelize(a, schema=schema)
# TypeError: Arrow field types must match the supplied schema
```

The explicit schema is valid, but the Arrow inference path classifies nested `x` as `ttuple(int32)` and rejects it. Mark Vail-specific physical encodings using Arrow field/schema metadata and only decode those when metadata is present; otherwise recursively infer a regular `tstruct`. Similar name-only sentinel checks apply to call, interval, and ndarray encodings and should use the same discriminator.

## Coverage notes

The changed Python tests cover successful typed tuple round trips and general expression/collection behavior, but miss tuple arity corruption, row-scope on zero-field structs, and Arrow structs that collide with the bridge's reserved physical names. The exact-option/result-metadata failures mentioned in the task were not reported as runtime findings.

### Design clarification: `filter_intervals` eagerly evaluates inputs, matching Hail

Primary review checked upstream Hail `hail/python/hail/methods/misc.py:415`, which also calls `hl.eval(intervals)`. This is therefore not a confirmed parity defect; making it fully lazy would meet Vail's architectural preference but change this aspect of Hail's behavior.

`python/vail/__init__.py:1366-1376` calls `eval()` on an array-of-intervals expression (or a list containing interval expressions) before it builds the filter IR. This makes a seemingly lazy table transformation execute immediately and can force source global reads/errors during construction. Verified with a watched `hl.eval` wrapper:

```python
t = hl.Table.parallelize([{'k': 1}, {'k': 2}], key='k')
intervals = hl.literal([Interval(1, 1, True, True)],
                       hl.tarray(hl.tinterval(hl.tint32)))
hl.filter_intervals(t, intervals)
# hl.eval is called once before the result Table is returned
```

The same path accepts unindexed/global expressions by its declared interface, so a global-backed interval expression can perform actual I/O here. Lower expression-valued intervals into the lazy IR/global bindings and keep Arrow IPC as literal data transport; alternatively, if only concrete intervals are supported, reject expressions rather than evaluating them implicitly. Add a test asserting construction does not invoke evaluation and that failure remains deferred until an action.
