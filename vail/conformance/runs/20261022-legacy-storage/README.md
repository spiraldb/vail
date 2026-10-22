# Legacy Hail storage validation

The `live-oracle.tar.gz` archive contains two complete native datasets written
and read back by the independent PyPI Hail `0.2.139-5e5eedb87ff8` runtime, together
with canonical values, keys, and logical schemas collected by Hail. The version
is distinct from the repository's pinned upstream source checkout. The archive
is 24 KB compressed; it contains about 2 MB of expected JSON because the table
includes repeated long Unicode strings. `fixture-sha256.json` identifies every
native input file. The archive is exercised through public Vail APIs in
`python/tests/test_legacy.py`; Hail/Spark is not required to replay it.

The varied table has 23 rows in five partitions. It covers missing nested
structs and arrays, large Unicode strings spanning compression blocks, signed
64-bit extrema, NaN/Infinity, phased calls, integer intervals, ndarrays, sets
containing missing values, array-valued dictionary keys, escaped field names,
and typed missing globals. The matrix has nine rows and seven columns in four
partitions, duplicate row and column keys, original column order, filtered entry
structs, and present entries whose genotype is missing.

`validate.py` is the repeatable independent writer/oracle and public reader
check. Run each mode in the appropriate Python environment:

```sh
# Hail 0.2.139, Spark local[2]; generation used Java 21.0.9 on macOS arm64.
# Hail warns that it was tested with Java 11; generation completed successfully.
JAVA_HOME=/opt/homebrew/opt/openjdk@21 SPARK_LOCAL_IP=127.0.0.1 \
  /path/to/hail-python validate.py hail /tmp/legacy-oracle

# Vail; this mode compares full values, schemas and keys with the Hail snapshot.
PYTHONPATH=/path/to/vail/python /path/to/vail-python \
  validate.py vail /tmp/legacy-oracle

# Run each resource case in a separate process; RSS is macOS bytes.
PYTHONPATH=/path/to/vail/python /path/to/vail-python \
  validate.py measure /tmp/legacy-oracle array-100000.ht
PYTHONPATH=/path/to/vail/python /path/to/vail-python \
  validate.py measure /tmp/legacy-oracle array-400000.ht
PYTHONPATH=/path/to/vail/python /path/to/vail-python \
  validate.py measure /tmp/legacy-oracle scale.mt
```

Generation also writes the three resource datasets. Resource cases assert every
nullable array element against a closed-form expected sequence, matrix axes
against `(5000, 100)`, and the filtered entry count against an independent
formula. These are bounded scale checks, not a production capacity guarantee.
Import wall time is reported separately from the complete validation wall time.
CPU excludes interpreter startup; peak RSS includes the full Python process and
validation. Concurrent local compilation can inflate wall time.
