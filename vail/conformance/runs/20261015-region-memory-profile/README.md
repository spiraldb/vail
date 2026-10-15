# Large-array region-memory workload profile

`run_case.py` reproduces the four upstream table region-memory benchmark bodies: map/filter, head/tail, inner join and left join. The package root must contain matching Vail Python and native files; missing or mismatched packages are rejected.

A separate guard enforces a60-second wall deadline starting before construction and a6GiB physical-footprint limit. The runner records10Hz process CPU, RAM, disk-byte and hardware-counter samples plus package/native identity and action result. Use a fresh output directory and `samply record --save-only --unstable-presymbolicate --rate 500 --duration 45 --output PROFILE -- python run_case.py CASE PACKAGE OUTPUT_DIR` (with spaces between flags and their values).

The first continuation run (`map-filter-final`) is invalid as query-performance evidence: diagnostic logical-plan formatting expanded a100M-element literal into a300MB string before the action. That instrumentation was removed and the large generated plan deleted. Its guard also had a PID-path parsing bug, corrected before the clean `map-filter-action` rerun. Do not use timings or partial telemetry from the interrupted run as query results.
