# Narrow local reducer comparison

All18attempts pass; three reps/engine, serial engines. One unrelated legacyreview worker used about1core. Exact upstream SUM and two representative composite cases are distinguished below.

|Case|Vail wall/core s|Hail wall/core s|H/V wall/core|
|---|---:|---:|---:|
|test_large_range_matrix_table_sum|3.2141/13.5903|21.5039/219.6253|6.690/16.160|
|representative_matrix_coordinate_reducers|7.2603/63.5707|1.4072/12.5124|0.194/0.197|
|representative_table_coordinate_reducers|0.0593/0.3442|0.3511/2.6555|5.922/7.714|

The matrix composite is still about5× slower than Hail; output first/last values agree. A follow-up profile will distinguish reducer work from repeated entry preparation/materialization. No claim that dense reducers solve that whole path.

Exact source, matching native hash, CPU settings, query-only scopes, RSS caveats, perattempt ranges, raw10Hz telemetry and driver are retained. Hail initialization was outside timing; GO gate released only after own builds/profiles finished.
