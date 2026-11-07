# Full supported benchmark rerun

**Integrity status: FINAL_ACCEPTED**

Current `c82d03056`; baseline last full-suite `517c8753`/native4761. Three fresh processes per version/case, original upstream actions. Old-to-current measures cumulative changes.

| Input | Comparator | Pairs | Median wall speedup | Median CPU speedup | Slower wall | Higher CPU |
|---|---|---:|---:|---:|---:|---:|
| vail-vail | old | 111/112 | 1.005× | 1.008× | 49 | 40 |
| vail-vail | hail | 111/112 | 4.495× | 5.079× | 4 | 10 |
| vail-hail | old | 67/67 | 1.016× | 1.010× | 18 | 18 |
| vail-hail | hail | 67/67 | 3.349× | 4.086× | 9 | 8 |

## split_multi_hts

```json
{
  "case_id": "benchmark_methods::test_split_multi_hts",
  "mode": "vail-hail",
  "enrolled_repetitions": 3,
  "current": {
    "wall_s": {
      "median": 2.949043058999905,
      "min": 2.937508036000054,
      "max": 2.99403542999994,
      "samples": [
        2.937508036000054,
        2.949043058999905,
        2.99403542999994
      ],
      "by_repetition": {
        "1": 2.937508036000054,
        "2": 2.949043058999905,
        "3": 2.99403542999994
      }
    },
    "cpu_s": {
      "median": 42.449999999999996,
      "min": 42.169999999999995,
      "max": 43.51,
      "samples": [
        42.169999999999995,
        42.449999999999996,
        43.51
      ],
      "by_repetition": {
        "1": 42.169999999999995,
        "2": 42.449999999999996,
        "3": 43.51
      }
    },
    "rss_gib": {
      "median": 1.318817138671875,
      "min": 1.2719688415527344,
      "max": 1.3369064331054688,
      "samples": [
        1.318817138671875,
        1.2719688415527344,
        1.3369064331054688
      ],
      "by_repetition": {
        "1": 1.318817138671875,
        "2": 1.2719688415527344,
        "3": 1.3369064331054688
      }
    }
  },
  "old": {
    "wall_s": {
      "median": 77.26972514299996,
      "min": 76.04029462299991,
      "max": 79.43533630700017,
      "samples": [
        77.26972514299996,
        76.04029462299991,
        79.43533630700017
      ],
      "by_repetition": {
        "1": 77.26972514299996,
        "2": 76.04029462299991,
        "3": 79.43533630700017
      }
    },
    "cpu_s": {
      "median": 99.58,
      "min": 99.22,
      "max": 102.85999999999999,
      "samples": [
        99.58,
        99.22,
        102.85999999999999
      ],
      "by_repetition": {
        "1": 99.58,
        "2": 99.22,
        "3": 102.85999999999999
      }
    },
    "rss_gib": {
      "median": 6.840606689453125,
      "min": 6.767429351806641,
      "max": 6.891452789306641,
      "samples": [
        6.891452789306641,
        6.767429351806641,
        6.840606689453125
      ],
      "by_repetition": {
        "1": 6.891452789306641,
        "2": 6.767429351806641,
        "3": 6.840606689453125
      }
    }
  },
  "hail": {
    "wall_s": {
      "median": 6.104672786999799,
      "min": 6.034407947000091,
      "max": 6.320972541999936,
      "samples": [
        6.320972541999936,
        6.104672786999799,
        6.034407947000091
      ],
      "by_repetition": {
        "1": 6.320972541999936,
        "2": 6.104672786999799,
        "3": 6.034407947000091
      }
    },
    "cpu_s": {
      "median": 52.440000000000005,
      "min": 51.220000000000006,
      "max": 52.97,
      "samples": [
        52.440000000000005,
        52.97,
        51.220000000000006
      ],
      "by_repetition": {
        "1": 52.440000000000005,
        "2": 52.97,
        "3": 51.220000000000006
      }
    },
    "rss_gib": {
      "median": 1.21990966796875,
      "min": 1.1964683532714844,
      "max": 1.22021484375,
      "samples": [
        1.21990966796875,
        1.1964683532714844,
        1.22021484375
      ],
      "by_repetition": {
        "1": 1.21990966796875,
        "2": 1.1964683532714844,
        "3": 1.22021484375
      }
    }
  },
  "comparisons": {
    "old": {
      "wall_s": {
        "speedup": 26.201626628403343,
        "current_over": 0.038165569419876026,
        "per_repetition_current_over_comparator": {
          "1": 0.038016286851852085,
          "2": 0.03878263588563093,
          "3": 0.037691480507222236
        }
      },
      "cpu_s": {
        "speedup": 2.3458186101295646,
        "current_over": 0.4262904197630046,
        "per_repetition_current_over_comparator": {
          "1": 0.42347861016268323,
          "2": 0.4278371296109655,
          "3": 0.423002138829477
        }
      }
    },
    "hail": {
      "wall_s": {
        "speedup": 2.07005210329823,
        "current_over": 0.48307962799906934,
        "per_repetition_current_over_comparator": {
          "1": 0.46472406207773775,
          "2": 0.48307962799906934,
          "3": 0.4961605937643604
        }
      },
      "cpu_s": {
        "speedup": 1.2353356890459366,
        "current_over": 0.8094965675057206,
        "per_repetition_current_over_comparator": {
          "1": 0.8041571319603354,
          "2": 0.8013970171795355,
          "3": 0.8494728621632174
        }
      }
    }
  }
}
```
```json
{
  "case_id": "benchmark_methods::test_split_multi_hts",
  "mode": "vail-vail",
  "enrolled_repetitions": 3,
  "current": {
    "wall_s": {
      "median": 2.3126903630000015,
      "min": 2.263604654999881,
      "max": 2.386425890000055,
      "samples": [
        2.386425890000055,
        2.263604654999881,
        2.3126903630000015
      ],
      "by_repetition": {
        "1": 2.386425890000055,
        "2": 2.263604654999881,
        "3": 2.3126903630000015
      }
    },
    "cpu_s": {
      "median": 28.84,
      "min": 28.810000000000002,
      "max": 28.95,
      "samples": [
        28.810000000000002,
        28.84,
        28.95
      ],
      "by_repetition": {
        "1": 28.810000000000002,
        "2": 28.84,
        "3": 28.95
      }
    },
    "rss_gib": {
      "median": 1.4418067932128906,
      "min": 1.4256477355957031,
      "max": 1.4915504455566406,
      "samples": [
        1.4256477355957031,
        1.4418067932128906,
        1.4915504455566406
      ],
      "by_repetition": {
        "1": 1.4256477355957031,
        "2": 1.4418067932128906,
        "3": 1.4915504455566406
      }
    }
  },
  "old": {
    "wall_s": {
      "median": 71.25393450699994,
      "min": 70.11881015099993,
      "max": 72.25327356499997,
      "samples": [
        71.25393450699994,
        70.11881015099993,
        72.25327356499997
      ],
      "by_repetition": {
        "1": 71.25393450699994,
        "2": 70.11881015099993,
        "3": 72.25327356499997
      }
    },
    "cpu_s": {
      "median": 100.78999999999998,
      "min": 99.18,
      "max": 102.23999999999998,
      "samples": [
        100.78999999999998,
        99.18,
        102.23999999999998
      ],
      "by_repetition": {
        "1": 100.78999999999998,
        "2": 99.18,
        "3": 102.23999999999998
      }
    },
    "rss_gib": {
      "median": 5.886257171630859,
      "min": 5.834461212158203,
      "max": 5.965980529785156,
      "samples": [
        5.965980529785156,
        5.834461212158203,
        5.886257171630859
      ],
      "by_repetition": {
        "1": 5.965980529785156,
        "2": 5.834461212158203,
        "3": 5.886257171630859
      }
    }
  },
  "hail": {
    "wall_s": {
      "median": 6.104672786999799,
      "min": 6.034407947000091,
      "max": 6.320972541999936,
      "samples": [
        6.320972541999936,
        6.104672786999799,
        6.034407947000091
      ],
      "by_repetition": {
        "1": 6.320972541999936,
        "2": 6.104672786999799,
        "3": 6.034407947000091
      }
    },
    "cpu_s": {
      "median": 52.440000000000005,
      "min": 51.220000000000006,
      "max": 52.97,
      "samples": [
        52.440000000000005,
        52.97,
        51.220000000000006
      ],
      "by_repetition": {
        "1": 52.440000000000005,
        "2": 52.97,
        "3": 51.220000000000006
      }
    },
    "rss_gib": {
      "median": 1.21990966796875,
      "min": 1.1964683532714844,
      "max": 1.22021484375,
      "samples": [
        1.21990966796875,
        1.1964683532714844,
        1.22021484375
      ],
      "by_repetition": {
        "1": 1.21990966796875,
        "2": 1.1964683532714844,
        "3": 1.22021484375
      }
    }
  },
  "comparisons": {
    "old": {
      "wall_s": {
        "speedup": 30.80997596866792,
        "current_over": 0.03245701979829344,
        "per_repetition_current_over_comparator": {
          "1": 0.033491847243405405,
          "2": 0.03228241680264166,
          "3": 0.03200810494654579
        }
      },
      "cpu_s": {
        "speedup": 3.4947988904299576,
        "current_over": 0.28613949796606813,
        "per_repetition_current_over_comparator": {
          "1": 0.2858418493898205,
          "2": 0.2907844323452309,
          "3": 0.2831572769953052
        }
      }
    },
    "hail": {
      "wall_s": {
        "speedup": 2.6396412095049646,
        "current_over": 0.3788393651376351,
        "per_repetition_current_over_comparator": {
          "1": 0.3775409360099826,
          "2": 0.37079868716638476,
          "3": 0.38325058287610775
        }
      },
      "cpu_s": {
        "speedup": 1.8183079056865465,
        "current_over": 0.5499618611746757,
        "per_repetition_current_over_comparator": {
          "1": 0.5493897787948131,
          "2": 0.5444591278081933,
          "3": 0.5652089027723545
        }
      }
    }
  }
}
```

## Largest wall-time regressions versus Hail

| Case | Input | Vail seconds | Hail seconds | Vail/Hail |
|---|---|---:|---:|---:|
| benchmark_table::test_group_by_collect_per_row | vail-hail | 12.1327 | 3.08795 | 3.929× |
| benchmark_matrix_table::test_gnomad_coverage_stats_optimized | vail-hail | 17.7325 | 5.3516 | 3.313× |
| benchmark_methods::test_variant_qc | vail-hail | 13.574 | 4.14823 | 3.272× |
| benchmark_matrix_table::test_matrix_table_many_aggs_row_wise | vail-hail | 11.3112 | 4.60269 | 2.458× |
| benchmark_methods::test_export_vcf | vail-hail | 12.2588 | 6.41222 | 1.912× |
| benchmark_table::test_group_by_take_rekey | vail-hail | 9.48091 | 5.03854 | 1.882× |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | vail-hail | 11.1827 | 5.98989 | 1.867× |
| benchmark_matrix_table::test_matrix_table_entries_show | vail-vail | 8.89279 | 5.12375 | 1.736× |
| benchmark_matrix_table::test_kyle_sex_specific_qc | vail-hail | 6.35431 | 4.59253 | 1.384× |
| benchmark_shuffle::test_shuffle_key_rows_by_mt | vail-vail | 8.26937 | 5.98989 | 1.381× |
| benchmark_matrix_table::test_mt_localize_and_collect | vail-hail | 12.0272 | 9.59377 | 1.254× |
| benchmark_matrix_table::test_write_profile_mt | vail-vail | 5.12635 | 4.44845 | 1.152× |
| benchmark_sentinel::test_sentinel_read_gunzip | vail-vail | 11.0623 | 10.6848 | 1.035× |

Planning observations: 9; nonpass/withheld observations: 0. All39policy exclusions and85supplemental omissions retained. Missing/failed samples never receive invented timings.

CPU is observed descendant accounting at10Hz, a lower bound. Sampled summed RSS can double-count shared pages. No cross-engine semantic equality claim; original action/assertion evidence only. Fixed cyclic order and uncontrolled OS cache. All adverse wall/CPU observations remain in all-regressions.json; all cases and samples are linked in CSV/JSON. Final input integrity acceptance is recorded separately.
