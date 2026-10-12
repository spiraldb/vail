# Complete Hail VCF test inventory

Original `VCFTests`: 61 definitions. Import-focused selection: 38 definitions; 33 pass. Other definitions are export or VDS/combiner integration and have not been run by this reader task.

Original assertion bodies are retained. Hail imports are replaced by Vail; decorators are omitted; native exception classes are mapped to RuntimeError/ValueError. No-codegen flags are irrelevant to this backend.

Three golden comparisons separately pass: `ex.vcf` (9 rows), `sample.vcf` (346 rows × 100 samples), and `HG00096.g.vcf.gz` (300,070 rows). Their original test bodies remain fixture-blocked until the Hail-format `.mt` baselines are adapted; runtime Hail-format compatibility is not being added.

| Definition | Result | First blocker / note |
| --- | --- | --- |
| `test_info_char` | PASS |  |
| `test_info_float64` | PASS |  |
| `test_glob` | PASS |  |
| `test_undeclared_info` | PASS |  |
| `test_can_import_bad_number_flag` | PASS |  |
| `test_malformed` | PASS |  |
| `test_not_identical_headers` | PASS |  |
| `test_filter` | PASS |  |
| `test_find_replace` | PASS |  |
| `test_haploid` | PASS |  |
| `test_call_fields` | PASS |  |
| `test_import_vcf` | BLOCKED / FAIL | DataFusion uses Unicode scalar positions; general Hail UTF-16 string slicing/indexing/reversal is unsupported |
| `test_import_vcf_empty` | PASS |  |
| `test_import_vcf_no_reference_specified` | PASS |  |
| `test_import_vcf_bad_reference_allele` | PASS |  |
| `test_import_vcf_flags_are_defined` | PASS |  |
| `test_import_vcf_can_import_float_array_format` | PASS |  |
| `test_import_vcf_can_import_float32_array_format` | PASS |  |
| `test_import_vcf_can_import_negative_numbers` | PASS |  |
| `test_import_vcf_has_good_error_message_when_info_fields_have_missing_elements` | PASS |  |
| `test_import_vcf_array_elements_required_is_false_parses_info_fields_with_missing_elements` | PASS |  |
| `test_import_vcf_missing_format_field_elements` | PASS |  |
| `test_vcf_unsorted_alleles` | PASS |  |
| `test_vcf_unsorted_alleles_no_codegen` | PASS |  |
| `test_import_vcf_skip_invalid_loci` | PASS |  |
| `test_import_vcf_set_field_missing` | PASS |  |
| `test_import_vcf_dosages_as_doubles_or_floats` | PASS |  |
| `test_import_vcf_invalid_float_type` | PASS |  |
| `test_export_vcf` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_quotes_and_backslash_in_description` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_empty_format` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_no_gt` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_no_alt_alleles` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_sites_only_from_table` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_invalid_info_types` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_invalid_format_types` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_export_vcf_haploid` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_tabix_export` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_tabix_export_file_exists` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_import_gvcfs` | BLOCKED / FAIL | 'tarray' object has no attribute '_convert_to_json' |
| `test_import_gvcfs_long_line` | PASS |  |
| `test_vcf_parser_golden_master__ex_GRCh37` | BLOCKED / FAIL | Original Hail snapshot format unsupported; separate reader comparison PASS. |
| `test_vcf_parser_golden_master__sample_GRCh37` | BLOCKED / FAIL | Original Hail snapshot format unsupported; separate reader comparison PASS. |
| `test_vcf_parser_golden_master__gvcf_GRCh37` | BLOCKED / FAIL | Original Hail snapshot format unsupported; separate reader comparison PASS. |
| `test_combiner_works` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_haploid_combiner_ok` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_combiner_parse_allele_specific_annotations` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_flag_at_eol` | PASS |  |
| `test_missing_float_entries` | PASS |  |
| `test_same_bgzip` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_vcf_parallel_separate_header_export` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_custom_rg_import` | PASS |  |
| `test_sorted` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_empty_read_write` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_empty_import_vcf_group_by_collect` | PASS |  |
| `test_format_header` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_format_genotypes` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_contigs_header` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_metadata_argument` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_invalid_info_fields` | NOT RUN | Export or VDS/combiner integration; outside import-focused selection. |
| `test_vcf_different_info_errors` | PASS |  |
