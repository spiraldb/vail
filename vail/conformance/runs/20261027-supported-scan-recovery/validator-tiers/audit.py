import ast,json,pathlib,hashlib,collections
root=pathlib.Path('/private/tmp/vail-owning-coordinate-sum/vail/conformance/runs/20261027-supported-scan-recovery')
out=pathlib.Path('/private/tmp/vail-supported-validator-audit-20261027')
pool=json.loads((root/'positive-support-pool.json').read_text())
actions={'_force_count','_force_count_rows','_force_count_cols','count','count_rows','aggregate','aggregate_rows','aggregate_cols','aggregate_entries','take','show','collect','write','export','export_vcf'}
scalar_expected={
 'test_table_range_join':'Exact inner count min(m,n); integer scalar, no float tolerance.',
 'test_import_vcf_count_rows':'Exact fixture VCF variant-row count from independently frozen input manifest.',
 'test_table_big_aggregate_compilation':'999 fractions all1.0 (single idx0, divisors1..999); complete tuple.',
 'test_table_big_aggregate_compile_and_execute':'999 fractions ceil(1,000,000/i)/1,000,000, i1..999; full tuple with declared float tolerance.',
 'test_table_aggregate_array_sum':'100-element int64 array [10,000,000*j for j0..99], exact.',
 'test_table_aggregate_int_stats':'Full9stats structs (5intfields,2array sums,2exploded arrays); sharedfixture Hail oracle with counts/min/max exact, mean/stdev/sum tolerance.',
 'test_table_aggregate_counter':'Full8counters incl missing keys/counts; canonical key encoding and total-count invariants, compare frozen Hail results.',
 'test_table_aggregate_take_by_strings':'Full18arrays of25strings; ordering ties need explicit equivalence contract, not arbitrary exact tie order.',
 'test_table_aggregate_downsample_dense':'Full3bounded downsample arrays; grid/cell coverage, representative validity and label checks; exact cross-engine choice unresolved if algorithm/ties differ.',
 'test_table_aggregate_downsample_worst_case':'Bounded downsample array for250M diagonal points; exact representative selection contract unresolved.',
 'test_table_aggregate_linreg':'Full75regression structs; coefficients/stderr/statistics, rank/nonfinite/missing behavior against Hail tolerance oracle.',
 'test_matrix_table_aggregate_entries':'Complete GQstats scalar struct; exact n/min/max/missing and tolerance moments.',
}
count_expected={
 'test_import_gvcf_force_count':'gVCF input record-row count from frozen GRCh38fixture manifest; validate schema/entry values separately.',
 'test_matrix_table_decode_and_count':'profile25 fixture row count; full logical row/entry schema+streamed value/null digest in separate validation action.',
 'test_matrix_table_decode_and_count_just_gt':'profile25 row count; projected GTschema+calls/null digest.',
 'test_matrix_table_entries_table':'Logical nonfiltered entry count from frozen entry-presence baseline; verify row+column key multiplicity and typed entry digest.',
 'test_matrix_table_entries_table_no_key':'Same entry-presence count as keyed version; order-independent multiset digest incl row/column data.',
 'test_matrix_table_rows_force_count':'profile25 row count and exact row schema/digest.',
 'test_read_decode_gnomad_coverage':'250,000 rows,1,000columns; immutable convertedfixture data/null digest.',
 'test_table_range_force_count':'100,000,000 exact.',
 'test_table_read_force_count_ints':'Exact immutable TSV/import manifest rowcount/schema/null/value digest.',
 'test_table_read_force_count_strings':'Exact immutable TSV/import manifest rowcount/schema/string/null digest.',
 'test_table_import_ints':'Exact immutable TSV manifest rowcount, declared int/array schemas and parsed values.',
 'test_table_import_ints_impute':'Rowcount plus inferred schema AND values from frozen TSV oracle (not just successful import).',
 'test_table_import_strings':'Rowcount/string schema and frozen TSV oracle.',
 'test_read_force_count_partitions':'10,000,000 exact; range idx coverage digest invariant independent of physical partitions.',
 'test_read_with_index':'10,000,000 exact:1000contiguous half-open intervals tile[0,10M); key coverage and no duplication.',
 'test_union_partitions_table':'20,000,000 exact; every idx0..9,999,999 multiplicity2; streamed multiset digest.',
 'test_join_partitions_table':'10,000,000 exact; one row per idx, key/schema and value digest; partition parameters cannot affect logical result.',
 'test_test_map_filter_region_memory':'15 exact (already asserted); retained even idx0..28 and each big_array length100M/zero content validation outside timing.',
 'test_test_head_and_tail_region_memory':'30 exact; idx0..29 and each big_array length100M/zero values outside timing.',
 'test_test_inner_join_region_memory':'30 exact; one matching row peridx and both50Mzero arrays shape/content.',
 'test_test_left_join_region_memory':'30 exact; matching rightside present for everyidx and both50Mzero arrays shape/content.',
}
# These consumers execute, but their integer result cannot validate the work named by the benchmark.
field_targets={
 'test_matrix_table_array_arithmetic':'Biallelic selectedrow count plus EVERY dosage result, missing/PL-length semantics; compare full typed streamed checksum/tolerant float aggregates to Hail.',
 'test_matrix_table_rows_is_transition':'Boolean output is actually named is_snp and uses hl.is_snp (not transition); verify that exact expression perallele.',
 'test_matrix_table_filter_entries':'Rows unchanged; entries presence mask must equal defined(GQ>8 and DP>2); full surviving/null entry digest.',
 'test_matrix_table_filter_entries_unfilter':'Rows unchanged; original rectangular entries restored with null structs for filtered entries; validate logical absence vs entryfield missing.',
 'test_matrix_table_nested_annotate_rows_annotate_entries':'r0..r19=AF[0]+1..20 and e0..e19=GQ+5..24 incl missing; streamed alloutput field checks.',
 'test_matrix_table_many_aggs_row_wise':'All33x0..x32 rowaggregate outputs from many_aggs helper: call predicates,AD/PL fractions,GQ/DP fractions; validate every field, not just count.',
 'test_matrix_table_many_aggs_col_wise':'All33x0..x32 columnaggregate outputs; keys/shape and each typed field incl fraction missing/empty behavior.',
 'test_matrix_table_call_stats_star_star':'AllAC/AF/AN/homozygote_count rowfields, arraylengths/allelealignment and missing calls.',
 'test_gnomad_coverage_stats_optimized':'mean/count_array100bins/median/above thresholds; rowwise independenthistogram oracle, include x>=100 clippedbin and empty semantics.',
 'test_per_row_stats_star_star':'Allrow stats for x incl n/sum/min/max/mean/stdev; immutable fixture with numeric tolerance.',
 'test_import_bgen_force_count_just_gp':'BGENmanifest row/samplecount and GP arrays/nulls; count alone does not verify probabilitydecode.',
 'test_import_bgen_force_count_all':'BGEN GT/GP/dosage alltyped values/nulls and shape.',
 'test_import_bgen_info_score':'Info score outputvalue and n_included perrow vs Hail tolerant oracle.',
 'test_import_bgen_filter_count':'Count equals exact A/Tallele filter on BGENmanifest; no lost/duplicate keys,GT/GP parsed values.',
 'test_large_range_matrix_table_sum':'500,000 columns; foo[col]=10,000*col+49,995,000 in int64; exact eachcolumn, total count alone insufficient.',
 'test_kyle_sex_specific_qc':'Capture generated sexassignment+num_males/females and derive male/female counts,call_rate/AC/AN perrow. Cross-engine RNGequivalence not assumed; common sexfixture or invariant oracle required.',
 'test_matrix_table_scan_count_rows_2':'200Mrows; x[row]=row_idx exact,10columns; scan boundaries/reset checked by streamed digest.',
 'test_matrix_table_scan_count_cols_2':'10Mcolumns; x[col]=col_idx exact,10rows. Timed terminal is FORCE_COUNT_ROWS, so separate colresult validator essential.',
 'test_mt_group_by_memory_usage':'3rowgroups with new_idx0..2, percolumn means of original randomdoubles; missing/float tolerance, no orderassumption.',
 'test_variant_qc':'Allvariant_qc struct fields perrow; call/count/missing exact, summarymoments/HWE tolerances and undefined values explicit.',
 'test_split_multi_hts':'Complete splitrowkeys/alleles/minrep/a_index/was_split and transformed GT/AD/PL values. Sortedness/duplicates and movedloci need full semantic stream, count alone insufficient.',
 'test_shuffle_key_rows_by_mt':'Fullrow+entry multiset preserved with new struct key(contig,-position), verify global keysorted order; count alone insufficient.',
 'test_shuffle_order_by_10m_int':'10M exact; ordered idx9,999,999..0, streamed order-sensitive digest.',
 'test_shuffle_key_rows_by_4096_byte_rows':'100,000rows×1024cols, entry=row_idx*col_idx; new backward key=-row_idx, sorted keys and exact valueformula.',
 'test_shuffle_key_rows_by_65k_byte_rows':'10,000rows×16,384cols, entry=row_idx*col_idx; backwardkey and payloadformula.',
 'test_shuffle_key_by_aggregate_bad_locality':'Group key i0%1000; exact counts/countsum=source n, tolerant i2mean, fullgroup set.',
 'test_shuffle_key_by_aggregate_good_locality':'Exact floating-floor bucket key idx//(7,500,000/51), count and i2mean; mirror actual expression, do not substitute integerdivision.',
 'test_table_key_by_shuffle':'1Mrows, key x=1M-idx sorted ascending1..1M; exact originalidx mapping.',
 'test_table_group_by_aggregate_sorted':'10,000groups keys0..9999 each y1000, sorted keys.',
 'test_table_group_by_aggregate_unsorted':'10,001groups: x0count999,x10000count1, x1..9999count1000; preserve integerboundary semantics.',
 'test_table_foreign_key_join':'mrows, xlookup key (m-1-idx)%n exact; struct schema/missing and indexedjoins values, not merely mcount.',
 'test_table_annotate_many_flat':'1Mrows×100addedcolumns xi=i+idx, allfields exact.',
 'test_table_range_means':'10Mrows, m=None for idx%1111==0 else (idx%1111-1)/2; missing+Float64checked.',
 'test_group_by_collect_per_row':'One groupper originalrowkey; complete nested row_value list including e/c globals, logical null masks and multiplicity; unordered collect order contract.',
 'test_group_by_take_rekey':'Groups k=int(row_idx/50), each returnedrow belongs to group, n=1limit, schema/payload; nondeterministic tie/selection cannot require arbitraryfirstrow equality.',
 'test_table_scan_sum_1k_partitions':'1Mrows; x=idx*(idx-1)/2 exclusiveprefixsum in int64, across1000partitionboundaries.',
 'test_table_scan_prev_non_null':'100Mrows; yNone atidx0, else y.idx=idx-1 and y.x=range((idx-1)%25), complete nested arrays.',
}
write_expected={
 'test_write_range_matrix_table_p100':('tmp.mt','1Mrows×10cols; x=row_idx+col_idx exact; schema/keys/globals/fullstream digest'),
 'test_write_profile_mt':('tmp.mt','Exact logical snapshot equality to immutable profilefixture incl globals,row/colkeys/entrypresence and payload'),
 'test_import_vcf_write':('out.mt','VCFmanifest rows/samples/schema/keys/globals plus parsedcall/entrydigest vs Hail oracle'),
 'test_write_range_table':('tmp.ht','nrows exact,idx0..n-1, key/schema; physical partition count separately only if public writecontract requires'),
 'test_export_vcf':('out.vcf.bgz','Parse BGZF/VCF allrecords/header/samplecolumns; semantic fields vs input, no compressed-byte hash acrossengines/timestamps'),
 'test_export_range_matrix_table_entry_field_p100':('result.txt','1Mrows×10entryvalues; parsed header,rowidx,colidx and x=row_idx+col_idx; logical streamhash+counts'),
}
small_targets={
 'test_matrix_table_take_entry':'100call values; rowmajor entry order iff public expression.take contract guarantees; logical null/call/phasing and exact type.',
 'test_matrix_table_take_row':'100AF arrays complete including missing,length,float values; roworder contract.',
 'test_matrix_table_take_col':'100sample strings exact definedprefix.',
 'test_table_take':'100complete rows, schema/null values/ordercontract.',
 'test_table_expr_take':'100tuples(f1,f2), type/nulls/ordering.',
 'test_mt_localize_and_collect':'150complete localized rows incl allentries/globalcolumns; boundedrows but potentially wide, compare nested typed values and entryabsence.',
}
show_targets={
 'test_matrix_table_show':'100displayed rows with standard column truncation; compare normalized display/header/missing not whitespace or runtime stats.',
 'test_matrix_table_rows_show':'100displayed rowrecords incl rowfields.',
 'test_matrix_table_cols_show':'100displayed sample columns.',
 'test_matrix_table_entries_show':'defaultdisplay prefix of entries; printed schema/values/absence.',
 'test_table_show':'100displayed rows/selectedstringcolumns.',
}
rows=[]
for c in pool['cases']:
 mod=c['case']['module'];name=c['case']['name'];p=root/'upstream'/mod;s=p.read_text();t=ast.parse(s);f=next(n for n in t.body if isinstance(n,ast.FunctionDef) and n.name==name)
 calls=[]
 for n in ast.walk(f):
  if isinstance(n,ast.Call):
   called=ast.unparse(n.func);end=called.rsplit('.',1)[-1]
   if end in actions:calls.append({'call':ast.unparse(n),'line':n.lineno,'action':end})
 calls.sort(key=lambda x:x['line'])
 asserts=[ast.unparse(n.test) for n in ast.walk(f) if isinstance(n,ast.Assert)]
 clas='';target='';gap=[]
 if name in write_expected:clas='FILE_SEMANTIC';path,target=write_expected[name];gap=['OutputmustvalidatebeforeTemporaryDirectorycleanup','Bytehashalonecannotvalidatecrossformatlogicalequivalence']
 elif name in scalar_expected:clas='SCALAR_COMPLETE';target=scalar_expected[name]
 elif name in small_targets:clas='BOUNDED_RESULT';target=small_targets[name]
 elif name in show_targets:clas='DISPLAY';target=show_targets[name];gap=['Displayonlyverifiestruncatedprefix,nottablethroughput/fullconsumption']
 elif name=='test_table_python_construction':clas='PLAN_ONLY';target='Retain final Table object and inspect publicschema100fields x0..x99=Int32 plus idx/key; optionally separate untimed100rowevaluation. DoNOTaddactiontotimedPythonconstruction.';gap=['Noexecutionactionbydesign;workerfnreturnNoneandlocalhtdiscarded']
 elif name=='test_sentinel_read_gunzip':clas='HOST_STREAM';target='EOFsuccessfulgzipCRC+entiredecompressedbytecount/linecount+canonicalSHA256 against immutablefilemanifest; currentloopdrainsbutdiscardslines.'
 elif name=='test_sentinel_cpu_hash_1':clas='HOST_SCALAR';target='Capture final Pythonx; independent fixedloop expected for pinnedPython/platform integerhashsemantics. Currentlocalxdiscarded; noquery.'
 elif name in field_targets:clas='STREAM_FIELDS';target=field_targets[name];gap=['Integercountdoesnotvalidatecomputedpayloadorordering']
 elif name in count_expected:clas='COUNT_AND_SCHEMA';target=count_expected[name]
 else:raise RuntimeError((mod,name))
 consumed='eager_public_action_result_discarded'
 if clas=='PLAN_ONLY':consumed='lazy_query_construction_only'
 elif clas in ['HOST_STREAM','HOST_SCALAR']:consumed='eager_host_loop_local_values_discarded'
 elif clas=='FILE_SEMANTIC':consumed='eager_file_side_effect_not_checked_then_removed'
 elif clas=='DISPLAY':consumed='eager_bounded_display_stdout_not_checked'
 if asserts:consumed='eager_count_asserted_only'
 if name=='test_kyle_sex_specific_qc':gap+=['Randomsexassignmentisconsumedbyintermediateaggregatebutnotasserted','Noexplicitsharedseedineachworker;captureassignmentorprovidecommonfixture']
 if name in ['test_table_aggregate_take_by_strings','test_table_aggregate_downsample_dense','test_table_aggregate_downsample_worst_case','test_group_by_take_rekey']:gap+=['Exactnondeterminism/ties/representativeselectioncontractunresolved']
 if name in ['test_matrix_table_many_aggs_row_wise','test_matrix_table_many_aggs_col_wise']:helpers=['many_aggs:returns33HLaggregationexpressions,nochecks']
 elif mod=='benchmark_combiner.py':helpers=['import_vcf:purehl.import_vcfGRCh38forceTrue,nochecks']
 else:helpers=[]
 rows.append({'case_id':c['case_id'],'function':name,'module':mod,'source_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'source_lines':[f.lineno,f.end_lineno],'arguments':c['case']['arguments'],'fixture_parameters':c['case']['fixture_parameters'],'format_applicability':c['format_applicability'],'public_actions':calls,'existing_inline_assertions':asserts,'helper_checks':helpers,'current_consumption':consumed,'validator_class':clas,'proposed_minimum_validator':target,'unresolved_gaps':gap,'runtime_validated':False})
assert len(rows)==112 and len({r['case_id'] for r in rows})==112
(out/'case-validator-map.json').write_text(json.dumps({'metadata':{'source_pool_sha256':hashlib.sha256((root/'positive-support-pool.json').read_bytes()).hexdigest(),'upstream_revision':'e10cc355 (recovered frozen9files)','audit':'Source-only fullbody+helper reads; NOquery/build/VM','cases':112,'unique_functions':len(set((r['module'],r['function']) for r in rows))},'cases':rows},indent=2))
print(collections.Counter(r['validator_class'] for r in rows));print('actions',collections.Counter(r['current_consumption'] for r in rows))
