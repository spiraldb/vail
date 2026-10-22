# Partial VDS port

This module is an experimental subset, not a completed Hail VDS replacement.
The attempt stopped after the direct densification translation failed the
performance requirement. Do not use its passing correctness tests as evidence
of cohort-scale performance.

Available operations:

- `VariantDataset`: schema/data validation, conversion from a merged sparse
  MatrixTable, sample count, reference genome, row union, Vail-native write,
  checkpoint, and read. `store_ref_block_max_length` stores extra metadata.
- `lgt_to_gt`, `local_to_global`: composed native expressions.
- `to_dense_mt`, `to_merged_sparse_mt`, `filter_samples`, `filter_variants`,
  and `filter_chromosomes`: existing MatrixTable/Table operations. Dense
  conversion uses an ordinary outer join and a bounded previous-entry scan.
- `combiner.transform_gvcf`, `combine_references`, `combine_gvcfs`, and
  `combine_variant_datasets`: transformations and merges using existing
  expressions and `Table.multi_way_zip_join`.

Not ported: the restartable `new_combiner`/`load_combiner` workflow, multi-write,
VDS sample QC, sparse splitting, interval coverage/filtering/segmentation,
reference-block truncation/merging, and the specialized `read_dense_mt` path.
`read_vds(n_partitions=...)` explicitly rejects partition planning.
Reading an existing Hail-native VDS requires its native MatrixTable storage
reader, a substantial additional feature. This module reads only Vail-native
VDS components and does not silently convert Hail storage.

The production code does not collect genomic rows into Python. Sample filtering
uses existing column semi/anti joins. The reference scan retains one entry per
sample rather than the history of reference rows. Missing reference entries are
explicitly guarded when adding the default reference genotype; Vail's general
struct `annotate` currently does not preserve a missing parent in the same way
as Hail. That broader API was not changed by this port.

Hail-derived implementations retain its MIT license in
`../third_party/HAIL_LICENSE`. See `conformance/runs/20261022-vds` for the attempt
report and independently extracted Hail fixture provenance.
