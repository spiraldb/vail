# Explicitly registered reference genomes

Coordinate-only reference genomes are implemented in the Python and native
type layers. A registered definition contains the ordered contig names and
lengths, X/Y/MT classifications, and pseudoautosomal intervals (PAR). This
supports typed loci and coordinate operations without a FASTA. Sequence
storage, sequence context, and liftover remain future work and APIs requiring
them raise `NotImplementedError`.

## Data model

The logical sequence relation is `(contig, position, reference_base)`, keyed
by position within a contig. Positions are one-based. The reference base is
the assembly's sequence at that coordinate, rather than a catalog of possible
alternate alleles. Retain IUPAC ambiguity bases, including `N`.

A reference consists of a small coordinate definition and an optional large
sequence dataset. Coordinate operations do not require the sequence dataset:
validity, ordering, contig identity, global positions, and X/Y/MT/PAR predicates
need only the definition.

The definition has an ordered contig table:

```text
contig: string
ordinal: int32
length: int64
```

It also contains X/Y/MT classifications, physical PAR intervals
`(contig, start, end)`, an explicit coordinate convention. Contig order is declared; it is not inferred from
lexical chromosome names. Loci use one-based positions, and PAR intervals use
the Hail half-open convention `[start, end)`. Equality and ordering require
equal reference definitions, not merely matching reference names.

Physically store sequence in a Viceberg table of bounded consecutive chunks:

```text
contig: string
contig_ordinal: int32
start: int64
sequence: string
key: (contig_ordinal, start)
```

Use a fixed chunk size initially, for example 65,536 bases. The final chunk
of each contig can be shorter. Validate that chunks cover each contig exactly
once, without overlaps or gaps. This retains the per-base logical relation
while avoiding billions of individual physical rows. Preserve sequence case
in storage and make any case normalization an explicit lookup/API rule.

Sequence files can remain on local disk now, and be published to R2 later.
An immutable table snapshot and content fingerprints identify the sequence;
its storage URI can change without changing the coordinate type.

## Registration and identity

Registration resolves the small definition into an immutable object. Tests
register small synthetic references; startup does not download or preload
sequence data. Conflicting definitions under one name are rejected.

Coordinate type identity currently includes the name and the entire immutable
definition: contig order, lengths, classifications, and PAR intervals. A plan
captures this definition, so later registration cannot change an already
constructed lazy query. A future sequence binding should pin its snapshot
and content digest separately. Conflicting registrations
under the same name fail explicitly.

Persist small reference definitions with Table type/storage metadata using
the existing `reference_genomes` descriptor field. Persist sequence bindings
for operations that depend on sequence. Reading a table validates reference
identity; matching a display name alone is insufficient. Explicit registration
must remain possible from these saved definitions.

This resolves the apparent self-reference: bootstrap the definition from
plain contig/ordinal/length rows first, then attach plain contig/start/sequence
rows. Neither bootstrap table needs a Locus column. A view can subsequently
expose those coordinates as loci belonging to this already-defined reference.

## Locus and DataFusion

The logical `Locus(reference_definition)` type is implemented. Its physical coordinates
remain contig and position; reference identity belongs to the type/schema.
Validate contig membership and `1 <= position <= contig_length` on construction.
Keep coordinates/reference identity through Arrow/Vortex and expression IR.

Locus ordering is `(contig_ordinal, position)` within its reference. Loci with
incompatible reference definitions are different types; comparisons, grouping
and joins must reject them rather than comparing unrelated chromosomes.
Use the shared canonical key mechanism for sorting, grouping, joins, intervals
and fragment bounds. Its reference context must be explicit and immutable,
including during storage certification; avoid a comparator that consults a
mutable process-global registry.

Coordinate operations lower to native DataFusion expressions. These batch
expressions capture the immutable coordinate definition and produce Arrow
arrays. Physical locus structs carry reference metadata, ordinal, contig, and
position; canonical keys validate these coordinates. Sequence operations
described below are a future design, and their APIs currently reject requests
with `NotImplementedError`. The sequence relation will stay on disk.

For point sequence lookup, compute the chunk start from the coordinate, select
the `(contig_ordinal, start)` key, then use a DataFusion substring expression.
For a window, select the intersecting chunks, concatenate in start order, and
slice the requested region. Push key ranges into the storage scan and use a
bounded cache for repeatedly accessed chunks. Preserve input rows, ordering,
missingness, and the reference's boundary behavior. A lookup must never scan
or broadcast the whole sequence table by default.

## FASTA import

Use an established FASTA/index library. The preparation script uses `pyfaidx`
for indexing and bounded random access; a native importer can use a suitable
Rust FASTA library. FASTA parsing and indexing are not new Vail algorithms.

An importer should stream indexed chunks in declared contig order into Arrow
batches and the existing bounded Viceberg writer. Expose this as a source
node/scan or a dedicated streaming import action; constructing one enormous
Python `Table.parallelize` value is not a bounded importer.

FASTA supplies sequence names and lengths. It does not supply biological X/Y/MT
classifications or PAR boundaries. Generic references require explicit metadata
for those features. For GRCh38 tests, validate the FASTA names and lengths
against the pinned Hail definition and preserve its declared contig order.

## Prepared GRCh38 fixture

Source: Broad's public hg38/v0
`Homo_sapiens_assembly38.fasta`, documented in the
[GATK resource bundle](https://gatk.broadinstitute.org/hc/en-us/articles/360035890811-Resource-bundle).
The selected object generation is `1575676516681666`, with size 3,249,912,778
bytes and publisher MD5 `7ff134953dcca8c8997453bbb80b6b5e`.
Its FAI contains exactly the 3,366 contigs and lengths in the pinned Hail
`hail/hail/resources/reference/grch38.json`; this avoids substituting a
primary-chromosome-only FASTA for Hail's full coordinate definition.

The data directory is `vail/local-data/references/GRCh38/` relative to the outer
checkout and is ignored by Git. Keep preparation scripts, design and tiny
synthetic fixtures in Git; keep multi-gigabyte downloaded data out of Git.

From the outer checkout, prepare the metadata manifest with:

```sh
uv run --with pyfaidx --directory vail python scripts/prepare_reference_manifest.py \
  local-data/references/GRCh38/Homo_sapiens_assembly38.fasta \
  local-data/references/GRCh38/reference.json --name GRCh38 \
  --metadata ../hail/hail/hail/resources/reference/grch38.json
```

The manifest is a preparation artifact, not an already registered Vail reference.
The adjacent `download-provenance.json` records the verified FASTA MD5 and SHA-256.

Use small explicitly registered synthetic references for unit tests. Full GRCh38
integration tests should opt in to the prepared local fixture and check known
sequence windows, contig boundaries, missing inputs, invalid loci, PAR metadata,
reference mismatches, storage round trips and fragment ordering. Later the
same tests can bind to a pinned R2 table snapshot.
