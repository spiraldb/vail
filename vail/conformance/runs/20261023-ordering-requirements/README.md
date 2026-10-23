# Ordering requirements validation

Material revision: `d49f4a97` on `dk/ordering-requirements`, based on frozen main
`e79cf6ff`. Main and cloud benchmark workers were not changed.

The optimizer distinguishes unordered count demand from available ordering.
Built-in count aggregation invokes the pass before and after standard physical
rules. Force count invokes it around the existing expansion-count rewrite.
Only certified transparent nodes are traversed. Unknown, sequence-sensitive,
volatile/computed, limit, top-k and custom matrix expansion nodes remain barriers.
Established input ordering is explicitly required by PreviousNonNull,
SourceOrdinal and contiguous GroupIndex. The separate entries-count branch owns
MatrixEntriesByKeyRuns' ordering contract and count-specific weak implementation.
See `docs/ORDERING_REQUIREMENTS.md` for the audit and deliberate limitations.

Validation used normal Cargo with kache on macOS arm64, dedicated target
`/private/tmp/vail-ordering-check-target`, `CARGO_BUILD_JOBS=2`:

- `cargo check -p vail-table --tests`: passed during implementation.
- Final material revision: `cargo test -p vail-table --lib --test consumer_actions`:
  **42 library tests and 6 public consumer-action integration tests passed**.
- Four new tests execute real Arrow sources, checking complete row multiplicity,
  global/local partition distribution, head/top-k selection, and distinguishing
  built-in count from order-sensitive collection aggregation.
- Public regressions cover ordered collection/head after force count, eager
  projected errors in all partitions, grouped row payloads, and independent
  replay/exchange actions.

The first test fixture incorrectly fed a two-partition input directly to a global
SortExec, violating its singleton distribution requirement. It was corrected to
Coalesce -> Sort; all final tests passed. No production bug was concealed.

Existing compiler deprecation/dead-code warnings and the macOS large unwind-table
linker warning are preserved in `validation.log`. No performance claim is made
for this generic rule until measured. No Python extension or cached shared object
was copied or reused.
