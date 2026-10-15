# UTF-8 codepoint string semantics

Deliberate compatibility decision: Vail counts Unicode scalar values, not
Hail's UTF-16 code units. Strings remain valid UTF-8. Operations never expose
surrogate halves or count UTF-8 bytes. Combining marks and emoji joiners are
separate codepoints, not grapheme clusters. The public README and
docs/STRING_OPERATIONS.md record this contract.

Removed the literal/BMP-only restrictions from indexing, slicing, reversal,
bounded find, translation and reverse complement. Dynamic indices have explicit
bounds errors; missing inputs propagate. Indexing and slicing bind their inputs
lexically so nested expressions do not repeatedly inline the same operand.
Stepped slices use existing array kernels. General translation supports
supplementary codepoints, dynamic dictionaries and multi-character replacements.

Regex methods now accept expression-valued patterns/replacements and split
limits. Matching/replacement use DataFusion built-ins; native scalar kernels
provide regex splitting and first-capture extraction. Patterns use Rust regex
syntax; Java-only constructs remain unsupported by that engine. Added native
codepoint hamming distance and entropy, exposed hl.reversed, and enabled
non-string hl.delimit through existing Vail string conversion. Java printf,
Java-specific numeric parsing and Hail typed JSON APIs remain separate gaps.

Validation:

- Focused string tests: **33 passed**, including dynamic Unicode text/indices,
  negative/stepped slices, bounds/missingness/lazy branches, bounded search,
  regex groups/replacements/limits, dictionary translation, scalar broadcasting,
  Unicode ordering and Vortex storage. The final focused run includes the
  missing-needle/empty-search-interval regression.
- Full local Python suite plus both harness test files: **1,252 passed,
  1 skipped, 19 passing subtests**, in 100.58 seconds. An earlier full run also
  passed with the same counts, before the final operand-binding/API refinements.
- Native string integration tests: **2 passed**. The length test now asserts
  that `a😀b` has codepoint length 3 through DataFusion's built-in kernel.
- **11 original upstream string definitions passed** through the isolated-body
  adapter with their body assertions retained. Imports/decorators/fixtures are
  adapted as documented by that runner. The tests cover translate, reverse
  complement, matches, reverse, first-match captures, join, multiply, basic
  string operations, hamming, Unicode values and hl.reversed.
- The original PLINK export test passes the previous UTF-16 rejection and now
  stops on missing `vail.TemporaryFilename`. It is not counted as passing. A
  preceding attempt timed out at 60 seconds; the subsequent operand-binding
  version completed and reported this missing API.
- A nested minimum-representation query over AC/AT and ACG/ATG produces locus
  1:2 with alleles C/T, exercising the DNA string-indexing use case directly.

Complete logs, upstream definition results, and source/native hashes accompany
this report. These results do not claim Hail UTF-16 parity or complete upstream
suite compatibility. Historical Hail Unicode oracle data remains unchanged.
