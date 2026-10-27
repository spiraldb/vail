# Read-only Linux gate recovery

Approved source: a4a88c11a0740be385ebcebecd230bc12854d883. One metadata-only recovery completed; no Cargo, tests, engine query, source writes or repair were run. Archive SHA256: 08890e91b7183e1781f85202ab2f94652ea7c27899955b65b883f4cf21fef725. Provider positively TERMINATED; no unresolved host groups or provider cleanup errors.

Original result records `RuntimeError('Read-only Python runtime/package inventory drift')`, no commands, baseline/candidate null, safe_to_restore true, candidate_restored false and scope_clean false. All twenty optional command files are absent. Original failure evidence is unchanged.

Known previous517 source bytes match all 2,101 fingerprints. Complete build-set equality is not established by that historical manifest: eight existing Vortex conformance Rust paths are outside its reference roster. Candidate matches 1,748/1,768 known bytes; baseline 1,752/1,768, each with two missing build files. Neither new source revision is installed. The frozen native4761 hash and all 69 frontend hashes match. No compiler processes were observed after this new boot; that does not establish historical process cleanup.

The recovered JSON runtime/package inventory exactly equals the expected JSON inventory. Source inspection identifies a representation mismatch: gate.py line65 constructs package tuples, while expected.json loads package lists, and line66 compares them directly. Recovery uses the same tuple construction and therefore reports runtime_matches false, while its serialized inventory equals expected.json. This is an observed guard representation error, not evidence of an actual package mutation. No correction or gate rerun was executed.

Reader copied 243 original receipt bytes, hashed 285,472,642 bytes in place (including the installed native), and completed in 3.476419043 seconds. Source files/binaries were not copied. Proposed next step is a separately reviewed minimal list-normalization correction plus complete build-roster reconciliation before any gate rerun.
