# Guest SHA256 portability correction

All three guest hash sites (self, each metadata/artifact, final archive) now call the same standard hashlib.sha256 streaming helper with reads at most1MiB. Existing total-read/file/copy/archive/time caps are unchanged. Runtime inventory explicitly records sys.version, version_info, executable and file_digest/Path.is_relative_to capability availability; the prior exception did not establish the guest Python version, and we do not infer it.

Audit: guest uses argparse, hashlib.sha256/update/hexdigest, json, os/proc, signal.alarm, tarfile and pathlib. The newest remaining used API is Path.is_relative_to (Python3.9); syntax parses at3.9. No Python3.11-only API remains in guest. Actual guest compatibility is still subject to the single future authorized run, not proved by local syntax. Host file_digest remains unchanged on the already-working local interpreter. Local tests extract only the actual hash function from the guest AST to avoid executing its CLI/metadata walk: literal empty/abc checks plus a real temporary file spanning multiple chunks and a tail passed. No engine import/query/benchmark.

Fresh remote path becomes /home/danielking/supported-scan-metadata-20261027-attempt-4; it must not exist, and the deadline unit is fresh. No interpreter substitution, installation or workload change. No VM boot performed by this source correction.
