# Read-only derived cache inventory

Inventory phase was read-only. Sources, active Cargo targets, receipts and installed environments were excluded.

Kache path `/Users/danielking/Library/Caches/kache`: tool reports193.16GB blob store,35.14GB disk-private,158.00GB cloned into target outputs. Cache is regenerable, but full cache cleanup discards future hits; physical reclaim differs from logical due APFS sharing. Entries unused30days14.47GB logical,14days15.19GB; no documented selective cache-entry cleanup switch found. `kache clean --cache` is supported cache-only operation; target clean modes deliberately excluded.

UV path `/Users/danielking/.cache/uv`:148GiB total;114GiB archive-v0,23GiB builds-v0,5.6GiB git-v0,5.5GiB sdists-v9. builds-v0 has1222 hidden temporary build directories, including2025 andJune2026 dates, outside project runtime environments. Supported default `uv cache prune` removes dangling entries/cached environments and performs in-use checks; no --force proposed. Actual reclaim unknown due installed hardlinks/APFS sharing.

All directories owned by danielking. No compiler jobs active during inventory. Root subsequently authorized supported default uv cache prune only. See uv-prune-result.json and uv-prune.log: exit0,84.58s, free1.056GB→105.886GB. No force or Cargo/kache cache cleanup; accepted20f and741 native hashes were verified unchanged afterward.
