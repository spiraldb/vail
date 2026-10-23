"""Versioned scope policy for upstream benchmark comparisons."""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

POLICY_PATH = Path(__file__).with_name("known_unsupported_benchmarks.json")


def load_policy() -> tuple[dict, dict]:
    raw = POLICY_PATH.read_bytes()
    policy = json.loads(raw)
    if policy.get("schema_version") != 1 or not isinstance(policy.get("cases"), dict):
        raise ValueError(f"Invalid benchmark exclusion policy: {POLICY_PATH}")
    metadata = {
        "path": str(POLICY_PATH),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "version": policy["policy_version"],
    }
    return policy, metadata


def exclusion_result(policy: dict, case_id: str, engine: str) -> dict | None:
    entry = policy["cases"].get(case_id)
    if entry is None:
        return None
    return {
        "status": "comparison_not_applicable" if engine == "hail" else "unsupported",
        "actual_sample": False,
        "reason": entry["reason"],
        "exclusion_kind": entry["kind"],
        "exclusion_policy_version": policy["policy_version"],
        "exclusion_policy_case": case_id,
    }
