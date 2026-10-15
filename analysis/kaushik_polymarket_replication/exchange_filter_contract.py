"""Standard-library-only lineage contract shared by producers and consumers."""
from __future__ import annotations

from pathlib import Path
import re

POLICY = {
    "policy": "exclude_source_exchange_wallets",
    "addresses": ["0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e",
                  "0xc5d563a36ae78145c45a50134d48a1215220f80a",
                  "0xe111180000d2663c0091e4f400237545b87b996b",
                  "0xe2222d279d744050d28e00520010520000310f59"],
    "fields": ["proxyWallet", "counterparty"],
    "comparison": "lowercase_exact_no_trim", "operator": "either",
    "null_blank_match": False,
    "diagnostic_only_addresses": ["0xe3333700ca9d93003f00f0f71f8515005f6c00aa"],
    "unsupported_eligible_match_blocks_admission": True,
    "emitting_contract_filter": False, "other_filters_added": False,
}
FILTERED_INPUT_SCHEMA = "kaushik_replication_filtered_inputs_v1"
FILTERED_INPUT_ACCEPTANCE_SCHEMA = "kaushik_replication_filtered_input_acceptance_v1"
ESTIMATE_SCHEMAS = {
    1: ("kaushik_replication_estimates_v1", "kaushik_replication_estimate_stage_v1",
        "kaushik_replication_estimate_acceptance_v1"),
    2: ("kaushik_replication_estimates_v2", "kaushik_replication_estimate_stage_v2",
        "kaushik_replication_estimate_acceptance_v2"),
}
FILTER_FIELDS = ("wallet_filter", "wallet_policy", "filtered_input_binding", "input_representation")


def require(condition, message):
    if not condition:
        raise ValueError("exchange-filter contract: " + message)


def _binding(value, *, audit=False):
    first = "audit" if audit else "manifest"
    expected = {"directory", first + "_path", first + "_sha256",
                "acceptance_path", "acceptance_sha256"}
    require(type(value) is dict and set(value) == expected, "binding field inventory differs")
    directory = value["directory"]
    require(isinstance(directory, str) and Path(directory).is_absolute() and
            str(Path(directory)) == directory and ".." not in Path(directory).parts,
            "absolute canonical binding directory required")
    for name in (first, "acceptance"):
        path, digest = value[name + "_path"], value[name + "_sha256"]
        require(isinstance(path, str) and path == str(Path(directory) / (name + ".json")),
                "binding JSON path/directory differs")
        require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest) is not None,
                "binding SHA256 absent")
    return value


def binding_from_identities(manifest, acceptance):
    value = {"directory": str(Path(manifest["path"]).parent),
             "manifest_path": manifest["path"], "manifest_sha256": manifest["sha256"],
             "acceptance_path": acceptance["path"], "acceptance_sha256": acceptance["sha256"]}
    return _binding(value)


def validate_filtered_input_binding(value):
    return _binding(value)


def validate_wallet_filter(value):
    require(type(value) is dict and set(value) == {
        "wallet_policy", "input_representation", "parent_binding", "mask_audit_binding"},
        "wallet_filter field inventory differs")
    # JSON equality alone admits 0/1 for false/true; require identical types too.
    def exact(left, right):
        if type(left) is not type(right):
            return False
        if isinstance(right, dict):
            return set(left) == set(right) and all(exact(left[k], v) for k, v in right.items())
        if isinstance(right, list):
            return len(left) == len(right) and all(exact(a, b) for a, b in zip(left, right))
        return left == right
    require(exact(value["wallet_policy"], POLICY), "audited wallet policy differs")
    require(value["input_representation"] == "parent_base_plus_mask", "input representation differs")
    _binding(value["parent_binding"])
    _binding(value["mask_audit_binding"], audit=True)
    require(value["parent_binding"]["directory"] != value["mask_audit_binding"]["directory"],
            "parent and audit directories must differ")
    return value


def reject_filter_fields(value):
    require(isinstance(value, dict), "document object required")
    require(not any(key in value for key in FILTER_FIELDS), "legacy document carries filtered lineage")
    definitions = value.get("definitions", {})
    require(isinstance(definitions, dict) and not any(key in definitions for key in FILTER_FIELDS),
            "legacy definitions carry filtered lineage")
    original_contract = value.get("input_preflight", {}).get("contract", {})
    require(isinstance(original_contract, dict) and not any(key in original_contract for key in FILTER_FIELDS),
            "legacy input contract carries filtered lineage")


def validate_estimate_filter_binding(data, manifest, acceptance):
    """Return None for legacy estimates, or their coherent filtered lineage."""
    versions = (data.get("schema_version"), manifest.get("schema_version"), acceptance.get("schema_version"))
    if versions == ESTIMATE_SCHEMAS[1]:
        for value in (data, manifest, acceptance):
            reject_filter_fields(value)
        base = manifest.get("preflight", {}).get("base_manifest")
        if base is not None:
            reject_filter_fields(base)
            require(base.get("schema_version") != FILTERED_INPUT_SCHEMA, "legacy estimate uses filtered inputs")
        return None
    require(versions == ESTIMATE_SCHEMAS[2], "estimate schema family differs")
    for value in (data, manifest, acceptance):
        require("wallet_policy" not in value and "input_representation" not in value,
                "filtered document carries a conflicting filter alias")
    require(not any(k in data.get("definitions", {}) for k in
                    ("wallet_policy", "input_representation", "filtered_input_binding")),
            "filtered definitions carry a conflicting filter alias")
    wallet_filter = validate_wallet_filter(data.get("wallet_filter"))
    require(validate_wallet_filter(data.get("definitions", {}).get("wallet_filter")) == wallet_filter,
            "summary definitions/filter lineage differs")
    binding = validate_filtered_input_binding(data.get("filtered_input_binding"))
    for value in (manifest, acceptance):
        require(validate_wallet_filter(value.get("wallet_filter")) == wallet_filter and
                validate_filtered_input_binding(value.get("filtered_input_binding")) == binding,
                "estimate stage/receipt/filter binding differs")
    preflight = manifest.get("preflight", {})
    base = preflight.get("base_manifest", {})
    require(base.get("schema_version") == FILTERED_INPUT_SCHEMA and
            validate_wallet_filter(base.get("wallet_filter")) == wallet_filter,
            "filtered input manifest lineage differs")
    require(all(value.get("sample_scope") == "all_available_dates" for value in
                (data, manifest, acceptance, base, data.get("definitions", {}))),
            "filtered estimates require the full-date sample")
    identities = preflight.get("base_binding", {})
    require(set(identities) >= {"manifest", "acceptance"}, "filtered input receipt identities absent")
    require(binding_from_identities(identities["manifest"], identities["acceptance"]) == binding,
            "wrapper binding differs from accepted preflight identities")
    require(binding["directory"] not in (wallet_filter["parent_binding"]["directory"],
                                          wallet_filter["mask_audit_binding"]["directory"]),
            "filtered wrapper must be a distinct stage")
    require(manifest.get("base_binding") == identities, "stage/base preflight binding differs")
    return wallet_filter
