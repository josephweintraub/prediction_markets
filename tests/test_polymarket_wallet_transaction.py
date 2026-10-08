"""Synthetic-only transaction tracing; no production guard bypass or service calls."""
from __future__ import annotations

import json
import hashlib
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_polymarket_lineage as lineage
from scripts import audit_polymarket_wallet_transaction as trace


def native(tx="tx-a", log=1, order=None, block=2, **changes):
    row = dict(zip(lineage.RAW_FIELDS, (
        order or f"order-{tx}-{log}", "MakerCase", "TakerCase", "1", "0",
        100_000_000, 40_000_000, 0, block, tx, log, lineage.OLD_EXCHANGES[0])))
    return {**row, **changes}


def resolved(row):
    return {**row, "condition_id": "market", "outcome": "YES", "market_slug": "m",
            "event_slug": "source-event", "question": "question", "outcome_token_side": "maker",
            "winning_outcome": "YES"}


def memory_fixture(raw_rows=None, resolved_rows=None, *, change_wallets=True, change_label=True):
    raw_rows = raw_rows or [native()]
    resolved_rows = resolved_rows if resolved_rows is not None else [resolved(row) for row in raw_rows]
    con = lineage.connection()
    con.register("raw", pa.Table.from_pylist(raw_rows))
    con.register("resolved", pa.Table.from_pylist(resolved_rows))
    con.execute("CREATE VIEW raw_window AS SELECT * FROM raw")
    con.execute("CREATE VIEW resolved_window AS SELECT * FROM resolved")
    con.register("timestamp_slice", pa.Table.from_pylist([
        {"block_number": block, "timestamp": lineage.epoch(trace.WINDOWS[0][1])}
        for block in sorted({row["block_number"] for row in raw_rows})]))
    con.register("token_map", pa.Table.from_pylist([
        {"token_id": "1", "condition_id": "market", "outcome": "YES", "market_slug": "m",
         "event_slug": "source-event", "question": "question"}]))
    con.register("resolutions", pa.Table.from_pylist([
        {"token_id": "1", "condition_id": "market", "winning_outcome": "YES"}]))
    lineage.create_expansion(con, "resolved_window", "fixture_expanded")
    values = trace.query_rows(con, f"SELECT {trace.columns(lineage.VALUE_FIELDS)} FROM fixture_expanded")
    for row in values:
        if change_wallets:
            row["proxyWallet"] = "PublishedCase"
            row["counterparty"] = "PublishedCounterparty"
        if change_label:
            row["eventSlug"] = "published-event"
    table = pa.Table.from_pylist(values)
    con.register("root_transformed", table)
    clean = con.execute(f"SELECT DISTINCT {trace.columns(lineage.VALUE_FIELDS)} FROM root_transformed").to_arrow_table()
    con.register("clean", clean)
    return con


def test_complete_trace_preserves_direction_labels_values_and_strict_failure():
    con = memory_fixture()
    try:
        result = trace.build_trace(con)
        assert result["selected_transaction"]["transaction_hash"] == "tx-a"
        assert result["data_certified"] is False
        assert result["full11_window_difference"] == {"left_only_rows": 2, "right_only_rows": 2}
        assert result["distinct_root_to_clean_candidate_difference"] == {"left_only_rows": 0, "right_only_rows": 0}
        ledger = result["native_transaction_ledger"][0]
        assert ledger["maker"] == "MakerCase" and ledger["taker"] == "TakerCase"
        assert ledger["maker_own_side"] == "SELL" and ledger["legacy_counterparty_side"] == "BUY"
        assert "synthetic" in ledger["counterparty_side_basis"]
        assert result["all_root_value_candidates"][0]["eventSlug"] == "published-event"
        assert result["reconstructed_transaction_rows"][0]["eventSlug"] == "source-event"
        assert all(row["usdcSize"] == 40.0 and row["price"].hex() == (0.4).hex() for row in result["all_root_value_candidates"])
        assert json.loads(trace.exact_json_bytes(result))["selected_transaction"] == result["selected_transaction"]
    finally:
        con.close()


@pytest.mark.parametrize("reverse", [False, True])
def test_first_transaction_is_stable_and_not_replaced_when_it_collides(reverse):
    raw_rows = [native("tx-z", block=3), native("tx-b", log=2), native("tx-a")]
    if reverse:
        raw_rows.reverse()
    con = memory_fixture(raw_rows)
    try:
        result = trace.build_trace(con)
        assert result["selected_transaction"]["transaction_hash"] == "tx-a"
        assert all(row["candidate_transaction_hashes"] == ["tx-a", "tx-b", "tx-z"] for row in result["signature_classes"])
        assert all(row["cross_transaction_collision"] for row in result["signature_classes"])
        assert len(result["all_native_role_candidates"]) == 6
        assert all(row["multiplicities"] == {"expected_whole_minute": 3, "selected_transaction": 1, "root": 3, "clean": 1} for row in result["signature_classes"])
        assert result["clean_removal_native_attribution_valid"] is False
    finally:
        con.close()


def test_same_transaction_logs_all_emitters_and_all_exclusion_statuses_are_preserved():
    admitted = native(order="repeat")
    aggregate = native(log=2, taker=lineage.OLD_EXCHANGES[1], exchange_address=lineage.OLD_EXCHANGES[1])
    ranked_out = native(log=3, order="repeat")
    missing_mapping = native(log=4, maker_asset_id="missing")
    missing_resolution = native(log=5, maker_asset_id="unresolved")
    price_excluded = native(log=6, maker_amount_filled=1_000_000, taker_amount_filled=2_000_000)
    con = memory_fixture([admitted, aggregate, ranked_out, missing_mapping, missing_resolution, price_excluded],
                         [resolved(admitted), resolved(price_excluded)])
    try:
        old_map = trace.query_rows(con, "SELECT * FROM token_map")
        con.unregister("token_map")
        con.register("token_map", pa.Table.from_pylist(old_map + [{**old_map[0], "token_id": "unresolved"}]))
        result = trace.build_trace(con)
        assert result["selected_transaction"]["frozen_transaction_source_rows"] == 6
        assert [row["disposition"] for row in sorted(result["native_transaction_ledger"], key=lambda row: row["log_index"])] == [
            "admitted_legacy_expansion", "old_exchange_aggregate_exclusion", "canonical_transaction_order_rank_removal",
            "missing_mapping", "mapped_without_cached_resolution", "canonical_price_exclusion"]
        assert len(result["reconstructed_transaction_rows"]) == 2
    finally:
        con.close()


def test_identical_replay_count_is_not_a_conflicting_retention_tie():
    row = native()
    con = memory_fixture([row, dict(row)], [resolved(row)])
    try:
        result = trace.build_trace(con)
        assert result["reconciliation"]["canonical_rank_ties"] == {
            "conflicting_lowest_log_groups": 0, "identical_lowest_log_replay_surplus": 1}
        assert result["native_transaction_ledger"][0]["multiplicity"] == 2
        assert result["native_transaction_ledger"][0]["canonical_retained_occurrences"] == 1
        ledger = result["native_transaction_ledger"][0]
        assert ledger["canonical_rank_removed_occurrences"] == 1
        assert ledger["identical_source_replay_surplus"] == 1
        assert ledger["retained_stage_occurrences"]["admitted_legacy_expansion"] == 1
        assert ledger["old_aggregate_excluded_occurrences"]+ledger["canonical_rank_removed_occurrences"]+sum(ledger["retained_stage_occurrences"].values()) == ledger["multiplicity"]
    finally:
        con.close()


def test_conflicting_minimum_rank_across_emitters_blocks_retention():
    first = native(order="same")
    other = native(order="same", exchange_address=lineage.OLD_EXCHANGES[1])
    con = memory_fixture([first, other], [resolved(first)])
    try:
        with pytest.raises(lineage.AuditBlocked, match="lowest-log"):
            trace.build_trace(con)
    finally:
        con.close()


def test_excluded_aggregate_at_same_rank_does_not_create_false_tie():
    first = native(order="same")
    excluded = native(order="same", exchange_address=lineage.OLD_EXCHANGES[1], taker=lineage.OLD_EXCHANGES[1])
    con = memory_fixture([first, excluded], [resolved(first)])
    try:
        result = trace.build_trace(con)
        assert result["reconciliation"]["canonical_rank_ties"]["conflicting_lowest_log_groups"] == 0
        assert {row["disposition"] for row in result["native_transaction_ledger"]} == {"admitted_legacy_expansion", "old_exchange_aggregate_exclusion"}
    finally:
        con.close()


def test_label_only_change_does_not_select_a_wallet_failure():
    con = memory_fixture(change_wallets=False)
    try:
        with pytest.raises(lineage.AuditBlocked, match="no transaction implicated"):
            trace.build_trace(con)
    finally:
        con.close()


def test_absent_published_candidates_are_reported_without_a_substitute():
    con = memory_fixture()
    try:
        root_rows = trace.query_rows(con, "SELECT * FROM root_transformed")
        for row in root_rows:
            row["price"] = 0.41
        con.unregister("root_transformed")
        con.unregister("clean")
        con.register("root_transformed", pa.Table.from_pylist(root_rows))
        con.register("clean", pa.Table.from_pylist(root_rows))
        result = trace.build_trace(con)
        assert result["selected_transaction"]["transaction_hash"] == "tx-a"
        assert result["all_root_value_candidates"] == []
        assert all(row["multiplicities"]["root"] == 0 for row in result["signature_classes"])
    finally:
        con.close()


def test_every_candidate_wallet_payload_and_exact_multiplicity_is_retained():
    con = memory_fixture()
    try:
        roots = trace.query_rows(con, "SELECT * FROM root_transformed")
        roots.append({**roots[0], "proxyWallet": "AnotherWallet", "counterparty": "AnotherCounterparty"})
        con.unregister("root_transformed")
        con.unregister("clean")
        con.register("root_transformed", pa.Table.from_pylist(roots))
        con.register("clean", pa.Table.from_pylist(roots))
        result = trace.build_trace(con)
        assert len(result["all_root_value_candidates"]) == 3
        assert {row["proxyWallet"] for row in result["all_root_value_candidates"]} == {"PublishedCase", "AnotherWallet"}
        assert sorted(row["multiplicities"]["root"] for row in result["signature_classes"]) == [1, 2]
    finally:
        con.close()


def test_transaction_hash_spanning_two_frozen_blocks_fails_closed():
    con = memory_fixture([native(log=1), native(log=2, block=3)])
    try:
        with pytest.raises(lineage.AuditBlocked, match="contradictory frozen block"):
            trace.build_trace(con)
    finally:
        con.close()


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf")])
def test_nonfinite_json_evidence_fails_closed(value):
    with pytest.raises(ValueError):
        trace.exact_json_bytes({"price": value})


def test_exact_json_roundtrip_preserves_large_integers_mixed_case_and_signed_zero():
    value = {"integer": 2**63-1, "token": "9"*77, "wallet": "MixedCase", "amount": -0.0}
    decoded = json.loads(trace.exact_json_bytes(value))
    assert decoded["integer"] == value["integer"] and decoded["wallet"] == "MixedCase"
    assert decoded["amount"].hex() == value["amount"].hex()


@pytest.mark.parametrize("expected", ["short", "g"*40, "A"*40])
def test_expected_head_rejects_noncanonical_commit_hash(expected):
    with pytest.raises(lineage.AuditBlocked, match="expected head"):
        trace.require_expected_head("a"*40, expected)


def test_expected_head_mismatch_fails_before_source_work():
    with pytest.raises(lineage.AuditBlocked, match="root-approved expected head"):
        trace.require_expected_head("a"*40, "b"*40)
    trace.require_expected_head("a"*40, "a"*40)


def test_reviewed_manifest_digest_and_parse_are_bound_to_one_read():
    encoded = b'{"status":"preflight_complete","approved":true}'

    class ChangingFile:
        reads = 0

        def read_bytes(self):
            self.reads += 1
            return encoded if self.reads == 1 else b'{"approved":false}'

    path = ChangingFile()
    digest = hashlib.sha256(encoded).hexdigest()
    parsed, recorded = trace.read_reviewed_manifest(path, digest)
    assert path.reads == 1 and parsed["approved"] is True and recorded == digest


@pytest.mark.parametrize("digest", ["short", "g"*64, "A"*64])
def test_reviewed_manifest_invalid_digest_refuses_before_read(tmp_path, digest):
    with pytest.raises(lineage.AuditBlocked, match="SHA256"):
        trace.read_reviewed_manifest(tmp_path / "absent.json", digest)


def test_reviewed_manifest_mismatch_refuses_unapproved_bytes(tmp_path):
    trace.write_immutable(tmp_path / "fixture", {"status": "preflight_complete"})
    path = tmp_path / "fixture/manifest.json"
    with pytest.raises(lineage.AuditBlocked, match="root-approved SHA256"):
        trace.read_reviewed_manifest(path, "0"*64)


def test_complete_evidence_and_manifest_caps_never_truncate(tmp_path, monkeypatch):
    con = memory_fixture()
    try:
        monkeypatch.setattr(trace, "MAX_TRACE_BYTES", 1)
        with pytest.raises(lineage.AuditBlocked, match="no truncation"):
            trace.build_trace(con)
    finally:
        con.close()
    monkeypatch.setattr(trace, "MAX_OUTPUT_BYTES", 1)
    destination = tmp_path / "never_created"
    with pytest.raises(lineage.AuditBlocked, match="no truncation"):
        trace.write_immutable(destination, {"evidence": "complete"})
    assert not destination.exists()


def test_immutable_outputs_roundtrip_and_never_overwrite(tmp_path):
    destination = tmp_path / "run"
    trace.write_immutable(destination, {"data_certified": False, "price": 0.4})
    assert json.loads((destination / "manifest.json").read_text()) == {"data_certified": False, "price": 0.4}
    with pytest.raises(FileExistsError):
        trace.write_immutable(destination, {"data_certified": True})


def disk_inputs(tmp_path):
    con = memory_fixture()
    try:
        relations = ("raw", "resolved", "root_transformed", "clean", "token_map", "resolutions")
        inputs = {}
        for name in relations:
            table = con.execute(f"SELECT * FROM {name}").to_arrow_table()
            if name in {"root_transformed", "clean"}:
                for month in lineage.canonical_months():
                    directory = tmp_path / name / f"year_month={month}"
                    directory.mkdir(parents=True)
                    rows = table.to_pylist()
                    if month != "2026-03":
                        rows = [{**row, "timestamp": lineage.month_bounds(month)[0]+1, "year_month": month} for row in rows]
                    current = pa.Table.from_pylist(rows, schema=table.schema)
                    if name == "root_transformed":
                        current = current.drop(["year_month"])
                    pq.write_table(current, directory / "data.parquet", row_group_size=1)
                inputs[name] = str(tmp_path / name)
            else:
                path = tmp_path / f"{name}.parquet"
                pq.write_table(table, path, row_group_size=1)
                inputs[name] = str(path)
        begin, end = map(lineage.epoch, trace.WINDOWS[0][1:])
        path = tmp_path / "timestamps.parquet"
        pq.write_table(pa.Table.from_pylist([{"block_number": 1, "timestamp": begin-1},
            {"block_number": 2, "timestamp": begin}, {"block_number": 3, "timestamp": end}]), path)
        inputs["timestamps"] = str(path)
        return inputs
    finally:
        con.close()


def frozen_preflight(inputs):
    con = lineage.connection()
    try:
        preflight = lineage.preflight(inputs, con, stream_filtered=True, windows=trace.WINDOWS)
    finally:
        con.close()
    assert len(preflight["windows"]) == 1
    assert preflight["status"] == "preflight_complete", preflight
    return preflight, [info for infos in preflight["inventories"].values() for info in infos]


def test_march_only_disk_fixture_reopens_inputs_and_preserves_failure(tmp_path):
    inputs = disk_inputs(tmp_path)
    preflight, frozen = frozen_preflight(inputs)
    result = trace.execute_trace(inputs, preflight["windows"][0], frozen)
    assert result["final_input_identity_reopened"] is True
    assert result["trace"]["data_certified"] is False
    assert result["trace"]["full11_window_difference"] == {"left_only_rows": 2, "right_only_rows": 2}
    assert result["filtered_footprints"]["raw"]["fetched_rows"] == 1


def test_preflight_compact_summary_exposes_exact_source_identities(tmp_path):
    inputs = disk_inputs(tmp_path)
    preflight, frozen = frozen_preflight(inputs)
    preflight.update(source_snapshot={"baseline_commit": trace.SOURCE_BASELINE, "source_blobs": {},
                                     "script_sha256": {path: "fixture" for path in trace.SCRIPT_PATHS}},
                     actual_audit_commit="fixture-head", contract=trace.CONTRACT, data_certified=False)
    destination = tmp_path / "preflight_output"
    trace.write_immutable(destination, preflight)
    summary = json.loads((destination / "summary.json").read_text())
    assert (destination / "summary.json").stat().st_size <= 1024**2
    assert len(summary["frozen_inputs"]["clean"]) == 44
    identity = summary["frozen_inputs"]["raw"][0]
    assert all(identity[field] == frozen[0][field] for field in (
        "path", "bytes", "mtime_ns", "footer_sha256", "schema", "fields"))
    assert summary["actual_audit_commit"] == "fixture-head"
    assert set(summary["source_snapshot"]["script_sha256"]) == set(trace.SCRIPT_PATHS)


def test_end_second_native_and_published_rows_are_not_mixed_into_selection(tmp_path):
    inputs = disk_inputs(tmp_path)
    end = lineage.epoch(trace.WINDOWS[0][2])
    for name, extra in (("raw", native("tx-end", block=3)), ("resolved", resolved(native("tx-end", block=3)))):
        path = Path(inputs[name])
        table = pq.read_table(path)
        pq.write_table(pa.Table.from_pylist(table.to_pylist()+[extra], schema=table.schema), path, row_group_size=1)
    for name in ("root_transformed", "clean"):
        path = Path(inputs[name]) / "year_month=2026-03/data.parquet"
        table = pq.read_table(path, partitioning=None)
        rows = table.to_pylist()
        extra = [{**row, "timestamp": end} for row in rows]
        pq.write_table(pa.Table.from_pylist(rows+extra, schema=table.schema), path, row_group_size=1)
    preflight, frozen = frozen_preflight(inputs)
    result = trace.execute_trace(inputs, preflight["windows"][0], frozen)
    assert result["filtered_footprints"]["raw"]["fetched_rows"] == 2
    assert result["trace"]["selected_transaction"]["transaction_hash"] == "tx-a"
    assert all(row["transaction_hash"] == "tx-a" for row in result["trace"]["all_native_role_candidates"])
    assert result["filtered_footprints"]["root_transformed"]["fetched_rows"] == 2


def test_frozen_mutation_blocks_before_any_fetch(tmp_path, monkeypatch):
    inputs = disk_inputs(tmp_path)
    preflight, frozen = frozen_preflight(inputs)
    path = Path(inputs["raw"])
    table = pq.read_table(path)
    pq.write_table(table, path, row_group_size=2)
    monkeypatch.setattr(lineage, "fetch_streamed", lambda *args: pytest.fail("fetch after mutation"))
    with pytest.raises(lineage.AuditBlocked, match="frozen input changed"):
        trace.execute_trace(inputs, preflight["windows"][0], frozen)


def test_resource_refusal_happens_before_any_fetch(tmp_path, monkeypatch):
    inputs = disk_inputs(tmp_path)
    preflight, frozen = frozen_preflight(inputs)
    monkeypatch.setattr(lineage, "MAX_ROWS", 1)
    monkeypatch.setattr(lineage, "fetch_streamed", lambda *args: pytest.fail("fetch before caps"))
    with pytest.raises(lineage.AuditBlocked, match="exact filtered rows"):
        trace.execute_trace(inputs, preflight["windows"][0], frozen)


def test_mutation_during_trace_blocks_final_reopen(tmp_path, monkeypatch):
    inputs = disk_inputs(tmp_path)
    preflight, frozen = frozen_preflight(inputs)
    original = trace.build_trace

    def mutate_after_trace(con):
        result = original(con)
        path = Path(inputs["raw"])
        table = pq.read_table(path)
        rows = table.to_pylist()
        rows[0]["fee"] += 1
        pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
        return result

    monkeypatch.setattr(trace, "build_trace", mutate_after_trace)
    with pytest.raises(lineage.AuditBlocked, match="frozen input changed"):
        trace.execute_trace(inputs, preflight["windows"][0], frozen)


def test_help_is_dependency_light_and_guard_blocks_local_production(tmp_path):
    script = Path(trace.__file__)
    result = subprocess.run([sys.executable, "-S", str(script), "--help"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        result = subprocess.run([sys.executable, "-S", str(script), "--preflight", "--expected-head", "0"*40, "--run-dir", str(tmp_path / "never")], capture_output=True, text=True)
        assert result.returncode != 0 and "canonical EC2" in result.stderr
        assert not (tmp_path / "never").exists()
