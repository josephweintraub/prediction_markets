"""Small synthetic-only lineage fixtures; production guard is never bypassed."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts import audit_polymarket_lineage as audit


def native(*, tx="tx", log=1, order="order", exchange=audit.OLD_EXCHANGES[0],
           maker="maker", taker="taker", block=2, cash=40_000_000,
           quantity=100_000_000):
    return dict(zip(audit.RAW_FIELDS, (
        order, maker, taker, "1", "0", quantity, cash, 0, block, tx, log, exchange)))


def resolved(row):
    return {**row, "condition_id": "market", "outcome": "YES", "market_slug": "m",
            "event_slug": "event", "question": "question", "outcome_token_side": "maker",
            "winning_outcome": "YES"}


def register(con, name, rows):
    con.register(name, pa.Table.from_pylist(rows))


def group(index, low, high, rows=1, *, nulls=0, size=1):
    return {"index": index, "rows": rows, "compressed_bytes": size,
            "uncompressed_bytes": size,
            "stats": {"block_number": {"min": low, "max": high, "null_count": nulls}}}


def test_complete_group_selection_keeps_split_boundaries_and_excludes_end():
    info = {"path": "fixture", "row_groups": [
        group(0, 1, 2, 2), group(1, 2, 3, 2), group(2, 4, 4)]}
    selected = audit.select_groups([info], "block_number", 2, 4)
    assert [item["index"] for item in selected["groups"]] == [0, 1]
    assert selected["selected_rows"] == 4
    with pytest.raises(audit.AuditBlocked, match="requires 4 rows"):
        audit.select_groups([info], "block_number", 2, 4, cap=3)


@pytest.mark.parametrize("stats", [None, {"min": 2, "max": 3, "null_count": 1},
                                    {"min": 3, "max": 2, "null_count": 0}])
def test_unknown_null_or_reversed_statistics_never_silently_prune(stats):
    info = {"path": "fixture", "row_groups": [group(0, 2, 3)]}
    info["row_groups"][0]["stats"]["block_number"] = stats
    with pytest.raises(audit.AuditBlocked):
        audit.select_groups([info], "block_number", 2, 4)


def test_footer_is_frozen_and_exact_boundary_row_groups_are_read(tmp_path):
    path = tmp_path / "raw.parquet"
    rows = [native(block=1), native(block=2, log=2),
            native(block=2, log=3), native(block=3, log=4), native(block=4, log=5)]
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=2)
    info = audit.footer_info(path, "raw")
    assert info["rows"] == 5 and len(info["footer_sha256"]) == 64
    selection = audit.select_groups([info], "block_number", 2, 4)
    table = audit.read_selection(selection, audit.RAW_FIELDS)
    assert table["block_number"].to_pylist() == [2, 2, 3]
    assert [g["index"] for g in selection["groups"]] == [0, 1]


def test_schema_gate_does_not_infer_missing_native_fields(tmp_path):
    path = tmp_path / "wrong.parquet"
    pq.write_table(pa.table({"block_number": [2]}), path)
    with pytest.raises(audit.AuditBlocked, match="required schema"):
        audit.footer_info(path, "raw")


def test_exact_timestamp_fences_keep_all_same_second_blocks_and_end_exclusive(tmp_path):
    path = tmp_path / "ts.parquet"
    rows = [{"block_number": b, "timestamp": ts} for b, ts in
            [(1, 99), (2, 100), (3, 100), (4, 159), (5, 160), (6, 161)]]
    pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=2)
    con = audit.connection()
    try:
        fences = audit.timestamp_fences(con, str(path), 100, 160)
        assert fences["lower_fence_block"] == 1
        assert fences["first_window_block"] == 2 and fences["last_window_block"] == 4
        assert fences["upper_fence_block"] == 5
        assert fences["all_integer_blocks_cached"]
    finally:
        con.close()


def test_duplicate_timestamp_block_key_fails_even_when_payload_agrees(tmp_path):
    path = tmp_path / "ts.parquet"
    pq.write_table(pa.Table.from_pylist([
        {"block_number": 1, "timestamp": 99}, {"block_number": 2, "timestamp": 100},
        {"block_number": 2, "timestamp": 100}, {"block_number": 3, "timestamp": 160}]), path)
    con = audit.connection()
    try:
        with pytest.raises(audit.AuditBlocked, match="conflicting/null/nonmonotonic"):
            audit.timestamp_fences(con, str(path), 100, 160)
    finally:
        con.close()


def test_replay_conflict_and_repeated_orders_are_separate():
    row = native()
    con = audit.connection()
    try:
        register(con, "raw", [row, dict(row), native(log=2)])
        result = audit.native_summary(con)
        assert result["identity"]["identical_replay_surplus"] == 1
        assert result["identity"]["conflicting_native_keys"] == 0
        assert result["repeated_order_candidates"]["candidate_logs_beyond_first"] == 1
        assert result["canonical_retention"]["canonical_retained_native_rows"] == 1
        assert "full batch evidence" in result["repeated_order_note"]
        con.unregister("raw")
        register(con, "raw", [row, {**row, "maker_amount_filled": 100_000_001}])
        result = audit.native_summary(con)
        assert result["identity"]["identical_replay_surplus"] == 0
        assert result["identity"]["conflicting_native_keys"] == 1
    finally:
        con.close()


@pytest.mark.parametrize("changes", [{"transaction_hash": ""}, {"transaction_hash": None},
                                     {"exchange_address": ""}, {"log_index": -1}])
def test_null_blank_invalid_identity_visible(changes):
    con = audit.connection()
    try:
        register(con, "raw", [native(), {**native(log=2), **changes}])
        assert audit.native_summary(con)["integrity"]["invalid_native_keys"] == 1
    finally:
        con.close()


def test_exchange_roles_and_collateral_denominators_do_not_double_count_replay():
    legacy = native(log=1)
    aggregate = native(log=2, order="aggregate", maker="active",
                       taker=audit.NEW_EXCHANGES[0], exchange=audit.NEW_EXCHANGES[0])
    con = audit.connection()
    try:
        register(con, "raw", [legacy, dict(legacy), aggregate])
        result = audit.native_summary(con)
        roles = result["roles"]
        assert result["integrity"]["observed_source_rows"] == 3
        assert roles["distinct_payload_rows"] == 2
        assert roles["exchange_facing_rows"] == 1
        assert roles["all_native_recorded_collateral_micro"] == 80_000_000
        assert roles["aggregate_recorded_collateral_micro"] == 40_000_000
        assert roles["nonaggregate_recorded_collateral_micro"] == 40_000_000
        # Current canonical filter excludes only the old addresses.
        assert result["canonical_retention"]["canonical_retained_native_rows"] == 2
        assert "not unique economic volume" in result["denominator_note"]
    finally:
        con.close()


def test_equal_value_distinct_ids_and_same_id_replays_have_exact_attribution():
    con = audit.connection()
    try:
        row = native()
        register(con, "resolved", [resolved(row), resolved(dict(row)), resolved(native(tx="other", log=2))])
        register(con, "timestamp_slice", [{"block_number": 2, "timestamp": 100}])
        audit.create_expansion(con, "resolved", "expanded")
        result = audit.value_summary(con, "expanded", identities=True)
        assert result["row_count"] == 6 and result["value_groups"] == 2
        assert result["expanded_recorded_cash"] == 240
        assert result["value_surplus"] == 4
        assert result["same_native_role_value_surplus"] == 2
        assert result["distinct_native_role_value_surplus"] == 2
        assert result["equal_value_distinct_native_groups"] == 2
    finally:
        con.close()


def test_exclusive_endpoint_and_payload_multiplicity_reconciliation():
    con = audit.connection()
    try:
        register(con, "left_rows", [{"n": 1}, {"n": 1}, {"n": 2}])
        register(con, "right_rows", [{"n": 1}, {"n": 3}])
        assert audit.multiplicity_difference(con, "left_rows", "right_rows", ("n",)) == {
            "left_only_rows": 2, "right_only_rows": 1}
    finally:
        con.close()


def fixture_inputs(tmp_path):
    inputs = {}
    ts_rows = []
    raw_rows = []
    resolved_rows = []
    for index, (_, begin, end) in enumerate(audit.WINDOWS):
        low = index * 10 + 1
        for block, ts in [(low, audit.epoch(begin)-1), (low+1, audit.epoch(begin)),
                          (low+2, audit.epoch(end))]:
            ts_rows.append({"block_number": block, "timestamp": ts})
        row = native(tx=f"tx-{index}", block=low+1)
        raw_rows.append(row)
        resolved_rows.append(resolved(row))
        con = audit.connection()
        try:
            register(con, "resolved", [resolved(row)])
            register(con, "timestamp_slice", [ts_rows[-2]])
            audit.create_expansion(con, "resolved", "expanded")
            values = con.execute("SELECT " + ','.join(audit.qname(f) for f in audit.VALUE_FIELDS)
                                 + " FROM expanded").fetch_arrow_table()
        finally:
            con.close()
        for relation in ("root_transformed", "clean"):
            directory = tmp_path / relation / f"year_month={begin[:7]}"
            directory.mkdir(parents=True)
            physical = values if relation == "clean" else values.drop(["year_month"])
            pq.write_table(physical, directory / "data.parquet", row_group_size=1)
            inputs[relation] = str(directory.parent)
    datasets = {
        "raw": raw_rows, "resolved": resolved_rows, "timestamps": ts_rows,
        "token_map": [{"token_id": "1", "condition_id": "market", "outcome": "YES",
                       "market_slug": "m", "event_slug": "event", "question": "question"}],
        "resolutions": [{"token_id": "1", "condition_id": "market", "winning_outcome": "YES"}],
    }
    for name, rows in datasets.items():
        path = tmp_path / f"{name}.parquet"
        pq.write_table(pa.Table.from_pylist(rows), path, row_group_size=1)
        inputs[name] = str(path)
    return inputs


def test_predeclared_fixture_windows_reconcile_and_outputs_never_overwrite(tmp_path):
    inputs = fixture_inputs(tmp_path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con)
    finally:
        con.close()
    assert preflight["status"] == "preflight_complete"
    assert [w["start_utc"] for w in preflight["windows"]] == [w[1] for w in audit.WINDOWS]
    for window in preflight["windows"]:
        result = audit.audit_window(inputs, window)
        assert result["status"] == "bounded_reconciliation_complete", result
        assert result["waterfall"]["raw_observed_rows"] == 1
        assert result["root_transformed"]["row_count"] == 2
        assert result["clean"]["row_count"] == 2
        assert result["clean_removal"]["removed_expanded_value_rows"] == 0
    destination = tmp_path / "run"
    audit.write_immutable(destination, preflight)
    assert json.loads((destination / "manifest.json").read_text())["status"] == "preflight_complete"
    with pytest.raises(FileExistsError):
        audit.write_immutable(destination, preflight)


def test_row_cap_passes_but_byte_cap_fails_before_body_reads(tmp_path, monkeypatch):
    inputs = fixture_inputs(tmp_path)
    monkeypatch.setattr(audit, "MAX_COMBINED_UNCOMPRESSED_BYTES", 1)
    con = audit.connection()
    try:
        result = audit.preflight(inputs, con)
    finally:
        con.close()
    assert result["status"] == "preflight_blocked"
    assert all("uncompressed bytes" in item["reason"] for item in result["blocks"])


@pytest.mark.parametrize("relation,field,value,gate", [
    ("resolutions", "condition_id", "OTHER", "resolution_market_conflicts"),
    ("resolutions", "winning_outcome", "garbage", "winner_outside_market_outcome_universe"),
    ("token_map", "outcome", "", "invalid_touched_token_metadata"),
    ("token_map", "condition_id", None, "invalid_touched_token_metadata"),
])
def test_reproduced_invalid_metadata_cannot_certify_lineage(tmp_path, relation, field, value, gate):
    inputs = fixture_inputs(tmp_path)
    table = pq.read_table(inputs[relation])
    rows = table.to_pylist()
    rows[0][field] = value
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), inputs[relation])
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con)
    finally:
        con.close()
    assert preflight["status"] == "preflight_complete"
    result = audit.audit_window(inputs, preflight["windows"][0])
    assert result["status"] == "blocked_metadata_integrity"
    assert result["metadata_integrity"][gate] == 1


def test_required_timestamp_coverage_not_total_cache_size(tmp_path):
    inputs = fixture_inputs(tmp_path)
    con = audit.connection()
    try:
        result = audit.preflight(inputs, con)
    finally:
        con.close()
    # Extra unrelated cache key leaves total rows unchanged but cannot replace block2.
    cache = pq.read_table(inputs["timestamps"]).to_pylist()
    cache[1] = {"block_number": 999, "timestamp": cache[1]["timestamp"]}
    pq.write_table(pa.Table.from_pylist(cache), inputs["timestamps"], row_group_size=1)
    with pytest.raises(audit.AuditBlocked, match="missing exact required timestamps"):
        audit.audit_window(inputs, result["windows"][0])


def test_help_and_guard_work_without_site_dependencies(tmp_path):
    script = Path(audit.__file__)
    help_result = subprocess.run([sys.executable, "-S", str(script), "--help"],
                                 text=True, capture_output=True)
    assert help_result.returncode == 0, help_result.stderr
    from production_guard import require_production_host
    try:
        require_production_host()
    except RuntimeError:
        pass
    else:
        # The real canonical host is allowed past the guard. Keep the help
        # assertion above, but do not demand a nonproduction refusal here.
        return
    blocked = subprocess.run([sys.executable, "-S", str(script), "--preflight",
                              "--run-dir", str(tmp_path / "never-created")],
                             text=True, capture_output=True)
    assert blocked.returncode != 0
    assert "canonical EC2" in blocked.stderr
    assert not (tmp_path / "never-created").exists()


def all_month_inputs(tmp_path):
    inputs = fixture_inputs(tmp_path)
    seed = pq.read_table(Path(inputs["clean"]) / "year_month=2026-03" / "data.parquet",
                         partitioning=None).to_pylist()
    for month in audit.canonical_months():
        for relation in ("root_transformed", "clean"):
            directory = Path(inputs[relation]) / f"year_month={month}"
            if directory.exists():
                continue
            directory.mkdir()
            rows = [{**row, "timestamp": audit.month_bounds(month)[0] + 3600, "year_month": month}
                    for row in seed]
            table = pa.Table.from_pylist(rows)
            pq.write_table(table if relation == "clean" else table.drop(["year_month"]),
                           directory / "data.parquet")
    return inputs


def test_streamed_mode_materializes_whole_exact_minute_from_broad_month_groups(tmp_path):
    inputs = all_month_inputs(tmp_path)
    # A broad group spans a whole month; only two records belong to the frozen minute.
    for relation in ("root_transformed", "clean"):
        path = Path(inputs[relation]) / "year_month=2026-03" / "data.parquet"
        table = pq.read_table(path, partitioning=None)
        rows = table.to_pylist()
        extra = [{**rows[0], "timestamp": audit.month_bounds("2026-03")[0] + i}
                 for i in range(100)]
        pq.write_table(pa.Table.from_pylist(rows + extra, schema=table.schema), path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    assert preflight["status"] == "preflight_complete"
    assert len(preflight["published_month_proofs"]["clean"]) == 44
    window = preflight["windows"][0]
    assert window["selections"]["clean"]["selected_rows"] == 102
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    result = audit.audit_window(inputs, window, frozen)
    assert result["status"] == "bounded_reconciliation_complete", result
    assert result["filtered_footprints"]["clean"]["filtered_rows"] == 2
    assert result["filtered_footprints"]["clean"]["fetched_rows"] == 2
    assert result["peak_rss_bytes"] > 0
    query = audit.selected_query(window["selections"]["clean"], audit.VALUE_FIELDS, "clean", "2026-03")
    assert "LIMIT" not in query.upper() and 'WHERE "timestamp">=' in query


@pytest.mark.parametrize("changes,match", [
    ({"raw": {"filtered_rows": 2_000_001, "arrow_payload_estimate_bytes": 1}}, "exact filtered rows"),
    ({"raw": {"filtered_rows": 1, "arrow_payload_estimate_bytes": 536_870_913}}, "Arrow payload"),
])
def test_streamed_count_and_buffer_caps_never_truncate(changes, match):
    with pytest.raises(audit.AuditBlocked, match=match):
        audit.enforce_stream_footprints(changes)


def test_streamed_utf8_estimator_counts_nulls_multibyte_and_hive_offsets(tmp_path):
    con = audit.connection()
    try:
        rows = [{field: "é" for field in audit.VALUE_FIELDS} for _ in range(2)]
        for index, row in enumerate(rows):
            row.update(timestamp=100, usdcSize=1.0, price=0.5, is_maker=True)
        rows[1]["proxyWallet"] = None
        register(con, "values_fixture", rows)
        counts = audit.streamed_footprint(con, "SELECT * FROM values_fixture", "clean")
        assert counts["filtered_rows"] == 2
        assert counts["utf8_payload_bytes"] == 26  # Seven string fields, one null.
        table = audit.fetch_streamed(con, "SELECT * FROM values_fixture", counts)
        assert table.num_rows == 2 and table.nbytes <= counts["arrow_payload_estimate_bytes"]
        wrong = {**counts, "filtered_rows": 1}
        with pytest.raises(audit.AuditBlocked, match="count differs"):
            audit.fetch_streamed(con, "SELECT * FROM values_fixture", wrong)
    finally:
        con.close()


@pytest.mark.parametrize("bad_timestamp", [None, audit.epoch("2026-03-01T18:00:00Z")])
def test_neighbor_month_cannot_hide_null_or_window_boundary_rows(tmp_path, bad_timestamp):
    inputs = all_month_inputs(tmp_path)
    path = Path(inputs["clean"]) / "year_month=2026-02" / "data.parquet"
    table = pq.read_table(path, partitioning=None)
    rows = table.to_pylist()
    rows[0]["timestamp"] = bad_timestamp
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path)
    con = audit.connection()
    try:
        result = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    assert result["status"] == "preflight_blocked"
    assert result["blocks"][0]["stage"] == "all_month_boundary_proof"


def test_snapshot_mutation_blocks_after_scalar_stage_and_preserves_rss(tmp_path):
    inputs = all_month_inputs(tmp_path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    path = Path(inputs["raw"])
    table = pq.read_table(path)
    rows = table.to_pylist()
    rows[0]["fee"] = 1
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), path, row_group_size=1)
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    assert result["status"] == "blocked"
    assert "frozen input changed" in result["reason"]
    assert result["peak_rss_bytes"] > 0
    assert not any("fetched_rows" in item for item in result["filtered_footprints"].values())


def test_streamed_rejection_before_fetch_never_publishes_success(tmp_path, monkeypatch):
    inputs = all_month_inputs(tmp_path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    monkeypatch.setattr(audit, "MAX_ROWS", 1)
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    assert result["status"] == "blocked" and "exact filtered rows" in result["reason"]
    assert not any("fetched_rows" in item for item in result["filtered_footprints"].values())
    assert result["peak_rss_bytes"] > 0


@pytest.mark.parametrize("relative", ["extra.parquet", "year_month=2026-03/nested/extra.parquet", "extra.txt"])
def test_exact_published_layout_rejects_paths_canonical_glob_could_omit(tmp_path, relative):
    inputs = all_month_inputs(tmp_path)
    path = Path(inputs["clean"]) / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pq.read_table(Path(inputs["clean"]) / "year_month=2026-03/data.parquet", partitioning=None)
    pq.write_table(table, path)
    con = audit.connection()
    try:
        result = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    assert result["status"] == "preflight_blocked"
    assert "layout" in result["blocks"][0]["reason"]


def test_post_preflight_partition_file_addition_is_not_silently_omitted(tmp_path):
    inputs = all_month_inputs(tmp_path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    month = Path(inputs["clean"]) / "year_month=2026-03"
    pq.write_table(pq.read_table(month / "data.parquet", partitioning=None), month / "additional.parquet")
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    assert result["status"] == "blocked"
    assert "directory/file set changed" in result["reason"]
    assert not result["filtered_footprints"]


def test_streamed_query_preserves_wrong_physical_root_month_for_reconciliation(tmp_path):
    inputs = all_month_inputs(tmp_path)
    path = Path(inputs["root_transformed"]) / "year_month=2026-03/data.parquet"
    table = pq.read_table(path, partitioning=None)
    pq.write_table(table.append_column("year_month", pa.array(["WRONG"] * table.num_rows)), path)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    assert result["status"] == "bounded_reconciliation_failed"
    assert result["expanded_to_root_membership"] == {"left_only_rows": 2, "right_only_rows": 2}


@pytest.mark.parametrize("zero_rows", [1, 2])
def test_actual_collateral_join_keys_block_hijack_and_reproduced_fanout(tmp_path, zero_rows):
    inputs = all_month_inputs(tmp_path)
    token_table = pq.read_table(inputs["token_map"])
    rows = token_table.to_pylist()
    rows.extend({**rows[0], "token_id": "0"} for _ in range(zero_rows))
    pq.write_table(pa.Table.from_pylist(rows, schema=token_table.schema), inputs["token_map"])
    if zero_rows == 2:
        # Reproduce the original pipeline's duplicated resolved and transformed
        # rows; clean remains full-value DISTINCT. Reproduction must not pass QA.
        table = pq.read_table(inputs["resolved"])
        pq.write_table(pa.concat_tables([table, table]), inputs["resolved"], row_group_size=1)
        for month in ("2026-03", "2026-06"):
            path = Path(inputs["root_transformed"]) / f"year_month={month}/data.parquet"
            table = pq.read_table(path, partitioning=None)
            pq.write_table(pa.concat_tables([table, table]), path, row_group_size=1)
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    if zero_rows == 1:
        assert result["status"] == "blocked_metadata_integrity"
        assert result["metadata_integrity"]["invalid_touched_token_metadata"] == 1
    else:
        assert result["status"] == "blocked_metadata_fanout"
        assert any(gate["gate"] == "token_map_duplicate_touched_keys" for gate in result["gates"])


@pytest.mark.parametrize("other_winner", ["NO", None, ""])
def test_resolution_tokens_for_touched_market_must_share_one_nonblank_winner(tmp_path, other_winner):
    inputs = all_month_inputs(tmp_path)
    table = pq.read_table(inputs["token_map"])
    rows = table.to_pylist()
    rows.append({**rows[0], "token_id": "2", "outcome": "NO"})
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), inputs["token_map"])
    table = pq.read_table(inputs["resolutions"])
    rows = table.to_pylist()
    rows.append({**rows[0], "token_id": "2", "winning_outcome": other_winner})
    pq.write_table(pa.Table.from_pylist(rows, schema=table.schema), inputs["resolutions"])
    con = audit.connection()
    try:
        preflight = audit.preflight(inputs, con, stream_filtered=True)
    finally:
        con.close()
    frozen = [info for infos in preflight["inventories"].values() for info in infos]
    result = audit.audit_window(inputs, preflight["windows"][0], frozen)
    assert result["status"] == "blocked_metadata_integrity"
    assert result["metadata_integrity"]["conflicting_market_winner_groups"] == 1


def test_canonical_price_exclusions_reconcile_without_execution_claim():
    con = audit.connection()
    try:
        rows = [resolved(native()), resolved(native(tx="invalid", log=2, quantity=1_000_000, cash=2_000_000)),
                resolved(native(tx="aggregate", log=3, quantity=1_000_000, cash=2_000_000,
                                exchange=audit.NEW_EXCHANGES[0], taker=audit.NEW_EXCHANGES[0]))]
        register(con, "resolved", rows)
        register(con, "timestamp_slice", [{"block_number": 2, "timestamp": 100}])
        audit.create_expansion(con, "resolved", "expanded")
        result = audit.stage6_price_summary(con, "resolved", "expanded")
        assert result["current_resolved_native_rows"] == 3
        assert result["potential_expanded_rows"] == 6
        assert result["canonical_price_excluded_native_rows"] == 2
        assert result["canonical_price_excluded_expanded_rows"] == 4
        assert result["price_excluded_exchange_facing_records"] == 1
        assert result["admitted_expanded_rows"] == 2 and result["count_reconciles"]
        assert "not certified execution price" in result["price_note"]
    finally:
        con.close()


def diagnostic_values():
    common = {"timestamp": 100, "conditionId": "token", "usdcSize": 40.0, "price": 0.4,
              "outcome": "YES", "eventSlug": "event", "year_month": "2026-03"}
    return [{**common, "proxyWallet": "wallet_a", "counterparty": "wallet_b", "side": "BUY", "is_maker": True},
            {**common, "proxyWallet": "wallet_b", "counterparty": "wallet_a", "side": "SELL", "is_maker": False}]


@pytest.mark.parametrize("field,value", [("timestamp", 101), ("eventSlug", "backfilled-event")])
def test_membership_diagnostics_isolate_single_field_representation(field, value):
    rows = diagnostic_values()
    changed = [{**row, field: value} for row in rows]
    con = audit.connection()
    try:
        register(con, "left_values", rows)
        register(con, "right_values", changed)
        full = audit.multiplicity_difference(con, "left_values", "right_values", audit.VALUE_FIELDS)
        diagnostic = audit.field_membership_diagnostics(con, "left_values", "right_values")
        assert full == {"left_only_rows": 2, "right_only_rows": 2}
        assert diagnostic["per_field_marginal"][field] == full
        assert diagnostic["full_payload_drop_one_field"][field] == {"left_only_rows": 0, "right_only_rows": 0}
        assert all(not any(counts.values()) for name, counts in diagnostic["per_field_marginal"].items() if name != field)
        assert all(any(counts.values()) for name, counts in diagnostic["full_payload_drop_one_field"].items() if name != field)
        assert "never relaxes" in diagnostic["note"]
    finally:
        con.close()


@pytest.mark.parametrize("swap_role", [False, True])
def test_membership_diagnostics_distinguish_matching_marginals_from_role_correlation(swap_role):
    rows = diagnostic_values()
    changed = [{**row, "side": "SELL" if row["side"] == "BUY" else "BUY",
                "is_maker": not row["is_maker"] if swap_role else row["is_maker"]} for row in rows]
    con = audit.connection()
    try:
        register(con, "left_values", rows)
        register(con, "right_values", changed)
        diagnostic = audit.field_membership_diagnostics(con, "left_values", "right_values")
        assert audit.multiplicity_difference(con, "left_values", "right_values", audit.VALUE_FIELDS) == {
            "left_only_rows": 2, "right_only_rows": 2}
        assert all(not any(counts.values()) for counts in diagnostic["per_field_marginal"].values())
        resolved_omissions = [name for name, counts in diagnostic["full_payload_drop_one_field"].items() if not any(counts.values())]
        assert resolved_omissions == ([] if swap_role else ["side"])
    finally:
        con.close()


def test_membership_diagnostics_preserve_duplicate_counts():
    rows = diagnostic_values()
    con = audit.connection()
    try:
        register(con, "left_values", rows + [dict(rows[0])])
        register(con, "right_values", rows)
        diagnostic = audit.field_membership_diagnostics(con, "left_values", "right_values")
        assert all(counts == {"left_only_rows": 1, "right_only_rows": 0}
                   for counts in diagnostic["per_field_marginal"].values())
        assert all(counts == {"left_only_rows": 1, "right_only_rows": 0}
                   for counts in diagnostic["full_payload_drop_one_field"].values())
    finally:
        con.close()
