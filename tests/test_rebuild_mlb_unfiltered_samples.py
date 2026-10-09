from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import duckdb
import pytest

from scripts import rebuild_mlb_unfiltered_samples as rebuild


HEAD = "1" * 40
SNAPSHOT = Path(__file__).resolve().parents[1] / "output/mlb_unfiltered_rebuild_2026-10-09_v1/source_inputs_v1"


def write_parquet(path: Path, schema: str, rows: list[tuple]) -> None:
    con = duckdb.connect()
    try:
        con.execute("CREATE TABLE fixture(" + schema + ")")
        if rows:
            con.executemany("INSERT INTO fixture VALUES (" + ",".join("?" for _ in rows[0]) + ")", rows)
        con.execute(f"COPY fixture TO '{rebuild.quote(path)}' (FORMAT PARQUET)")
    finally:
        con.close()


def fixture(tmp_path, monkeypatch):
    canonical = rebuild.REPO / "analysis/mlb_game_dynamics"
    folder = canonical if (canonical / "build_exact_trades.py").is_file() else SNAPSHOT
    helper = rebuild.load_frozen_helper(folder / "build_exact_trades.py", folder / "timestamp_provenance.py")
    monkeypatch.setattr(rebuild, "load_helper", lambda: helper)
    monkeypatch.setattr(rebuild, "read_head", lambda: HEAD)
    monkeypatch.setattr(rebuild, "available_memory", lambda: 512000000000)
    monkeypatch.setattr(rebuild, "CAPS", {**rebuild.CAPS, "memory_limit": "128MB", "memory_bytes": 128000000,
        "threads": 1, "spill_bytes": 0, "output_bytes": 20000000, "minimum_free_bytes": 0})
    expected = {"distinct_candidate_fills": 9, "old_exact_rows": 6,
                "old_accepted_all_rows": 4, "old_accepted_filtered_rows": 3}
    monkeypatch.setattr(rebuild, "FROZEN_COUNTS", expected)
    base = 1750000000
    specs = [
        (1, 100, "a", 2000000, 1000000, "0xBASE", "maker"),
        (2, 100, "a", 2000000, 1000000, "0xBASE", "maker"),
        (3, 101, "a", 1000000, 2000000, "0xNEWBOT", "taker"),
        (4, 102, "a", 100000000, 1000000, "0xextreme", "maker"),
        (5, 103, "a", 2000000, 1000000, "0xlegacy", "maker"),
        (6, 104, "a", 2000000, 1000000, "0xpost", "maker"),
        (7, 105, "a", 2000000, 1000000, "0xend", "maker"),
        (8, 106, "b", 2000000, 1000000, "0xoutside", "maker"),
        (9, 107, "a", 10000000, 11000000, "0xinvalidprice", "maker"),
    ]
    rows = []
    for identity, block, market, maker_amount, taker_amount, buyer, side in specs:
        rows.append((f"order{identity}", buyer if side == "taker" else "0xseller",
                     buyer if side == "maker" else "0xseller", "0" if side == "taker" else "yes",
                     "yes" if side == "taker" else "0", maker_amount, taker_amount, 0, block,
                     f"TX{identity}", identity, "0xEXCHANGE", market, "home", "home", side))
    rows.append(rows[0])
    paths = {name: tmp_path / (name + (".json" if name == "timestamp_provenance" else ".parquet"))
             for name in rebuild.INPUT_NAMES}
    schema = "order_hash VARCHAR,maker VARCHAR,taker VARCHAR,maker_asset_id VARCHAR,taker_asset_id VARCHAR," \
             "maker_amount_filled BIGINT,taker_amount_filled BIGINT,fee BIGINT,block_number BIGINT," \
             "transaction_hash VARCHAR,log_index BIGINT,exchange_address VARCHAR,condition_id VARCHAR," \
             "outcome VARCHAR,winning_outcome VARCHAR,outcome_token_side VARCHAR"
    write_parquet(paths["raw"], schema, rows)
    write_parquet(paths["candidates"], "market_id VARCHAR", [("a",), ("b",)])
    timestamps = [(100, base-1), (101, base), (102, base), (103, base+1),
                  (104, base+11), (105, base+10), (106, base-1), (107, base)]
    write_parquet(paths["cache"], "block_number BIGINT,timestamp BIGINT", timestamps)
    write_parquet(paths["wallet_flags"], "proxyWallet VARCHAR,is_nonhuman BOOLEAN",
                  [("0xbase", None), ("0xnewbot", True), ("0xpost", False),
                   ("0xoutside", False), ("0xextreme", False), ("0xinvalidprice", False)])
    start = datetime.fromtimestamp(base, timezone.utc)
    end = datetime.fromtimestamp(base+10, timezone.utc)
    write_parquet(paths["phase"], "market_id VARCHAR,game_pk BIGINT,official_date DATE,winning_outcome VARCHAR," \
                  "actual_start_utc TIMESTAMPTZ,actual_end_utc TIMESTAMPTZ", [("a", 1, start.date(), "home", start, end)])
    declaration = {"schema_version": 1, "method": "polygon_rpc_block_timestamp", "timestamp_unit": "unix_seconds",
        "cache": {"path": str(paths["cache"]), "format": "parquet", "rows": 8, "sha256": rebuild.sha256(paths["cache"])},
        "build_metadata": {"used_exact_cache": True, "source_distinct_blocks": 8, "cache_distinct_blocks": 8,
                           "missing_blocks": 0, "fallback_rows": 0}}
    paths["timestamp_provenance"].write_text(json.dumps(declaration))
    legacy_flags = tmp_path / "legacy_flags.parquet"
    write_parquet(legacy_flags, "proxyWallet VARCHAR,is_nonhuman BOOLEAN", [("0xlegacy", True)])
    con = duckdb.connect()
    try:
        con.execute("SET threads=1"); con.execute("SET memory_limit='128MB'")
        helper.build_exact_trades(con, paths["raw"], paths["candidates"], paths["timestamp_provenance"], legacy_flags, tmp_path/"legacy")
    finally:
        con.close()
    paths["old_exact"] = tmp_path / "legacy/exact_trades.parquet"
    return paths, expected, helper


def run(paths, expected, helper, target):
    reviewed = rebuild.preflight(paths, target, HEAD, helper)
    hashes = {name: rebuild.sha256(path) for name, path in paths.items()}
    return rebuild.build_run(paths, target, HEAD, reviewed, expected_hashes=hashes, expected_counts=expected,
                             command=["fixture_only"])


def alter(path: Path, sql: str) -> None:
    con = duckdb.connect()
    replacement = path.with_suffix(".replacement.parquet")
    try:
        con.execute(f"CREATE TABLE changed AS SELECT * FROM read_parquet('{rebuild.quote(path)}')")
        con.execute(sql)
        con.execute(f"COPY changed TO '{rebuild.quote(replacement)}' (FORMAT PARQUET)")
        replacement.replace(path)
    finally:
        con.close()


def test_complete_fixture_restores_rows_without_changing_payload_or_legacy_loader(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    target = tmp_path / "samples"
    summary = run(paths, expected, helper, target)
    c = summary["counts"]
    assert (c["raw_candidate_rows"], c["distinct_candidate_fills"], c["duplicate_payload_rows"]) == (10, 9, 1)
    assert (c["prefilter_exact_rows"], c["new_accepted_all_rows"], c["new_accepted_filtered_rows"]) == (9, 6, 4)
    assert (c["restored_all_rows"], c["restored_filtered_rows"]) == (2, 1)
    assert (c["outside_accepted_market_rows"], c["post_end_rows"], c["invalid_sample_price_rows"]) == (1, 1, 1)
    assert (c["filtered_extreme_price_exclusions"], c["filtered_flagged_interior_exclusions"]) == (1, 1)
    assert (c["accepted_missing_flag_rows"], c["accepted_null_flag_rows"]) == (2, 2)
    assert (c["candidate_markets"], c["accepted_metadata_markets"], c["accepted_metadata_events"]) == (2, 1, 1)
    assert all(summary["reconciliation"].values()) and summary["data_certified"] is False
    assert summary["scientific_estimators_rerun"] is False
    assert json.loads((target/"summary.json").read_text()) == summary
    manifest = json.loads((target/"manifest.json").read_text())
    assert manifest["summary_artifact"]["sha256"] == rebuild.sha256(target/"summary.json")
    assert sorted(x.name for x in target.iterdir()) == ["all_trades.parquet", "exact_trades.parquet", "filtered_trades.parquet", "manifest.json", "summary.json"]
    con = duckdb.connect()
    try:
        assert con.execute(f"SELECT count(*) FROM read_parquet('{target/'exact_trades.parquet'}') WHERE price>=1").fetchone()[0] == 1
        maker = con.execute(f"SELECT proxyWallet,counterparty,is_maker,price FROM read_parquet('{target/'exact_trades.parquet'}') WHERE transaction_hash='tx3'").fetchone()
        assert maker == ("0xnewbot", "0xseller", True, 0.5)
        assert con.execute(f"SELECT realized_time FROM read_parquet('{target/'all_trades.parquet'}') WHERE transaction_hash='tx7'").fetchone()[0] == 1.0
        assert con.execute(f"SELECT min(realized_time) FROM read_parquet('{target/'all_trades.parquet'}')").fetchone()[0] == -0.1
        assert con.execute(f"SELECT count(*) FROM read_parquet('{target/'filtered_trades.parquet'}') WHERE transaction_hash='tx5'").fetchone()[0] == 1
    finally:
        con.close()
    assert summary["outputs"]["exact_trades.parquet"]["schema"][0] == ["market_id", "VARCHAR"]
    assert summary["environment"]["runtime_settings"]["TimeZone"] == "UTC"
    assert all(x["stat_before"] == x["stat_after"] for x in summary["inputs"].values())


@pytest.mark.parametrize("name,sql,match", [
    ("wallet_flags", "INSERT INTO changed VALUES('0xBASE',false)", "nonunique lowercased"),
    ("wallet_flags", "UPDATE changed SET proxyWallet=NULL WHERE proxyWallet='0xbase'", "null/blank flag"),
    ("phase", "INSERT INTO changed SELECT market_id,game_pk,official_date,winning_outcome,actual_start_utc-INTERVAL 1 SECOND,actual_end_utc FROM changed", "nonunique market"),
    ("phase", "UPDATE changed SET actual_end_utc=actual_start_utc", "invalid timing"),
    ("phase", "UPDATE changed SET actual_start_utc='infinity'::TIMESTAMPTZ", "invalid timing"),
    ("phase", "UPDATE changed SET winning_outcome='away'", "winning outcome contradiction"),
    ("raw", "UPDATE changed SET maker_amount_filled=0 WHERE transaction_hash='TX4'", "invalid candidate payload"),
    ("raw", "UPDATE changed SET outcome=NULL WHERE transaction_hash='TX4'", "null required payload"),
    ("raw", "UPDATE changed SET order_hash='' WHERE transaction_hash='TX4'", "invalid candidate payload"),
    ("raw", "UPDATE changed SET maker_asset_id='' WHERE transaction_hash='TX4'", "invalid candidate payload"),
    ("raw", "ALTER TABLE changed ALTER COLUMN transaction_hash TYPE BIGINT USING 1", "raw string payload field type"),
    ("raw", "ALTER TABLE changed ALTER COLUMN exchange_address TYPE BIGINT USING 1", "raw string payload field type"),
    ("raw", "UPDATE changed SET outcome='away' WHERE transaction_hash='TX4'", "ambiguous token/outcome"),
    ("old_exact", "UPDATE changed SET price=price+0.000000000001 WHERE transaction_hash='tx1'", "full payload missing"),
    ("old_exact", "INSERT INTO changed SELECT * FROM changed LIMIT 1", "duplicate fill identity"),
])
def test_adverse_inputs_fail_closed_and_preserve_failure_evidence(tmp_path, monkeypatch, name, sql, match):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    alter(paths[name], sql)
    target = tmp_path/"samples"
    with pytest.raises(ValueError, match=match):
        run(paths, expected, helper, target)
    assert not target.exists()
    failures = list(tmp_path.glob(".samples.staging-*/failure.json"))
    assert len(failures) == 1 and json.loads(failures[0].read_text())["status"] == "blocked_mlb_rebuild"


@pytest.mark.parametrize("timestamp", [0, None])
def test_nonpositive_or_null_exact_timestamp_blocks(tmp_path, monkeypatch, timestamp):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    alter(paths["cache"], "UPDATE changed SET timestamp=" + ("NULL" if timestamp is None else str(timestamp)) + " WHERE block_number=100")
    declaration = json.loads(paths["timestamp_provenance"].read_text())
    declaration["cache"]["sha256"] = rebuild.sha256(paths["cache"])
    paths["timestamp_provenance"].write_text(json.dumps(declaration))
    with pytest.raises(ValueError, match="nonpositive|contains null"):
        run(paths, expected, helper, tmp_path/"samples")


def test_known_hash_and_reviewed_stat_mismatch_refuse_before_body(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    target = tmp_path/"samples"
    reviewed = rebuild.preflight(paths, target, HEAD, helper)
    with pytest.raises(rebuild.RebuildBlocked, match="frozen input SHA-256 differs"):
        rebuild.build_run(paths, target, HEAD, reviewed, expected_counts=expected)
    assert not target.exists()
    alter(paths["wallet_flags"], "UPDATE changed SET is_nonhuman=false")
    with pytest.raises(rebuild.RebuildBlocked, match="reviewed preflight differs"):
        rebuild.build_run(paths, target, HEAD, reviewed, expected_counts=expected)


def test_old_loader_count_is_reproduction_gate_not_new_filtered_target(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    with pytest.raises(rebuild.RebuildBlocked, match="old-loader/frozen count differs"):
        run(paths, {**expected, "old_accepted_filtered_rows": 4}, helper, tmp_path/"samples")


def test_resource_admission_has_no_body_and_refuses_low_disk_ram_and_source_change(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    monkeypatch.setattr(rebuild, "create_relations", lambda *_a: (_ for _ in ()).throw(AssertionError("body must not run")))
    assert rebuild.preflight(paths, tmp_path/"preflight", HEAD, helper)["full_input_hashes_verified"] is False
    with monkeypatch.context() as patch:
        patch.setattr(rebuild.shutil, "disk_usage", lambda _p: type("Disk", (), {"free": 0})())
        with pytest.raises(rebuild.RebuildBlocked, match="insufficient disk"):
            rebuild.preflight(paths, tmp_path/"preflight", HEAD, helper)
    with monkeypatch.context() as patch:
        patch.setattr(rebuild, "available_memory", lambda: 0)
        with pytest.raises(rebuild.RebuildBlocked, match="insufficient available RAM"):
            rebuild.preflight(paths, tmp_path/"preflight", HEAD, helper)
    with monkeypatch.context() as patch:
        patch.setattr(rebuild, "read_head", lambda: "2"*40)
        with pytest.raises(rebuild.RebuildBlocked, match="HEAD differs"):
            rebuild.preflight(paths, tmp_path/"preflight", HEAD, helper)


def test_publication_existing_and_racing_destination_never_overwritten(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    target = tmp_path/"samples"; target.mkdir(); sentinel=target/"sentinel.txt"; sentinel.write_text("keep")
    with pytest.raises(FileExistsError):
        run(paths, expected, helper, target)
    assert sentinel.read_text() == "keep"
    stage=tmp_path/"source_stage"; stage.mkdir(); (stage/"new.txt").write_text("new")
    with pytest.raises(OSError):
        rebuild.atomic_publish(stage,target)
    assert sentinel.read_text() == "keep" and stage.exists()


def test_after_body_input_mutation_and_output_cap_refuse(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    original=rebuild.create_relations
    def changed(*args):
        result=original(*args)
        with paths["timestamp_provenance"].open("a") as stream:stream.write(" ")
        return result
    with monkeypatch.context() as patch:
        patch.setattr(rebuild,"create_relations",changed)
        with pytest.raises(rebuild.RebuildBlocked,match="input changed before publication"):
            run(paths,expected,helper,tmp_path/"samples")
    with monkeypatch.context() as patch:
        patch.setitem(rebuild.CAPS,"output_bytes",1)
        with pytest.raises(rebuild.RebuildBlocked,match="output cap"):
            run(paths,expected,helper,tmp_path/"smallcap")


def test_cli_guard_and_no_hash_count_override_flags(monkeypatch):
    import production_guard
    monkeypatch.setattr(production_guard,"require_production_host",lambda: (_ for _ in ()).throw(RuntimeError("guard")))
    with pytest.raises(RuntimeError,match="guard"):
        rebuild.main([])
    with pytest.raises(SystemExit):
        rebuild.parse_args(["--expected-distinct-fills","9"])


def test_output_free_floor_is_checked_after_copy(tmp_path, monkeypatch):
    paths, expected, helper = fixture(tmp_path, monkeypatch)
    target = tmp_path/"samples"
    original = rebuild.shutil.disk_usage
    def remaining(path):
        if Path(path).name.startswith(".samples.staging-") and (Path(path)/"exact_trades.parquet").exists():
            return type("Disk", (), {"free": -1})()
        return original(path)
    monkeypatch.setattr(rebuild.shutil, "disk_usage", remaining)
    with pytest.raises(rebuild.RebuildBlocked, match="free disk floor breached after output"):
        run(paths, expected, helper, target)
    assert not target.exists()
    assert len(list(tmp_path.glob(".samples.staging-*/failure.json"))) == 1


@pytest.mark.parametrize("failed_call", ["ls-files", "diff"])
def test_production_source_gate_refuses_untracked_or_modified_sources(monkeypatch, failed_call):
    monkeypatch.setattr(rebuild, "source_files", lambda _: {"scripts/rebuild_mlb_unfiltered_samples.py": {}})
    calls = []
    def checked(command, **_kwargs):
        calls.append(command)
        return type("Result", (), {"returncode": int(command[1] == failed_call)})()
    monkeypatch.setattr(rebuild.subprocess, "run", checked)
    with pytest.raises(rebuild.RebuildBlocked, match="committed and unchanged"):
        rebuild.require_committed_sources(None)
    assert calls[0][1:3] == ["ls-files", "--error-unmatch"]
    assert calls[1][1:4] == ["diff", "--quiet", "HEAD"]
