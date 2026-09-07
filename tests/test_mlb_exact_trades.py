from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import duckdb
import pandas as pd
import pytest


MODULE_DIR = Path(__file__).parents[1] / "analysis" / "mlb_game_dynamics"
sys.path.insert(0, str(MODULE_DIR))

import build_exact_trades as exact_trades_module  # noqa: E402
from build_exact_trades import (  # noqa: E402
    ExactTradeBuildError,
    build_exact_trades,
    verify_output_timestamps,
)
from timestamp_provenance import TimestampProvenanceError  # noqa: E402


def _fill(
    identity: str,
    *,
    log_index: int,
    block_number: int,
    outcome_token_side: str = "maker",
    maker_amount: int = 2_000_000,
    taker_amount: int = 1_000_000,
    maker: str = "0xmaker",
    taker: str = "0xtaker",
    market_id: str = "mlb-market",
) -> dict[str, object]:
    return {
        "order_hash": f"order-{identity}",
        "maker": maker,
        "taker": taker,
        "maker_asset_id": "yes-token" if outcome_token_side == "maker" else "0",
        "taker_asset_id": "0" if outcome_token_side == "maker" else "yes-token",
        "maker_amount_filled": maker_amount,
        "taker_amount_filled": taker_amount,
        "fee": 0,
        "block_number": block_number,
        "transaction_hash": f"tx-{identity}",
        "log_index": log_index,
        "exchange_address": "0xexchange",
        "condition_id": market_id,
        "outcome": "Yankees",
        "winning_outcome": "Yankees",
        "outcome_token_side": outcome_token_side,
    }


def _write_inputs(
    tmp_path: Path,
    rows: list[dict[str, object]],
    cache_rows: list[tuple[int, int]] | None = None,
    bot_wallets: tuple[str, ...] = (),
    candidate_ids: tuple[str, ...] = ("mlb-market",),
) -> dict[str, Path]:
    raw = tmp_path / "resolved_trades.parquet"
    candidates = tmp_path / "candidates.parquet"
    cache = tmp_path / "block_timestamps.parquet"
    provenance = tmp_path / "timestamp_provenance.json"
    wallet_flags = tmp_path / "wallet_flags.parquet"
    run_dir = tmp_path / "run"

    pd.DataFrame(rows).to_parquet(raw, index=False)
    pd.DataFrame(
        {"market_id": pd.Series(candidate_ids, dtype="string")}
    ).to_parquet(candidates, index=False)
    if cache_rows is None:
        blocks = sorted({int(row["block_number"]) for row in rows})
        cache_rows = [(block, 1_750_000_000 + block) for block in blocks]
    pd.DataFrame(cache_rows, columns=["block_number", "timestamp"]).to_parquet(
        cache, index=False
    )
    flag_rows = [
        {"proxyWallet": wallet, "is_nonhuman": True} for wallet in bot_wallets
    ]
    if not flag_rows:
        flag_rows = [{"proxyWallet": "0xnobody", "is_nonhuman": False}]
    pd.DataFrame(flag_rows).to_parquet(wallet_flags, index=False)

    cache_sha256 = hashlib.sha256(cache.read_bytes()).hexdigest()
    declaration = {
        "schema_version": 1,
        "method": "polygon_rpc_block_timestamp",
        "timestamp_unit": "unix_seconds",
        "cache": {
            "path": str(cache),
            "format": "parquet",
            "rows": len(cache_rows),
            "sha256": cache_sha256,
            "required_columns": ["block_number", "timestamp"],
        },
        "build_metadata": {
            "used_exact_cache": True,
            "source_distinct_blocks": len(cache_rows),
            "cache_distinct_blocks": len(cache_rows),
            "missing_blocks": 0,
            "fallback_rows": 0,
        },
    }
    provenance.write_text(json.dumps(declaration), encoding="utf-8")
    return {
        "raw": raw,
        "candidates": candidates,
        "provenance": provenance,
        "cache": cache,
        "wallet_flags": wallet_flags,
        "run_dir": run_dir,
    }


def _build(paths: dict[str, Path]) -> dict:
    con = duckdb.connect()
    try:
        return build_exact_trades(
            con,
            paths["raw"],
            paths["candidates"],
            paths["provenance"],
            paths["wallet_flags"],
            paths["run_dir"],
        )
    finally:
        con.close()


def test_exact_extractor_maps_buyers_deduplicates_only_replays_and_reconciles(
    tmp_path: Path,
) -> None:
    maker_token = _fill("maker-token", log_index=1, block_number=100)
    replay = maker_token.copy()
    economically_identical_distinct_fill = maker_token | {
        "order_hash": "order-distinct",
        "transaction_hash": "tx-distinct",
        "log_index": 2,
    }
    taker_token = _fill(
        "taker-token",
        log_index=3,
        block_number=101,
        outcome_token_side="taker",
        maker_amount=3_000_000,
        taker_amount=4_000_000,
        maker="0xbuyer-maker",
        taker="0xseller-taker",
    )
    price_excluded = _fill(
        "price-edge",
        log_index=4,
        block_number=102,
        maker_amount=100_000_000,
        taker_amount=1_000_000,
    )
    bot_excluded = _fill(
        "bot",
        log_index=5,
        block_number=103,
        maker_amount=5_000_000,
        taker_amount=2_000_000,
        taker="0xbot",
    )
    paths = _write_inputs(
        tmp_path,
        [
            maker_token,
            replay,
            economically_identical_distinct_fill,
            taker_token,
            price_excluded,
            bot_excluded,
        ],
        bot_wallets=("0xbot",),
    )

    report = _build(paths)
    output_path = paths["run_dir"] / "exact_trades.parquet"
    audit_path = paths["run_dir"] / "build_audit.json"
    output = pd.read_parquet(output_path)
    disk_audit = json.loads(audit_path.read_text())

    assert report == disk_audit
    assert sorted(path.name for path in paths["run_dir"].iterdir()) == [
        "build_audit.json",
        "exact_trades.parquet",
    ]
    assert report["counts"] == {
        "raw_candidate_rows": 6,
        "distinct_fills": 5,
        "duplicate_replays": 1,
        "source_blocks": 4,
        "matched_blocks": 4,
        "missing_blocks": 0,
        "price_exclusions": 1,
        "bot_exclusions": 1,
        "output_buy_rows": 3,
        "output_buy_dollars": 5.0,
    }
    assert report["timestamp_verification"] == {
        "rows_checked": 3,
        "missing_cache_rows": 0,
        "timestamp_mismatches": 0,
    }
    provenance_report = report["inputs"]["timestamp_provenance_validation"]
    assert provenance_report["status"] == "passed"
    assert provenance_report["analysis_extract_verified"] is False
    assert len(output) == 3
    assert set(output["transaction_hash"]) == {
        "tx-maker-token",
        "tx-distinct",
        "tx-taker-token",
    }

    maker_buy = output.set_index("transaction_hash").loc["tx-taker-token"]
    assert maker_buy["proxyWallet"] == "0xbuyer-maker"
    assert maker_buy["counterparty"] == "0xseller-taker"
    assert bool(maker_buy["is_maker"]) is True
    assert maker_buy["token_id"] == "yes-token"
    assert maker_buy["price"] == pytest.approx(0.75)
    assert maker_buy["usdcSize"] == pytest.approx(3.0)

    taker_buy = output.set_index("transaction_hash").loc["tx-maker-token"]
    assert taker_buy["proxyWallet"] == "0xtaker"
    assert taker_buy["counterparty"] == "0xmaker"
    assert bool(taker_buy["is_maker"]) is False
    assert taker_buy["price"] == pytest.approx(0.5)
    assert taker_buy["timestamp"] == 1_750_000_100


def test_exact_extractor_rejects_missing_cache_block(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path,
        [_fill("missing", log_index=1, block_number=100)],
        cache_rows=[(99, 1_750_000_099)],
    )

    with pytest.raises(ExactTradeBuildError, match="missing 1 candidate-source blocks"):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_untrusted_cache_cannot_bypass_timestamp_provenance(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path, [_fill("sha", log_index=1, block_number=100)]
    )
    pd.DataFrame(
        {"block_number": [100], "timestamp": [1_234_567_890]}
    ).to_parquet(paths["cache"], index=False)

    with pytest.raises(TimestampProvenanceError, match="SHA-256 mismatch"):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_exact_extractor_rejects_contradictory_event_identity(tmp_path: Path) -> None:
    original = _fill("contradiction", log_index=1, block_number=100)
    contradiction = original | {"taker_amount_filled": 1_100_000}
    paths = _write_inputs(tmp_path, [original, contradiction])

    with pytest.raises(ExactTradeBuildError, match="contradictory payloads"):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_exact_extractor_rejects_zero_candidates(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path,
        [_fill("zero", log_index=1, block_number=100)],
        candidate_ids=(),
    )

    with pytest.raises(ExactTradeBuildError, match="must be nonempty"):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_exact_extractor_reconciles_every_candidate_to_source(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path,
        [_fill("present", log_index=1, block_number=100)],
        candidate_ids=("mlb-market", "missing-market"),
    )

    with pytest.raises(
        ExactTradeBuildError, match="1 candidate markets.*missing-market"
    ):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_exact_extractor_rejects_empty_candidate_source(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path,
        [_fill("other", log_index=1, block_number=100, market_id="other-market")],
    )

    with pytest.raises(ExactTradeBuildError, match="Candidate source is empty"):
        _build(paths)
    assert not paths["run_dir"].exists()


def test_exact_extractor_refuses_existing_immutable_run(tmp_path: Path) -> None:
    paths = _write_inputs(
        tmp_path, [_fill("existing", log_index=1, block_number=100)]
    )
    paths["run_dir"].mkdir()
    sentinel = paths["run_dir"] / "keep.txt"
    sentinel.write_text("do not replace", encoding="utf-8")

    with pytest.raises(FileExistsError, match="already exists"):
        _build(paths)
    assert sentinel.read_text(encoding="utf-8") == "do not replace"


def test_failed_staging_verification_leaves_no_partial_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _write_inputs(
        tmp_path, [_fill("injected", log_index=1, block_number=100)]
    )

    def injected_failure(*_args: object, **_kwargs: object) -> dict:
        raise RuntimeError("injected verification failure")

    monkeypatch.setattr(
        exact_trades_module, "verify_output_timestamps", injected_failure
    )
    with pytest.raises(RuntimeError, match="injected verification failure"):
        _build(paths)

    assert not paths["run_dir"].exists()
    assert list(tmp_path.glob(".run.staging-*")) == []


def test_output_timestamp_verifier_rejects_altered_timestamp() -> None:
    con = duckdb.connect()
    con.execute("CREATE TABLE exact_cache(block_number BIGINT, timestamp BIGINT)")
    con.execute("INSERT INTO exact_cache VALUES (100, 1750000100)")
    con.execute("CREATE TABLE altered_output(block_number BIGINT, timestamp BIGINT)")
    con.execute("INSERT INTO altered_output VALUES (100, 1750000101)")

    with pytest.raises(ExactTradeBuildError, match="timestamp mismatches=1"):
        verify_output_timestamps(con, "altered_output", "exact_cache")
    con.close()
