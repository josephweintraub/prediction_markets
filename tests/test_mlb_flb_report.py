from __future__ import annotations

import hashlib
import json
from pathlib import Path
import sys
from typing import Any

import duckdb
import pytest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "analysis" / "mlb_game_dynamics"))

from render_flb_report import (  # noqa: E402
    BOUNDARY_SAMPLES,
    CLOSE_DEFINITIONS,
    CLOSING_SCHEMA,
    FlbReportError,
    GAME_CLOSE_PROVENANCE_TYPES,
    PAIRED_SCHEMA,
    PHASES,
    STAGE09_DEFINITION_KEYS,
    STAGE09_RECONCILIATION_KEYS,
    TAIL_SCHEMA,
    TRADE_SCHEMA,
    render_flb_report,
)


def _fingerprint(path: Path) -> dict[str, Any]:
    content = path.read_bytes()
    return {
        "path": str(path.resolve()),
        "bytes": len(content),
        "sha256": hashlib.sha256(content).hexdigest(),
    }


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_parquet(
    path: Path,
    schema: tuple[tuple[str, str], ...],
    rows: list[tuple[Any, ...]],
) -> None:
    con = duckdb.connect()
    try:
        columns = ",".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE output ({columns})")
        placeholders = ",".join("?" for _ in schema)
        con.executemany(f"INSERT INTO output VALUES ({placeholders})", rows)
        escaped = str(path).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM output) TO '{escaped}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def _bin_label(decile: int) -> str:
    closing = "]" if decile == 10 else ")"
    return f"[{(decile - 1) / 10:.1f},{decile / 10:.1f}{closing}"


def _estimate_values(probability: float, calibration: float) -> tuple[float, ...]:
    se = 0.01
    return (
        probability,
        probability + calibration,
        calibration,
        se,
        calibration - 1.96 * se,
        calibration + 1.96 * se,
    )


def _calibration_for_decile(decile: int) -> float:
    if decile == 1:
        return -0.02
    if decile == 10:
        return 0.02
    return (decile - 5.5) * 0.002


def _stage08_definitions() -> dict[str, str]:
    return {
        "calibration_error": "won - price",
        "price_decile": "least(floor(price * 10), 9) + 1",
        "close_timestamp_unit": "Unix seconds",
        "primary_close": "last raw pregame BUY fill with 0 < price < 1; no wallet filter",
        "close_sensitivity": (
            "last pregame BUY fill with 0.01 < price < 0.99 after buyer-bot exclusion"
        ),
        "paired_probability_difference": (
            "primary probability - sensitivity probability (A - C)"
        ),
        "paired_calibration_difference": (
            "primary calibration - sensitivity calibration (A - C)"
        ),
        "paired_brier_difference": (
            "primary Brier score - sensitivity Brier score (A - C)"
        ),
        "close_weighting": "one equal-weight observation per game",
        "trade_weighting": "one equal-weight observation per eligible BUY fill",
        "boundary_primary": "literal audited half-open phase boundaries",
        "boundary_sensitivity": (
            "exclude abs(trade timestamp - any boundary) <= 30 seconds"
        ),
        "suppression": "estimate fields null when n < 50; counts remain",
        "closing_se": (
            "one-way official-date clustered normal SE; 95% CI = estimate +/- 1.96 SE"
        ),
        "trade_se": (
            "existing CGM day x wallet x game clustered normal SE; "
            "95% CI = estimate +/- 1.96 SE"
        ),
    }


def _create_bundle(root: Path) -> tuple[Path, Path, Path]:
    stage07 = root / "stage07"
    stage08 = root / "stage08"
    stage09 = root / "stage09"
    stage07.mkdir()
    stage08.mkdir()
    stage09.mkdir()

    def market_id(game: int) -> str:
        return "0x" + "a" * 64 if game == 599 else f"market-{game:04d}"

    game_schema = tuple(GAME_CLOSE_PROVENANCE_TYPES.items())
    game_rows = [
        (
            market_id(game),
            100_000 + game,
            True,
            game < 590,
            game < 100,
        )
        for game in range(600)
    ]
    game_closes = stage07 / "game_closes.parquet"
    _write_parquet(game_closes, game_schema, game_rows)

    phase_fingerprint = {
        "path": "/immutable/stage07/phase_trades.parquet",
        "bytes": 123456,
        "sha256": "a" * 64,
    }
    stage07_summary = {
        "schema_version": 1,
        "method": "exact_polygon_dual_pregame_close",
        "definitions": {
            "primary": "last BUY before actual_start_utc with 0 < price < 1",
            "sensitivity": (
                "last BUY before actual_start_utc with 0.01 < price < 0.99 "
                "and buyer proxyWallet not flagged is_nonhuman"
            ),
            "bot_semantics": (
                "only the outcome-token buyer is filtered; a flagged counterparty "
                "does not exclude a fill"
            ),
            "close_order": "block_number DESC, log_index DESC, transaction_hash DESC",
            "home_normalization": "home-token price; 1 - price for an away-token BUY",
            "timestamp_fallback_rows": 0,
        },
        "inputs": {
            "timestamp_provenance_validation": {
                "status": "passed",
                "scope": "cache_declaration_only",
                "build_metadata": {
                    "used_exact_cache": True,
                    "fallback_rows": 0,
                    "missing_blocks": 0,
                },
            }
        },
        "counts": {
            "eligible_games": 600,
            "primary_closes": 600,
            "primary_missing": 0,
            "sensitivity_closes": 590,
            "sensitivity_missing": 10,
            "both_closes": 590,
            "primary_only": 10,
            "sensitivity_only": 0,
            "same_close_identity": 300,
            "different_close_identity": 290,
            "sensitivity_closes_with_flagged_bot_counterparty": 100,
            "raw_candidate_rows": 10000,
            "distinct_fills": 9900,
            "duplicate_ingestion_replays": 100,
            "source_distinct_blocks": 2500,
        },
        "missing_reasons": {"primary": {}, "sensitivity": {"test_missing": 10}},
        "reconciliation": {
            "raw_equals_distinct_plus_replays": True,
            "distinct_fills_equal_per_game_raw_sum": True,
            "output_one_row_per_eligible_game": True,
            "all_close_timestamps_from_exact_cache": True,
            "close_availability_partitions_reconcile": True,
            "same_and_different_identity_partition_both_closes": True,
            "sensitivity_is_a_subset_of_primary": True,
        },
        "outputs": {
            "game_closes": str(game_closes.resolve()),
            "reconciliation": str((stage07 / "reconciliation.json").resolve()),
        },
    }
    _write_json(stage07 / "reconciliation.json", stage07_summary)

    closing_rows: list[tuple[Any, ...]] = []
    for definition in CLOSE_DEFINITIONS:
        overall_n = 600 if definition == "primary" else 590
        overall = _estimate_values(0.5, 0.01 if definition == "primary" else 0.005)
        closing_rows.append(
            (
                definition,
                "overall",
                None,
                "overall",
                overall_n,
                False,
                "reported",
                *overall,
                0.24,
            )
        )
        for decile in range(1, 11):
            n = 60
            if definition == "primary" and decile == 1:
                n = 49
            elif definition == "primary" and decile == 2:
                n = 71
            elif definition == "sensitivity" and decile == 1:
                n = 49
            elif definition == "sensitivity" and decile == 2:
                n = 61
            suppressed = n < 50
            metrics: tuple[Any, ...]
            if suppressed:
                metrics = (None,) * 7
            else:
                values = _estimate_values((decile - 0.5) / 10, _calibration_for_decile(decile))
                metrics = (*values, 0.20)
            closing_rows.append(
                (
                    definition,
                    "price_decile",
                    decile,
                    _bin_label(decile),
                    n,
                    suppressed,
                    "suppressed_n_lt_50" if suppressed else "reported",
                    *metrics,
                )
            )
    closing_path = stage08 / "closing_calibration.parquet"
    _write_parquet(closing_path, CLOSING_SCHEMA, closing_rows)

    paired_rows: list[tuple[Any, ...]] = []
    for decile in [None, *range(1, 11)]:
        overall = decile is None
        n = 590 if overall else 59
        same = 300 if overall else 30
        different = n - same
        primary_probability = 0.5 if overall else (decile - 0.5) / 10
        sensitivity_probability = primary_probability + 0.01
        primary_calibration = 0.02
        sensitivity_calibration = 0.01
        paired_rows.append(
            (
                "overall" if overall else "primary_price_decile",
                decile,
                "overall" if overall else _bin_label(decile),
                n,
                False,
                "reported",
                same,
                different,
                350 if overall else 35,
                240 if overall else 24,
                primary_probability,
                sensitivity_probability,
                -0.01,
                0.01,
                primary_calibration,
                sensitivity_calibration,
                0.01,
                0.21,
                0.20,
                0.01,
            )
        )
    paired_path = stage08 / "closing_paired_sensitivity.parquet"
    _write_parquet(paired_path, PAIRED_SCHEMA, paired_rows)

    trade_rows: list[tuple[Any, ...]] = []
    trade_samples = []
    for sample in BOUNDARY_SAMPLES:
        for phase, phase_order, _ in PHASES:
            total_rows = 0
            total_dollars = 0.0
            for decile in range(1, 11):
                n = (70 if sample == "literal" else 60) + decile
                if phase == "pregame" and decile == 10:
                    n = 20
                games = n - 10
                dollars = n * 100.0
                total_rows += n
                total_dollars += dollars
                suppressed = n < 50
                multiplier = 0.5 + phase_order * 0.1 + (
                    0.05 if sample == "exclude_within_30s" else 0.0
                )
                values: tuple[Any, ...] = (
                    (None,) * 6
                    if suppressed
                    else _estimate_values(
                        (decile - 0.5) / 10,
                        _calibration_for_decile(decile) * multiplier,
                    )
                )
                trade_rows.append(
                    (
                        sample,
                        phase,
                        phase_order,
                        decile,
                        _bin_label(decile),
                        n,
                        games,
                        dollars,
                        suppressed,
                        "suppressed_n_lt_50" if suppressed else "reported",
                        *values,
                    )
                )
            trade_samples.append(
                {
                    "boundary_sample": sample,
                    "phase": phase,
                    "trade_rows": total_rows,
                    "games": 590,
                    "dollars": total_dollars,
                }
            )
    trade_path = stage08 / "trade_phase_calibration.parquet"
    _write_parquet(trade_path, TRADE_SCHEMA, trade_rows)

    stage08_summary = {
        "schema_version": 1,
        "method": "descriptive_fixed_width_calibration_v1",
        "inputs": {
            "phase_trades": phase_fingerprint,
            "dual_closes": _fingerprint(game_closes),
        },
        "definitions": _stage08_definitions(),
        "counts": {
            "phase_input": {"rows": 2960, "games": 599, "dollars": 296000.0},
            "closing_coverage": {
                "games": 600,
                "primary_coverage": 600,
                "primary_missing": 0,
                "sensitivity_coverage": 590,
                "sensitivity_missing": 10,
                "close_only_games": 1,
                "close_only_game_sample": [
                    {"market_id": market_id(599), "game_pk": 100599}
                ],
                "primary_missing_games": [],
                "sensitivity_missing_games": [
                    {"market_id": market_id(game), "game_pk": 100000 + game}
                    for game in range(590, 600)
                ],
            },
            "trade_samples": trade_samples,
            "output_rows": {
                "closing_calibration": 22,
                "closing_paired_sensitivity": 11,
                "trade_phase_calibration": 80,
            },
        },
        "outputs": {
            "closing_calibration": "closing_calibration.parquet",
            "closing_paired_sensitivity": "closing_paired_sensitivity.parquet",
            "trade_phase_calibration": "trade_phase_calibration.parquet",
            "summary": "estimator_summary.json",
        },
        "interpretation_status": "exploratory_descriptive",
    }
    estimator_summary_path = stage08 / "estimator_summary.json"
    _write_json(estimator_summary_path, stage08_summary)

    closing_by_key = {
        (row[0], row[2]): row for row in closing_rows if row[1] == "price_decile"
    }
    trade_by_key = {(row[0], row[1], row[3]): row for row in trade_rows}
    closing_index = {name: index for index, (name, _) in enumerate(CLOSING_SCHEMA)}
    trade_index = {name: index for index, (name, _) in enumerate(TRADE_SCHEMA)}
    tail_rows: list[tuple[Any, ...]] = []

    def tail_estimates(source: tuple[Any, ...], indices: dict[str, int], probability: str) -> tuple[Any, ...]:
        return (
            source[indices[probability]],
            source[indices["win_rate"]],
            source[indices["mean_calibration"]],
            source[indices["calibration_se"]],
            source[indices["calibration_ci95_low"]],
            source[indices["calibration_ci95_high"]],
        )

    for definition in CLOSE_DEFINITIONS:
        d1 = closing_by_key[(definition, 1)]
        d10 = closing_by_key[(definition, 10)]
        d1_n = d1[closing_index["game_count"]]
        d10_n = d10[closing_index["game_count"]]
        suppressed = d1_n < 50 or d10_n < 50
        if suppressed:
            estimate_fields: tuple[Any, ...] = (None,) * 16
            status = "suppressed_tail_n_lt_50"
            pattern = "suppressed"
        else:
            d1_values = tail_estimates(d1, closing_index, "mean_probability")
            d10_values = tail_estimates(d10, closing_index, "mean_probability")
            spread = d10_values[2] - d1_values[2]
            spread_se = 0.015
            estimate_fields = (
                *d1_values,
                *d10_values,
                spread,
                spread_se,
                spread - 1.96 * spread_se,
                spread + 1.96 * spread_se,
            )
            status = "reported"
            pattern = "classic_flb_signs"
        tail_rows.append(
            (
                "closing",
                definition,
                None,
                "pregame_close",
                d1_n,
                d1_n,
                d1_n * 100.0,
                d10_n,
                d10_n,
                d10_n * 100.0,
                suppressed,
                status,
                pattern,
                *estimate_fields,
            )
        )
    for sample in BOUNDARY_SAMPLES:
        for phase, _, _ in PHASES:
            d1 = trade_by_key[(sample, phase, 1)]
            d10 = trade_by_key[(sample, phase, 10)]
            d1_n = d1[trade_index["trade_count"]]
            d10_n = d10[trade_index["trade_count"]]
            suppressed = d1_n < 50 or d10_n < 50
            if suppressed:
                estimate_fields = (None,) * 16
                status = "suppressed_tail_n_lt_50"
                pattern = "suppressed"
            else:
                d1_values = tail_estimates(d1, trade_index, "mean_price")
                d10_values = tail_estimates(d10, trade_index, "mean_price")
                spread = d10_values[2] - d1_values[2]
                spread_se = 0.015
                estimate_fields = (
                    *d1_values,
                    *d10_values,
                    spread,
                    spread_se,
                    spread - 1.96 * spread_se,
                    spread + 1.96 * spread_se,
                )
                status = "reported"
                pattern = "classic_flb_signs"
            tail_rows.append(
                (
                    "trade_phase",
                    None,
                    sample,
                    phase,
                    d1_n,
                    d1[trade_index["game_count"]],
                    d1[trade_index["dollars"]],
                    d10_n,
                    d10[trade_index["game_count"]],
                    d10[trade_index["dollars"]],
                    suppressed,
                    status,
                    pattern,
                    *estimate_fields,
                )
            )
    tail_path = stage09 / "flb_tail_summary.parquet"
    _write_parquet(tail_path, TAIL_SCHEMA, tail_rows)

    stage09_summary = {
        "schema_version": 1,
        "method": "mlb_fixed_bin_flb_tail_summary_v1",
        "inputs": {
            "closing_calibration": _fingerprint(closing_path),
            "trade_phase_calibration": _fingerprint(trade_path),
            "estimator_summary": _fingerprint(estimator_summary_path),
            "game_closes": _fingerprint(game_closes),
            "phase_trades": phase_fingerprint,
        },
        "source_estimator": {
            "schema_version": 1,
            "method": "descriptive_fixed_width_calibration_v1",
            "interpretation_status": "exploratory_descriptive",
        },
        "definitions": {key: f"frozen {key}" for key in STAGE09_DEFINITION_KEYS},
        "counts": {
            "closing_profile_rows": 22,
            "trade_phase_profile_rows": 80,
            "tail_summary_rows": 10,
            "closing_tail_rows": 2,
            "trade_phase_tail_rows": 8,
            "reported_tail_rows": 6,
            "suppressed_tail_rows": 4,
        },
        "reconciliation": {key: True for key in STAGE09_RECONCILIATION_KEYS},
        "outputs": {
            "flb_tail_summary": "flb_tail_summary.parquet",
            "summary": "flb_summary.json",
        },
    }
    _write_json(stage09 / "flb_summary.json", stage09_summary)
    return stage07, stage08, stage09


def test_render_is_accessible_standalone_and_byte_deterministic(tmp_path: Path) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    out_a = tmp_path / "report-a"
    out_b = tmp_path / "report-b"

    first = render_flb_report(stage07, stage08, stage09, out_a)
    second = render_flb_report(stage07, stage08, stage09, out_b)

    assert (out_a / "mlb_flb_report.html").read_bytes() == (
        out_b / "mlb_flb_report.html"
    ).read_bytes()
    assert (out_a / "report_manifest.json").read_bytes() == (
        out_b / "report_manifest.json"
    ).read_bytes()
    assert sorted(path.name for path in out_a.iterdir()) == [
        "mlb_flb_report.html",
        "report_manifest.json",
    ]
    document = (out_a / "mlb_flb_report.html").read_text(encoding="utf-8")
    lowered = document.lower()
    assert "http://" not in lowered and "https://" not in lowered
    assert "<script" not in lowered and "<link rel=" not in lowered
    assert 'name="viewport"' in lowered
    assert "@media(max-width:700px)" in lowered
    assert "@media print" in lowered
    assert "min-width:34rem" in lowered
    assert "code{display:inline-block;max-width:100%;font-size:.9em;overflow-wrap:anywhere;word-break:break-all;vertical-align:bottom}" in lowered
    assert f"<code>0x{'a' * 64} / mlb 100599</code>" in lowered
    assert ".table-scroll{overflow:visible}" in lowered
    assert "table{table-layout:fixed}" in lowered
    assert "th,td{white-space:normal;overflow-wrap:anywhere;word-break:break-word}" in lowered
    assert lowered.count('<figure class="chart-card" tabindex="0">') == 5
    assert lowered.count('<svg role="img"') == 5
    assert lowered.count("<title id=") == 5
    assert lowered.count("<desc id=") == 5
    assert lowered.count("<caption>") >= 10
    assert "significant" not in lowered
    assert "this report estimates no quantity named clv" in lowered
    assert "primary a minus sensitivity c" in lowered
    assert "and is not clv" in lowered
    assert "flagged seller/counterparty does not remove a fill" in lowered
    assert "inclusive of any boundary" in lowered
    assert "0.01 &lt; price &lt; 0.99" in lowered
    assert "exclude flagged outcome-token buyers" in lowered
    assert "this is a buyer-centered sample, not a bot-free or human-to-human series" in lowered
    assert "each phase panel contains the complete d1–d10 profile for both timing samples" in lowered
    assert lowered.count("adjacent d1/d10 tail contrasts</caption>") == 4
    assert lowered.count('<th scope="row">literal boundaries</th>') == 4
    assert lowered.count('<th scope="row">exclude within ±30s</th>') == 4
    assert (
        '<tr><th scope="row">literal boundaries</th>'
        '<td>71 / 61 / $7,100.00</td><td>-0.014000</td>'
        '<td>80 / 70 / $8,000.00</td><td>+0.014000</td>'
        '<td>+0.028000 [-0.001400, +0.057400]</td>'
        '<td>classic_flb_signs</td><td>reported</td></tr>'
    ) in lowered
    assert (
        '<tr><th scope="row">exclude within ±30s</th>'
        '<td>61 / 51 / $6,100.00</td><td>-0.015000</td>'
        '<td>70 / 60 / $7,000.00</td><td>+0.015000</td>'
        '<td>+0.030000 [+0.000600, +0.059400]</td>'
        '<td>classic_flb_signs</td><td>reported</td></tr>'
    ) in lowered
    assert lowered.count(
        "suppressed_tail_n_lt_50 because d10 n=20 (&lt;50)"
    ) == 2
    assert "withheld" in lowered
    assert "suppressed_tail_n_lt_50" in lowered
    assert lowered.count('data-status="reported"') == 96
    assert 'data-status="suppressed"' not in lowered
    assert first == second
    assert first["analysis_state"] == "exploratory_descriptive"
    assert first["validations"]["responsive_viewports"] == [320, 375, 768, 1440]
    html_bytes = (out_a / "mlb_flb_report.html").read_bytes()
    assert first["outputs"]["html"]["sha256"] == hashlib.sha256(html_bytes).hexdigest()


def test_renderer_refuses_overwrite_without_mutating_existing_output(tmp_path: Path) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    output = tmp_path / "report"
    render_flb_report(stage07, stage08, stage09, output)
    before = {path.name: path.read_bytes() for path in output.iterdir()}

    with pytest.raises(FileExistsError, match="already exists"):
        render_flb_report(stage07, stage08, stage09, output)

    assert {path.name: path.read_bytes() for path in output.iterdir()} == before


def test_renderer_rejects_source_fingerprint_mismatch_before_publication(tmp_path: Path) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    summary_path = stage09 / "flb_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["inputs"]["closing_calibration"]["sha256"] = "0" * 64
    _write_json(summary_path, summary)
    output = tmp_path / "report"

    with pytest.raises(FlbReportError, match="fingerprint mismatch"):
        render_flb_report(stage07, stage08, stage09, output)

    assert not output.exists()
    assert not list(tmp_path.glob(".report.staging-*"))


def test_renderer_rejects_suppressed_tail_estimate_leak(tmp_path: Path) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    path = stage09 / "flb_tail_summary.parquet"
    con = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        con.execute(f"CREATE TABLE tails AS SELECT * FROM read_parquet('{escaped}')")
        con.execute(
            "UPDATE tails SET d1_mean_calibration=0.0 "
            "WHERE analysis_scope='closing' AND close_definition='sensitivity'"
        )
        replacement = stage09 / "replacement.parquet"
        escaped_replacement = str(replacement).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM tails) TO '{escaped_replacement}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    path.unlink()
    replacement.rename(path)
    output = tmp_path / "report"

    with pytest.raises(FlbReportError, match="leaks suppressed estimates"):
        render_flb_report(stage07, stage08, stage09, output)

    assert not output.exists()


@pytest.mark.parametrize(
    ("replacement", "message"),
    (
        ("literal", "unique grain"),
        ("unexpected_sample", "expected grid is incomplete"),
    ),
)
def test_renderer_rejects_duplicate_or_mismatched_phase_tail_key(
    tmp_path: Path, replacement: str, message: str
) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    path = stage09 / "flb_tail_summary.parquet"
    con = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        con.execute(f"CREATE TABLE tails AS SELECT * FROM read_parquet('{escaped}')")
        con.execute(
            "UPDATE tails SET boundary_sample=? "
            "WHERE analysis_scope='trade_phase' "
            "AND boundary_sample='exclude_within_30s' AND phase='innings_1_3'",
            [replacement],
        )
        replacement_path = stage09 / "replacement.parquet"
        escaped_replacement = str(replacement_path).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM tails) TO '{escaped_replacement}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    path.unlink()
    replacement_path.rename(path)
    output = tmp_path / "report"

    with pytest.raises(FlbReportError, match=message):
        render_flb_report(stage07, stage08, stage09, output)

    assert not output.exists()


def test_renderer_rejects_non_nested_boundary_sample(tmp_path: Path) -> None:
    stage07, stage08, stage09 = _create_bundle(tmp_path)
    path = stage08 / "trade_phase_calibration.parquet"
    con = duckdb.connect()
    try:
        escaped = str(path).replace("'", "''")
        con.execute(f"CREATE TABLE trades AS SELECT * FROM read_parquet('{escaped}')")
        con.execute(
            "UPDATE trades SET trade_count=999 "
            "WHERE boundary_sample='exclude_within_30s' "
            "AND phase='pregame' AND price_decile=1"
        )
        replacement = stage08 / "replacement.parquet"
        escaped_replacement = str(replacement).replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM trades) TO '{escaped_replacement}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()
    path.unlink()
    replacement.rename(path)
    summary_path = stage09 / "flb_summary.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    summary["inputs"]["trade_phase_calibration"] = _fingerprint(path)
    _write_json(summary_path, summary)
    output = tmp_path / "report"

    with pytest.raises(FlbReportError, match="not a trade-count subset"):
        render_flb_report(stage07, stage08, stage09, output)

    assert not output.exists()
