from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import duckdb
from matplotlib.axes import Axes

from analysis.multisport_game_dynamics.render_latex import (
    SPORT_LABELS,
    SPORT_ORDER,
    render_report,
)


def _write_parquet(
    path: Path,
    schema: tuple[tuple[str, str], ...],
    rows: list[tuple[object, ...]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    try:
        columns = ",".join(f'"{name}" {kind}' for name, kind in schema)
        con.execute(f"CREATE TABLE artifact({columns})")
        placeholders = ",".join("?" for _ in schema)
        con.executemany(f"INSERT INTO artifact VALUES ({placeholders})", rows)
        destination = str(path.resolve()).replace("'", "''")
        con.execute(
            f"COPY artifact TO '{destination}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        con.close()


def test_render_latex_end_to_end_preserves_report_contract(tmp_path: Path) -> None:
    estimator = tmp_path / "04_estimates"
    timing = tmp_path / "02_timing"
    output = tmp_path / "05_report"

    phase_schema = (
        ("sport", "VARCHAR"),
        ("phase", "VARCHAR"),
        ("phase_order", "INTEGER"),
        ("phase_label", "VARCHAR"),
        ("price_decile", "INTEGER"),
        ("trade_count", "BIGINT"),
        ("event_count", "BIGINT"),
        ("dollars", "DOUBLE"),
        ("mean_price", "DOUBLE"),
        ("win_rate", "DOUBLE"),
        ("mean_calibration", "DOUBLE"),
        ("calibration_ci95_low", "DOUBLE"),
        ("calibration_ci95_high", "DOUBLE"),
        ("suppressed", "BOOLEAN"),
    )
    _write_parquet(
        estimator / "phase_calibration.parquet",
        phase_schema,
        [
            (
                sport,
                "pregame",
                1,
                "Pregame",
                1,
                50,
                5,
                100.0,
                0.05,
                0.05,
                0.0,
                -0.01,
                0.01,
                False,
            )
            for sport in SPORT_ORDER
        ],
    )

    closing_schema = (
        ("sport", "VARCHAR"),
        ("close_sample", "VARCHAR"),
        ("price_decile", "INTEGER"),
        ("close_count", "BIGINT"),
        ("event_count", "BIGINT"),
        ("dollars", "DOUBLE"),
        ("mean_price", "DOUBLE"),
        ("win_rate", "DOUBLE"),
        ("mean_calibration", "DOUBLE"),
        ("calibration_ci95_low", "DOUBLE"),
        ("calibration_ci95_high", "DOUBLE"),
        ("brier_score", "DOUBLE"),
        ("suppressed", "BOOLEAN"),
    )
    _write_parquet(
        estimator / "closing_calibration.parquet",
        closing_schema,
        [
            (
                sport,
                sample,
                1,
                50,
                5,
                100.0,
                0.05,
                0.05,
                0.0,
                -0.01,
                0.01,
                0.0475,
                False,
            )
            for sport in SPORT_ORDER
            for sample in ("all_trades", "filtered_trades")
        ],
    )

    tail_schema = (
        ("analysis_scope", "VARCHAR"),
        ("sport", "VARCHAR"),
        ("phase", "VARCHAR"),
        ("phase_order", "INTEGER"),
        ("phase_label", "VARCHAR"),
        ("d1_n", "BIGINT"),
        ("d1_mean_calibration", "DOUBLE"),
        ("d10_n", "BIGINT"),
        ("d10_mean_calibration", "DOUBLE"),
        ("spread_d10_minus_d1", "DOUBLE"),
        ("spread_ci95_low", "DOUBLE"),
        ("spread_ci95_high", "DOUBLE"),
    )
    _write_parquet(
        estimator / "flb_spreads.parquet",
        tail_schema,
        [
            (
                "trade_phase",
                sport,
                "pregame",
                1,
                "Pregame",
                50,
                -0.01,
                50,
                0.01,
                0.02,
                0.0,
                0.04,
            )
            for sport in SPORT_ORDER
        ],
    )

    _write_parquet(
        estimator / "normalized_phase_trades.parquet",
        (("sport", "VARCHAR"), ("event_id", "VARCHAR"), ("usdc", "DOUBLE")),
        [(sport, f"{sport}-event", 100.0) for sport in SPORT_ORDER],
    )
    _write_parquet(
        timing / "event_timing.parquet",
        (("sport", "VARCHAR"), ("event_slug", "VARCHAR")),
        [(sport, f"{sport}-event") for sport in SPORT_ORDER],
    )

    line_styles: list[object] = []
    original_errorbar = Axes.errorbar

    def record_errorbar(self: Axes, *args: object, **kwargs: object) -> object:
        line_styles.append(kwargs.get("linestyle"))
        return original_errorbar(self, *args, **kwargs)

    with patch.object(Axes, "errorbar", new=record_errorbar):
        manifest = render_report(estimator, timing, output)

    source_path = output / "major_sports_game_dynamics.tex"
    assert source_path.is_file()
    source = source_path.read_text(encoding="utf-8")

    assert "\\caption{Closing calibration: all valid trades}" in source
    assert "\\caption{Closing calibration: filtered trades}" in source
    assert "\\label{tab:closing-all}" in source
    assert "\\label{tab:closing-filtered}" in source
    for sport in SPORT_ORDER:
        assert f"\\section{{{SPORT_LABELS[sport]}}}" in source
    # One support table plus two headers for each of 13 longtables.
    assert source.count("& Dollars &") == 27

    assert line_styles
    assert set(line_styles) == {"none"}
    assert source.count("Points are isolated fixed-bin estimates.") == 2

    forbidden_sections = ("Interpretation limits", "Caveats", "Conclusion")
    for title in forbidden_sections:
        assert f"\\section{{{title}}}" not in source

    assert manifest["counts"] == {
        "phase_rows": 11,
        "closing_rows": 22,
        "tail_rows": 11,
        "figures": 13,
    }
    assert len(list((output / "figures").glob("*.pdf"))) == 13
