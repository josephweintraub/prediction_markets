"""Publish reader-facing descriptive ratios from saved maker audit counts only."""
from __future__ import annotations

import argparse
import json
import platform
import sys
from pathlib import Path
from typing import Any

import duckdb

from analysis.sports_game_dynamics.artifacts import (
    artifact_fingerprint, fingerprint, fresh_run, quoted, require_columns, write_json,
)


def summarize_wallet_audit(wallet_run: Path, run_dir: Path) -> dict[str, Any]:
    terminal = wallet_run/"terminal_maker_summary.parquet"
    profile = wallet_run/"linked_buy_profile.parquet"
    original_manifest = wallet_run/"manifest.json"
    inputs = (terminal, profile, original_manifest)
    if any(not path.is_file() for path in inputs):
        raise FileNotFoundError([str(path) for path in inputs if not path.is_file()])
    if json.loads(original_manifest.read_text()).get("completion_status") != "complete":
        raise ValueError("Maker audit source run is not complete")
    with fresh_run(run_dir, inputs) as staging:
        con = duckdb.connect()
        try:
            con.execute(f"CREATE TEMP VIEW terminal AS SELECT * FROM read_parquet('{quoted(terminal)}')")
            con.execute(f"CREATE TEMP VIEW profile AS SELECT * FROM read_parquet('{quoted(profile)}')")
            require_columns(con, "terminal", ("sample", "window_id", "sport", "action_group",
                "n_fills", "n_events", "n_wallets", "quantity", "dollars"), "Saved terminal maker counts")
            require_columns(con, "profile", ("sample", "window_id", "sport", "buy_group",
                "price_bin", "n_fills", "n_events", "n_wallets", "quantity", "dollars"), "Saved maker BUY counts")
            for relation, keys in (("terminal", "sample,window_id,sport,action_group"),
                                   ("profile", "sample,window_id,sport,buy_group,price_bin")):
                if con.execute(f"SELECT count(*) FROM (SELECT {keys} FROM {relation} GROUP BY ALL HAVING count(*)<>1)").fetchone()[0]:
                    raise ValueError(f"Saved {relation} has duplicate summary keys")
            specifications = (
                ("winner_sell_prior_buy", "winner_sells_after_prior_same_token_buy", "maker_winner_sells"),
                ("high_price_sell_lower_prior_buy", "high_price_sells_after_lower_price_buy", "maker_high_price_sells"),
                ("winner_sell_prevalence", "maker_winner_sells", "all_maker_actions"),
            )
            queries = [f"""SELECT n.sample,n.window_id,n.sport,'{measure}' measure,
                n.n_fills numerator_fills,d.n_fills denominator_fills,
                n.n_events numerator_events,d.n_events denominator_events,
                n.n_wallets numerator_wallets,d.n_wallets denominator_wallets,
                n.quantity numerator_quantity,d.quantity denominator_quantity,
                n.dollars numerator_dollars,d.dollars denominator_dollars
                FROM terminal n JOIN terminal d USING(sample,window_id,sport)
                WHERE n.action_group='{numerator}' AND d.action_group='{denominator}'"""
                for measure, numerator, denominator in specifications]
            queries.append("""SELECT n.sample,n.window_id,n.sport,'longshot_buy_prior_winner_buy' measure,
                n.n_fills numerator_fills,d.n_fills denominator_fills,
                n.n_events numerator_events,d.n_events denominator_events,
                n.n_wallets numerator_wallets,d.n_wallets denominator_wallets,
                n.quantity numerator_quantity,d.quantity denominator_quantity,
                n.dollars numerator_dollars,d.dollars denominator_dollars
                FROM terminal n JOIN profile d USING(sample,window_id,sport)
                WHERE n.action_group='longshot_buys_after_prior_winner_buy'
                  AND d.buy_group='all_maker_buys' AND d.price_bin=1""")
            con.execute("CREATE TEMP TABLE ratio_counts AS " + " UNION ALL ".join(queries))
            con.execute("""CREATE TEMP TABLE conditional_shares AS SELECT *,
                numerator_fills/nullif(denominator_fills,0)::DOUBLE fill_share,
                numerator_quantity/nullif(denominator_quantity,0)::DOUBLE quantity_share,
                numerator_dollars/nullif(denominator_dollars,0)::DOUBLE dollar_share,
                'descriptive_counts_no_minimum'::VARCHAR support_status
                FROM ratio_counts""")
            if con.execute("""SELECT count(*) FROM conditional_shares WHERE numerator_fills>denominator_fills
                OR numerator_quantity>denominator_quantity+1e-8
                OR numerator_dollars>denominator_dollars+1e-8
                OR numerator_fills<0 OR denominator_fills<0""").fetchone()[0]:
                raise ValueError("Linked maker counts/amounts exceed their saved parent group")
            # Independent saved group reconciliation; no suppressed estimates are needed.
            if con.execute("""SELECT count(*) FROM terminal t JOIN profile p USING(sample,window_id,sport)
                WHERE t.action_group='longshot_buys_after_prior_winner_buy'
                AND p.buy_group='prior_winner_buy_link' AND p.price_bin=1
                AND t.n_fills<>p.n_fills""").fetchone()[0]:
                raise ValueError("Saved D1 prior-link counts disagree across artifacts")
            con.execute("""CREATE TEMP TABLE phase_comparisons AS SELECT a.sample,a.sport,a.measure,
                a.window_id early_window_id,b.window_id late_window_id,
                a.numerator_fills early_numerator_fills,a.denominator_fills early_denominator_fills,
                b.numerator_fills late_numerator_fills,b.denominator_fills late_denominator_fills,
                a.fill_share early_fill_share,b.fill_share late_fill_share,
                (b.fill_share-a.fill_share)::DOUBLE change_in_fill_share,
                a.quantity_share early_quantity_share,b.quantity_share late_quantity_share,
                (b.quantity_share-a.quantity_share)::DOUBLE change_in_quantity_share,
                a.dollar_share early_dollar_share,b.dollar_share late_dollar_share,
                (b.dollar_share-a.dollar_share)::DOUBLE change_in_dollar_share
                FROM conditional_shares a JOIN conditional_shares b USING(sample,sport,measure)
                WHERE a.window_id='t80_90' AND b.window_id='t99_100'""")
            counts = {}
            for relation, keys in (("conditional_shares", "sample,window_id,sport,measure"),
                                   ("phase_comparisons", "sample,sport,measure")):
                path = staging/(relation+".parquet")
                con.execute(f"COPY (SELECT * FROM {relation} ORDER BY {keys}) TO '{quoted(path)}' (FORMAT PARQUET,COMPRESSION ZSTD)")
                counts[relation] = int(con.execute(f"SELECT count(*) FROM read_parquet('{quoted(path)}')").fetchone()[0])
                if con.execute(f"SELECT count(*) FROM (SELECT {keys} FROM read_parquet('{quoted(path)}') GROUP BY ALL HAVING count(*)<>1)").fetchone()[0]:
                    raise ValueError(f"Published {relation} is not unique")
        finally:
            con.close()
        manifest = {
            "schema_version": 1, "stage": "wallet_reader_descriptive_shares_v1",
            "command": sys.argv, "environment": {"python": platform.python_version(), "duckdb": duckdb.__version__},
            "code": {"script": fingerprint(Path(__file__))},
            "contract": {
                "unit": "saved retained maker OrderFilled count, gross token quantity, or gross collateral amount",
                "winner_sell_prior_buy": "winner SELLs linked to prior same-token maker BUY / all maker winner SELLs",
                "longshot_buy_prior_winner_buy": "D1 maker BUYs linked to prior complementary eventual-winner maker BUY / all D1 maker BUYs",
                "high_price_sell_lower_prior_buy": "maker SELLs at price>=0.9 with an earlier lower-price same-token maker BUY / all maker SELLs at price>=0.9",
                "winner_sell_prevalence": "maker winner SELLs / all maker actions in the same sport/sample/window",
                "empty_denominator": "null share; zero counts retained",
                "uncertainty": "direct descriptive ratios without confidence intervals or minimum count suppression",
                "phase_comparison": "[.80,.90) versus [.99,1], same saved cohort and sample definition",
                "limits": "Observed partial maker history only; prior links do not establish remaining inventory, profitable exits or causation. Eventual winners are retrospective. EPL units are binary propositions. Frozen ATP clocks remain scheduled-start/duration clocks.",
            },
            "counts": counts, "inputs": {f"input_{i:02d}": fingerprint(path) for i,path in enumerate(inputs,1)},
            "outputs": {name: artifact_fingerprint(staging/name) for name in ("conditional_shares.parquet", "phase_comparisons.parquet")},
            "completion_status": "complete",
        }
        write_json(staging/"manifest.json", manifest)
    return manifest


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wallet-run", required=True)
    parser.add_argument("--run-dir", required=True)
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = parse_args()
    print(json.dumps(summarize_wallet_audit(Path(args.wallet_run), Path(args.run_dir)), indent=2, sort_keys=True))
