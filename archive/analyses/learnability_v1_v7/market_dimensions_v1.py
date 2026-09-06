"""market_dimensions_v1 — the consolidated per-market measurement layer.

One row per market (0x condition_id) joining every construct built so far:

  labels v2      topic / subcategory / mechanic / event_family / vote_margin,
                 series_slug, recurrence, resolution_source, uma_status,
                 automatically_resolved, neg_risk, rules_len, created_at
  embeddings     novelty at birth (sim_k25_x, strict predecessors, same-series/
                 event excluded), n_prior_valid, within-vintage novelty decile
                 over trade-viable markets (d1 = most novel of its era),
                 k=50 / k=200 k-means cluster assignments (question embeddings)
  trades         market lifetime (first->last BUY trade), trade/$ totals in the
                 mature (25-80%) and full windows, first/last trade day
                 (from the standard-filtered horizon_flb_v2 intermediate)
  derived        anchor_class ladder from resolution_source domain (explicit
                 map below), prior_instances (earlier-created markets in the
                 same series), vintage year

Output: /mnt/data/learnability/output/market_dimensions_v1.parquet
Sources of record: market_labels_v2.parquet, novelty_q_torch.parquet,
scheme_cluster_k{50,200}.parquet, horizon_flb_v2_trades.parquet.
"""
from __future__ import annotations

import time

import duckdb

LABELS = "/mnt/data/learnability/native/market_labels_v2.parquet"
NOVELTY = "/mnt/data/embedding_difficulty/novelty_q_torch.parquet"
K200 = "/mnt/data/embedding_difficulty/schemes/scheme_cluster_k200.parquet"
K50 = "/mnt/data/embedding_difficulty/schemes/scheme_cluster_k50.parquet"
TRADES_INT = "/mnt/data/learnability/output/horizon_flb_v2_trades.parquet/**/*.parquet"
OUT = "/mnt/data/learnability/output/market_dimensions_v1.parquet"

# resolution_source domain -> anchor class. Explicit, auditable map of the top
# domains (99%+ of non-empty rows by construction of the tail); anything else
# non-empty falls to other_url. none/'' = judgment-resolved.
DATA_FEED = ["data.chain.link", "binance.com", "pythdata.app",
             "finance.yahoo.com", "wunderground.com"]
SCORER = ["atptour.com", "nba.com", "hltv.org", "itftennis.com", "dotabuff.com",
          "gol.gg", "ncaa.com", "mlb.com", "wtatennis.com", "fifa.com",
          "liquipedia.net", "nfl.com", "vlr.gg", "nhl.com", "uefa.com",
          "laliga.com", "pgatour.com", "efl.com", "jleague.jp", "ausopen.com",
          "bundesliga.com", "premierleague.com", "cbf.com.br", "sofascore.com",
          "legaseriea.it", "ligue1.com", "mlssoccer.com", "ufc.com",
          "formula1.com", "espncricinfo.com", "frmf.ma", "csl-china.com",
          "eredivisie.nl", "flashscore.com", "espn.com"]
SOCIAL = ["x.com", "twitter.com", "twitch.tv", "youtube.com",
          "truthsocial.com", "instagram.com"]


def sql_list(xs):
    return "(" + ",".join(f"'{x}'" for x in xs) + ")"


def main():
    t0 = time.time()
    con = duckdb.connect()
    con.execute("SET temp_directory='/mnt/data/duckdb_tmp'")

    con.execute(f"""
        CREATE TEMP TABLE tr AS
        SELECT market_id,
               any_value(life_d)                                   AS life_d,
               min(trade_day)                                      AS first_trade_day,
               max(trade_day)                                      AS last_trade_day,
               count(*)                                            AS n_trades_full,
               sum(usdc)                                           AS usd_full,
               count(*) FILTER (WHERE pos BETWEEN 0.25 AND 0.80)   AS n_trades_mature,
               sum(usdc) FILTER (WHERE pos BETWEEN 0.25 AND 0.80)  AS usd_mature
        FROM read_parquet('{TRADES_INT}', hive_partitioning=1)
        GROUP BY market_id
    """)
    print(f"trade aggregates: {con.sql('SELECT count(*) FROM tr').fetchone()[0]:,} markets",
          flush=True)

    con.execute(f"""
        CREATE TEMP TABLE base AS
        SELECT
            l.condition_id, l.question, l.topic, l.subcategory, l.mechanic,
            l.event_family, l.vote_margin, l.abstain, l.label_source,
            l.series_slug, l.recurrence, l.resolution_source, l.uma_status,
            l.automatically_resolved, l.neg_risk, l.rules_len,
            TRY_CAST(l.created_at AS TIMESTAMP)  AS created_ts,
            TRY_CAST(l.closed_time AS TIMESTAMP) AS closed_ts,
            l.liquidity AS gamma_liquidity, l.volume_num AS gamma_volume,
            l.comment_count,
            nv.sim_k25_x, nv.n_prior_valid, nv.birth_at, nv.birth_fallback,
            k200.slice AS cluster_k200, k50.slice AS cluster_k50,
            t.life_d, t.first_trade_day, t.last_trade_day,
            t.n_trades_full, t.usd_full, t.n_trades_mature, t.usd_mature,
            regexp_extract(l.resolution_source,
                           'https?://(?:www\\.)?([^/]+)', 1) AS res_domain
        FROM read_parquet('{LABELS}') l
        LEFT JOIN read_parquet('{NOVELTY}') nv ON l.condition_id = nv.market_id
        LEFT JOIN read_parquet('{K200}') k200  ON l.condition_id = k200.market_id
        LEFT JOIN read_parquet('{K50}')  k50   ON l.condition_id = k50.market_id
        LEFT JOIN tr t                          ON l.condition_id = t.market_id
    """)

    con.execute(f"""
        CREATE TEMP TABLE dims AS
        SELECT b.*,
            CASE
                WHEN b.resolution_source IS NULL OR b.resolution_source = ''
                    THEN 'none'
                WHEN b.res_domain IN {sql_list(DATA_FEED)} THEN 'data_feed'
                WHEN b.res_domain IN {sql_list(SCORER)}    THEN 'official_scorer'
                WHEN b.res_domain IN {sql_list(SOCIAL)}    THEN 'social'
                ELSE 'other_url'
            END AS anchor_class,
            year(coalesce(b.created_ts, TRY_CAST(b.birth_at AS TIMESTAMP)))
                AS vintage_year,
            coalesce(pi.prior_instances, 0) AS prior_instances,
            (b.series_slug IS NOT NULL)::INT AS in_series
        FROM base b
        LEFT JOIN (
            SELECT condition_id,
                   count(*) OVER (PARTITION BY series_slug ORDER BY created_ts,
                                  condition_id ROWS BETWEEN UNBOUNDED PRECEDING
                                  AND 1 PRECEDING) AS prior_instances
            FROM base WHERE series_slug IS NOT NULL
        ) pi USING (condition_id)
    """)

    # within-vintage novelty decile over trade-viable markets (d1 = most novel)
    con.execute(f"""
        COPY (
            SELECT d.*, nd.novelty_vint_decile
            FROM dims d
            LEFT JOIN (
                SELECT condition_id,
                       NTILE(10) OVER (PARTITION BY vintage_year
                                       ORDER BY sim_k25_x ASC, condition_id)
                           AS novelty_vint_decile
                FROM dims
                WHERE sim_k25_x IS NOT NULL AND n_trades_full >= 1
            ) nd USING (condition_id)
        ) TO '{OUT}' (FORMAT PARQUET)
    """)

    n = con.sql(f"SELECT count(*) FROM read_parquet('{OUT}')").fetchone()[0]
    cov = con.sql(f"""
        SELECT
          count(*) FILTER (WHERE n_trades_full >= 1)   AS traded,
          count(*) FILTER (WHERE n_trades_full >= 1 AND sim_k25_x IS NOT NULL)
              AS traded_with_novelty,
          count(*) FILTER (WHERE n_trades_full >= 1 AND cluster_k200 IS NOT NULL)
              AS traded_with_cluster,
          count(*) FILTER (WHERE n_trades_full >= 1 AND topic IS NOT NULL)
              AS traded_with_topic
        FROM read_parquet('{OUT}')
    """).df()
    print(f"wrote {OUT}: {n:,} rows")
    print(cov.to_string())
    anch = con.sql(f"""
        SELECT anchor_class, count(*) n_markets,
               sum(n_trades_mature) n_tr_mat, round(sum(usd_full)/1e6,1) usd_m
        FROM read_parquet('{OUT}') WHERE n_trades_full >= 1
        GROUP BY 1 ORDER BY 3 DESC
    """).df()
    print(anch.to_string())
    print(f"done in {(time.time()-t0)/60:.1f} min")


if __name__ == "__main__":
    main()
