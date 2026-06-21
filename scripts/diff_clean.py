"""Validate clean vs raw: row counts, total $ volume, per-primary_category deltas."""
import duckdb
RAW  = "/home/ubuntu/pipeline/output/trades.parquet/**/*.parquet"
CLEAN= "/mnt/data/pipeline_output/trades_clean.parquet/**/*.parquet"
AUG  = "/mnt/data/learnability/stage2_per_contract_augmented.parquet"
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET memory_limit='150GB'"); con.execute("SET threads=14")
con.execute("SET preserve_insertion_order=false")
con.execute(f"CREATE VIEW raw   AS SELECT * FROM read_parquet('{RAW}')")
con.execute(f"CREATE VIEW clean AS SELECT * FROM read_parquet('{CLEAN}')")
print("=== TOTALS ===", flush=True)
for name in ("raw","clean"):
    r = con.execute(f"SELECT COUNT(*) n, SUM(usdcSize) vol, AVG(price) ap FROM {name}").fetchone()
    print(f"{name:6s} rows={r[0]:>14,}  vol=${r[1]:>18,.0f}  avg_price={r[2]:.4f}", flush=True)
d = con.execute("""SELECT (SELECT COUNT(*) FROM raw)-(SELECT COUNT(*) FROM clean),
                          (SELECT SUM(usdcSize) FROM raw)-(SELECT SUM(usdcSize) FROM clean)""").fetchone()
print(f"removed rows={d[0]:,}   removed vol=${d[1]:,.0f}", flush=True)
print("\n=== profile raw vs clean (size & price extremes) ===", flush=True)
for nm in ("raw","clean"):
    q = con.execute(f"""SELECT AVG(usdcSize), MEDIAN(usdcSize),
        SUM(CASE WHEN usdcSize<1 THEN 1 ELSE 0 END)*1.0/COUNT(*),
        SUM(CASE WHEN price<=0.02 OR price>=0.98 THEN 1 ELSE 0 END)*1.0/COUNT(*) FROM {nm}""").fetchone()
    print(f"{nm:6s} avg_sz={q[0]:.2f} med_sz={q[1]:.2f} frac_usdc<1={q[2]:.4f} frac_price_extreme={q[3]:.4f}", flush=True)
print("\n=== per primary_category: raw vs clean ===", flush=True)
con.execute(f"CREATE VIEW dims AS SELECT token_id, primary_category FROM read_parquet('{AUG}')")
rows = con.execute("""
  WITH r AS (SELECT d.primary_category pc, COUNT(*) n, SUM(t.usdcSize) v
             FROM raw t JOIN dims d ON t.conditionId=d.token_id GROUP BY 1),
       c AS (SELECT d.primary_category pc, COUNT(*) n, SUM(t.usdcSize) v
             FROM clean t JOIN dims d ON t.conditionId=d.token_id GROUP BY 1)
  SELECT COALESCE(r.pc,c.pc) pc, r.n rn, c.n cn,
         100.0*(r.n-c.n)/NULLIF(r.n,0), 100.0*(r.v-c.v)/NULLIF(r.v,0)
  FROM r FULL JOIN c ON r.pc=c.pc ORDER BY r.n DESC
""").fetchall()
print(f"{'category':<16} {'raw_rows':>14} {'clean_rows':>14} {'row_drop%':>9} {'vol_drop%':>9}", flush=True)
for r in rows:
    pc=(r[0] or 'NULL')[:15]
    print(f"{pc:<16} {r[1]:>14,} {r[2]:>14,} {r[3]:>9.2f} {r[4]:>9.2f}", flush=True)
print("\nDONE", flush=True)
