"""Build canonical clean trades parquet = full-row DISTINCT per partition.
Removes only byte-for-byte exact duplicates (ingestion replays). Keeps raw untouched."""
import duckdb, glob, json, os, time
BASE = "/home/ubuntu/pipeline/output/trades.parquet"
OUT  = "/mnt/data/pipeline_output/trades_clean.parquet"
os.makedirs(OUT, exist_ok=True)
# clear any stale spill
for f in glob.glob("/mnt/data/tmp/duckdb_temp_storage_*.tmp"):
    try: os.remove(f)
    except OSError: pass
parts = sorted(glob.glob(f"{BASE}/year_month=*"))
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET max_temp_directory_size='200GiB'")
con.execute("SET memory_limit='150GB'"); con.execute("SET threads=14")
con.execute("SET preserve_insertion_order=false")
stats=[]
for i,p in enumerate(parts):
    ym = p.split("year_month=")[1]
    od = f"{OUT}/year_month={ym}"; os.makedirs(od, exist_ok=True)
    of = f"{od}/data.parquet"
    t0=time.time()
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}/*.parquet', hive_partitioning=false)").fetchone()[0]
    con.execute(f"""COPY (SELECT DISTINCT * FROM read_parquet('{p}/*.parquet', hive_partitioning=false))
                    TO '{of}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    m = con.execute(f"SELECT COUNT(*) FROM read_parquet('{of}')").fetchone()[0]
    rec=dict(ym=ym,raw=n,clean=m,removed=n-m,pct=round(100*(n-m)/n,3))
    stats.append(rec)
    print(f"[{i+1:2d}/{len(parts)}] {ym}  raw={n:>12,}  clean={m:>12,}  removed={n-m:>10,} ({rec['pct']:5.2f}%)  {time.time()-t0:.0f}s", flush=True)
    json.dump(stats, open('/mnt/data/tmp/build_stats.json','w'), indent=2)  # checkpoint each partition
R=sum(s['raw'] for s in stats); C=sum(s['clean'] for s in stats)
print(f"\n==== TOTAL  raw={R:,}  clean={C:,}  removed={R-C:,}  ({100*(R-C)/R:.3f}%) ====", flush=True)
json.dump(dict(total=dict(raw=R,clean=C,removed=R-C,pct=round(100*(R-C)/R,3)), parts=stats),
          open('/mnt/data/tmp/build_stats.json','w'), indent=2)
print("DONE", flush=True)
