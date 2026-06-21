"""Re-sort clean parquet by (conditionId,timestamp) per partition, in place (atomic).
Recovers run-length compression on the 77-char conditionId column."""
import duckdb, glob, os, time, json
OUT = "/mnt/data/pipeline_output/trades_clean.parquet"
parts = sorted(glob.glob(f"{OUT}/year_month=*"))
con = duckdb.connect()
con.execute("SET temp_directory='/mnt/data/tmp'"); con.execute("SET max_temp_directory_size='220GiB'")
con.execute("SET memory_limit='150GB'"); con.execute("SET threads=14")
con.execute("SET preserve_insertion_order=true")  # honor ORDER BY on write
stats=[]
for i,p in enumerate(parts):
    ym=p.split("year_month=")[1]; f=f"{p}/data.parquet"; tmp=f"{p}/data.sorted.parquet"
    if not os.path.exists(f):
        # partition may have multiple files; glob them
        fs=glob.glob(f"{p}/*.parquet"); src=f"{p}/*.parquet"
    else:
        src=f
    t0=time.time(); sz0=sum(os.path.getsize(x) for x in glob.glob(f"{p}/*.parquet"))
    n0=con.execute(f"SELECT COUNT(*) FROM read_parquet('{p}/*.parquet')").fetchone()[0]
    con.execute(f"""COPY (SELECT * FROM read_parquet('{p}/*.parquet') ORDER BY conditionId, timestamp)
                    TO '{tmp}' (FORMAT PARQUET, COMPRESSION ZSTD)""")
    n1=con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp}')").fetchone()[0]
    assert n1==n0, f"{ym} count mismatch {n0}!={n1}"
    # remove old file(s), put sorted in place
    for x in glob.glob(f"{p}/*.parquet"):
        if x!=tmp: os.remove(x)
    os.replace(tmp, f"{p}/data.parquet")
    sz1=os.path.getsize(f"{p}/data.parquet")
    stats.append(dict(ym=ym,n=n1,sz_mb_before=round(sz0/1e6,1),sz_mb_after=round(sz1/1e6,1)))
    print(f"[{i+1:2d}/{len(parts)}] {ym} n={n1:,} {sz0/1e6:.0f}MB->{sz1/1e6:.0f}MB ({100*sz1/sz0:.0f}%) {time.time()-t0:.0f}s", flush=True)
    json.dump(stats, open('/mnt/data/tmp/resort_stats.json','w'), indent=2)
B=sum(s['sz_mb_before'] for s in stats); A=sum(s['sz_mb_after'] for s in stats)
print(f"\n==== TOTAL {B/1000:.1f}GB -> {A/1000:.1f}GB ({100*A/B:.0f}%) ====", flush=True)
print("DONE", flush=True)
