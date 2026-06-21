"""Recompute bot wallet_flags on the CLEAN trades dataset (config already repointed).
Backs up the raw-based cache, writes new flags."""
import sys, os, shutil, time
sys.path.insert(0, '/home/ubuntu/prediction_markets/analysis')
sys.path.insert(0, '/home/ubuntu')
from data_loader import get_connection
from bot_filter import build_wallet_flags
con = get_connection(memory_limit='200GB', threads=14, force_new=True)
con.execute("SET temp_directory='/mnt/data/tmp'")
con.execute("SET max_temp_directory_size='220GiB'")
con.execute("SET preserve_insertion_order=false")
n = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
print(f"trades view (clean, ts-filtered): {n:,}", flush=True)
t0 = time.time()
stats = build_wallet_flags(con, verbose=True)
print(f"\nbuild_wallet_flags done in {time.time()-t0:.0f}s", flush=True)
print("stats:", stats, flush=True)
out = "/mnt/data/learnability/cache/wallet_flags.parquet"
if os.path.exists(out) and not os.path.exists(out + ".raw_bak"):
    shutil.copy(out, out + ".raw_bak"); print("backed up raw-based flags -> .raw_bak", flush=True)
con.execute(f"COPY (SELECT * FROM wallet_flags) TO '{out}' (FORMAT PARQUET)")
print(f"wrote {out}", flush=True)
print("DONE", flush=True)
