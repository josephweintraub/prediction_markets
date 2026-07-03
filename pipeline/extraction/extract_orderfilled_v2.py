#!/usr/bin/env python3
"""Stage 1 v2: extract NEW OrderFilled events (CTF Exchange V2 + NegRisk V2),
decode the new ABI, and EMIT in the OLD raw_events 12-col schema so all
downstream stages run unchanged. Polymarket migrated contracts ~2026-04-28."""
import sys, os, argparse, logging, time, glob
from multiprocessing import Process, Queue
from pathlib import Path
import requests
from eth_abi import decode as abidecode
sys.path.insert(0, "/home/ubuntu/pipeline")
from config import RPC_URL

EXCHANGES = ["0xe111180000d2663c0091e4f400237545b87b996b",   # CTF Exchange V2
             "0xe2222d279d744050d28e00520010520000310f59"]   # NegRisk CTF Exchange V2
TOPIC0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"
DATA_TYPES = ["uint8","uint256","uint256","uint256","uint256","bytes32","bytes32"]
DATA_DIR = Path("/home/ubuntu/pipeline/extraction/data")
CHUNK_DIR = DATA_DIR / "chunks_v2"
MAX_RETRIES=8; BACKOFF=1.5; FLUSH=50000
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] w%(process)d: %(message)s",
                    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(DATA_DIR/"v2_extract.log")])
log=logging.getLogger(__name__)

class TooLarge(Exception): pass
def get_logs(s, fr, to):
    p={"jsonrpc":"2.0","id":1,"method":"eth_getLogs","params":[{"fromBlock":hex(fr),"toBlock":hex(to),"address":EXCHANGES,"topics":[TOPIC0]}]}
    for a in range(MAX_RETRIES):
        try:
            r=s.post(RPC_URL,json=p,timeout=90)
            if r.status_code==400: raise TooLarge()
            r.raise_for_status(); j=r.json()
            if "error" in j:
                if "response size" in str(j["error"]).lower() or "larger than" in str(j["error"]).lower(): raise TooLarge()
                raise RuntimeError(j["error"])
            return j["result"]
        except TooLarge: raise
        except Exception as e:
            if a<MAX_RETRIES-1: time.sleep(min(BACKOFF**a,60))
            else: raise
def decode_v2(l):
    side,tok,mA,tA,fee,_,_=abidecode(DATA_TYPES, bytes.fromhex(l["data"][2:]))
    tok=str(tok)
    if side==0:  mai,tai = "0",tok      # BUY: maker pays USDC(0), gets token
    else:        mai,tai = tok,"0"      # SELL: maker gives token, gets USDC(0)
    return {"order_hash":l["topics"][1],"maker":"0x"+l["topics"][2][-40:],"taker":"0x"+l["topics"][3][-40:],
            "maker_asset_id":mai,"taker_asset_id":tai,"maker_amount_filled":int(mA),"taker_amount_filled":int(tA),
            "fee":int(fee),"block_number":int(l["blockNumber"],16),"transaction_hash":l["transactionHash"],
            "log_index":int(l["logIndex"],16),"exchange_address":l["address"].lower()}
def flush(evs, wid, n):
    import pyarrow as pa, pyarrow.parquet as pq
    cols={k:[e[k] for e in evs] for k in evs[0]}
    pq.write_table(pa.table(cols), CHUNK_DIR/f"chunk_{wid:03d}_{n:04d}.parquet", compression="zstd")
def worker(wid, start, end, q):
    s=requests.Session(); s.headers["Content-Type"]="application/json"
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    cur=start; evs=[]; tot=0; n=0; csz=1000; lastlog=time.time()
    while cur<=end:
        ce=min(cur+csz-1,end)
        try:
            logs=get_logs(s,cur,ce)
            if csz<1000: csz=min(csz*2,1000)
        except TooLarge:
            csz=max(csz//2,5); continue
        except Exception:
            log.error("w%d fatal at %d",wid,cur); break
        for e in logs:
            evs.append(decode_v2(e))
        cur=ce+1
        if len(evs)>=FLUSH:
            flush(evs,wid,n); tot+=len(evs); n+=1; evs=[]
        if time.time()-lastlog>30:
            log.info("w%d %.1f%% blk %d/%d ev=%d",wid,100*(cur-start)/max(end-start,1),cur,end,tot+len(evs)); lastlog=time.time()
    if evs: flush(evs,wid,n); tot+=len(evs)
    log.info("w%d done: %d events",wid,tot); q.put(tot)
def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--start",type=int,required=True); ap.add_argument("--end",type=int,required=True)
    ap.add_argument("--workers",type=int,default=50); ap.add_argument("--out",default=str(DATA_DIR/"raw_events_v2_inc.parquet"))
    a=ap.parse_args()
    import shutil
    if CHUNK_DIR.exists(): shutil.rmtree(CHUNK_DIR)
    CHUNK_DIR.mkdir(parents=True,exist_ok=True)
    log.info("v2 extract blocks %d -> %d, %d workers",a.start,a.end,a.workers)
    per=(a.end-a.start+1)//a.workers; procs=[]; q=Queue()
    for i in range(a.workers):
        ws=a.start+i*per; we=a.end if i==a.workers-1 else a.start+(i+1)*per-1
        p=Process(target=worker,args=(i,ws,we,q)); procs.append(p); p.start(); time.sleep(0.05)
    for p in procs: p.join()
    tot=sum(q.get() for _ in range(len(procs)) if not q.empty())
    import duckdb; con=duckdb.connect(); con.execute("SET memory_limit='16GB'")
    files=str(CHUNK_DIR/"chunk_*.parquet")
    if glob.glob(files):
        con.execute(f"COPY (SELECT * FROM read_parquet('{files}')) TO '{a.out}' (FORMAT PARQUET, COMPRESSION ZSTD)")
        n=con.execute(f"SELECT COUNT(*) FROM read_parquet('{a.out}')").fetchone()[0]
        log.info("MERGED %d events -> %s",n,a.out); shutil.rmtree(CHUNK_DIR)
    else: log.warning("no chunks produced")
if __name__=="__main__": main()
