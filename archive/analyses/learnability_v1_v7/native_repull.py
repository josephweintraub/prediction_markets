"""Pull native Polymarket (Gamma) market metadata for the v7 learnability dims.
Output: /mnt/data/learnability/native/native_market_meta.parquet (one row per conditionId).
Gamma caps limit=100 and 422s on deep offset, so markets/events use KEYSET (cursor) pagination.
Env: MAX_PAGES (0=all, else cap pages for validation), OUT (override output path)."""
import os, json, time
import requests, pandas as pd
GAMMA="https://gamma-api.polymarket.com"
OUT=os.environ.get("OUT","/mnt/data/learnability/native/native_market_meta.parquet")
MAX_PAGES=int(os.environ.get("MAX_PAGES","0"))
S=requests.Session(); S.headers.update({"User-Agent":"Mozilla/5.0 (polymarket-research)"})
def get(path, **p):
    for a in range(6):
        try:
            r=S.get(f"{GAMMA}{path}", params=p, timeout=30)
            if r.status_code==200: return r.json()
        except Exception: pass
        time.sleep(1.0*(a+1))
    return None
def keyset(path, key, **base):
    rows=[]; cur=None; pg=0
    while True:
        p=dict(base, limit=100)
        if cur: p["after_cursor"]=cur
        d=get(path, **p)
        if not isinstance(d,dict): break
        b=d.get(key) or []
        rows+=b; cur=d.get("next_cursor"); pg+=1
        if pg%100==0: print(f"  {path}: {len(rows)} rows",flush=True)
        if MAX_PAGES and pg>=MAX_PAGES: break
        if not cur or not b: break
        time.sleep(0.05)
    return rows
def off_small(path):  # small endpoints (series): correct 100-stride, cap-safe
    rows=[]; off=0
    while off<=4900:
        b=get(path, limit=100, offset=off)
        if not isinstance(b,list) or not b: break
        rows+=b; off+=100
    return rows
def jl(x):
    try: return json.loads(x) if isinstance(x,str) else (x or [])
    except Exception: return []
print("series...",flush=True)
ser={s.get("slug"):s.get("recurrence") for s in off_small("/series")}
print(f"  {len(ser)} series",flush=True)
print("events (keyset)...",flush=True)
evs=keyset("/events/keyset","events",closed="true")
by_cond={}; by_eslug={}
for e in evs:
    sslug=e.get("seriesSlug") or ((e.get("series") or [{}])[0].get("slug") if e.get("series") else None)
    ef=dict(event_slug=e.get("slug"),category=e.get("category"),
            tags=[t.get("label") for t in (e.get("tags") or [])],
            series_slug=sslug,recurrence=ser.get(sslug),
            event_description=e.get("description"),liquidity=e.get("liquidity"),
            event_volume=e.get("volume"),volume24hr=e.get("volume24hr"),
            open_interest=e.get("openInterest"),comment_count=e.get("commentCount"),
            competitive=e.get("competitive"))
    if e.get("slug"): by_eslug[e["slug"]]=ef
    for m in (e.get("markets") or []):
        c=m.get("conditionId")
        if c: by_cond[c]=ef
print(f"  {len(evs)} events | {len(by_cond)} conds, {len(by_eslug)} eslugs",flush=True)
print("markets (keyset)...",flush=True)
mks=keyset("/markets/keyset","markets",closed="true")
recs=[]
for m in mks:
    c=m.get("conditionId")
    if not c: continue
    ev=by_cond.get(c)
    if ev is None:
        mev=m.get("events") or []
        if mev: ev=by_eslug.get((mev[0] or {}).get("slug"))
    ev=ev or {}
    outs=jl(m.get("outcomes"))
    recs.append(dict(condition_id=c,question=m.get("question"),market_slug=m.get("slug"),
        description=m.get("description"),resolution_source=m.get("resolutionSource"),
        n_outcomes=len(outs) if outs else None,neg_risk=m.get("negRisk"),
        sports_market_type=m.get("sportsMarketType"),line=m.get("line"),
        created_at=m.get("createdAt"),end_date=m.get("endDate"),closed_time=m.get("closedTime"),
        uma_status=m.get("umaResolutionStatus"),uma_statuses=json.dumps(jl(m.get("umaResolutionStatuses"))),
        automatically_resolved=m.get("automaticallyResolved"),volume_num=m.get("volumeNum"),
        group_item_title=m.get("groupItemTitle"),**ev))
df=pd.DataFrame(recs).drop_duplicates("condition_id")
os.makedirs(os.path.dirname(OUT),exist_ok=True); df.to_parquet(OUT,index=False)
print(f"WROTE {OUT}: {len(df)} rows x {len(df.columns)} cols",flush=True)
