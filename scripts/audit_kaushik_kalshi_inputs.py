#!/usr/bin/env python3
"""Independent saved-body audit, bounded raw-day replay and gated publication.

The all-tape UUID gate is reopened as a committed builder proof, not described
as an independent full-tape replay. Only the eight explicitly pinned native days
are independently replayed. No acceptance is published by either audit phase.
"""
from __future__ import annotations

import argparse
import csv
from datetime import date, timedelta
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
import production_guard

RUN_ROOT = Path("/mnt/data/runs")
JSON_LIMIT = 16_000_000
METADATA_ROWS, METADATA_FILES = 166_961_153, 80
RAW_REPLAY_DAYS = ("2021-06-30", "2025-03-12", "2025-03-13", "2025-04-01", "2026-04-01", "2026-06-01", "2026-09-01", "2026-10-01")
RAW_REPLAY_BYTES = 2_000_000_000
OUTPUT_LIMIT = 8_000_000_000
PUBLICATION_RESERVE = 32_000_000
EXECUTION_FIELDS=('baseline_execution_seconds_min','baseline_execution_seconds_max',
                  'duration_tail_execution_seconds_min','duration_tail_execution_seconds_max')
SPORTS_SERIES = {"KXMLBGAME":"mlb","KXNFLGAME":"nfl","KXNBAGAME":"nba","KXNHLGAME":"nhl",
                 "KXNCAAMBGAME":"cbb","KXATPMATCH":"atp","KXEPLGAME":"epl","KXNCAAFGAME":"cfb","KXWNBAGAME":"wnba"}
CHECKS = ("schema_validated", "body_hashes_validated", "source_commit_validated", "global_trade_ids_unique",
          "source_rows_reconciled", "no_join_fanout", "cell_moments_validated", "duration_membership_validated",
          "claim_invariants_validated", "codes_validated")
RAW_DEPENDENT = ("no_join_fanout", "cell_moments_validated", "duration_membership_validated")
PREFIXES = ("KXMVE", "KXOSCARWINNERS", "KXCITIESWEATHER")
SOURCE_FILES = ("scripts/build_kaushik_kalshi_inputs.py", "analysis/kaushik_kalshi_replication/build.py",
                "analysis/kaushik_kalshi_replication/inputs.py", "analysis/kaushik_kalshi_replication/metadata.py",
                "analysis/kaushik_kalshi_replication/staging.py", "analysis/kaushik_kalshi_replication/source_contract.py",
                "production_guard.py")
PINNED_MANIFESTS = {"trade_manifest": "37aae4a94b1125cea81eb2222caba0daaa089eb9c241a0e6dd90bb0144fdf0ee",
                    "metadata_manifest": "0ad2a1c6167f5171a5323e3bee021a27ae58516ee68ca23aab64d10ca5683ecf"}
PINNED_REFS = {"events": (43666531, "f535371e2b2c50946125e5ef0004aa418c1ad0fe1dea186c90768717a0480c47"),
               "series": (2277714, "270c6de4d1a9d7d05820af126e631694ccb4f64e7dcb5d4e25ce976a5614960d"),
               "collections": (262933, "69fbcd18676aad55394235da8408044c61cd70ec0926c63af7f1185a82b04ac5")}
UTC_PATTERN = (r"[0-9]{4}-(?:0[1-9]|1[0-2])-(?:0[1-9]|[12][0-9]|3[01])[T ]"
               r"(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](?:\.[0-9]{1,6})?(?:Z|[+-]00:00)")


class AuditBlocked(ValueError):
    pass


def require(value, reason):
    if not value:
        raise AuditBlocked(reason)


def count(value):
    require(type(value) is int and value >= 0, "nonnegative exact count required")
    return value


def ident(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value), "simple SQL name required")
    return '"' + value + '"'


def literal(value):
    return "'" + str(value).replace("'", "''") + "'"


def digest(path):
    result = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1_048_576), b""):
            result.update(block)
    return result.hexdigest()


def identity(path):
    value = Path(path).lstat()
    return value.st_dev, value.st_ino, value.st_size, value.st_mtime_ns, value.st_ctime_ns


def _unique(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate JSON key")
        value[key] = item
    return value


def read_json(path, expected_sha=None):
    path = Path(path)
    require(path.is_file() and not path.is_symlink() and 0 < path.stat().st_size <= JSON_LIMIT, "bounded regular JSON required")
    before = identity(path)
    raw = path.read_bytes()
    hashed = hashlib.sha256(raw).hexdigest()
    require(identity(path) == before and (expected_sha is None or hashed == expected_sha), "JSON hash/drift gate failed")
    value = json.loads(raw, object_pairs_hook=_unique,
                       parse_constant=lambda token: (_ for _ in ()).throw(AuditBlocked("nonfinite JSON")))
    require(type(value) is dict, "JSON object required")
    return value, {"path": str(path), "bytes": len(raw), "sha256": hashed}


def _relative_path(value):
    require(isinstance(value, str) and value and "\\" not in value and ":" not in value
            and not Path(value).is_absolute() and all(part not in ("", ".", "..") for part in value.split("/")),
            "normalized relative path required")
    return value


def child(root, value):
    path = Path(root) / _relative_path(value)
    require(path.resolve(strict=True) == path and path.is_relative_to(root), "artifact path escape/symlink")
    return path


def verify_source_snapshot(snapshot, expected_head):
    """Compare the builder's recorded blobs with its explicit historical commit."""
    require(re.fullmatch(r"[0-9a-f]{40}", expected_head or "") and snapshot.get("head") == expected_head,
            "explicit builder commit binding differs")
    files = snapshot.get("files", {})
    require(type(files) is dict and set(SOURCE_FILES) <= set(files), "critical committed builder source inventory missing")
    resolved = subprocess.check_output(["git", "rev-parse", expected_head + "^{commit}"], cwd=REPO, text=True).strip()
    require(resolved == expected_head, "builder source reference is not the pinned commit")
    for relative, expected in files.items():
        _relative_path(relative)
        require(re.fullmatch(r"[0-9a-f]{64}", expected or ""), "source blob fingerprint missing")
        blob = subprocess.check_output(["git", "show", expected_head + ":" + relative], cwd=REPO)
        require(len(blob) <= 2_000_000 and hashlib.sha256(blob).hexdigest() == expected, "committed builder source blob differs: " + relative)
    return {"commit": expected_head, "files": len(files), "proof_mode": "git_show_explicit_historical_builder_commit"}


class Binder:
    """Reopen and bind every referenced body without modifying any input."""

    def __init__(self):
        self.records = {}

    def file(self, root, record, *, parquet=False):
        import pyarrow.parquet as pq
        require(type(record) is dict and type(record.get("bytes")) is int and record["bytes"] > 0
                and re.fullmatch(r"[0-9a-f]{64}", record.get("sha256", "")), "explicit body size/hash required")
        path = child(Path(root), record.get("path"))
        require(path.is_file() and not path.is_symlink(), "regular body required")
        before = identity(path)
        require(path.stat().st_size == record["bytes"] and digest(path) == record["sha256"], "body size/hash differs: " + str(path))
        item = {"path": str(path), "bytes": record["bytes"], "sha256": record["sha256"], "rows": None, "schema_sha256": None}
        if parquet:
            metadata = pq.ParquetFile(path)
            require(metadata.metadata.num_rows == record.get("rows") and str(metadata.schema_arrow) == record.get("schema"), "body footer/schema differs")
            item.update(rows=metadata.metadata.num_rows, schema_sha256=hashlib.sha256(str(metadata.schema_arrow).encode()).hexdigest())
        require(identity(path) == before, "body changed while reopening")
        previous = self.records.get(str(path))
        require(previous is None or previous == item, "one path has conflicting body roles")
        self.records[str(path)] = item
        return path

    def json(self, root, record):
        path = self.file(root, record)
        return read_json(path, record["sha256"])[0]


def view(con, name, paths):
    require(paths, "nonempty explicit body inventory required")
    con.execute(f"CREATE TEMP VIEW {ident(name)} AS SELECT * FROM read_parquet([{','.join(literal(path) for path in paths)}],hive_partitioning=false)")


def scalar(con, sql):
    return con.execute(sql).fetchone()[0]


def _same_query(con, left, right, reason):
    mismatch = scalar(con, "SELECT count(*) FROM (((" + left + ") EXCEPT ALL (" + right + ")) UNION ALL ((" + right + ") EXCEPT ALL (" + left + ")))")
    require(mismatch == 0, reason)


def _same_aggregate(con, left, right, keys, integers, floats, reason, *, scale_columns=None):
    """Counts are exact; binary64 sums permit only bounded reduction roundoff."""
    join = " AND ".join("a."+ident(key)+" IS NOT DISTINCT FROM b."+ident(key) for key in keys)
    tests = ["a."+ident(key)+" IS DISTINCT FROM b."+ident(key) for key in integers]
    for key in floats:
        scale='' if key not in (scale_columns or {}) else ',a.'+ident(scale_columns[key])
        tests += [f"a.{ident(key)} IS NULL OR b.{ident(key)} IS NULL OR NOT isfinite(a.{ident(key)}) OR NOT isfinite(b.{ident(key)}) "
                  f"OR abs(a.{ident(key)}-b.{ident(key)})>1e-10*greatest(1.0,abs(a.{ident(key)}),abs(b.{ident(key)}){scale})"]
    tests += ['a.'+ident(column)+' IS NULL OR NOT isfinite(a.'+ident(column)+')' for column in (scale_columns or {}).values()]
    sql = ("SELECT count(*) FROM (SELECT *,true present FROM ("+left+")) a FULL JOIN "
           "(SELECT *,true present FROM ("+right+")) b ON "+join+" WHERE a.present IS NULL OR b.present IS NULL OR "+" OR ".join(tests))
    require(scalar(con,sql)==0,reason)


def _record(root,path,*,parquet=False):
    value={"path":str(path.relative_to(root)),"bytes":path.stat().st_size,"sha256":digest(path)}
    if parquet:
        import pyarrow.parquet as pq
        footer=pq.ParquetFile(path)
        value.update(rows=footer.metadata.num_rows,schema=str(footer.schema_arrow))
    return value


def _used_output(root):
    def persistent(path):
        parts=path.relative_to(root).parts
        return parts[0]!='spill' and not any(parts[index]=='source_stage_v1' and parts[index+1].startswith('shard-') for index in range(len(parts)-1))
    return sum(path.stat().st_size for path in root.rglob('*') if path.is_file() and persistent(path))


def _write_parquet(con,root,path,query):
    """Stream Arrow batches through an exclusive, hard-byte-limited output."""
    import pyarrow.parquet as pq
    remaining=OUTPUT_LIMIT-_used_output(root)-PUBLICATION_RESERVE
    require(remaining>0,"audit output ceiling exhausted")
    class Sink(io.RawIOBase):
        def __init__(self,stream):
            super().__init__();self.stream=stream
        def writable(self):return True
        def tell(self):return self.stream.tell()
        def write(self,block):
            require(self.tell()+len(block)<=remaining,"hard audit output ceiling exceeded")
            return self.stream.write(block)
        def flush(self):
            if not self.stream.closed:self.stream.flush()
    with path.open('xb') as stream:
        sink=Sink(stream)
        reader=con.execute(query).fetch_record_batch(65536)
        with pq.ParquetWriter(sink,reader.schema,compression='zstd') as writer:
            for batch in reader:writer.write_batch(batch)
        stream.flush();os.fsync(stream.fileno())
    return _record(root,path,parquet=True)


def _utc_seconds(expression):
    parsed = f"CASE WHEN regexp_full_match({expression},{literal(UTC_PATTERN)}) THEN epoch(try_cast({expression} AS TIMESTAMPTZ)) END"
    return f"CASE WHEN {parsed}>=0 AND {parsed}<253402300800 THEN {parsed} END"


def _opaque_identifier(expression):
    invalid = r"[\s\p{Z}\x{0000}-\x{001f}\x{007f}\x{0085}]"
    return f"({expression} IS NOT NULL AND {expression}<>'' AND {expression}=trim({expression}) AND NOT regexp_matches(replace({expression},' ',''),{literal(invalid)}))"


def _manifest_entries(records, kind, binder):
    item = records[kind + "_manifest"]
    path = Path(item["absolute_path"])
    require(item["sha256"] == PINNED_MANIFESTS[kind + "_manifest"], "original source manifest pin differs")
    binder.file(path.parent, {**item, "path": path.name})
    fields = ["file", "bytes", "sha256"] if kind == "trade" else ["path", "bytes", "rows", "sha256"]
    reader = csv.DictReader(io.StringIO(path.read_text(), newline=""))
    require(reader.fieldnames == fields, "original source manifest header differs")
    entries, seen = {}, set()
    for raw in reader:
        require(None not in raw and all(value is not None for value in raw.values()), "malformed source manifest row")
        source_path = raw[fields[0]]
        require(source_path not in seen, "duplicate original manifest path")
        seen.add(source_path)
        if kind == "trade":
            if not source_path.startswith("trades_all/"):
                require(re.fullmatch(r"bid_ask_1m/month=\d{4}-\d{2}/part-000\.parquet", source_path), "unadmitted original manifest path")
                continue
            require(re.fullmatch(r"trades_all/dt=\d{4}-\d{2}-\d{2}/part-000\.parquet", source_path), "daily source path differs")
            mapped = source_path
        else:
            require(re.fullmatch(r"contract_matching/runs/markets_meta_all_v2/(?:b[12]|f[12]|v)_\d+\.parquet", source_path), "metadata original source path differs")
            mapped = "markets_metadata/" + Path(source_path).name
        require(mapped not in entries and int(raw["bytes"]) > 0 and re.fullmatch(r"[0-9a-f]{64}", raw["sha256"]), "source mapping/size/hash differs")
        entries[mapped] = {"path": mapped, "size_bytes": int(raw["bytes"]), "sha256": raw["sha256"]}
        if kind == "metadata":
            entries[mapped]["rows"] = int(raw["rows"])
    return entries


def expected_days():
    first, last = date(2021, 6, 30), date(2026, 10, 1)
    return {(first + timedelta(days=index)).isoformat() for index in range((last-first).days+1)} - {"2021-07-03"}


def _receipt_index(root, record, binder):
    index = binder.json(root, record)
    require(index.get("schema_version") == "kaushik_kalshi_receipt_inventory_v1" and type(index.get("receipts")) is list,
            "receipt inventory contract differs")
    require(len({item["path"] for item in index["receipts"]}) == len(index["receipts"]), "duplicate receipt inventory path")
    return [(item, binder.json(root, item)) for item in index["receipts"]]


def _stage_receipts(root, index_record, entries, binder):
    seen, source_rows = set(), 0
    for _, receipt in _receipt_index(root, index_record, binder):
        require(receipt.get("schema_version") == "kalshi_source_stage_receipt_v1" and receipt.get("status") == "processed_verified", "verified staging receipt required")
        entry = receipt.get("entry", {})
        path = entry.get("path")
        require(path in entries and path not in seen and entry == {key: entries[path][key] for key in ("path", "size_bytes", "sha256")}, "staged source entry differs")
        require(receipt.get("remote") == "dropbox:Polymarket Data and Code/kalshi/" + path,
                "staging remote/source identity differs")
        require(receipt.get("verified_identity", {}).get("bytes") == entry["size_bytes"], "staging verified-size proof missing")
        provenance = receipt.get("callback_provenance",{})
        if path.startswith("trades_all/"):
            day = path.split("dt=",1)[1].split("/",1)[0]
            item = provenance.get("day_receipt",{})
            require(item.get("path")=="cells/day="+day+"/receipt.json", "staging callback/day body binding missing")
            body = binder.json(root,item)
            require(body.get("source_day")==day and body.get("source_rows")==provenance.get("source_rows"), "staging callback/source footer count differs")
            source_rows += count(provenance.get("source_rows"))
        else:
            require(provenance.get("metadata_source_rows")==entries[path]["rows"], "staging callback/metadata manifest count differs")
            health = binder.json(root,provenance.get("health",{}))
            require(health.get("source_entry")==entry, "staging callback/metadata health source differs")
            source_rows += entries[path]["rows"]
        seen.add(path)
    require(seen == set(entries), "complete staging receipt/source inventory differs")
    return {"files": len(seen), "source_rows":source_rows, "proof_mode": "reopened_source_commit_bound_SHA_verified_staging_receipts"}


def _schema(con, relation):
    return {row[0]: row[1] for row in con.execute("DESCRIBE SELECT * FROM " + ident(relation)).fetchall()}


def _analytic_schema(role):
    text=lambda names:{name:'VARCHAR' for name in names.split()}
    moment=lambda width:{'n_rows':'BIGINT',**{'a'+str(i):'DOUBLE' for i in range(width)},
                         **{f'c{i}_{j}':'DOUBLE' for i in range(width) for j in range(i,width)}}
    shapes={
        'baseline':{**text('event_cluster'),'bin':'TINYINT','n_rows':'BIGINT',**{name:'DOUBLE' for name in ('sum_price','sum_outcome','sum_payoff','sum_roi')}},
        'duration_raw':{**text('event_cluster category trade_month'),'P':'DOUBLE','tail':'TINYINT','lifespan_gt1':'BOOLEAN','remaining_gt1':'BOOLEAN',**moment(4)},
        'claim_raw':{**text('event_cluster claim_id'),**moment(6)},
        'categories':{**text('category'),'n_rows':'BIGINT'},
        'markets':{**text('ticker event_cluster category result'),'opening_seconds':'DOUBLE','endpoint_seconds':'DOUBLE'},
        'claims':{**text('claim_id ticker taker_side event_cluster category'),'Y':'INTEGER',**{name:'BIGINT' for name in ('n_rows','n_d1','n_d10','duration_rows')}},
        'exclusions':{**text('eligibility_reason'),'source_ordinal':'BIGINT'},
        'sports_candidates':{**text('ticker claim_id event_cluster sport'),'timestamp':'DOUBLE','P':'DOUBLE','Y':'INTEGER','payoff':'DOUBLE','roi':'DOUBLE','bin':'TINYINT'},
        'cat_dictionary':{'tail':'TINYINT','category':'VARCHAR','cat_code':'BIGINT'},
        'price_dictionary':{'tail':'TINYINT','P':'DOUBLE','price_code':'BIGINT'},
        'month_dictionary':{'tail':'TINYINT','trade_month':'VARCHAR','month_code':'BIGINT'},
        'claim_dictionary':{'claim_id':'VARCHAR','claim_fe_code':'BIGINT'}}
    shapes['duration']={key:value for key,value in shapes['duration_raw'].items() if key not in ('category','P','trade_month')}
    shapes['duration'].update({key:'BIGINT' for key in ('cat_code','price_code','month_code')})
    shapes['claim']={key:value for key,value in shapes['claim_raw'].items() if key!='claim_id'}
    shapes['claim']['claim_fe_code']='BIGINT'
    return shapes[role]


def _validate_saved_fields(con,relation,role):
    """All required saved values fail explicitly, never via SQL UNKNOWN/SUM."""
    fields=_analytic_schema(role)
    bad=[]
    for field,kind in fields.items():
        column='c.'+ident(field)
        optional=role=='markets' and field in ('opening_seconds','endpoint_seconds')
        if not optional:bad.append(column+' IS NULL')
        if kind=='DOUBLE':bad.append(('('+column+' IS NOT NULL AND NOT isfinite('+column+'))') if optional else 'NOT isfinite('+column+')')
        if kind=='VARCHAR':bad.append(column+"=''")
    for field in ('ticker','claim_id','event_cluster'):
        if field in fields:bad.append('NOT '+_opaque_identifier('c.'+ident(field)))
    if 'n_rows' in fields:bad += ['c.n_rows<=0','c.n_rows>9007199254740992']
    for field in ('cat_code','price_code','month_code','claim_fe_code'):
        if field in fields:bad.append('c.'+field+'<0')
    if 'tail' in fields:bad.append('c.tail NOT IN(0,1)')
    if role=='claims':
        tails='(CAST(c.n_d1 AS HUGEINT)+CAST(c.n_d10 AS HUGEINT))'
        bad += ["c.taker_side NOT IN('yes','no')","c.Y NOT IN(0,1)",
                "c.claim_id IS DISTINCT FROM 'kalshi:claim:'||c.ticker||':'||c.taker_side",
                'c.n_d1<0','c.n_d10<0',tails+'>c.n_rows','c.duration_rows<0','c.duration_rows>'+tails]
    if role=='markets':bad += ["c.result NOT IN('yes','no')",'c.opening_seconds<0','c.endpoint_seconds<0']
    if role in ('duration_raw','price_dictionary'):
        bad += ['c.P<=0','c.P>=1','(c.tail=0 AND c.P>=.1)','(c.tail=1 AND c.P<.9)']
    if role=='baseline':
        bad += ['c.bin NOT BETWEEN 1 AND 10','c.sum_price<=0','c.sum_price>=c.n_rows','c.sum_outcome<0','c.sum_outcome>c.n_rows',
                'c.sum_outcome<>floor(c.sum_outcome)',
                'abs(c.sum_payoff-100*(c.sum_outcome-c.sum_price))>1e-9*greatest(1.0,100*c.n_rows)',
                'c.sum_roi < -100*c.n_rows-1e-9*greatest(1.0,100*c.n_rows)',
                'c.sum_price<(c.bin-1)*.1*c.n_rows-1e-12*c.n_rows','c.sum_price>c.bin*.1*c.n_rows+1e-12*c.n_rows']
    if role=='sports_candidates':
        bins=' '.join(f'WHEN c.P<{upper!r} THEN {index}' for index,upper in enumerate((.1,.2,.3,.4,.5,.6,.7,.8,.9),1))
        bad += ["c.sport NOT IN('mlb','nfl','nba','nhl','cbb','atp','epl','cfb','wnba')",'c.timestamp<0','c.timestamp>=253402300800','c.P<=0','c.P>=1','c.Y NOT IN(0,1)',
                'c.bin IS DISTINCT FROM CASE '+bins+' ELSE 10 END',
                "c.claim_id NOT IN('kalshi:claim:'||c.ticker||':yes','kalshi:claim:'||c.ticker||':no')",
                'abs(c.payoff-100*(c.Y-c.P))>1e-9*greatest(1.0,abs(c.payoff))',
                'abs(c.roi-100*(c.Y/c.P-1))>1e-9*greatest(1.0,abs(c.roi))']
    require(scalar(con,'SELECT count(*) FROM '+ident(relation)+' c WHERE '+' OR '.join('('+test+')' for test in bad))==0,
            'required saved fields/values invalid: '+role)


def _moments(con, relation, width):
    import numpy as np
    fields = _schema(con, relation)
    needed = {"n_rows", *["a"+str(i) for i in range(width)], *[f"c{i}_{j}" for i in range(width) for j in range(i,width)]}
    require(needed <= set(fields), "lossless moment fields missing")
    names = ["n_rows", *["a"+str(i) for i in range(width)], *[f"c{i}_{j}" for i in range(width) for j in range(i,width)]]
    batches = con.execute("SELECT " + ",".join(ident(name) for name in names) + " FROM " + ident(relation)).fetch_record_batch(65536)
    groups, observations, smallest = 0, 0, None
    for batch in batches:
        columns = {name: batch.column(index).to_numpy(zero_copy_only=False) for index, name in enumerate(names)}
        n = columns["n_rows"].astype(float)
        require(np.all(np.isfinite(n) & (n > 0) & (n <= 2**53) & (n == np.floor(n))), "moment frequency invalid")
        sums = np.column_stack([columns["a"+str(i)] for i in range(width)])
        centered = np.empty((len(n),width,width))
        for i in range(width):
            for j in range(i,width):
                centered[:,i,j] = centered[:,j,i] = columns[f"c{i}_{j}"]
        require(np.isfinite(sums).all() and np.isfinite(centered).all(), "moment fields nonfinite/null")
        scale = np.maximum(1.,np.abs(centered).max(axis=(1,2)))
        eigen = np.linalg.eigvalsh(centered)[:,0]
        require(np.all(eigen >= -1e-8*scale), "centered moment matrix is not positive semidefinite")
        if width == 6:
            h = sums[:,0]
            require(np.all((h >= 0) & (h <= n) & (h == np.floor(h))), "claim H sum is not an exact print count")
            require(np.all(np.abs(centered[:,0,0] - (h-h*h/n)) <= 1e-8*np.maximum(1,n)), "claim binary-H centered moment identity differs (SUM versus mean)")
        groups += len(n)
        observations += int(n.sum())
        smallest = float(eigen.min()) if smallest is None else min(smallest,float(eigen.min()))
    return {"groups": groups, "observations": observations, "minimum_centered_eigenvalue": smallest,
            "moment_encoding": "a_i SUM; c_i_j centered within-partial-cell sum; no merging of centered partials"}


def _dictionaries(con):
    for family, column in (("cat","category"),("price","P"),("month","trade_month")):
        relation = "final_" + family + "_dictionary"
        code = family + "_code"
        n, distinct, low, high = con.execute(f"SELECT count(*),count(DISTINCT {code}),min({code}),max({code}) FROM {relation}").fetchone()
        require(n == distinct and (n == 0 or (low == 0 and high == n-1)), "dictionary codes not unique and dense: " + family)
        require(scalar(con,f"SELECT count(*) FROM {relation} WHERE tail IS NULL OR tail NOT IN (0,1) OR {ident(column)} IS NULL") == 0,
                "dictionary values/tails missing")
        require(scalar(con,f"SELECT count(*) FROM (SELECT tail,{ident(column)} FROM {relation} GROUP BY ALL HAVING count(*)<>1)") == 0, "duplicate tail/value dictionary entry")
        require(scalar(con,f"SELECT count(*) FROM final_duration d LEFT JOIN {relation} v USING({code}) WHERE v.{code} IS NULL OR d.tail<>v.tail") == 0,
                "duration dictionary join or tail-specific code differs")
        require(scalar(con,f"SELECT count(*) FROM {relation} v LEFT JOIN (SELECT DISTINCT {code} FROM final_duration) d USING({code}) WHERE d.{code} IS NULL")==0,
                "unused duration dictionary code differs")
    require(scalar(con,"SELECT count(*) FROM final_price_dictionary WHERE NOT isfinite(P) OR P<=0 OR P>=1 OR (tail=0 AND P>=.1) OR (tail=1 AND P<.9)") == 0,
            "exact price dictionary/tail boundary differs")
    require(scalar(con,"SELECT count(*) FROM final_month_dictionary WHERE NOT regexp_full_match(trade_month,'[0-9]{4}-(0[1-9]|1[0-2])') OR trade_month<'2021-06' OR trade_month>'2026-10'")==0,
            "duration native month dictionary differs")
    n, unique, low, high = con.execute("SELECT count(*),count(DISTINCT claim_fe_code),min(claim_fe_code),max(claim_fe_code) FROM final_claim_dictionary").fetchone()
    require(n == unique and (n == 0 or (low==0 and high==n-1)), "claim dictionary not dense and unique")
    require(scalar(con,"SELECT count(*)-count(DISTINCT claim_id) FROM final_claim_dictionary")==0,"claim dictionary IDs not unique/nonnull")
    require(scalar(con,"SELECT count(*) FROM final_claim c LEFT JOIN final_claim_dictionary d USING(claim_fe_code) WHERE d.claim_fe_code IS NULL") == 0,
            "claim dictionary join misses cells")
    require(scalar(con,"SELECT count(*) FROM final_claim_dictionary d LEFT JOIN (SELECT DISTINCT claim_fe_code FROM final_claim) c USING(claim_fe_code) WHERE c.claim_fe_code IS NULL")==0,
            "unused claim dictionary code differs")


def _final_relations(con, root, manifest, binder):
    expected = {"baseline","duration","claim","categories","markets","claims","cat_dictionary","price_dictionary","month_dictionary","claim_dictionary"}
    require(set(manifest.get("artifacts", {})) == expected, "exact final artifact-role inventory differs")
    for role, record in manifest["artifacts"].items():
        require(record.get("path") == "cells/final/" + role + ".parquet", "final artifact role/path differs")
        view(con, "final_"+role, [binder.file(root,record,parquet=True)])
        require(_schema(con,'final_'+role)==_analytic_schema(role),'independent final analytic schema differs: '+role)
        _validate_saved_fields(con,'final_'+role,role)
    require(set(_schema(con,"final_baseline")) == {"event_cluster","bin","n_rows","sum_price","sum_outcome","sum_payoff","sum_roi"}, "baseline schema differs")
    for relation in ("final_baseline","final_categories","final_claims"):
        require(scalar(con,f"SELECT count(*) FROM {relation} WHERE n_rows IS NULL OR n_rows<=0 OR n_rows>9007199254740992") == 0, "invalid print frequencies")
    require(scalar(con,"""SELECT count(*) FROM final_baseline WHERE event_cluster IS NULL OR event_cluster='' OR bin NOT BETWEEN 1 AND 10 OR bin IS NULL
      OR NOT isfinite(sum_price) OR NOT isfinite(sum_outcome) OR NOT isfinite(sum_payoff) OR NOT isfinite(sum_roi)
      OR sum_price IS NULL OR sum_outcome IS NULL OR sum_payoff IS NULL OR sum_roi IS NULL
      OR sum_price<=0 OR sum_price>=n_rows OR sum_outcome<0 OR sum_outcome>n_rows OR sum_outcome<>floor(sum_outcome)
      OR abs(sum_payoff-100*(sum_outcome-sum_price))>1e-9*greatest(1.0,100*n_rows)
      OR sum_roi < -100*n_rows-1e-9*greatest(1.0,100*n_rows)
      OR sum_price < (bin-1)*.1*n_rows-1e-12*n_rows OR sum_price>bin*.1*n_rows+1e-12*n_rows""") == 0, "baseline value/bin/payoff identities differ")
    require(scalar(con,"SELECT count(*) FROM (SELECT event_cluster,bin FROM final_baseline GROUP BY ALL HAVING count(*)<>1)") == 0, "baseline event/bin rows duplicated")
    _dictionaries(con)
    duration, claim = _moments(con,"final_duration",4), _moments(con,"final_claim",6)
    view_sql = "SELECT d.*,p.P,c.category,m.trade_month FROM final_duration d JOIN final_price_dictionary p USING(price_code,tail) JOIN final_cat_dictionary c USING(cat_code,tail) JOIN final_month_dictionary m USING(month_code,tail)"
    con.execute("CREATE TEMP VIEW audit_duration_values AS " + view_sql)
    require(scalar(con,"""SELECT count(*) FROM audit_duration_values WHERE tail NOT IN(0,1) OR tail IS NULL OR lifespan_gt1 IS NULL OR remaining_gt1 IS NULL
      OR a0<=0 OR a1<0 OR a1>a0+1e-9*greatest(1.0,a0) OR remaining_gt1 AND NOT lifespan_gt1
      OR lifespan_gt1 AND a0<n_rows-1e-9*n_rows OR NOT lifespan_gt1 AND a0>n_rows+1e-9*n_rows
      OR remaining_gt1 AND a1<n_rows-1e-9*n_rows OR NOT remaining_gt1 AND a1>n_rows+1e-9*n_rows
      OR abs(a3-a2/P)>1e-8*greatest(1.0,abs(a3),abs(a2/P))
      OR a2/100+P*n_rows < -1e-8*n_rows OR a2/100+P*n_rows>n_rows+1e-8*n_rows
      OR abs((a2/100+P*n_rows)-round(a2/100+P*n_rows))>1e-7*greatest(1.0,n_rows)""") == 0, "duration support/clock/price/payoff-return algebra differs")
    require(scalar(con,"SELECT count(*) FROM (SELECT ticker FROM final_markets GROUP BY 1 HAVING count(*)<>1)") == 0, "final market ticker not unique")
    require(scalar(con,"SELECT count(*) FROM (SELECT claim_id FROM final_claims GROUP BY 1 HAVING count(*)<>1)") == 0, "final claim ID not unique")
    require(scalar(con,"""SELECT count(*) FROM audit_duration_values d LEFT JOIN
        (SELECT event_cluster,min(category) category,count(DISTINCT category) categories FROM final_markets GROUP BY 1) m USING(event_cluster)
        WHERE m.event_cluster IS NULL OR m.categories<>1 OR d.category IS DISTINCT FROM m.category""")==0,"duration cells disagree with native event category")
    require(scalar(con,"""SELECT count(*) FROM final_claims c LEFT JOIN final_markets m USING(ticker)
      WHERE m.ticker IS NULL OR c.claim_id IS DISTINCT FROM 'kalshi:claim:'||c.ticker||':'||c.taker_side
      OR c.Y IS DISTINCT FROM (c.taker_side=m.result)::INTEGER OR c.event_cluster IS DISTINCT FROM m.event_cluster OR c.category IS DISTINCT FROM m.category""") == 0,
            "claim identity/payout/support/market invariants differ")
    _same_query(con,"SELECT event_cluster,sum(n_rows)::HUGEINT n FROM final_baseline GROUP BY 1", "SELECT event_cluster,sum(n_rows)::HUGEINT n FROM final_claims GROUP BY 1", "baseline/claim event count disagreement")
    _same_query(con,"SELECT category,sum(n_rows)::HUGEINT n FROM final_categories GROUP BY 1", "SELECT category,sum(n_rows)::HUGEINT n FROM final_claims GROUP BY 1", "native category count disagreement")
    _same_query(con,"SELECT event_cluster,sum(n_rows)::HUGEINT n FROM final_duration GROUP BY 1", "SELECT event_cluster,sum(n_rows)::HUGEINT n FROM final_claim GROUP BY 1", "duration/A1 event population disagreement")
    _same_query(con,"SELECT event_cluster,sum(n_rows)::HUGEINT n FROM final_duration WHERE tail=1 GROUP BY 1", "SELECT event_cluster,sum(a0)::HUGEINT n FROM final_claim GROUP BY 1 HAVING sum(a0)>0", "duration/A1 favorite print counts disagree")
    _same_aggregate(con,"SELECT event_cluster,sum(a2) payoff,sum(a3) roi FROM final_duration GROUP BY 1",
                    "SELECT event_cluster,sum(a4) payoff,sum(a5) roi FROM final_claim GROUP BY 1",
                    ["event_cluster"],[],["payoff","roi"],"duration/A1 payoff and return sums disagree")
    require(scalar(con,"SELECT coalesce(sum(duration_rows),0) FROM final_claims") == duration["observations"] == claim["observations"], "claim/common-duration support mismatch")
    require(scalar(con,"SELECT count(*) FROM final_claim c LEFT JOIN final_claim_dictionary d USING(claim_fe_code) LEFT JOIN final_claims s USING(claim_id) WHERE s.claim_id IS NULL OR c.event_cluster IS DISTINCT FROM s.event_cluster") == 0,
            "claim FE dictionary/event mapping differs")
    return {"baseline_rows": int(scalar(con,"SELECT coalesce(sum(n_rows),0) FROM final_baseline")), "duration_tail_rows": duration["observations"],
            "duration_moments": duration, "claim_moments": claim,
            "native_categories": con.execute("SELECT category,n_rows FROM final_categories ORDER BY category").fetchall()}


def _daily_receipts(con, root, manifest, entries, binder):
    receipts = _receipt_index(root, manifest["daily_receipts"], binder)
    seen, totals, paths = set(), {"source_rows":0,"baseline_rows":0,"duration_rows":0,"excluded_rows":0,'sports_candidate_rows':0}, {name:[] for name in ("baseline","duration_raw","claim_raw","categories","markets","claims","sports_candidates")}
    execution={key:[] for key in EXECUTION_FIELDS}
    diagnostic_days=0
    for index, receipt in receipts:
        day = receipt.get("source_day")
        source_path = "trades_all/dt=" + str(day) + "/part-000.parquet"
        require(source_path in entries and day not in seen and index["path"] == "cells/day="+day+"/receipt.json", "daily receipt/day inventory differs")
        require(receipt.get("schema_version") == "kaushik_kalshi_day_cells_v1" and receipt.get("status") == "durable_pending_global_admission", "daily receipt contract differs")
        source_rows = count(receipt.get("source_rows"))
        require(source_rows == count(receipt.get("footer_rows")) == count(receipt.get("native_ids_appended")) and receipt.get("no_join_fanout") is True,
                "daily source/footer/locator saved proof differs")
        require(set(receipt.get("known_compound_prefixes",[])) == set(PREFIXES), "daily explicit compound exclusion rule differs")
        counts = receipt.get("exclusions",[])
        require(len({item["eligibility_reason"] for item in counts}) == len(counts) and sum(count(item["rows"]) for item in counts) == source_rows,
                "daily first-failure exclusion counts do not conserve source")
        reasons = {item["eligibility_reason"]:item["rows"] for item in counts}
        require(reasons.get("eligible",0) == receipt["baseline_rows"] and receipt["baseline_rows"]+receipt["excluded_rows"] == source_rows,
                "daily admitted/excluded counts disagree")
        require(sum(count(item["rows"]) for item in receipt["duration_exclusions"]) == receipt["baseline_rows"], "daily duration reasons do not conserve baseline")
        artifacts = receipt.get("artifacts",{})
        require(set(artifacts) == {"baseline","duration_raw","claim_raw","categories","markets","claims","exclusions","sports_candidates"}, "daily artifact inventory differs")
        for role, record in artifacts.items():
            require(record.get("path") == "day="+day+"/"+role+".parquet", "daily artifact path differs")
            path = binder.file(root / "cells",record,parquet=True)
            view(con,'audit_day_schema',[path])
            require(_schema(con,'audit_day_schema')==_analytic_schema(role),'independent daily analytic schema differs: '+role)
            _validate_saved_fields(con,'audit_day_schema',role)
            if role in ('baseline','duration_raw','claim_raw','categories','claims'):
                expected=receipt['duration_rows'] if role in ('duration_raw','claim_raw') else receipt['baseline_rows']
                analytic_rows,bad=con.execute('SELECT coalesce(sum(n_rows),0),count(*) FILTER(WHERE n_rows IS NULL OR n_rows<=0 OR n_rows>9007199254740992) FROM audit_day_schema').fetchone()
                require(analytic_rows==expected and bad==0,'per-day body print conservation/frequency differs: '+role)
            if role=='duration_raw':
                require(scalar(con,'SELECT count(*) FROM audit_day_schema WHERE trade_month IS DISTINCT FROM '+literal(day[:7]))==0,'daily duration execution-month choice differs')
            if role=='claims':
                tail_rows=scalar(con,'SELECT coalesce(sum(n_d1+n_d10),0) FROM audit_day_schema')
            if role=='sports_candidates':
                require(record['rows']==receipt.get('sports_candidate_rows'),'daily sports candidate footer/count differs')
                require(scalar(con,'SELECT count(*) FROM audit_day_schema WHERE CAST(to_timestamp(timestamp) AS DATE) IS DISTINCT FROM DATE '+literal(day))==0,
                        'sports cache timestamp/source UTC partition differs')
            con.execute('DROP VIEW audit_day_schema')
            if role in paths:
                paths[role].append(path)
        excluded = binder.file(root / "cells",artifacts["exclusions"],parquet=True)
        view(con,"day_exclusions",[excluded])
        require(scalar(con,"SELECT count(*) FROM day_exclusions") == receipt["excluded_rows"], "daily excluded physical-locator count differs")
        require(scalar(con,f"SELECT count(*) FROM day_exclusions WHERE source_ordinal IS NULL OR source_ordinal<0 OR source_ordinal>={source_rows} OR eligibility_reason IS NULL OR eligibility_reason='eligible'") == 0,
                "daily excluded physical locator invalid")
        require(scalar(con,"SELECT count(*)-count(DISTINCT source_ordinal) FROM day_exclusions") == 0, "excluded physical locators duplicated")
        observed = dict(con.execute('SELECT eligibility_reason,count(*) FROM day_exclusions GROUP BY 1').fetchall())
        require(observed == {key:value for key,value in reasons.items() if key!="eligible" and value>0}, "exclusion body/reason counts differ")
        con.execute("DROP VIEW day_exclusions")
        if any(key in receipt for key in EXECUTION_FIELDS) or 'duration_tail_exclusions' in receipt:
            require(all(key in receipt for key in EXECUTION_FIELDS) and 'duration_tail_exclusions' in receipt,'complete cohort-specific diagnostic fields required')
            tail_reasons=receipt['duration_tail_exclusions']
            require(len({item['duration_reason'] for item in tail_reasons})==len(tail_reasons)
                    and sum(count(item['rows']) for item in tail_reasons)==tail_rows
                    and {item['duration_reason']:item['rows'] for item in tail_reasons}.get('eligible',0)==receipt['duration_rows']
                    and not {'not_tail','baseline_ineligible'}&{item['duration_reason'] for item in tail_reasons},'tail-only duration diagnostic conservation differs')
            for prefix,cohort_rows in (('baseline',receipt['baseline_rows']),('duration_tail',receipt['duration_rows'])):
                low,high=(receipt[prefix+'_execution_seconds_'+suffix] for suffix in ('min','max'))
                require((low is None and high is None) if cohort_rows==0 else
                        type(low) in (int,float) and type(high) in (int,float) and low<=high,
                        'cohort-specific saved execution bounds invalid')
            for key in EXECUTION_FIELDS:
                if receipt[key] is not None:execution[key].append(receipt[key])
            diagnostic_days+=1
        for key in totals:
            totals[key] += count(receipt[key])
        seen.add(day)
    require(seen == expected_days(), "complete daily source receipts differ")
    require(diagnostic_days in (0,len(seen)),'mixed legacy/new daily diagnostics not admitted')
    if diagnostic_days:
        for key,values in execution.items():
            totals[key]=(min(values) if key.endswith('_min') else max(values)) if values else None
            require(manifest.get('sample_metadata',{}).get(key)==totals[key],'candidate/cohort-specific execution bounds differ')
    for role, values in paths.items():
        view(con,"parts_"+role,values)
    require(scalar(con,'SELECT count(*) FROM parts_sports_candidates')==totals['sports_candidate_rows']==manifest.get('sports',{}).get('candidate_rows'),
            'complete sports cache body/footer/manifest row counts differ')
    for role in ('baseline','claims','sports_candidates'):
        con.execute('CREATE TEMP VIEW '+ident('cache_'+role+'_by_day')+" AS SELECT * EXCLUDE(filename),regexp_extract(filename,'/day=([0-9]{4}-[0-9]{2}-[0-9]{2})/',1) source_day FROM read_parquet(["+
                    ','.join(literal(path) for path in paths[role])+'],filename=true,hive_partitioning=false)')
    _same_query(con,'SELECT source_day,claim_id,sum(n_rows)::BIGINT n FROM cache_claims_by_day JOIN metadata_sports USING(ticker) GROUP BY 1,2',
                'SELECT source_day,claim_id,count(*)::BIGINT n FROM cache_sports_candidates_by_day GROUP BY 1,2',
                'per-day per-claim sports cache multiplicity differs from admitted daily claims')
    con.execute("CREATE TEMP VIEW cache_sports_events AS SELECT DISTINCT event_cluster FROM metadata_sports WHERE market_reason='eligible'")
    require(scalar(con,"""SELECT count(*) FROM metadata_metadata m JOIN cache_sports_events e USING(event_cluster)
         LEFT JOIN metadata_sports s ON m.ticker=s.ticker WHERE m.market_reason='eligible' AND s.ticker IS NULL""")==0,
            'native winner-series event scope is not complete across admitted markets')
    _same_aggregate(con,'SELECT source_day,event_cluster,bin,count(*)::BIGINT n_rows,sum(P) sum_price,sum(Y) sum_outcome,sum(payoff) sum_payoff,sum(roi) sum_roi,sum(abs(payoff)) absolute_payoff,sum(abs(roi)) absolute_roi FROM cache_sports_candidates_by_day GROUP BY ALL',
                'SELECT source_day,event_cluster,bin,n_rows,sum_price,sum_outcome,sum_payoff,sum_roi FROM cache_baseline_by_day JOIN cache_sports_events USING(event_cluster)',
                ['source_day','event_cluster','bin'],['n_rows','sum_outcome'],['sum_price','sum_payoff','sum_roi'],
                'all-day sports cache/native winner-event baseline totals differ',scale_columns={'sum_payoff':'absolute_payoff','sum_roi':'absolute_roi'})
    for role, expected in (("baseline",totals["baseline_rows"]),("duration_raw",totals["duration_rows"]),("claim_raw",totals["duration_rows"]),("categories",totals["baseline_rows"]),("claims",totals["baseline_rows"])):
        require(scalar(con,"SELECT coalesce(sum(n_rows),0) FROM parts_"+role) == expected, "partial cell/support print counts disagree: "+role)
    _same_aggregate(con,"SELECT event_cluster,bin,sum(n_rows)::BIGINT n_rows,sum(sum_price) sum_price,sum(sum_outcome) sum_outcome,sum(sum_payoff) sum_payoff,sum(sum_roi) sum_roi FROM parts_baseline GROUP BY ALL",
                "SELECT * FROM final_baseline",["event_cluster","bin"],["n_rows","sum_outcome"],["sum_price","sum_payoff","sum_roi"], "final baseline differs from retained partial cells")
    fields = list(_schema(con,"parts_duration_raw"))
    columns = ",".join(ident(name) for name in fields)
    _same_query(con,"SELECT "+columns+" FROM parts_duration_raw","SELECT "+columns+" FROM audit_duration_values", "final duration partials/dictionary decoding differ")
    fields = list(_schema(con,"parts_claim_raw"))
    columns = ",".join(ident(name) for name in fields)
    _same_query(con,"SELECT "+columns+" FROM parts_claim_raw","SELECT "+columns+" FROM final_claim JOIN final_claim_dictionary USING(claim_fe_code)", "final A1 partials/dictionary decoding differ")
    _same_query(con,"SELECT DISTINCT * FROM parts_markets","SELECT * FROM final_markets", "final market support differs from all daily bodies")
    _same_query(con,"SELECT category,sum(n_rows)::BIGINT n_rows FROM parts_categories GROUP BY 1","SELECT * FROM final_categories", "final category support differs from all daily bodies")
    _same_query(con,"SELECT claim_id,ticker,taker_side,event_cluster,category,Y,sum(n_rows)::BIGINT n_rows,sum(n_d1)::BIGINT n_d1,sum(n_d10)::BIGINT n_d10,sum(duration_rows)::BIGINT duration_rows FROM parts_claims GROUP BY ALL",
                "SELECT * FROM final_claims", "final claim support differs from all daily bodies")
    bins=' '.join(f'WHEN s.P<{upper!r} THEN {index}' for index,upper in enumerate((.1,.2,.3,.4,.5,.6,.7,.8,.9),1))
    require(scalar(con,f"""SELECT count(*) FROM parts_sports_candidates s LEFT JOIN final_claims c USING(claim_id)
       LEFT JOIN metadata_sports m ON s.ticker=m.ticker WHERE c.claim_id IS NULL OR m.ticker IS NULL
       OR s.ticker IS DISTINCT FROM c.ticker OR s.event_cluster IS DISTINCT FROM c.event_cluster OR s.Y IS DISTINCT FROM c.Y OR s.sport IS DISTINCT FROM m.sport
       OR s.P IS NULL OR NOT isfinite(s.P) OR s.P<=0 OR s.P>=1 OR s.bin IS DISTINCT FROM CASE {bins} ELSE 10 END
       OR s.timestamp IS NULL OR NOT isfinite(s.timestamp) OR s.payoff IS NULL OR s.roi IS NULL OR NOT isfinite(s.payoff) OR NOT isfinite(s.roi)
       OR abs(s.payoff-100.0*(s.Y-s.P))>1e-9*greatest(1.0,abs(s.payoff))
       OR abs(s.roi-100.0*(s.Y/s.P-1.0))>1e-9*greatest(1.0,abs(s.roi))""")==0,"all saved sports print fields/payout/native mapping differ")
    return totals


def _metadata_audit(con, root, manifest, binder):
    binding = manifest.get("metadata_binding",{})
    meta_path = Path(binding.get("manifest",{}).get("path",""))
    meta, _ = read_json(meta_path,binding.get("manifest",{}).get("sha256"))
    binder.file(meta_path.parent,{**binding["manifest"],"path":meta_path.name})
    require(not (meta_path.parent/"failure.json").exists() and meta.get("status") == "metadata_complete_pending_independent_audit", "metadata stage failed/incomplete")
    metadata_source=verify_source_snapshot(meta.get("source",{}),meta.get("source",{}).get("head"))
    inventory = binder.json(meta_path.parent,meta["input_inventory"])
    entries = _manifest_entries(inventory["source_records"],"metadata",binder)
    require(len(entries) == METADATA_FILES and sum(value["rows"] for value in entries.values()) == METADATA_ROWS, "all pinned metadata files/rows required")
    proof = binder.json(meta_path.parent,meta["global_metadata_proof"])
    require(proof.get("source")==meta.get("source") and proof.get("source_manifest_sha256")==PINNED_MANIFESTS["metadata_manifest"]
            and binding.get("proof",{}).get("sha256")==meta["global_metadata_proof"]["sha256"], "metadata global proof/source/manifest binding differs")
    health = proof.get("health",{})
    require(proof.get("status") == "structural_global_metadata_keys_passed" and health.get("rows") == health.get("ticker_groups") == health.get("nonnull_ticker_groups") == METADATA_ROWS
            and health.get("duplicate_ticker_keys") == health.get("invalid_ticker_rows") == health.get("conflicting_result_keys") == health.get('conflicting_status_keys') == health.get('duplicate_extra_rows') == 0
            and set(health.get("observed_source_shards",[])) == set(health.get("expected_source_shards",[])) == set(entries)
            and len(health['observed_source_shards'])==len(health['expected_source_shards'])==len(entries), "global metadata saved exact-key proof differs")
    _stage_receipts(meta_path.parent,meta["staging_receipts"],entries,binder)
    references = inventory["source_records"]["references"]
    for key,(size,hashed) in PINNED_REFS.items():
        item = references[key]
        require((item["bytes"],item["sha256"]) == (size,hashed), "native reference source pin differs")
        path = Path(item["absolute_path"])
        binder.file(path.parent,{**item,"path":path.name})
        view(con,"native_"+key,[path])
    require(scalar(con,"SELECT count(*)-count(DISTINCT event_ticker) FROM native_events") == 0, "native event reference join fanout")
    policies = meta.get("source_version_policy",{})
    require(set(policies) == {"bulk_v2","forward","event_v1"}, "explicit metadata source-version policy inventory differs")
    for policy in policies.values():
        require(policy.get("payout_rule") == "standard_binary_venue_contract" and policy.get("notional_value_unit") == "dollars"
                and policy.get("allow_missing_or_zero_notional_under_venue_rule") is True and policy.get("settlement_ts_encoding") == "ISO8601_UTC"
                and policy.get("settlement_value_unit") is None and bool(policy.get("payout_evidence_rule")) and bool(policy.get("settlement_ts_evidence_rule")),
                "metadata source-unit/clock policy differs")
    paths = {role:[] for role in ("metadata","sports","exclusions")}
    require(len(meta.get("artifacts",{})) == 3*len(entries), "metadata saved artifact inventory differs")
    for source_path in entries:
        shard = Path(source_path).stem
        for role in paths:
            item = meta["artifacts"][role+"_"+shard]
            require(item["path"] == "artifacts/"+role+"/"+shard+".parquet", "metadata artifact mapping differs")
            paths[role].append(binder.file(meta_path.parent,item,parquet=True))
        projected=meta['artifacts']['metadata_'+shard]
        excluded=paths['exclusions'][-1]
        combo,raw=con.execute('SELECT coalesce(sum("rows") FILTER(WHERE market_reason=\'combo_excluded\'),0),coalesce(sum("rows"),0) FROM read_parquet('+literal(excluded)+',hive_partitioning=false)').fetchone()
        require(projected['rows']+combo==raw==entries[source_path]['rows'],'per-shard metadata projection/compound/footer conservation differs')
        require(scalar(con,'SELECT count(*) FROM read_parquet('+literal(paths['metadata'][-1])+',hive_partitioning=false) WHERE source_shard IS DISTINCT FROM '+literal(source_path))==0,
                'projected metadata native source shard differs')
    for role,values in paths.items():
        view(con,"metadata_"+role,values)
    require(scalar(con,"SELECT count(*)-count(DISTINCT ticker) FROM metadata_metadata") == 0, "projected metadata ticker fanout")
    require(scalar(con,'SELECT coalesce(sum("rows"),0) FROM metadata_exclusions') == METADATA_ROWS, "metadata exclusions do not conserve all raw rows")
    known = " OR ".join("starts_with(m.ticker,"+literal(prefix)+")" for prefix in PREFIXES)
    require(scalar(con,"SELECT count(*) FROM metadata_metadata m WHERE "+known) == 0, "known compounds entered metadata projection")
    raw_combo = scalar(con,'SELECT coalesce(sum("rows"),0) FROM metadata_exclusions WHERE market_reason=\'combo_excluded\'')
    require(scalar(con,"SELECT count(*) FROM metadata_metadata") + raw_combo == METADATA_ROWS, "metadata omitted noncompound/unknown rows")
    _same_query(con,'SELECT source_version,market_reason,sum("rows")::HUGEINT n FROM metadata_exclusions WHERE market_reason<>\'combo_excluded\' GROUP BY ALL',
                'SELECT source_version,market_reason,count(*)::HUGEINT n FROM metadata_metadata GROUP BY ALL', "metadata reason counts differ from projected bodies")
    require(scalar(con,"""SELECT count(*) FROM metadata_metadata m LEFT JOIN native_events e USING(event_ticker) WHERE
        m.combo_state IS DISTINCT FROM CASE WHEN e.event_ticker IS NULL THEN 'unknown' ELSE 'single' END
        OR m.category IS DISTINCT FROM coalesce(nullif(e.category,''),'Unclassified') OR m.series_ticker IS DISTINCT FROM e.series_ticker
        OR (m.market_reason='eligible' AND (e.event_ticker IS NULL OR m.market_type IS DISTINCT FROM 'binary' OR m.status IS DISTINCT FROM 'finalized'
          OR m.result IS NULL OR m.result NOT IN('yes','no') OR m.face_value_dollars IS DISTINCT FROM 1.0 OR m.unit_payoff_admitted IS NOT TRUE
          OR m.raw_notional_value IS NOT NULL AND m.raw_notional_value NOT IN(0,1) OR nullif(trim(m.raw_settlement_value),'') IS NOT NULL
          OR m.source_version NOT IN('bulk_v2','forward','event_v1')))
        OR m.combo_state='unknown' AND m.market_reason<>'unadmitted_combo_classification'
        """) == 0, "native metadata identity/category/binary/payout policy differs")
    valid_event = _opaque_identifier("m.event_ticker")
    require(scalar(con,f"""SELECT count(*) FROM metadata_metadata m WHERE
        m.event_cluster IS DISTINCT FROM CASE WHEN m.event_ticker IS NULL OR m.event_ticker='' THEN 'kalshi:market:'||m.ticker
          WHEN {valid_event} THEN 'kalshi:event:'||m.event_ticker END
        OR m.opening_seconds IS DISTINCT FROM {_utc_seconds('m.opening_raw')}
        OR m.endpoint_seconds IS DISTINCT FROM CASE WHEN m.endpoint_encoding='ISO8601_UTC' THEN {_utc_seconds('m.endpoint_raw')} END""")==0,
        "independently parsed metadata clocks/opaque clusters differ")
    con.execute("""CREATE TEMP VIEW metadata_expected_admission AS SELECT m.*,e.category raw_category,
        CASE WHEN m.source_version IN('bulk_v2','forward','event_v1') THEN
          CASE WHEN m.raw_notional_value IS NULL OR m.raw_notional_value=0 THEN 1.0
            WHEN isfinite(m.raw_notional_value) THEN m.raw_notional_value END END expected_face
        FROM metadata_metadata m LEFT JOIN native_events e USING(event_ticker)""")
    require(scalar(con,f"""SELECT count(*) FROM metadata_expected_admission m WHERE face_value_dollars IS DISTINCT FROM expected_face
       OR unit_payoff_admitted IS DISTINCT FROM coalesce(source_version IN('bulk_v2','forward','event_v1'),false)
       OR eligibility_reason IS DISTINCT FROM market_reason OR schema_version IS DISTINCT FROM 'kaushik_kalshi_normalization_v1'
       OR endpoint_encoding IS DISTINCT FROM CASE WHEN source_version IN('bulk_v2','forward','event_v1') THEN 'ISO8601_UTC' END
       OR face_value_basis IS DISTINCT FROM CASE WHEN source_version IN('bulk_v2','forward','event_v1') AND (raw_notional_value IS NULL OR raw_notional_value=0)
          THEN 'admitted_venue_standard_binary' ELSE 'observed_native_dollars' END
       OR notional_quality IS DISTINCT FROM CASE WHEN raw_notional_value IS NULL THEN 'missing'
          WHEN NOT isfinite(raw_notional_value) OR raw_notional_value<0 THEN 'invalid'
          WHEN raw_notional_value=0 THEN 'zero_sentinel_candidate'
          WHEN source_version IN('bulk_v2','forward','event_v1') AND raw_notional_value=1 THEN 'positive_unit_dollars'
          WHEN source_version IN('bulk_v2','forward','event_v1') THEN 'positive_nonunit_dollars' ELSE 'positive_unadmitted_units' END
       OR market_reason IS DISTINCT FROM CASE WHEN combo_state='unknown' THEN 'unadmitted_combo_classification'
        WHEN market_type IS DISTINCT FROM 'binary' THEN 'nonbinary_or_missing_market_type'
        WHEN status IS DISTINCT FROM 'finalized' THEN 'not_finalized'
        WHEN result IS NULL OR result NOT IN('yes','no') THEN 'nonbinary_or_missing_result'
        WHEN source_version IS NULL OR source_version NOT IN('bulk_v2','forward','event_v1') THEN 'unadmitted_unit_payout'
        WHEN expected_face IS NULL OR expected_face<>1 THEN 'missing_or_nonunit_face_value'
        WHEN nullif(trim(raw_settlement_value),'') IS NOT NULL THEN 'unadmitted_settlement_value_units'
        WHEN raw_category IS NOT NULL AND raw_category<>'' AND raw_category<>trim(raw_category) THEN 'unadmitted_native_category'
        WHEN event_ticker IS NOT NULL AND event_ticker<>'' AND NOT {valid_event} THEN 'invalid_native_event_ticker'
        ELSE 'eligible' END""")==0,"independent all-projected binary/source-unit/first-failure admission differs")
    require(scalar(con,"SELECT count(*)-count(DISTINCT ticker) FROM metadata_sports")==0,"sports metadata ticker fanout")
    columns=','.join(ident(name) for name in _schema(con,'metadata_metadata'))
    cases=' '.join('WHEN '+literal(series)+' THEN '+literal(sport) for series,sport in SPORTS_SERIES.items())
    _same_query(con,'SELECT '+columns+',sport FROM metadata_sports',
                'SELECT '+columns+',CASE series_ticker '+cases+' END sport FROM metadata_metadata WHERE combo_state=\'single\' AND series_ticker IN ('+','.join(literal(series) for series in SPORTS_SERIES)+')',
                "sports native winner whitelist/metadata lineage differs")
    health_total, health_shards, source_counts = 0,set(),{}
    for record in meta.get("health_records",[]):
        saved = binder.json(meta_path.parent,record)
        entry = saved.get("source_entry",{})
        source_path = entry.get("path")
        require(source_path in entries and source_path not in health_shards and entry == {key:entries[source_path][key] for key in ("path","size_bytes","sha256")}, "metadata health/source entry mapping differs")
        rows = entries[source_path]["rows"]
        require(saved.get("footer_rows") == saved.get("key_health",{}).get("rows") == rows
                and sum(count(item["rows"]) for item in saved.get("inspection",{}).get("source_summaries",[])) == rows,
                "metadata source/footer/health conservation differs")
        require(saved["key_health"].get("duplicate_ticker_keys") == saved["key_health"].get("invalid_ticker_rows") == 0, "metadata shard key proof differs")
        health_total += rows
        for source in saved['inspection']['source_summaries']:
            key=source.get('source_version')
            source_counts[key]=source_counts.get(key,0)+source['rows']
        health_shards.add(source_path)
    require(health_total == METADATA_ROWS and health_shards == set(entries), "complete metadata health inventory missing")
    require(source_counts==dict(con.execute('SELECT source_version,sum("rows")::BIGINT FROM metadata_exclusions GROUP BY 1').fetchall()),
            "raw metadata source-version health/exclusion counts differ")
    return {"metadata_root":str(meta_path.parent),"raw_source_rows":METADATA_ROWS,"projected_rows":scalar(con,"SELECT count(*) FROM metadata_metadata"),
            "known_compound_rows":int(raw_combo),"global_raw_key_proof_mode":"reopened_committed_builder_exact_all_80_raw_key_proof; not independently replayed raw metadata bodies",
            "manifest_binding":binding["manifest"],"builder_source":metadata_source}


def audit_saved_inputs(con, *, input_dir, candidate_sha256, expected_builder_head):
    """Fixture-accessible saved-body audit; production uses guarded main()."""
    root = Path(input_dir)
    require(root.is_absolute() and root.resolve(strict=True) == root and not (root/"failure.json").exists(), "canonical nonfailed candidate input root required")
    manifest,binding = read_json(root/"candidate_manifest.json",candidate_sha256)
    require(manifest.get("schema_version") == "kaushik_kalshi_inputs_v1" and manifest.get("status") == "inputs_complete_pending_independent_audit", "candidate input contract differs")
    source = verify_source_snapshot(manifest.get("source",{}),expected_builder_head)
    contract = manifest.get("scientific_contract",{})
    require(all(contract.get(key)==value for key,value in {"sample_scope":"all_available_dates","combos":"excluded","duration_endpoint":"settlement_ts","equal_print_weight":True,"cutoff_utc":None,"cutoff_seconds":None}.items())
            and set(contract.get("known_compound_prefixes",[]))==set(PREFIXES), "scientific contract differs")
    binder = Binder()
    inventory = binder.json(root,manifest["input_inventory"])
    entries = _manifest_entries(inventory["source_records"],"trade",binder)
    require({path.split("dt=",1)[1].split("/",1)[0] for path in entries} == expected_days(), "complete raw trade source inventory differs")
    require(inventory.get("entries") == [entries[path] for path in sorted(entries)], "candidate/original source inventory differs")
    stage = _stage_receipts(root,manifest["staging_receipts"],entries,binder)
    metadata = _metadata_audit(con,root,manifest,binder)
    final = _final_relations(con,root,manifest,binder)
    daily = _daily_receipts(con,root,manifest,entries,binder)
    counts = manifest.get("counts",{})
    require(daily["source_rows"] == stage["source_rows"] == counts.get("source_rows") and daily["baseline_rows"] == counts.get("baseline_rows") == final["baseline_rows"]
            and daily["duration_rows"] == counts.get("duration_tail_rows") == final["duration_tail_rows"]
            and daily["excluded_rows"] == counts.get("excluded_rows"), "candidate/saved body counts disagree")
    for key in ("baseline_rows","duration_tail_rows"):
        require(manifest.get("sample_metadata",{}).get(key)==counts[key], "sample/count binding differs")
    final_manifest, final_binding = read_json(root/"cells/final/manifest.json")
    binder.file(root,{**final_binding,"path":"cells/final/manifest.json"})
    require(final_manifest.get('status')=='cells_complete_pending_independent_acceptance'
            and set(final_manifest.get('days',[]))==expected_days() and len(final_manifest.get('days',[]))==len(expected_days())
            and final_manifest.get('source_rows')==counts['source_rows'] and final_manifest.get('baseline_rows')==counts['baseline_rows']
            and final_manifest.get('duration_rows')==counts['duration_tail_rows'] and final_manifest.get('excluded_rows')==counts['excluded_rows'],
            "final compact manifest/day/count contract differs")
    require(final_manifest.get('sports_candidate_rows')==daily['sports_candidate_rows'], 'final compact sports cache row total differs')
    require({role:{**item,'path':'cells/'+item['path']} for role,item in final_manifest.get('artifacts',{}).items()}==manifest['artifacts'],
            "candidate/final manifest artifact binding differs")
    identity_proof = manifest.get("identity",{})
    require(final_manifest.get("identity") == identity_proof and identity_proof.get("source_rows") == identity_proof.get("distinct_native_trade_ids") == counts["source_rows"]
            and identity_proof.get("global_trade_ids_unique") is True and "UUID" in identity_proof.get("identity",""), "saved exact global UUID proof differs")
    require(scalar(con,"SELECT count(*) FROM final_markets f LEFT JOIN metadata_metadata m USING(ticker) WHERE m.ticker IS NULL OR m.market_reason<>'eligible' OR f.event_cluster IS DISTINCT FROM m.event_cluster OR f.category IS DISTINCT FROM m.category OR f.result IS DISTINCT FROM m.result OR f.opening_seconds IS DISTINCT FROM m.opening_seconds OR f.endpoint_seconds IS DISTINCT FROM m.endpoint_seconds") == 0,
            "retained market metadata differs from admitted source projection")
    selected = [entries["trades_all/dt="+day+"/part-000.parquet"] for day in RAW_REPLAY_DAYS if "trades_all/dt="+day+"/part-000.parquet" in entries]
    sample_bytes = sum(item["size_bytes"] for item in selected)
    replay_plan = {"required_days":list(RAW_REPLAY_DAYS),"planned_source_bytes":sample_bytes,"maximum_source_bytes":RAW_REPLAY_BYTES,
                   "within_byte_bound":sample_bytes<=RAW_REPLAY_BYTES,"complete_required_day_inventory":len(selected)==len(RAW_REPLAY_DAYS)}
    checks = {key:key not in RAW_DEPENDENT for key in CHECKS}
    report = {"schema_version":"kaushik_kalshi_saved_input_audit_v1","status":"saved_proofs_passed_raw_replay_pending",
        "input_root":str(root),"candidate":binding,"builder_source":source,"checks":checks,
        "proof_modes":{"global_trade_ids_unique":"reopened_source_commit_bound_exact_global_UUID_count_proof; not an independent full-tape ID replay",
            "schema_validated":"every saved body/footer/schema reopened, independently validated analytic field structure; native source schemas checked on fixed raw days",
            "body_hashes_validated":"all final, daily, metadata, health, receipt and original manifest/reference bodies reopened",
            "source_commit_validated":"builder and metadata blobs reopened via git show their separately bound historical commits; audit executable separately pinned",
            "source_rows_reconciled":"saved source SHA/footer/stage proofs plus independent saved-body count/exclusion reconciliation",
            "claim_invariants_validated":"all final/daily claim support, payout, market/category/event/clock mappings and cross-baseline count identities reopened",
            "codes_validated":"all dictionaries unique/dense, tail-specific and decoded losslessly back to every retained partial cell",
            "sports_cache_integrity":"all saved per-day/per-claim counts/native Y and winner-event bin totals checked; raw individual price/timestamp identity replay is bounded to the fixed sample, not full tape",
            **{key:"pending required bounded raw-day replay" for key in RAW_DEPENDENT}},
        "counts":counts,"metadata":metadata,"final_cells":final,"daily_totals":daily,"staging":stage,
        "raw_replay_plan":replay_plan,"acceptance_publication":"withheld; required raw replay is not yet implemented by this saved-only phase"}
    return report,list(binder.records.values())


def _replay_prints(con,path,day,expected_rows):
    """Independent SQL oracle: no builder/oracle normalization calls are used."""
    import pyarrow as pa
    import pyarrow.parquet as pq
    footer=pq.ParquetFile(path)
    required={"trade_id":"string","ticker":"string","created_time":"string","trade_date":"date32[day]",
              "taker_side":"string","size":"double","yes_price":"double","no_price":"double","source":"string"}
    optional={"taker_book_side":"string","taker_outcome_side":"string","is_block_trade":"bool","src":"string"}
    fields={field.name:str(field.type) for field in footer.schema_arrow}
    require(len(fields)==len(footer.schema_arrow) and set(required)<=set(fields)<=set(required)|set(optional)
            and all(fields[name]==kind for name,kind in {**required,**optional}.items() if name in fields),"raw sample native schema differs")
    require(footer.metadata.num_rows==expected_rows,"raw replay/footer count differs")
    for name in ('audit_sample_prints','audit_replay_native'):
        con.execute('DROP TABLE IF EXISTS '+ident(name))
    con.execute(f"CREATE TEMP TABLE audit_replay_native AS SELECT *,file_row_number::BIGINT source_ordinal FROM read_parquet({literal(path)},file_row_number=true,hive_partitioning=false)")
    n,bad=con.execute("""SELECT count(*),count(*) FILTER(WHERE trade_id IS NULL OR try_cast(trade_id AS UUID) IS NULL
        OR encode(CAST(try_cast(trade_id AS UUID) AS VARCHAR)) IS DISTINCT FROM encode(trade_id)) FROM audit_replay_native""").fetchone()
    require(n==expected_rows and bad==0,"sample physical rows/canonical native IDs differ")
    con.execute("INSERT INTO audit_sample_ids SELECT CAST(trade_id AS UUID) FROM audit_replay_native")
    outcome="s.taker_outcome_side" if 'taker_outcome_side' in fields else 'NULL::VARCHAR'
    book="s.taker_book_side" if 'taker_book_side' in fields else 'NULL::VARCHAR'
    compounds=' OR '.join('starts_with(ticker,'+literal(prefix)+')' for prefix in PREFIXES)
    bins=' '.join(f'WHEN P<{upper!r} THEN {index}' for index,upper in enumerate((.1,.2,.3,.4,.5,.6,.7,.8,.9),1))
    con.execute(f"""CREATE TEMP TABLE audit_sample_prints AS WITH joined AS (
        SELECT s.*,m.ticker matched_ticker,m.event_cluster,m.category,m.market_reason,m.result,m.opening_seconds,m.endpoint_seconds,
          {outcome} outcome_side,{book} book_side,{_utc_seconds('s.created_time')} parsed_timestamp
        FROM audit_replay_native s LEFT JOIN audit_replay_metadata m ON s.ticker=m.ticker),
      checked AS (SELECT *,CASE
        WHEN NOT {_opaque_identifier('trade_id')} OR NOT {_opaque_identifier('ticker')} THEN 'invalid_trade_identifier'
        WHEN parsed_timestamp IS NULL THEN 'invalid_timestamp'
        WHEN trade_date IS NULL OR CAST(to_timestamp(parsed_timestamp) AS DATE)<>trade_date THEN 'invalid_or_mismatched_trade_date'
        WHEN trade_date<>DATE {literal(day)} THEN 'source_day_mismatch'
        WHEN taker_side IS NULL OR taker_side NOT IN('yes','no') THEN 'invalid_taker_side'
        WHEN outcome_side IS NOT NULL AND outcome_side NOT IN('','yes','no') THEN 'invalid_taker_outcome_side'
        WHEN outcome_side IN('yes','no') AND outcome_side<>taker_side THEN 'taker_outcome_side_disagreement'
        WHEN book_side IS NOT NULL AND book_side NOT IN('','bid','ask') THEN 'invalid_taker_book_side'
        WHEN book_side IN('bid','ask') AND (CASE WHEN book_side='bid' THEN 'yes' ELSE 'no' END)<>taker_side THEN 'taker_book_side_disagreement'
        WHEN size IS NULL OR NOT isfinite(size) OR size<=0 THEN 'invalid_size'
        WHEN yes_price IS NULL OR no_price IS NULL OR NOT isfinite(yes_price) OR NOT isfinite(no_price)
          OR yes_price<=0 OR yes_price>=1 OR no_price<=0 OR no_price>=1 THEN 'invalid_native_price'
        WHEN abs((yes_price+no_price)-1.0)>4.440892098500626e-16 THEN 'native_price_complement_disagreement'
        WHEN {compounds} THEN 'combo_excluded'
        WHEN matched_ticker IS NULL THEN 'unadmitted_market'
        WHEN market_reason<>'eligible' THEN market_reason ELSE 'eligible' END eligibility_reason,
        CASE WHEN opening_seconds IS NULL THEN 'missing_or_invalid_open_time'
          WHEN endpoint_seconds IS NULL THEN 'missing_or_invalid_settlement_ts'
          WHEN endpoint_seconds<=opening_seconds THEN 'nonpositive_lifespan'
          WHEN parsed_timestamp IS NULL OR parsed_timestamp<opening_seconds THEN 'trade_before_open_time'
          WHEN parsed_timestamp>endpoint_seconds THEN 'trade_after_settlement_ts' ELSE 'eligible' END clock_reason,
        (endpoint_seconds-opening_seconds)/86400.0 L,(endpoint_seconds-parsed_timestamp)/86400.0 R
        FROM joined), priced AS (SELECT *,eligibility_reason='eligible' baseline_eligible,
          CASE WHEN eligibility_reason='eligible' THEN CASE taker_side WHEN 'yes' THEN yes_price WHEN 'no' THEN no_price END END::DOUBLE P,
          CASE WHEN eligibility_reason='eligible' THEN (taker_side=result)::INTEGER END Y,
          CASE WHEN eligibility_reason='eligible' THEN 'kalshi:claim:'||ticker||':'||taker_side END claim_id
        FROM checked), measured AS (SELECT *,CASE WHEN baseline_eligible THEN CASE {bins} ELSE 10 END END::TINYINT bin,
          100.0*(Y-P) payoff,100.0*(Y/P-1.0) roi,strftime(trade_date,'%Y-%m') trade_month FROM priced),
      timed AS (SELECT *,CASE WHEN NOT baseline_eligible THEN 'baseline_ineligible' WHEN clock_reason<>'eligible' THEN clock_reason
          WHEN bin NOT IN(1,10) THEN 'not_tail' ELSE 'eligible' END duration_reason FROM measured)
      SELECT *,duration_reason='eligible' duration_eligible,(bin=10)::TINYINT tail,
        CASE WHEN duration_reason='eligible' THEN ln(1.0+L)/ln(2.0) END xL,
        CASE WHEN duration_reason='eligible' THEN ln(1.0+R)/ln(2.0) END xR,
        (L>1.0) lifespan_gt1,(R>1.0) remaining_gt1 FROM timed""")
    require(con.execute('SELECT count(*),count(DISTINCT source_ordinal) FROM audit_sample_prints').fetchone()==(n,n),"independent sample metadata join/physical ordinal fanout")
    require(scalar(con,"SELECT count(*) FROM audit_sample_prints WHERE baseline_eligible AND (NOT isfinite(payoff) OR NOT isfinite(roi) OR payoff IS NULL OR roi IS NULL)")==0,
            "nonfinite sample analytic outcome")
    return fields


def _direct_moments(keys,features):
    """Two-pass centered products, independently of builder COVAR_POP cells."""
    key_sql=','.join(ident(key) for key in keys)
    raw=','.join(expression+'::DOUBLE f'+str(index) for index,expression in enumerate(features))
    means=','.join('avg(f'+str(index)+') OVER(PARTITION BY '+key_sql+') u'+str(index) for index in range(len(features)))
    sums=','.join('sum(f'+str(index)+')::DOUBLE a'+str(index) for index in range(len(features)))
    cross=','.join(f'sum((f{i}-u{i})*(f{j}-u{j}))::DOUBLE c{i}_{j}' for i in range(len(features)) for j in range(i,len(features)))
    return f"WITH f AS (SELECT {key_sql},{raw} FROM audit_sample_prints WHERE duration_eligible), centered AS (SELECT *,{means} FROM f) SELECT {key_sql},count(*)::BIGINT n_rows,{sums},{cross} FROM centered GROUP BY {key_sql}"


def _compare_replay_day(con,root,day,receipt):
    artifacts=receipt['artifacts']
    for role,item in artifacts.items():
        con.execute('DROP VIEW IF EXISTS '+ident('sample_saved_'+role))
        view(con,'sample_saved_'+role,[child(root/'cells',item['path'])])
    _same_query(con,"SELECT source_ordinal,eligibility_reason FROM audit_sample_prints WHERE NOT baseline_eligible",
                "SELECT * FROM sample_saved_exclusions","raw replay physical-ordinal admitted/excluded sets disagree")
    counts=con.execute("SELECT count(*),count(*) FILTER(WHERE baseline_eligible),count(*) FILTER(WHERE duration_eligible) FROM audit_sample_prints").fetchone()
    require(counts==(receipt['source_rows'],receipt['baseline_rows'],receipt['duration_rows']),"raw replay source/baseline/duration membership counts disagree")
    reasons=dict(con.execute("SELECT eligibility_reason,count(*) FROM audit_sample_prints GROUP BY 1").fetchall())
    duration=dict(con.execute("SELECT duration_reason,count(*) FROM audit_sample_prints WHERE baseline_eligible GROUP BY 1").fetchall())
    require(reasons=={item['eligibility_reason']:item['rows'] for item in receipt['exclusions']}
            and duration=={item['duration_reason']:item['rows'] for item in receipt['duration_exclusions']},"raw replay ordered baseline/duration exclusion reasons disagree")
    if any(key in receipt for key in EXECUTION_FIELDS):
        bounds=con.execute('SELECT min(parsed_timestamp) FILTER(WHERE baseline_eligible),max(parsed_timestamp) FILTER(WHERE baseline_eligible),min(parsed_timestamp) FILTER(WHERE duration_eligible),max(parsed_timestamp) FILTER(WHERE duration_eligible) FROM audit_sample_prints').fetchone()
        require(all(receipt.get(key)==value for key,value in zip(EXECUTION_FIELDS,bounds)),'raw replay exact cohort execution extrema disagree')
    if 'duration_tail_exclusions' in receipt:
        tail_reasons=dict(con.execute('SELECT duration_reason,count(*) FROM audit_sample_prints WHERE baseline_eligible AND bin IN(1,10) GROUP BY 1').fetchall())
        require(tail_reasons=={item['duration_reason']:item['rows'] for item in receipt['duration_tail_exclusions']},'raw replay tail-only duration diagnostic reasons disagree')
    _same_aggregate(con,"SELECT event_cluster,bin,count(*)::BIGINT n_rows,sum(P) sum_price,sum(Y) sum_outcome,sum(payoff) sum_payoff,sum(roi) sum_roi FROM audit_sample_prints WHERE baseline_eligible GROUP BY ALL",
                    "SELECT * FROM sample_saved_baseline",['event_cluster','bin'],['n_rows','sum_outcome'],['sum_price','sum_payoff','sum_roi'],"raw replay direct baseline sums disagree")
    for role,keys,features in (
        ('duration_raw',['event_cluster','tail','category','P','trade_month','lifespan_gt1','remaining_gt1'],['xL','xR','payoff','roi']),
        ('claim_raw',['event_cluster','claim_id'],['(bin=10)::DOUBLE','(bin=10)::DOUBLE*xL','xR','(bin=10)::DOUBLE*xR','payoff','roi'])):
        width=len(features)
        floats=['a'+str(i) for i in range(width)]+[f'c{i}_{j}' for i in range(width) for j in range(i,width)]
        _same_aggregate(con,_direct_moments(keys,features),'SELECT * FROM sample_saved_'+role,keys,['n_rows'],floats,"raw replay direct centered moments disagree: "+role)
    _same_query(con,"SELECT category,count(*)::BIGINT n_rows FROM audit_sample_prints WHERE baseline_eligible GROUP BY 1","SELECT * FROM sample_saved_categories","raw replay native category counts disagree")
    _same_query(con,"SELECT DISTINCT ticker,event_cluster,category,result,opening_seconds,endpoint_seconds FROM audit_sample_prints WHERE baseline_eligible",
                "SELECT * FROM sample_saved_markets","raw replay market metadata/clock choices disagree")
    _same_query(con,"SELECT claim_id,ticker,taker_side,event_cluster,category,Y,count(*)::BIGINT n_rows,count(*) FILTER(WHERE bin=1)::BIGINT n_d1,count(*) FILTER(WHERE bin=10)::BIGINT n_d10,count(*) FILTER(WHERE duration_eligible)::BIGINT duration_rows FROM audit_sample_prints WHERE baseline_eligible GROUP BY ALL",
                "SELECT * FROM sample_saved_claims","raw replay claim/tail/payout/support membership disagrees")
    _same_query(con,'SELECT s.ticker,s.claim_id,s.event_cluster,w.sport,s.parsed_timestamp AS "timestamp",s.P,s.Y,s.payoff,s.roi,s.bin FROM audit_sample_prints s JOIN audit_replay_winners w USING(ticker) WHERE baseline_eligible',
                "SELECT * FROM sample_saved_sports_candidates","raw replay explicit winner candidate choices disagree")
    return {"source_rows":counts[0],"baseline_rows":counts[1],"duration_rows":counts[2],"excluded_rows":counts[0]-counts[1],
            "reason_counts":reasons,"duration_reason_counts":duration,
            "physical_locator_mode":"every source ordinal; admitted set is exact complement of saved excluded ordinals",
            "analytic_choice_mode":"independent per-print choices saved; candidate choices checked via exact grouping keys, support sets and direct means/centered products",
            "candidate_per_print_analytic_fields_persisted":False}


def _sample_cache(con):
    con.execute("CREATE TEMP TABLE audit_replay_metadata AS SELECT ticker,event_cluster,category,market_reason,opening_seconds,endpoint_seconds,result FROM metadata_metadata")
    require(scalar(con,'SELECT count(*)-count(DISTINCT ticker) FROM audit_replay_metadata')==0,"replay metadata cache join fanout")
    con.execute('CREATE TEMP TABLE audit_sample_ids(trade_id UUID NOT NULL)')
    cases=' '.join('WHEN '+literal(series)+' THEN '+literal(sport) for series,sport in SPORTS_SERIES.items())
    con.execute("CREATE TEMP TABLE audit_replay_winners AS SELECT ticker,CASE series_ticker "+cases+" END sport FROM metadata_metadata WHERE combo_state='single' AND series_ticker IN ("+','.join(literal(value) for value in SPORTS_SERIES)+")")
    require(scalar(con,'SELECT count(*)-count(DISTINCT ticker) FROM audit_replay_winners')==0,"replay winner mapping fanout")


def _stage_sample(staging,selected,target,process):
    """One source per owned subrun; reserve only the still-unspent output cap."""
    samples=target/'raw_sources';samples.mkdir()
    receipts=[]
    for index,entry in enumerate(selected):
        run=samples/f'{index:02d}';run.mkdir()
        remaining=max(0,OUTPUT_LIMIT-_used_output(target))
        receipts.extend(staging.stage_and_process([entry],remote_root='dropbox:Polymarket Data and Code/kalshi',run_dir=run,
            output_spill_reserve_bytes=remaining+4_000_000_000,process=process))
    return receipts


def replay_inputs(con,*,input_dir,audit_dir,report,bindings):
    """Stage precisely the fixed eight pinned days, serially, in a new audit run."""
    from analysis.kaushik_kalshi_replication import staging
    root,target=Path(input_dir),Path(audit_dir)
    require(report['status']=='saved_proofs_passed_raw_replay_pending',"saved audit prerequisite missing")
    manifest,_=read_json(root/'candidate_manifest.json',report['candidate']['sha256'])
    binder=Binder()
    inventory=binder.json(root,manifest['input_inventory'])
    entries=_manifest_entries(inventory['source_records'],'trade',binder)
    selected=[]
    for day in RAW_REPLAY_DAYS:
        item=entries.get('trades_all/dt='+day+'/part-000.parquet')
        require(item is not None,"required raw replay day unavailable; no reduced sample admitted")
        selected.append(staging.ManifestEntry(**item))
    require(sum(item.size_bytes for item in selected)<=RAW_REPLAY_BYTES,"fixed raw replay sample exceeds 2GB; explicit new authority required")
    _sample_cache(con)
    expected_sources={item.path:item for item in selected}
    completed=[]

    def process(path,entry):
        day=entry.path.split('dt=',1)[1].split('/',1)[0]
        require(entry==expected_sources.get(entry.path),"raw replay unpinned source")
        receipt,_=read_json(root/('cells/day='+day+'/receipt.json'))
        fields=_replay_prints(con,path,day,receipt['source_rows'])
        proof=_compare_replay_day(con,root,day,receipt)
        folder=target/'replay'/('day='+day)
        folder.mkdir(parents=True)
        chosen=('source_ordinal','ticker','claim_id','event_cluster','category','eligibility_reason','duration_reason','P','Y','bin',
                'opening_seconds','endpoint_seconds','parsed_timestamp','L','R','xL','xR','lifespan_gt1','remaining_gt1')
        prints=_write_parquet(con,target,folder/'prints.parquet','SELECT '+','.join(ident(name) for name in chosen)+' FROM audit_sample_prints')
        binder.file(target,prints,parquet=True)
        proof.update(source_day=day,source_entry={'path':entry.path,'size_bytes':entry.size_bytes,'sha256':entry.sha256},
                     source_footer_rows=receipt['source_rows'],source_schema=fields,independent_prints=prints)
        proof_path=folder/'proof.json'
        _write_json(proof_path,proof)
        record=_record(target,proof_path)
        binder.json(target,record)
        completed.append(record)
        con.execute('DROP TABLE audit_sample_prints');con.execute('DROP TABLE audit_replay_native')
        return {'independent_replay_proof':record,'source_rows':proof['source_rows']}

    receipts=_stage_sample(staging,selected,target,process)
    receipt_records=[_record(target,Path(path)) for path in receipts]
    index_path=target/'raw_staging_receipts.json'
    _write_json(index_path,{'schema_version':'kaushik_kalshi_receipt_inventory_v1','receipts':receipt_records})
    index_record=_record(target,index_path)
    observed=set()
    for _,value in _receipt_index(target,index_record,binder):
        entry=value.get('entry',{});path=entry.get('path')
        require(value.get('status')=='processed_verified' and value.get('schema_version')=='kalshi_source_stage_receipt_v1'
                and path in expected_sources and path not in observed and entry=={'path':path,'size_bytes':expected_sources[path].size_bytes,'sha256':expected_sources[path].sha256}
                and value.get('remote')=='dropbox:Polymarket Data and Code/kalshi/'+path
                and value.get('verified_identity',{}).get('bytes')==entry['size_bytes'],"raw replay verified staging chain differs")
        proof=binder.json(target,value.get('callback_provenance',{}).get('independent_replay_proof',{}))
        require(proof.get('source_entry')==entry,"raw replay source/proof binding differs")
        observed.add(path)
    require(observed==set(expected_sources) and len(completed)==len(RAW_REPLAY_DAYS),"complete fixed raw replay receipt inventory missing")
    n,unique=con.execute('SELECT count(*),count(DISTINCT trade_id) FROM audit_sample_ids').fetchone()
    require(n==unique and n==sum(read_json(target/item['path'])[0]['source_rows'] for item in completed),"independent sample native-ID uniqueness/source count differs")
    for name in ('audit_sample_ids','audit_replay_metadata','audit_replay_winners'):
        con.execute('DROP TABLE '+ident(name))
    bindings.extend(binder.records.values())
    # Repeat bindings are deliberately identical; publication rejects conflicts.
    unique_bindings={}
    for item in bindings:
        require(item['path'] not in unique_bindings or unique_bindings[item['path']]==item,"audit/replay body binding conflict")
        unique_bindings[item['path']]=item
    report.update(status='all_input_proofs_passed_pending_acceptance',checks={key:True for key in CHECKS},
        raw_replay={'required_days':list(RAW_REPLAY_DAYS),'source_bytes':sum(item.size_bytes for item in selected),'maximum_source_bytes':RAW_REPLAY_BYTES,
            'proofs':completed,'staging_receipts':index_record,'source_rows':n,'distinct_sample_native_ids':unique,
            'oracle':'independent SQL normalization and two-pass centered products; never builder annotate_day/process_day',
            'sample_scope':'exact fixed eight native days; not an independent full-archive replay'},
        acceptance_publication='withheld until separate publish command reopens all bound bodies')
    for key in RAW_DEPENDENT:report['proof_modes'][key]='independent fixed-day raw replay plus complete saved-body/source-bound builder proofs'
    return report,list(unique_bindings.values())


def _probe_queries():
    return {
        'baseline':"SELECT event_cluster,bin,count(*)::BIGINT n_rows,sum(P)::DOUBLE sum_price,sum(Y)::DOUBLE sum_outcome,sum(payoff)::DOUBLE sum_payoff,sum(roi)::DOUBLE sum_roi FROM audit_sample_prints WHERE baseline_eligible GROUP BY ALL",
        'duration_raw':_direct_moments(['event_cluster','tail','category','P','trade_month','lifespan_gt1','remaining_gt1'],['xL','xR','payoff','roi']),
        'claim_raw':_direct_moments(['event_cluster','claim_id'],['(bin=10)::DOUBLE','(bin=10)::DOUBLE*xL','xR','(bin=10)::DOUBLE*xR','payoff','roi']),
        'categories':"SELECT category,count(*)::BIGINT n_rows FROM audit_sample_prints WHERE baseline_eligible GROUP BY 1",
        'markets':"SELECT DISTINCT ticker,event_cluster,category,result,opening_seconds,endpoint_seconds FROM audit_sample_prints WHERE baseline_eligible",
        'claims':"SELECT claim_id,ticker,taker_side,event_cluster,category,Y,count(*)::BIGINT n_rows,count(*) FILTER(WHERE bin=1)::BIGINT n_d1,count(*) FILTER(WHERE bin=10)::BIGINT n_d10,count(*) FILTER(WHERE duration_eligible)::BIGINT duration_rows FROM audit_sample_prints WHERE baseline_eligible GROUP BY ALL",
        'exclusions':"SELECT source_ordinal,eligibility_reason FROM audit_sample_prints WHERE NOT baseline_eligible",
        'sports_candidates':'SELECT s.ticker,s.claim_id,s.event_cluster,w.sport,s.parsed_timestamp AS "timestamp",s.P,s.Y,s.payoff,s.roi,s.bin FROM audit_sample_prints s JOIN audit_replay_winners w USING(ticker) WHERE baseline_eligible'}


def probe_inputs(con,*,metadata_dir,metadata_manifest_sha256,expected_builder_head,audit_dir):
    """Capacity evidence only: actual same-shaped cells, never input admission."""
    from analysis.kaushik_kalshi_replication import staging
    import pyarrow.parquet as pq
    target,meta_root=Path(audit_dir),Path(metadata_dir)
    meta,meta_record=read_json(meta_root/'candidate_manifest.json',metadata_manifest_sha256)
    verify_source_snapshot(meta.get('source',{}),expected_builder_head)
    binder=Binder()
    metadata=_metadata_audit(con,None,{'metadata_binding':{'manifest':meta_record,'proof':meta['global_metadata_proof']}},binder)
    inventory=binder.json(meta_root,meta['input_inventory'])
    entries=_manifest_entries(inventory['source_records'],'trade',binder)
    require({path.split('dt=',1)[1].split('/',1)[0] for path in entries}==expected_days(),"probe complete pinned trade-day inventory required")
    selected=[]
    for day in RAW_REPLAY_DAYS:
        require('trades_all/dt='+day+'/part-000.parquet' in entries,"probe requires every fixed raw day")
        selected.append(staging.ManifestEntry(**entries['trades_all/dt='+day+'/part-000.parquet']))
    require(sum(item.size_bytes for item in selected)<=RAW_REPLAY_BYTES,"fixed probe sources exceed 2GB")
    _sample_cache(con)
    completed=[]
    paths={role:[] for role in _probe_queries()}

    def process(path,entry):
        day=entry.path.split('dt=',1)[1].split('/',1)[0]
        rows=pq.ParquetFile(path).metadata.num_rows
        fields=_replay_prints(con,path,day,rows)
        folder=target/'probe'/('day='+day)
        folder.mkdir(parents=True)
        artifacts={}
        for role,query in _probe_queries().items():
            item=_write_parquet(con,target,folder/(role+'.parquet'),query)
            binder.file(target,item,parquet=True)
            artifacts[role]=item
            paths[role].append(target/item['path'])
        _,baseline,duration=con.execute('SELECT count(*),count(*) FILTER(WHERE baseline_eligible),count(*) FILTER(WHERE duration_eligible) FROM audit_sample_prints').fetchone()
        proof={'source_day':day,'source_entry':{'path':entry.path,'size_bytes':entry.size_bytes,'sha256':entry.sha256},
               'source_rows':rows,'source_footer_rows':rows,'baseline_rows':baseline,'duration_rows':duration,'excluded_rows':rows-baseline,
               'source_schema':fields,'artifacts':artifacts,'actual_daily_cell_bytes':sum(item['bytes'] for item in artifacts.values())}
        proof_path=folder/'proof.json';_write_json(proof_path,proof)
        record=_record(target,proof_path);binder.json(target,record);completed.append(proof)
        con.execute('DROP TABLE audit_sample_prints');con.execute('DROP TABLE audit_replay_native')
        return {'independent_probe_proof':record,'source_rows':rows}

    receipts=_stage_sample(staging,selected,target,process)
    expected={item.path:item for item in selected};observed=set()
    for path in receipts:
        record=_record(target,Path(path));value=binder.json(target,record)
        entry=value.get('entry',{});source_path=entry.get('path')
        require(source_path in expected and source_path not in observed and value.get('status')=='processed_verified'
                and value.get('schema_version')=='kalshi_source_stage_receipt_v1'
                and entry=={'path':source_path,'size_bytes':expected[source_path].size_bytes,'sha256':expected[source_path].sha256}
                and value.get('remote')=='dropbox:Polymarket Data and Code/kalshi/'+source_path
                and value.get('verified_identity',{}).get('bytes')==entry['size_bytes'],"probe staging receipt/source verification differs")
        proof=binder.json(target,value.get('callback_provenance',{}).get('independent_probe_proof',{}))
        require(proof.get('source_entry')==entry,"probe source/proof binding differs")
        observed.add(source_path)
    require(observed==set(expected) and len(completed)==len(RAW_REPLAY_DAYS),"probe fixed day inventory incomplete")
    for role,values in paths.items():view(con,'probe_parts_'+role,values)
    for family,field in (('cat','category'),('price','P'),('month','trade_month')):
        con.execute(f"CREATE TEMP TABLE probe_{family}_dictionary AS SELECT tail,{ident(field)},(row_number() OVER(ORDER BY tail,{ident(field)})-1)::BIGINT {family}_code FROM (SELECT DISTINCT tail,{ident(field)} FROM probe_parts_duration_raw)")
    con.execute('CREATE TEMP TABLE probe_claim_dictionary AS SELECT claim_id,(row_number() OVER(ORDER BY claim_id)-1)::BIGINT claim_fe_code FROM (SELECT DISTINCT claim_id FROM probe_parts_claim_raw)')
    queries={
        'baseline':'SELECT event_cluster,bin,sum(n_rows)::BIGINT n_rows,sum(sum_price)::DOUBLE sum_price,sum(sum_outcome)::DOUBLE sum_outcome,sum(sum_payoff)::DOUBLE sum_payoff,sum(sum_roi)::DOUBLE sum_roi FROM probe_parts_baseline GROUP BY ALL',
        'duration':'SELECT d.* EXCLUDE(category,P,trade_month),c.cat_code,p.price_code,m.month_code FROM probe_parts_duration_raw d JOIN probe_cat_dictionary c USING(tail,category) JOIN probe_price_dictionary p USING(tail,P) JOIN probe_month_dictionary m USING(tail,trade_month)',
        'claim':'SELECT d.* EXCLUDE(claim_id),c.claim_fe_code FROM probe_parts_claim_raw d JOIN probe_claim_dictionary c USING(claim_id)',
        'categories':'SELECT category,sum(n_rows)::BIGINT n_rows FROM probe_parts_categories GROUP BY 1',
        'markets':'SELECT DISTINCT * FROM probe_parts_markets',
        'claims':'SELECT claim_id,ticker,taker_side,event_cluster,category,Y,sum(n_rows)::BIGINT n_rows,sum(n_d1)::BIGINT n_d1,sum(n_d10)::BIGINT n_d10,sum(duration_rows)::BIGINT duration_rows FROM probe_parts_claims GROUP BY ALL',
        **{family+'_dictionary':'SELECT * FROM probe_'+family+'_dictionary' for family in ('cat','price','month','claim')}}
    final_folder=target/'probe'/'final';final_folder.mkdir()
    final={}
    for role,query in queries.items():
        item=_write_parquet(con,target,final_folder/(role+'.parquet'),query);binder.file(target,item,parquet=True);final[role]=item
    rows,unique=con.execute('SELECT count(*),count(DISTINCT trade_id) FROM audit_sample_ids').fetchone()
    require(rows==unique==sum(item['source_rows'] for item in completed),"probe independent sample native-ID/source count differs")
    daily_bytes=sum(item['actual_daily_cell_bytes'] for item in completed)
    final_bytes=sum(item['bytes'] for item in final.values())
    return {'schema_version':'kaushik_kalshi_capacity_probe_v1','status':'capacity_probe_only_no_input_acceptance',
        'metadata':metadata,'fixed_days':list(RAW_REPLAY_DAYS),'actual_source_bytes':sum(item.size_bytes for item in selected),
        'actual_source_rows':rows,'distinct_sample_native_ids':unique,'days':completed,'final_sample_artifacts':final,
        'actual_daily_cell_bytes':daily_bytes,'actual_final_cell_bytes':final_bytes,'actual_combined_cell_bytes':daily_bytes+final_bytes,
        'provisional_extrapolation':{'upstream_stated_full_prints':1_610_423_776,'denominator_source':'upstream README stated native count; NOT independently verified',
            'daily_cell_bytes_linear':daily_bytes/rows*1_610_423_776 if rows else None,
            'final_cell_bytes_linear':final_bytes/rows*1_610_423_776 if rows else None,
            'combined_cell_bytes_linear':(daily_bytes+final_bytes)/rows*1_610_423_776 if rows else None,
            'input_ceiling_bytes':12_000_000_000,'not_capacity_admission':True,
            'limitations':'composition-sensitive8-day linear scenario; not a guaranteed whole-tape bound; final key reuse/dictionaries and compression can differ; excludes input-stage JSON/receipt overhead'},
        'acceptance_publication':'forbidden for capacity probes; this is not a completed production input run'},list(binder.records.values())


def _write_json(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+"\n").encode()
    require(len(raw)<=JSON_LIMIT,"audit JSON exceeds bound")
    with Path(path).open("xb") as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    _fsync_directory(Path(path).parent)


def _fsync_directory(path):
    descriptor=os.open(path,os.O_RDONLY)
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def _audit_source(expected_head):
    production_guard.require_production_host()
    require(Path(sys.executable)==Path('/home/ubuntu/venv/bin/python'),"canonical production Python required")
    require(re.fullmatch(r"[0-9a-f]{40}",expected_head or ""),"explicit audit commit required")
    actual=subprocess.check_output(['git','rev-parse','HEAD'],cwd=REPO,text=True).strip()
    require(actual==expected_head,"audit source HEAD differs")
    own=('scripts/audit_kaushik_kalshi_inputs.py','tests/test_kaushik_kalshi_input_audit.py','production_guard.py',
         'analysis/kaushik_kalshi_replication/staging.py')
    for relative in own:
        blob=subprocess.check_output(['git','show',actual+':'+relative],cwd=REPO)
        require(hashlib.sha256(blob).hexdigest()==digest(REPO/relative),"audit source is not committed/unchanged")
    return actual


def production_saved(args):
    actual=_audit_source(args.expected_audit_head)
    target=Path(args.output_dir)
    require(target.is_absolute() and target.resolve()==target and target.parent==RUN_ROOT and not os.path.lexists(target),"new top-level audit output directory required")
    require(shutil.disk_usage(target.parent).free>=34_000_000_000,"audit output8GB/sample2GB/spill4GB/free20GB reservation failed")
    available=re.search(r'^MemAvailable:\s+(\d+) kB$',Path('/proc/meminfo').read_text(),re.MULTILINE)
    require(available and int(available.group(1))*1024>=240_000_000_000 and (os.cpu_count() or 0)>=4,"audit RAM/thread admission failed")
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq
    target.mkdir(mode=0o700)
    con=None
    try:
        con=duckdb.connect()
        for sql in ("SET memory_limit='192GB'","SET threads=4","SET TimeZone='UTC'","SET preserve_insertion_order=true",
                    "SET max_temp_directory_size='4000000000B'","SET temp_directory="+literal(target/'spill')):
            con.execute(sql)
        if args.command=='probe':
            report,bindings=probe_inputs(con,metadata_dir=args.metadata_dir,metadata_manifest_sha256=args.metadata_manifest_sha256,
                expected_builder_head=args.expected_builder_head,audit_dir=target)
        else:
            report,bindings=audit_saved_inputs(con,input_dir=args.input_dir,candidate_sha256=args.candidate_sha256,expected_builder_head=args.expected_builder_head)
            if args.command=='replay':
                report,bindings=replay_inputs(con,input_dir=args.input_dir,audit_dir=target,report=report,bindings=bindings)
        body_path=target/'body_bindings.parquet'
        pq.write_table(pa.Table.from_pylist(bindings),body_path,compression='zstd')
        with body_path.open('rb') as stream:os.fsync(stream.fileno())
        report.update(audit_source_head=actual,body_bindings=_record(target,body_path,parquet=True),
            resources={"duckdb_memory_bytes":192_000_000_000,"threads":4,"spill_bytes":4_000_000_000,"maximum_audit_output_bytes":8_000_000_000,"free_floor_bytes":20_000_000_000})
        _write_json(target/'audit_candidate.json',report)
        require(sum(path.stat().st_size for path in target.rglob('*') if path.is_file() and path.relative_to(target).parts[0]!='spill')<=8_000_000_000,"audit persistent output ceiling exceeded")
        require(shutil.disk_usage(target).free>=20_000_000_000,"audit final free-space floor failed")
        return report
    except BaseException as error:
        if not (target/'failure.json').exists():
            _write_json(target/'failure.json',{"status":"audit_incomplete","error_type":type(error).__name__,"reason":str(error)[:4096]})
        raise
    finally:
        if con is not None:con.close()


def publish_acceptance(*,input_dir,candidate_sha256,audit_dir,audit_sha256,expected_audit_head):
    """Publication commit point is acceptance.json, created last and never replaced."""
    root,audit_root=Path(input_dir),Path(audit_dir)
    require(root.is_absolute() and root.resolve(strict=True)==root and audit_root.is_absolute()
            and audit_root.resolve(strict=True)==audit_root,"canonical input/audit roots required")
    require(not any(os.path.lexists(root/name) for name in ('manifest.json','acceptance.json','failure.json'))
            and not (audit_root/'failure.json').exists(),"accepted/failed input or audit root cannot be overwritten")
    report,report_binding=read_json(audit_root/'audit_candidate.json',audit_sha256)
    candidate,candidate_binding=read_json(root/'candidate_manifest.json',candidate_sha256)
    require(report.get('status')=='all_input_proofs_passed_pending_acceptance' and report.get('input_root')==str(root)
            and report.get('candidate')==candidate_binding and report.get('audit_source_head')==expected_audit_head,
            "explicit candidate/audit/source binding differs")
    require(set(report.get('checks',{}))==set(CHECKS) and all(report['checks'][key] is True for key in CHECKS)
            and all(isinstance(report.get('proof_modes',{}).get(key),str) and report['proof_modes'][key] for key in CHECKS),
            "all literal acceptance gates with explicit proof modes required")
    raw=report.get('raw_replay',{})
    require(raw.get('required_days')==list(RAW_REPLAY_DAYS) and len(raw.get('proofs',[]))==len(RAW_REPLAY_DAYS)
            and type(raw.get('source_bytes')) is int and 0<raw['source_bytes']<=RAW_REPLAY_BYTES
            and raw.get('maximum_source_bytes')==RAW_REPLAY_BYTES and raw.get('source_rows')==raw.get('distinct_sample_native_ids'),
            "complete bounded raw replay proof required")
    import pyarrow.parquet as pq
    binder=Binder()
    bindings=binder.file(audit_root,report.get('body_bindings',{}),parquet=True)
    require(bindings.stat().st_size<=16_000_000 and pq.ParquetFile(bindings).metadata.num_rows<=100_000,"bounded explicit audit body inventory required")
    seen=set()
    for batch in pq.ParquetFile(bindings).iter_batches(batch_size=1024):
        for item in batch.to_pylist():
            path=Path(item['path'])
            require(path.is_absolute() and path.resolve(strict=True)==path and path.is_file() and not path.is_symlink()
                    and str(path) not in seen,"canonical unique audit body path required")
            before=identity(path)
            require(path.stat().st_size==item['bytes'] and digest(path)==item['sha256'],"input/audit body drift before publication")
            if item['rows'] is not None:
                footer=pq.ParquetFile(path)
                require(footer.metadata.num_rows==item['rows'] and hashlib.sha256(str(footer.schema_arrow).encode()).hexdigest()==item['schema_sha256'],"publication footer/schema drift")
            require(before==identity(path),"publication body changed while reopening")
            seen.add(str(path))
    require(str(root/'input_inventory.json') in seen and all(str(root/item['path']) in seen for item in candidate['artifacts'].values()),
            "audit body inventory omits final input bodies")
    observed_days=set()
    for item in raw['proofs']:
        require(str(child(audit_root,item['path'])) in seen,"raw replay proof was not body-bound")
        proof=binder.json(audit_root,item)
        require(proof.get('source_day') in RAW_REPLAY_DAYS and proof['source_day'] not in observed_days
                and proof.get('source_rows')==proof.get('source_footer_rows'),"raw day proof inventory/count differs")
        prints=proof.get('independent_prints',{})
        require(str(child(audit_root,prints['path'])) in seen,"independent per-print replay body was not bound")
        binder.file(audit_root,prints,parquet=True)
        observed_days.add(proof['source_day'])
    require(observed_days==set(RAW_REPLAY_DAYS),"all required raw days must pass")
    require(candidate.get('status')=='inputs_complete_pending_independent_audit' and candidate.get('schema_version')=='kaushik_kalshi_inputs_v1',"pending candidate required")
    require(all(key in candidate.get('sample_metadata',{}) for key in EXECUTION_FIELDS),
            'acceptance requires cohort-specific execution diagnostics; legacy archive extrema cannot substitute')
    accepted={**candidate,'status':'accepted','independent_audit':{'path':str(audit_root/'audit_candidate.json'),
        'sha256':report_binding['sha256'],'bytes':report_binding['bytes'],'audit_source_head':expected_audit_head,
        'candidate_sha256':candidate_sha256,'proof_modes':report['proof_modes']}}
    require(shutil.disk_usage(root).free>=20_000_000_000,"publication free-space floor failed")
    _write_json(root/'manifest.json',accepted)
    acceptance={'schema_version':'kaushik_kalshi_inputs_acceptance_v1','status':'accepted','manifest_sha256':digest(root/'manifest.json'),
        'candidate_sha256':candidate_sha256,'audit_sha256':audit_sha256,'audit_source_head':expected_audit_head,
        'checks':report['checks'],'proof_modes':report['proof_modes']}
    _write_json(root/'acceptance.json',acceptance)
    return acceptance


def parser():
    result=argparse.ArgumentParser(description=__doc__)
    commands=result.add_subparsers(dest='command',required=True)
    for name in ('saved','replay'):
        saved=commands.add_parser(name,help='Audit saved proofs'+('; replay fixed8 raw days; acceptance still separate' if name=='replay' else '; acceptance withheld'))
        saved.add_argument('--input-dir',type=Path,required=True)
        saved.add_argument('--candidate-sha256',required=True)
        saved.add_argument('--expected-builder-head',required=True)
        saved.add_argument('--expected-audit-head',required=True)
        saved.add_argument('--output-dir',type=Path,required=True)
    publish=commands.add_parser('publish',help='Reopen fully bound successful audit and publish NEW acceptance files')
    for name in ('input-dir','audit-dir'):
        publish.add_argument('--'+name,type=Path,required=True)
    for name in ('candidate-sha256','audit-sha256','expected-audit-head'):
        publish.add_argument('--'+name,required=True)
    probe=commands.add_parser('probe',help='Independent fixed8-day actual cell-size probe, never input acceptance')
    probe.add_argument('--metadata-dir',type=Path,required=True)
    probe.add_argument('--metadata-manifest-sha256',required=True)
    probe.add_argument('--expected-builder-head',required=True)
    probe.add_argument('--expected-audit-head',required=True)
    probe.add_argument('--output-dir',type=Path,required=True)
    return result


def main(argv=None):
    args=parser().parse_args(argv)
    if args.command=='publish':
        _audit_source(args.expected_audit_head)
        return publish_acceptance(input_dir=args.input_dir,candidate_sha256=args.candidate_sha256,audit_dir=args.audit_dir,
            audit_sha256=args.audit_sha256,expected_audit_head=args.expected_audit_head)
    return production_saved(args)


if __name__=='__main__':main()
