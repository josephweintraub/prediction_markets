"""Vectorized native daily prints to bounded, lossless sufficient-cell artifacts.

The caller owns production-host/source-hash/resource admission and serial source
staging. Nothing here fetches data or starts production. Local calls are intended
for synthetic fixtures. Day receipts remain pending global ID/code admission;
source staging may release its own verified copy only after this callback returns.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import re
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

from analysis.kaushik_kalshi_replication import inputs as oracle
from analysis.kaushik_polymarket_replication import build_inputs as io


class BuildBlocked(ValueError):
    pass


def require(value, message):
    if not value:
        raise BuildBlocked(message)


@dataclass(frozen=True)
class BuildLimits:
    output_bytes: int = 12_000_000_000
    artifact_bytes: int = 2_000_000_000
    transient_bytes: int = 4_000_000_000
    spill_bytes: int = 4_000_000_000
    minimum_free_bytes: int = 20_000_000_000
    json_bytes: int = 1_048_576

    def validate(self):
        require(all(type(value) is int and value > 0 for value in asdict(self).values()),
                "positive integer build resource limits required")
        require(self.minimum_free_bytes >= 20_000_000_000, "20GB free-space floor cannot be relaxed")
        require(self.json_bytes <= self.artifact_bytes <= self.transient_bytes <= self.output_bytes,
                "build resource ceilings disagree")
        return self


def _relation(value):
    require(isinstance(value, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value),
            "simple SQL relation identifier required")
    return io.ident(value)


def initialize_ids(con):
    """Create the one lossless global table; refuse reuse or implicit resume."""
    con.execute("CREATE TEMP TABLE native_ids(trade_id UUID NOT NULL)")


_DAILY_CACHE_OWNER = "kaushik_kalshi_build_owned_daily_cache_v1"


def release_daily_cache(con):
    """Release only this builder's marked TEMP TABLE, never a Parquet view."""
    views = con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name='kk_daily_cache'").fetchone()[0]
    require(views == 0, "daily cache name is a view; refusing table cleanup")
    # DuckDB 1.4.4 incorrectly changes the informational `temporary` flag to
    # false after COMMENT ON TABLE. The authoritative temp database identity
    # remains unchanged; verify it rather than weakening the ownership gate.
    tables = con.execute("SELECT database_name,schema_name,comment FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchall()
    require(len(tables) <= 1, "daily cache table identity is ambiguous")
    if tables:
        require(tables[0] == ('temp','main',_DAILY_CACHE_OWNER), "daily cache is not this builder's owned TEMP TABLE")
    for name in ('kk_annotated','kk_measures','kk_checked','kk_joined','kk_source'):
        con.execute('DROP VIEW IF EXISTS '+io.ident(name))
    if tables:
        con.execute('DROP TABLE temp.main.kk_daily_cache')


def _materialize_daily_cache(con, expected_rows):
    require(con.execute("SELECT count(*) FROM duckdb_tables() WHERE table_name='kk_daily_cache'").fetchone()[0] == 0
            and con.execute("SELECT count(*) FROM duckdb_views() WHERE view_name='kk_daily_cache'").fetchone()[0] == 0,
            "daily cache destination already exists")
    con.execute('CREATE TEMP TABLE kk_daily_cache AS SELECT * FROM kk_annotated')
    con.execute('COMMENT ON TABLE temp.main.kk_daily_cache IS '+io.literal(_DAILY_CACHE_OWNER))
    count = con.execute('SELECT count(*) FROM temp.main.kk_daily_cache').fetchone()[0]
    require(count == expected_rows, "materialized daily cache/source-row conservation failed")
    con.execute('CREATE OR REPLACE TEMP VIEW kk_annotated AS SELECT * FROM temp.main.kk_daily_cache')
    return {"relation":"kk_daily_cache","rows":count,"ownership":_DAILY_CACHE_OWNER,
            "storage":"one owned TEMP TABLE within the caller's shared DuckDB memory/spill limits",
            "fields":"CTAS preserves the annotated native binary64 fields without recasting"}


def finalize_ids(con, expected_rows):
    """Exact archive-wide equality, including all analytically excluded prints."""
    require(type(expected_rows) is int and expected_rows >= 0, "expected total rows required")
    release_daily_cache(con)
    total, distinct = con.execute("SELECT count(*),count(DISTINCT trade_id) FROM native_ids").fetchone()
    require(total == expected_rows, "global native ID/source-row conservation failed")
    require(distinct == total, "duplicate native trade IDs across the complete archive")
    return {"source_rows": total, "distinct_native_trade_ids": distinct,
            "global_trade_ids_unique": True,
            "identity": "UUID only after byte-exact canonical raw-text roundtrip on every row"}


def _footer(path):
    require(path.is_file() and not path.is_symlink(), "regular staged Parquet required")
    item = pq.ParquetFile(path)
    fields = {}
    for field in item.schema_arrow:
        fields[field.name] = ("date" if pa.types.is_date32(field.type) else
                              "string" if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
                              else str(field.type))
    oracle.validate_trade_schema(fields)
    return item.metadata.num_rows, fields


def _valid_identifier(column):
    # Opaque native keys may contain internal ASCII spaces. Remove spaces only
    # from this validation expression, never from the joined/cluster identifier.
    return (f"({column} IS NOT NULL AND {column}<>'' AND {column}=trim({column}) "
            f"AND NOT regexp_matches(replace({column},' ',''), "
            "'[\\s\\p{Z}\\x{0000}-\\x{001f}\\x{007f}\\x{0085}]'))")


def annotate_day(con, trade_daily_path, source_day, *, expected_rows,
                 metadata_relation="prepared_metadata", known_compound_prefixes=()):
    """Create kk_annotated, append every canonical raw ID, and reconcile joins.

    Prepared metadata is already field/unit/combo/category admitted upstream.
    Its fields are ticker,event_cluster,category,market_reason,opening_seconds,
    endpoint_seconds,result. No native rule is inferred here.
    """
    day = oracle._day(source_day)
    require(day is not None and oracle.SOURCE_FIRST_DAY <= day <= oracle.SOURCE_LAST_DAY,
            "source day outside documented admitted inventory")
    require(type(expected_rows) is int and expected_rows >= 0, "nonnegative footer row count required")
    require(isinstance(known_compound_prefixes,(tuple,list)) and
            all(oracle._identifier(prefix) for prefix in known_compound_prefixes) and
            len(set(known_compound_prefixes))==len(known_compound_prefixes),
            "explicit unique admitted compound prefixes required")
    compound_test = ' OR '.join(f"starts_with(ticker,{io.literal(prefix)})" for prefix in known_compound_prefixes) or 'false'
    path = Path(trade_daily_path).resolve()
    footer_rows, fields = _footer(path)
    require(footer_rows == expected_rows, "daily source footer count differs")
    metadata = _relation(metadata_relation)
    names = {row[0] for row in con.execute(f"DESCRIBE {metadata}").fetchall()}
    required = {"ticker", "event_cluster", "category", "market_reason", "opening_seconds", "endpoint_seconds", "result"}
    require(required <= names, "prepared metadata fields absent")
    con.execute("SET TimeZone='UTC'")
    con.execute(f"CREATE OR REPLACE TEMP VIEW kk_source AS SELECT *,file_row_number::BIGINT source_ordinal FROM read_parquet({io.literal(path)},hive_partitioning=false,file_row_number=true)")
    count, noncanonical = con.execute("""SELECT count(*),count(*) FILTER(WHERE
        trade_id IS NULL OR try_cast(trade_id AS UUID) IS NULL OR
        encode(CAST(try_cast(trade_id AS UUID) AS VARCHAR)) IS DISTINCT FROM encode(trade_id))
        FROM kk_source""").fetchone()
    require(count == expected_rows, "daily native source count differs from footer")
    require(noncanonical == 0, "every raw native ID must have a byte-exact canonical UUID roundtrip")
    # Deliberately no WHERE: even malformed or excluded analytic rows participate.
    con.execute("INSERT INTO native_ids SELECT CAST(trade_id AS UUID) FROM kk_source")
    modern_outcome = "s.taker_outcome_side" if "taker_outcome_side" in fields else "NULL::VARCHAR"
    modern_book = "s.taker_book_side" if "taker_book_side" in fields else "NULL::VARCHAR"
    con.execute(f"""CREATE OR REPLACE TEMP VIEW kk_joined AS SELECT s.*,
        {modern_outcome} modern_outcome,{modern_book} modern_book,
        m.ticker market_ticker,m.event_cluster,m.category,m.market_reason,
        m.opening_seconds,m.endpoint_seconds,m.result,
        CASE WHEN regexp_full_match(s.created_time,{io.literal(oracle.ISO_UTC_PATTERN)})
          THEN epoch(try_cast(s.created_time AS TIMESTAMPTZ)) END parsed_timestamp,
        CASE WHEN s.taker_side='yes' THEN s.yes_price WHEN s.taker_side='no' THEN s.no_price END selected_price
        FROM kk_source s LEFT JOIN {metadata} m ON s.ticker=m.ticker""")
    rows, locators, invalid_metadata = con.execute("""SELECT count(*),count(DISTINCT source_ordinal),
        count(*) FILTER(WHERE market_ticker IS NOT NULL AND
          (market_reason IS NULL OR (market_reason='eligible' AND
            (event_cluster IS NULL OR event_cluster='' OR category IS NULL OR category='' OR result NOT IN ('yes','no') OR result IS NULL))
           OR (opening_seconds IS NOT NULL AND NOT isfinite(opening_seconds))
           OR (endpoint_seconds IS NOT NULL AND NOT isfinite(endpoint_seconds)))) FROM kk_joined""").fetchone()
    require(rows == locators == expected_rows, "prepared metadata join fanout/source-locator reconciliation failed")
    require(invalid_metadata == 0, "prepared metadata violates its admitted scalar contract")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW kk_checked AS SELECT *, CASE
        WHEN NOT {_valid_identifier('trade_id')} OR NOT {_valid_identifier('ticker')} THEN 'invalid_trade_identifier'
        WHEN parsed_timestamp IS NULL OR NOT isfinite(parsed_timestamp) OR parsed_timestamp<0 THEN 'invalid_timestamp'
        WHEN trade_date IS NULL OR CAST(to_timestamp(parsed_timestamp) AS DATE)<>trade_date THEN 'invalid_or_mismatched_trade_date'
        WHEN trade_date<>DATE {io.literal(day.isoformat())} THEN 'source_day_mismatch'
        WHEN taker_side IS NULL OR taker_side NOT IN ('yes','no') THEN 'invalid_taker_side'
        WHEN modern_outcome IS NOT NULL AND modern_outcome NOT IN ('','yes','no') THEN 'invalid_taker_outcome_side'
        WHEN modern_outcome IN ('yes','no') AND modern_outcome<>taker_side THEN 'taker_outcome_side_disagreement'
        WHEN modern_book IS NOT NULL AND modern_book NOT IN ('','bid','ask') THEN 'invalid_taker_book_side'
        WHEN modern_book IN ('bid','ask') AND (CASE WHEN modern_book='bid' THEN 'yes' ELSE 'no' END)<>taker_side THEN 'taker_book_side_disagreement'
        WHEN size IS NULL OR NOT isfinite(size) OR size<=0 THEN 'invalid_size'
        WHEN yes_price IS NULL OR no_price IS NULL OR NOT isfinite(yes_price) OR NOT isfinite(no_price)
          OR yes_price<=0 OR yes_price>=1 OR no_price<=0 OR no_price>=1 THEN 'invalid_native_price'
        WHEN abs((yes_price+no_price)-1.0)>{oracle.PRICE_SUM_TOLERANCE!r} THEN 'native_price_complement_disagreement'
        WHEN {compound_test} THEN 'combo_excluded'
        WHEN market_ticker IS NULL THEN 'unadmitted_market'
        WHEN ticker<>market_ticker THEN 'trade_market_ticker_disagreement'
        WHEN market_reason<>'eligible' THEN market_reason ELSE 'eligible' END eligibility_reason,
        CASE WHEN opening_seconds IS NULL THEN 'missing_or_invalid_open_time'
          WHEN endpoint_seconds IS NULL THEN 'missing_or_invalid_settlement_ts'
          WHEN endpoint_seconds<=opening_seconds THEN 'nonpositive_lifespan'
          WHEN parsed_timestamp IS NULL OR parsed_timestamp<opening_seconds THEN 'trade_before_open_time'
          WHEN parsed_timestamp>endpoint_seconds THEN 'trade_after_settlement_ts' ELSE 'eligible' END duration_clock_reason,
        (endpoint_seconds-opening_seconds)/86400.0 L,(endpoint_seconds-parsed_timestamp)/86400.0 R
        FROM kk_joined""")
    bins = " ".join(f"WHEN P<{bound!r} THEN {index}" for index, bound in enumerate((.1,.2,.3,.4,.5,.6,.7,.8,.9), 1))
    con.execute(f"""CREATE OR REPLACE TEMP VIEW kk_measures AS WITH priced AS (
        SELECT *,CASE WHEN eligibility_reason='eligible' THEN selected_price END::DOUBLE P,
          CASE WHEN eligibility_reason='eligible' THEN (taker_side=result)::INTEGER END Y,
          CASE WHEN eligibility_reason='eligible' THEN 'kalshi:claim:'||ticker||':'||taker_side END claim_id
        FROM kk_checked)
        SELECT *,eligibility_reason='eligible' baseline_eligible,
          CASE WHEN eligibility_reason='eligible' THEN CASE {bins} ELSE 10 END END::TINYINT bin,
          100.0*(Y-P) payoff,100.0*(Y/P-1.0) roi,size*P capital,
          strftime(trade_date,'%Y-%m') trade_month,parsed_timestamp AS "timestamp"
        FROM priced""")
    con.execute("""CREATE OR REPLACE TEMP VIEW kk_annotated AS WITH reasons AS (
        SELECT *,CASE WHEN NOT baseline_eligible THEN 'baseline_ineligible'
          WHEN duration_clock_reason<>'eligible' THEN duration_clock_reason
          WHEN bin NOT IN (1,10) THEN 'not_tail' ELSE 'eligible' END duration_reason
        FROM kk_measures)
        SELECT *,duration_reason='eligible' duration_eligible,
          CASE WHEN duration_reason='eligible' THEN log2(1.0+L) END xL,
          CASE WHEN duration_reason='eligible' THEN log2(1.0+R) END xR,
          coalesce(baseline_eligible AND isfinite(payoff) AND isfinite(roi),false) analytic_outcomes_finite
        FROM reasons""")
    date_audit = _records(con,f"""SELECT
        count(*) FILTER(WHERE trade_date IS NOT NULL AND trade_date<>DATE {io.literal(day.isoformat())})::BIGINT source_day_mismatch_rows,
        count(*) FILTER(WHERE parsed_timestamp IS NOT NULL AND parsed_timestamp>=0 AND trade_date IS NOT NULL
          AND CAST(to_timestamp(parsed_timestamp) AS DATE)<>trade_date)::BIGINT timestamp_trade_date_mismatch_rows
        FROM kk_joined""")[0]
    return {"source_day": day.isoformat(), "source_rows": rows, "footer_rows": footer_rows,
            "native_ids_appended": count, "no_join_fanout": True, "date_audit": date_audit, "schema": fields,
            "known_compound_prefixes":list(known_compound_prefixes)}


class _Writer:
    def __init__(self, root, limits):
        self.root, self.limits = Path(root).resolve(), limits.validate()
        require(self.root.is_dir() and not self.root.is_symlink(), "existing owned build output directory required")
        # The caller grants this serial callback exclusive ownership of this
        # artifact root. Count prior immutable days once, not before every COPY.
        self.used = sum(path.stat().st_size for path in self.root.rglob('*') if path.is_file())

    def reserve(self, allowance=0):
        require(self.used + allowance <= self.limits.output_bytes, "build total-output ceiling exceeded")
        needed = (self.limits.output_bytes - self.used + self.limits.transient_bytes
                  + self.limits.spill_bytes + self.limits.minimum_free_bytes)
        require(shutil.disk_usage(self.root).free >= needed, "build output/spill/free-floor reservation failed")

    def parquet(self, con, path, query):
        require(not path.exists(), "immutable build artifact already exists")
        self.reserve(self.limits.artifact_bytes)
        with io.copy_ceiling(self.limits.transient_bytes):
            con.execute(f"COPY ({query}) TO {io.literal(path)} (FORMAT PARQUET,COMPRESSION ZSTD,ROW_GROUP_SIZE 65536)")
        require(path.stat().st_size <= self.limits.artifact_bytes, "build artifact exceeds admitted ceiling")
        with path.open('rb') as stream:
            os.fsync(stream.fileno())
        self.used += path.stat().st_size
        self.reserve()
        item = pq.ParquetFile(path)
        return {"path": str(path.relative_to(self.root)), "bytes": path.stat().st_size,
                "rows": item.metadata.num_rows, "sha256": io.sha256(path), "schema": str(item.schema_arrow)}

    def json(self, path, value):
        raw = (json.dumps(value,sort_keys=True,indent=2,allow_nan=False)+'\n').encode()
        require(len(raw) <= self.limits.json_bytes, "build JSON ceiling exceeded")
        self.reserve(len(raw))
        with path.open('xb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        self.used += len(raw)
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        self.reserve()


def _moment_query(source, keys, features):
    """a_i stores SUM(f_i); c_i_j stores the centered within-cell sum.

    The existing numerical core divides a_i by n_rows to restore cell means.
    """
    aggregates = ["count(*)::BIGINT n_rows"]
    aggregates += [f"sum({value})::DOUBLE a{i}" for i,value in enumerate(features)]
    aggregates += [f"(covar_pop({features[i]},{features[j]})*count(*))::DOUBLE c{i}_{j}"
                   for i in range(len(features)) for j in range(i,len(features))]
    return f"SELECT {','.join(keys+aggregates)} FROM ({source}) GROUP BY ALL"


def _records(con, query):
    result = con.execute(query)
    names = [column[0] for column in result.description]
    return [dict(zip(names,row)) for row in result.fetchall()]


def process_day(con, trade_daily_path, source_day, output_stage, *, expected_rows,
                metadata_relation="prepared_metadata", sports_candidates_relation=None,
                known_compound_prefixes=(), limits=BuildLimits()):
    """Durably emit partial cells/coverage before the staging callback returns."""
    writer = _Writer(output_stage, limits)
    require(not Path(trade_daily_path).resolve().is_relative_to(writer.root),
            "source staging must be outside the build-artifact directory")
    day = oracle._day(source_day)
    require(day is not None and oracle.SOURCE_FIRST_DAY <= day <= oracle.SOURCE_LAST_DAY,
            "source day outside documented admitted inventory")
    folder = writer.root / ('day='+day.isoformat())
    require(not folder.exists(), "immutable daily build already exists")
    writer.reserve()
    folder.mkdir()
    release_daily_cache(con)
    info = annotate_day(con, trade_daily_path, source_day, expected_rows=expected_rows,
                        metadata_relation=metadata_relation,known_compound_prefixes=known_compound_prefixes)
    try:
        info['daily_cache'] = _materialize_daily_cache(con,expected_rows)
        return _emit_day(con,writer,folder,info,expected_rows,sports_candidates_relation,limits)
    finally:
        release_daily_cache(con)


def _emit_day(con,writer,folder,info,expected_rows,sports_candidates_relation,limits):
    """All repeated daily aggregates read the one materialized annotated table."""
    counts = con.execute("""SELECT count(*),count(*) FILTER(WHERE baseline_eligible),
        count(*) FILTER(WHERE duration_eligible),count(*) FILTER(WHERE baseline_eligible AND NOT analytic_outcomes_finite),
        min(timestamp),max(timestamp) FROM kk_annotated""").fetchone()
    require(counts[0] == expected_rows, "daily annotated source conservation failed")
    require(counts[3] == 0, "nonfinite admitted analytic outcomes block estimation")
    exclusions = _records(con,'SELECT eligibility_reason,count(*)::BIGINT AS "rows" FROM kk_annotated GROUP BY 1 ORDER BY 1')
    duration = _records(con,'SELECT duration_reason,count(*)::BIGINT AS "rows" FROM kk_annotated WHERE baseline_eligible GROUP BY 1 ORDER BY 1')
    src_audit = ({"field_present": True, **_records(con,"""SELECT
        count(*) FILTER(WHERE src IS NULL OR src='')::BIGINT missing_src_rows,
        count(*) FILTER(WHERE src IS NOT NULL AND src<>'' AND source IS NOT NULL AND source<>'' AND src<>source)::BIGINT disagreement_rows
        FROM kk_annotated""")[0]} if 'src' in info['schema'] else
        {"field_present": False, "missing_src_rows": expected_rows, "disagreement_rows": 0})
    require(sum(row['rows'] for row in exclusions)==expected_rows, "first-failure exclusions do not exhaust source")
    eligible = "SELECT * FROM kk_annotated WHERE baseline_eligible"
    tails = "SELECT *, (bin=10)::TINYINT tail,(L>1.0) lifespan_gt1,(R>1.0) remaining_gt1 FROM kk_annotated WHERE duration_eligible"
    baseline = f"SELECT event_cluster,bin,count(*)::BIGINT n_rows,sum(P)::DOUBLE sum_price,sum(Y)::DOUBLE sum_outcome,sum(payoff)::DOUBLE sum_payoff,sum(roi)::DOUBLE sum_roi FROM ({eligible}) GROUP BY ALL"
    durations = _moment_query(tails,["event_cluster","tail","category","P","trade_month","lifespan_gt1","remaining_gt1"],['xL','xR','payoff','roi'])
    claims_source = f"SELECT *, (bin=10)::DOUBLE H FROM ({tails})"
    claims = _moment_query(claims_source,['event_cluster','claim_id'],['H','H*xL','xR','H*xR','payoff','roi'])
    queries = {
        'baseline': baseline, 'duration_raw': durations, 'claim_raw': claims,
        'categories': f"SELECT category,count(*)::BIGINT n_rows FROM ({eligible}) GROUP BY 1",
        'markets': f"SELECT DISTINCT ticker,event_cluster,category,result,opening_seconds,endpoint_seconds FROM ({eligible})",
        'claims': f"SELECT claim_id,ticker,taker_side,event_cluster,category,Y,count(*)::BIGINT n_rows,count(*) FILTER(WHERE bin=1)::BIGINT n_d1,count(*) FILTER(WHERE bin=10)::BIGINT n_d10,count(*) FILTER(WHERE duration_eligible)::BIGINT duration_rows FROM ({eligible}) GROUP BY ALL",
        'exclusions': "SELECT source_ordinal,eligibility_reason FROM kk_annotated WHERE NOT baseline_eligible",
    }
    sports_rows = 0
    if sports_candidates_relation is not None:
        sports = _relation(sports_candidates_relation)
        require(con.execute(f"SELECT count(*) FROM (SELECT ticker FROM {sports} GROUP BY 1 HAVING count(*)<>1)").fetchone()[0]==0,
                "sports candidate mapping ticker is not unique")
        require(con.execute(f"SELECT count(*) FROM {sports} WHERE ticker IS NULL OR sport IS NULL OR sport NOT IN ('mlb','nba','nfl','nhl','cbb','atp','epl','cfb','wnba')").fetchone()[0]==0,
                "sports candidate mapping contains unadmitted fields")
        queries['sports_candidates'] = f"SELECT b.ticker,b.claim_id,b.event_cluster,s.sport,b.timestamp,b.P,b.Y,b.payoff,b.roi,b.bin FROM ({eligible}) b JOIN {sports} s USING(ticker)"
    artifacts = {name: writer.parquet(con,folder/(name+'.parquet'),query) for name,query in queries.items()}
    for name, expected in (('baseline',counts[1]),('duration_raw',counts[2]),('claim_raw',counts[2]),('categories',counts[1]),('claims',counts[1])):
        actual = con.execute(f"SELECT coalesce(sum(n_rows),0) FROM read_parquet({io.literal(writer.root/artifacts[name]['path'])},hive_partitioning=false)").fetchone()[0]
        require(actual==expected, "reopened sufficient-cell print counts differ: "+name)
    require(artifacts['exclusions']['rows']==expected_rows-counts[1],"reopened excluded-locator counts differ")
    if sports_candidates_relation is not None:
        sports_rows = artifacts['sports_candidates']['rows']
    receipt = {"schema_version":"kaushik_kalshi_day_cells_v1","status":"durable_pending_global_admission",
        **info,"baseline_rows":counts[1],"duration_rows":counts[2],"excluded_rows":expected_rows-counts[1],
        "execution_seconds_min":counts[4],"execution_seconds_max":counts[5],
        "exclusions":exclusions,"duration_exclusions":duration,"artifacts":artifacts,
        "source_src_audit":src_audit,
        "sports_candidate_rows":sports_rows,"sports_candidate_admission":"unbound candidates only; no game/timing admission",
        "caps":asdict(limits),"partial_cells":"a_i=SUM(feature_i); c_i_j=centered within-partial-cell cross-product sums; do not sum centered cells"}
    writer.json(folder/'receipt.json',receipt)
    return receipt


def finalize_cells(con, output_stage, day_receipts, *, expected_source_rows, limits=BuildLimits()):
    """Globally code retained partial cells; immutable outputs remain admission inputs.

    Dictionary codes are dense and tail-specific. Price values stay binary64.
    Repeated partial duration/claim groups are never merged by adding centered
    moments. The exact global ID gate is mandatory before any final output.
    """
    writer = _Writer(output_stage,limits)
    receipts = tuple(day_receipts)
    require(receipts and len({row['source_day'] for row in receipts})==len(receipts),"unique nonempty day receipt inventory required")
    require(sum(row['source_rows'] for row in receipts)==expected_source_rows,"day/source total differs")
    identity = finalize_ids(con,expected_source_rows)
    folder = writer.root/'final'
    require(not folder.exists(),"immutable final cell directory already exists")
    folder.mkdir()
    for name in ('baseline','duration_raw','claim_raw','categories','markets','claims'):
        paths=[]
        for receipt in receipts:
            item=receipt['artifacts'][name]
            path=(writer.root/item['path']).resolve()
            require(path.is_relative_to(writer.root) and path.is_file() and not path.is_symlink(),"cell input escapes owned stage")
            require(path.stat().st_size==item['bytes'] and io.sha256(path)==item['sha256'],"durable partial-cell input drift")
            paths.append(io.literal(path))
        con.execute(f"CREATE OR REPLACE TEMP VIEW kk_parts_{name} AS SELECT * FROM read_parquet([{','.join(paths)}],hive_partitioning=false)")
    bad_claims=con.execute("""SELECT count(*) FROM (SELECT claim_id FROM kk_parts_claims GROUP BY 1
        HAVING count(DISTINCT event_cluster)<>1 OR count(DISTINCT category)<>1 OR count(DISTINCT Y)<>1 OR count(DISTINCT ticker)<>1 OR count(DISTINCT taker_side)<>1)""").fetchone()[0]
    require(bad_claims==0,"claim event/category/payout invariants differ across days")
    bad_markets=con.execute("""SELECT count(*) FROM (SELECT ticker FROM kk_parts_markets GROUP BY 1
        HAVING count(DISTINCT (event_cluster,category,result,opening_seconds,endpoint_seconds))<>1)""").fetchone()[0]
    require(bad_markets==0,"market clocks/event/category/payout invariants differ across days")
    for family,field in (('cat','category'),('price','P'),('month','trade_month')):
        con.execute(f"CREATE TEMP TABLE kk_{family}_dictionary AS SELECT tail,{io.ident(field)},(row_number() OVER(ORDER BY tail,{io.ident(field)})-1)::BIGINT {family}_code FROM (SELECT DISTINCT tail,{io.ident(field)} FROM kk_parts_duration_raw)")
    con.execute("CREATE TEMP TABLE kk_claim_dictionary AS SELECT claim_id,(row_number() OVER(ORDER BY claim_id)-1)::BIGINT claim_fe_code FROM (SELECT DISTINCT claim_id FROM kk_parts_claim_raw)")
    queries={
        'baseline':"SELECT event_cluster,bin,sum(n_rows)::BIGINT n_rows,sum(sum_price)::DOUBLE sum_price,sum(sum_outcome)::DOUBLE sum_outcome,sum(sum_payoff)::DOUBLE sum_payoff,sum(sum_roi)::DOUBLE sum_roi FROM kk_parts_baseline GROUP BY ALL",
        'duration':"SELECT d.* EXCLUDE(category,P,trade_month),c.cat_code,p.price_code,m.month_code FROM kk_parts_duration_raw d JOIN kk_cat_dictionary c USING(tail,category) JOIN kk_price_dictionary p USING(tail,P) JOIN kk_month_dictionary m USING(tail,trade_month)",
        'claim':"SELECT d.* EXCLUDE(claim_id),c.claim_fe_code FROM kk_parts_claim_raw d JOIN kk_claim_dictionary c USING(claim_id)",
        'categories':"SELECT category,sum(n_rows)::BIGINT n_rows FROM kk_parts_categories GROUP BY 1",
        'markets':"SELECT DISTINCT * FROM kk_parts_markets",
        'claims':"SELECT claim_id,ticker,taker_side,event_cluster,category,Y,sum(n_rows)::BIGINT n_rows,sum(n_d1)::BIGINT n_d1,sum(n_d10)::BIGINT n_d10,sum(duration_rows)::BIGINT duration_rows FROM kk_parts_claims GROUP BY ALL",
        **{family+'_dictionary':'SELECT * FROM kk_'+family+'_dictionary' for family in ('cat','price','month','claim')},
    }
    artifacts={name:writer.parquet(con,folder/(name+'.parquet'),query) for name,query in queries.items()}
    baseline_rows=sum(row['baseline_rows'] for row in receipts)
    duration_rows=sum(row['duration_rows'] for row in receipts)
    for name,expected in (('baseline',baseline_rows),('duration',duration_rows),('claim',duration_rows)):
        actual=con.execute(f"SELECT coalesce(sum(n_rows),0) FROM read_parquet({io.literal(writer.root/artifacts[name]['path'])},hive_partitioning=false)").fetchone()[0]
        require(actual==expected,"global code joins changed print counts: "+name)
    summary={"schema_version":"kaushik_kalshi_compact_inputs_v1","status":"cells_complete_pending_independent_acceptance",
        "identity":identity,"source_rows":expected_source_rows,"baseline_rows":baseline_rows,
        "duration_rows":duration_rows,"excluded_rows":expected_source_rows-baseline_rows,
        "days":[row['source_day'] for row in receipts],"artifacts":artifacts,"caps":asdict(limits),
        "native_ids_memory_table":"retained until caller releases it after recording exact global proof",
        "sports_candidate_rows":sum(row.get('sports_candidate_rows',0) for row in receipts)}
    writer.json(folder/'manifest.json',summary)
    return summary
