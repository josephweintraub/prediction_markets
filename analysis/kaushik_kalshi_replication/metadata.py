"""DuckDB metadata inspection and explicit-policy projection for Kalshi v1.

Inputs are already verified, caller-registered relations. This module performs
no file transfer, file deletion, production execution or source-unit inference.
Every physical metadata key, including excluded compounds, must be saved from
``raw_keys_relation`` and pass the final all-shard uniqueness gate before use.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
import re
from typing import Iterable, Mapping

from . import inputs


SCHEMA_VERSION = "kaushik_kalshi_metadata_projection_v1"
REQUIRED_FIELDS = {"ticker": "VARCHAR", "event_ticker": "VARCHAR", "market_type": "VARCHAR",
                   "status": "VARCHAR", "result": "VARCHAR", "open_time": "VARCHAR",
                   "settlement_ts": "VARCHAR", "notional_value": "DOUBLE",
                   "settlement_value": "VARCHAR", "_source": "VARCHAR"}
ENDPOINT_ENCODINGS = (None, "ISO8601_UTC", "unix_seconds_string")
PAYOUT_RULES = ("unadmitted", "native_notional", "standard_binary_venue_contract")


class MetadataBlocked(inputs.InputBlocked):
    """A structural admission gate failed; health is retained for audit."""

    def __init__(self, message, *, health=None):
        super().__init__(message)
        self.health = health


def _require(condition, message, *, health=None):
    if not condition:
        raise MetadataBlocked(message, health=health)


def _evidence(value):
    return isinstance(value, str) and bool(value.strip())


def _name(value):
    _require(isinstance(value, str) and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value) is not None,
             "simple SQL relation/column identifier required")
    return '"' + value + '"'


def _literal(value):
    if value is None:
        return "NULL::VARCHAR"
    return "'" + str(value).replace("'", "''") + "'"


def _rows(con, sql):
    cursor = con.execute(sql)
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _schema(con, relation):
    return {row[0]: row[1] for row in con.execute("DESCRIBE SELECT * FROM " + _name(relation)).fetchall()}


def _source_schema(con, relation):
    schema = _schema(con, relation)
    _require(set(REQUIRED_FIELDS) <= set(schema), "required native metadata fields absent")
    for field, kind in REQUIRED_FIELDS.items():
        _require(schema[field] == kind, "native metadata field type differs: " + field)
    return schema


def _valid_identifier(expression):
    return (f"({expression} IS NOT NULL AND {expression} <> '' AND {expression}=trim({expression}) "
            f"AND NOT regexp_matches({expression}, '[[:space:][:cntrl:]]'))")


def _bounded_seconds(expression):
    return (f"CASE WHEN isfinite({expression}) AND {expression}>=0 "
            f"AND {expression}<253402300800 THEN {expression} ELSE NULL END")


def _iso_seconds(expression):
    pattern = inputs.ISO_UTC_PATTERN
    parsed = (f"CASE WHEN {expression}=trim({expression}) AND "
              f"regexp_full_match({expression}, {_literal(pattern)}) "
              f"THEN epoch(try_cast({expression} AS TIMESTAMPTZ)) ELSE NULL END")
    return _bounded_seconds(parsed)


def _number_text(expression):
    pattern = r"[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]+)?"
    return (f"CASE WHEN {expression}=trim({expression}) AND "
            f"regexp_full_match({expression}, {_literal(pattern)}) "
            f"THEN try_cast({expression} AS DOUBLE) ELSE NULL END")


def _unix_text_seconds(expression):
    return _bounded_seconds(_number_text(expression))


def _compound_family(expression):
    return (f"CASE WHEN {expression} LIKE 'KXMVE%' THEN 'KXMVE' "
            f"WHEN {expression} LIKE 'KXOSCARWINNERS%' THEN 'KXOSCARWINNERS' "
            f"WHEN {expression} LIKE 'KXCITIESWEATHER%' THEN 'KXCITIESWEATHER' ELSE NULL END")


@dataclass(frozen=True)
class SourceVersionPolicy:
    """Admitted semantics for one exact ``_source`` value; defaults exclude.

    ``standard_binary_venue_contract`` derives a dollar face value from explicit
    venue payout evidence, never by changing raw notional data. Its missing/zero
    exception must itself be explicitly allowed for that source version.
    Positive native values still require admitted dollar units and exact 1.0.
    """

    payout_rule: str = "unadmitted"
    notional_value_unit: str | None = None
    payout_evidence_rule: str | None = None
    allow_missing_or_zero_notional_under_venue_rule: bool = False
    settlement_ts_encoding: str | None = None
    settlement_ts_evidence_rule: str | None = None
    settlement_value_unit: str | None = None
    settlement_value_evidence_rule: str | None = None

    def __post_init__(self):
        _require(self.payout_rule in PAYOUT_RULES, "unadmitted payout policy name")
        _require(self.notional_value_unit in (None, "dollars"), "native notional units must be explicitly admitted dollars")
        _require(type(self.allow_missing_or_zero_notional_under_venue_rule) is bool, "sentinel policy must be boolean")
        _require(self.settlement_ts_encoding in ENDPOINT_ENCODINGS, "unadmitted settlement timestamp encoding")
        _require(self.settlement_value_unit in (None, "dollars"), "settlement value units must be explicitly admitted dollars")
        if self.payout_rule != "unadmitted":
            _require(_evidence(self.payout_evidence_rule), "payout evidence required")
        if self.payout_rule == "native_notional":
            _require(self.notional_value_unit == "dollars", "native notional payout requires admitted dollar units")
        if self.allow_missing_or_zero_notional_under_venue_rule:
            _require(self.payout_rule == "standard_binary_venue_contract", "sentinel exception requires explicit venue payout rule")
        if self.settlement_ts_encoding is not None:
            _require(_evidence(self.settlement_ts_evidence_rule), "settlement timestamp evidence required")
        if self.settlement_value_unit is not None:
            _require(_evidence(self.settlement_value_evidence_rule), "settlement value evidence required")


@dataclass(frozen=True)
class NativeReferences:
    """Pinned, admitted references registered by the caller.

    Exact events membership supplies non-multivariate identity and the category.
    Series and collections are retained as audit references only: membership in
    a collection's associated-event list does not make a single leg a compound.
    """

    events_relation: str
    events_evidence_rule: str
    series_relation: str | None = None
    series_evidence_rule: str | None = None
    collections_relation: str | None = None
    collections_evidence_rule: str | None = None

    def __post_init__(self):
        _name(self.events_relation)
        _require(_evidence(self.events_evidence_rule), "native events membership evidence required")
        for relation, evidence in ((self.series_relation, self.series_evidence_rule),
                                   (self.collections_relation, self.collections_evidence_rule)):
            if relation is not None:
                _name(relation)
                _require(_evidence(evidence), "admitted native reference evidence required")


def _key_health(con, relation):
    relation = _name(relation)
    group = (f"SELECT ticker,count(*) n, count(DISTINCT result)+max((result IS NULL)::INTEGER) result_variants, "
             f"count(DISTINCT status)+max((status IS NULL)::INTEGER) status_variants FROM {relation} GROUP BY ticker")
    sql = (f"WITH g AS ({group}) SELECT coalesce(sum(n),0)::BIGINT AS \"rows\", count(*)::BIGINT ticker_groups, "
           f"count(ticker)::BIGINT nonnull_ticker_groups, coalesce(sum(CASE WHEN NOT {_valid_identifier('ticker')} "
           f"THEN n ELSE 0 END),0)::BIGINT invalid_ticker_rows, count(*) FILTER(WHERE n>1)::BIGINT duplicate_ticker_keys, "
           f"coalesce(sum(CASE WHEN n>1 THEN n-1 ELSE 0 END),0)::BIGINT duplicate_extra_rows, "
           f"count(*) FILTER(WHERE result_variants>1)::BIGINT conflicting_result_keys, "
           f"count(*) FILTER(WHERE status_variants>1)::BIGINT conflicting_status_keys FROM g")
    health = _rows(con, sql)[0]
    health["duplicate_examples"] = _rows(con, f"SELECT * FROM ({group}) WHERE n>1 ORDER BY ticker NULLS FIRST LIMIT 20")
    return health


def validate_global_metadata_keys(con, relation, *, expected_source_shards: Iterable[str]):
    """Require all pinned shards and one physical row per ticker, without DISTINCT.

    This final gate must run over UNION ALL of the saved all-row key ledgers,
    including excluded compound rows. Identical duplicate outcomes also block.
    Conflicting result/status counts and bounded duplicate examples survive in
    ``MetadataBlocked.health``. This function cannot certify unprovided files.
    """
    expected = tuple(expected_source_shards)
    _require(bool(expected) and all(_evidence(item) for item in expected) and len(set(expected)) == len(expected),
             "explicit unique pinned source-shard inventory required")
    _require({"ticker", "result", "status", "source_shard"} <= set(_schema(con, relation)),
             "all-row raw-key ledger schema required")
    health = _key_health(con, relation)
    observed = con.execute(f"SELECT DISTINCT source_shard FROM {_name(relation)} LIMIT {len(expected)+1}").fetchall()
    health["observed_source_shards"] = sorted((row[0] for row in observed), key=lambda value: str(value))
    health["expected_source_shards"] = list(expected)
    _require(set(health["observed_source_shards"]) == set(expected), "metadata source-shard coverage differs", health=health)
    _require(health["invalid_ticker_rows"] == 0 and health["duplicate_ticker_keys"] == 0,
             "global metadata ticker uniqueness gate failed", health=health)
    return health


def inspect_metadata_shard(con, relation, *, maximum_notional_frequencies=16):
    """Return bounded raw source/unit/clock summaries; infer no source policy."""
    schema = _source_schema(con, relation)
    _require(type(maximum_notional_frequencies) is int and 1 <= maximum_notional_frequencies <= 64,
             "bounded notional frequency count required")
    native = _name(relation)
    numeric = _number_text("settlement_value")
    unix = _unix_text_seconds("settlement_ts")
    iso = _iso_seconds("settlement_ts")
    groups = _rows(con, f"""
        SELECT _source source_version,count(*)::BIGINT AS "rows",
          count(*) FILTER(WHERE notional_value IS NULL)::BIGINT notional_missing,
          count(*) FILTER(WHERE notional_value=0)::BIGINT notional_zero,
          count(*) FILTER(WHERE notional_value=1)::BIGINT notional_exact_one,
          count(*) FILTER(WHERE notional_value IS NOT NULL AND NOT isfinite(notional_value))::BIGINT notional_nonfinite,
          count(*) FILTER(WHERE nullif(trim(settlement_ts),'') IS NULL)::BIGINT endpoint_missing_or_blank,
          count(*) FILTER(WHERE ({unix}) IS NOT NULL)::BIGINT endpoint_parseable_unix_seconds_string,
          count(*) FILTER(WHERE ({iso}) IS NOT NULL)::BIGINT endpoint_parseable_iso8601_utc,
          count(*) FILTER(WHERE nullif(trim(settlement_value),'') IS NOT NULL)::BIGINT settlement_value_present,
          count(*) FILTER(WHERE isfinite({numeric}))::BIGINT settlement_value_finite_numeric_text
        FROM {native} GROUP BY _source ORDER BY _source NULLS FIRST LIMIT 65
    """)
    _require(len(groups) <= 64, "source-version summary bound exceeded")
    frequencies = _rows(con, f"""
        WITH f AS (SELECT _source source_version,cast(notional_value AS VARCHAR) raw_notional_value,count(*)::BIGINT AS "rows"
          FROM {native} GROUP BY _source,notional_value),
        ranked AS (SELECT *,row_number() OVER(PARTITION BY source_version ORDER BY "rows" DESC,raw_notional_value NULLS FIRST) rank,
          count(*) OVER(PARTITION BY source_version) value_groups FROM f)
        SELECT * FROM ranked WHERE rank<={maximum_notional_frequencies} ORDER BY source_version NULLS FIRST,rank
    """)
    fields = [name for name in ("ticker", "event_ticker", "market_type", "status", "result", "open_time", "close_time", "expiration_time",
              "settlement_ts", "notional_value", "settlement_value", "_source", "series_ticker", "title",
              "question", "yes_sub_title", "mve_collection_ticker") if name in schema]
    expressions = [f"count(*) FILTER(WHERE nullif(trim(cast({_name(name)} AS VARCHAR)),'') IS NOT NULL)::BIGINT {_name(name)}" for name in fields]
    coverage = _rows(con, "SELECT " + ",".join(expressions) + " FROM " + native)[0]
    return {"schema_version": SCHEMA_VERSION, "source_schema": schema, "source_summaries": groups,
            "notional_frequencies": frequencies, "field_nonempty_counts": coverage,
            "key_health": _key_health(con, relation),
            "clock_parseability_is_not_encoding_admission": True}


def _reference_health(con, refs):
    schema = _schema(con, refs.events_relation)
    _require(all(schema.get(name) == "VARCHAR" for name in ("event_ticker", "category", "series_ticker")),
             "native events reference requires text event_ticker/category/series_ticker")
    health = _rows(con, f"SELECT count(*)::BIGINT AS \"rows\",count(event_ticker)::BIGINT nonnull_keys, "
                   f"count(DISTINCT event_ticker)::BIGINT distinct_keys, "
                   f"count(*) FILTER(WHERE NOT {_valid_identifier('event_ticker')})::BIGINT invalid_keys "
                   f"FROM {_name(refs.events_relation)}")[0]
    _require(health["rows"] == health["nonnull_keys"] == health["distinct_keys"] and health["invalid_keys"] == 0,
             "native events reference uniqueness/key gate failed", health=health)
    result = {"events": {**health, "evidence_rule": refs.events_evidence_rule}}
    for label, relation, evidence in (("series", refs.series_relation, refs.series_evidence_rule),
                                       ("collections", refs.collections_relation, refs.collections_evidence_rule)):
        if relation is not None:
            result[label] = {"rows": con.execute("SELECT count(*) FROM " + _name(relation)).fetchone()[0],
                             "schema": _schema(con, relation), "evidence_rule": evidence,
                             "role": "audit_only_no_category_or_combo_precedence"}
    return result


def project_metadata_shard(con, relation, *, source_shard, source_version_policy: Mapping[str, SourceVersionPolicy],
                           native_references: NativeReferences, output_prefix, sports_category_label="Sports"):
    """Create temporary projection/audit views over one verified metadata shard.

    This is an intermediate stage, not final join admission. Persist every row
    from raw_keys_relation, including known compounds; pass their UNION ALL to
    validate_global_metadata_keys with all 80 pinned shard IDs before any trade
    join is accepted. No duplicate row is removed or resolved by precedence.
    The returned projection omits only known compounds; unknown identity and
    unresolved/nonbinary rows remain with explicit disjoint reasons.
    """
    schema = _source_schema(con, relation)
    _require(_evidence(source_shard), "pinned source-shard identity required")
    _name(output_prefix)
    _require(isinstance(source_version_policy, Mapping) and len(source_version_policy) <= 64,
             "explicit bounded source-version policy mapping required")
    for source, policy in source_version_policy.items():
        _require(_evidence(source) and source == source.strip() and type(policy) is SourceVersionPolicy,
                 "exact source-version policy required")
    _require(type(native_references) is NativeReferences, "admitted native references required")
    _require(_evidence(sports_category_label), "explicit native sports category label required")
    names = {suffix: output_prefix + "_" + suffix for suffix in
             ("raw_keys", "policy", "joined", "parsed", "admission", "all_reasons", "projection", "exclusions", "combo_keys", "sports")}
    for name in names.values():
        present = con.execute("SELECT 1 FROM information_schema.tables WHERE table_name=? LIMIT 1", [name]).fetchone()
        _require(present is None, "immutable metadata projection name already exists: " + name)
    raw_keys = names["raw_keys"]
    con.execute(f"CREATE TEMP VIEW {_name(raw_keys)} AS SELECT ticker,_source source_version,status,result, "
                f"{_literal(source_shard)}::VARCHAR source_shard,{_compound_family('ticker')} known_compound_family FROM {_name(relation)}")
    health = _key_health(con, raw_keys)
    health["raw_keys_relation"] = raw_keys
    _require(health["invalid_ticker_rows"] == 0 and health["duplicate_ticker_keys"] == 0,
             "metadata shard ticker uniqueness gate failed", health=health)
    references_health = _reference_health(con, native_references)
    fields = ("source_version", "payout_rule", "notional_value_unit", "payout_evidence_rule", "allow_sentinel",
              "endpoint_encoding", "endpoint_evidence_rule", "settlement_value_unit", "settlement_value_evidence_rule")
    selects = []
    for source, policy in source_version_policy.items():
        values = [source, policy.payout_rule, policy.notional_value_unit, policy.payout_evidence_rule,
                  "true" if policy.allow_missing_or_zero_notional_under_venue_rule else "false",
                  policy.settlement_ts_encoding, policy.settlement_ts_evidence_rule,
                  policy.settlement_value_unit, policy.settlement_value_evidence_rule]
        selects.append("SELECT " + ",".join(_literal(value) + "::VARCHAR " + _name(field) for field, value in zip(fields, values)))
    if not selects:
        selects = ["SELECT " + ",".join("NULL::VARCHAR " + _name(field) for field in fields) + " WHERE FALSE"]
    con.execute(f"CREATE TEMP VIEW {_name(names['policy'])} AS " + " UNION ALL ".join(selects))
    con.execute(f"""CREATE TEMP VIEW {_name(names['joined'])} AS
      SELECT m.*,e.event_ticker matched_event_ticker,e.category native_category,e.series_ticker native_series_ticker,
        p.*,{_compound_family('m.ticker')} known_compound_family
      FROM {_name(relation)} m LEFT JOIN {_name(native_references.events_relation)} e ON m.event_ticker=e.event_ticker
      LEFT JOIN {_name(names['policy'])} p ON m._source=p.source_version""")
    con.execute(f"""CREATE TEMP VIEW {_name(names['parsed'])} AS SELECT *,
        {_iso_seconds('open_time')} opening_seconds,
        CASE WHEN endpoint_encoding='ISO8601_UTC' THEN {_iso_seconds('settlement_ts')}
             WHEN endpoint_encoding='unix_seconds_string' THEN {_unix_text_seconds('settlement_ts')} ELSE NULL END endpoint_seconds,
        CASE WHEN notional_value_unit='dollars' AND isfinite(notional_value) THEN notional_value ELSE NULL END normalized_notional_dollars,
        nullif(trim(settlement_value),'') IS NOT NULL settlement_value_present,
        CASE WHEN settlement_value_unit='dollars' THEN {_number_text('settlement_value')} ELSE NULL END settlement_value_dollars
      FROM {_name(names['joined'])}""")
    con.execute(f"""CREATE TEMP VIEW {_name(names['admission'])} AS SELECT *,
        CASE WHEN known_compound_family IS NOT NULL THEN 'combo' WHEN matched_event_ticker IS NOT NULL THEN 'single' ELSE 'unknown' END combo_state,
        CASE WHEN known_compound_family IS NOT NULL THEN 'documented_compound_family:'||known_compound_family
             WHEN matched_event_ticker IS NOT NULL THEN {_literal(native_references.events_evidence_rule)} ELSE NULL END combo_evidence_rule,
        CASE WHEN notional_value IS NULL THEN 'missing' WHEN NOT isfinite(notional_value) OR notional_value<0 THEN 'invalid'
             WHEN notional_value=0 THEN 'zero_sentinel_candidate' WHEN normalized_notional_dollars=1 THEN 'positive_unit_dollars'
             WHEN normalized_notional_dollars IS NOT NULL THEN 'positive_nonunit_dollars' ELSE 'positive_unadmitted_units' END notional_quality,
        coalesce(payout_rule IN ('native_notional','standard_binary_venue_contract'),FALSE) unit_payoff_admitted,
        CASE WHEN payout_rule='standard_binary_venue_contract' AND allow_sentinel='true' AND (notional_value IS NULL OR notional_value=0)
             THEN 1.0 ELSE normalized_notional_dollars END face_value_dollars,
        CASE WHEN payout_rule='standard_binary_venue_contract' AND allow_sentinel='true' AND (notional_value IS NULL OR notional_value=0)
             THEN 'admitted_venue_standard_binary' ELSE 'observed_native_dollars' END face_value_basis
      FROM {_name(names['parsed'])}""")
    con.execute(f"""CREATE TEMP VIEW {_name(names['all_reasons'])} AS SELECT *,
      CASE WHEN combo_state='combo' THEN 'combo_excluded'
        WHEN combo_state='unknown' THEN 'unadmitted_combo_classification'
        WHEN market_type IS DISTINCT FROM 'binary' THEN 'nonbinary_or_missing_market_type'
        WHEN status IS DISTINCT FROM 'finalized' THEN 'not_finalized'
        WHEN result IS NULL OR result NOT IN ('yes','no') THEN 'nonbinary_or_missing_result'
        WHEN NOT unit_payoff_admitted THEN 'unadmitted_unit_payout'
        WHEN face_value_dollars IS NULL OR NOT isfinite(face_value_dollars) OR face_value_dollars<>1 THEN 'missing_or_nonunit_face_value'
        WHEN settlement_value_present AND settlement_value_unit IS NULL THEN 'unadmitted_settlement_value_units'
        WHEN settlement_value_present AND (settlement_value_dollars IS NULL OR NOT isfinite(settlement_value_dollars)
          OR settlement_value_dollars<>CASE WHEN result='yes' THEN 1.0 ELSE 0.0 END) THEN 'settlement_result_conflict'
        WHEN native_category IS NOT NULL AND native_category<>'' AND native_category<>trim(native_category) THEN 'unadmitted_native_category'
        WHEN event_ticker IS NOT NULL AND event_ticker<>'' AND NOT {_valid_identifier('event_ticker')} THEN 'invalid_native_event_ticker'
        ELSE 'eligible' END market_reason
      FROM {_name(names['admission'])}""")
    con.execute(f"""CREATE TEMP VIEW {_name(names['projection'])} AS SELECT
       {_literal(inputs.SCHEMA_VERSION)}::VARCHAR schema_version,ticker,event_ticker,native_series_ticker series_ticker,
       _source source_version,{_literal(source_shard)}::VARCHAR source_shard,market_type,status,result,
       combo_state,combo_evidence_rule,market_reason,market_reason eligibility_reason,
       CASE WHEN event_ticker IS NULL OR event_ticker='' THEN 'kalshi:market:'||ticker
         WHEN {_valid_identifier('event_ticker')} THEN 'kalshi:event:'||event_ticker ELSE NULL END event_cluster,
       CASE WHEN event_ticker IS NULL OR event_ticker='' THEN 'market_fallback' ELSE 'native_event' END cluster_source,
       coalesce(nullif(native_category,''),'Unclassified') category,(native_category IS NULL OR native_category='') category_missing,
       {_literal(native_references.events_evidence_rule)}::VARCHAR category_evidence_rule,
       unit_payoff_admitted,face_value_dollars,payout_evidence_rule,face_value_basis,notional_value raw_notional_value,notional_quality,
       settlement_value raw_settlement_value,settlement_value_dollars,
       open_time opening_raw,settlement_ts endpoint_raw,endpoint_encoding,opening_seconds,endpoint_seconds
      FROM {_name(names['all_reasons'])} WHERE combo_state<>'combo'""")
    con.execute(f"CREATE TEMP VIEW {_name(names['exclusions'])} AS SELECT _source source_version,combo_state,combo_evidence_rule,market_reason, "
                f"count(*)::BIGINT AS \"rows\" FROM {_name(names['all_reasons'])} GROUP BY ALL")
    con.execute(f"CREATE TEMP VIEW {_name(names['combo_keys'])} AS SELECT ticker,source_version,source_shard,known_compound_family, "
                f"count(*)::BIGINT AS \"rows\" FROM {_name(raw_keys)} WHERE known_compound_family IS NOT NULL GROUP BY ALL")
    text_fields = [f"m.{_name(name)} {_name(name)}" if name in schema else f"NULL::VARCHAR {_name(name)}"
                   for name in ("title", "question", "yes_sub_title", "no_sub_title", "subtitle", "contract_subtitle")]
    con.execute(f"CREATE TEMP VIEW {_name(names['sports'])} AS SELECT p.*," + ",".join(text_fields) +
                f" FROM {_name(names['projection'])} p JOIN {_name(relation)} m USING(ticker) "
                f"WHERE p.combo_state='single' AND p.category={_literal(sports_category_label)}")
    return {"schema_version": SCHEMA_VERSION, "projection_relation": names["projection"], "raw_keys_relation": raw_keys,
            "exclusions_relation": names["exclusions"], "combo_keys_relation": names["combo_keys"],
            "sports_relation": names["sports"], "temporary_relations": list(names.values()), "key_health": health,
            "reference_health": references_health, "source_version_policy": {key: asdict(value) for key, value in source_version_policy.items()},
            "global_gate_required": True, "expected_global_shards": "all 80 pinned source files; supplied explicitly by caller"}


def drop_projection_views(con, projection):
    """Release only this result's temporary VIEWs after caller-persisted outputs.

    No tables or files are deleted. Validate every named view before dropping
    any, and refuse a persistent or ambiguous replacement of an owned name.
    """
    _require(isinstance(projection, Mapping) and projection.get("schema_version") == SCHEMA_VERSION,
             "metadata projection result required for cleanup")
    names = projection.get("temporary_relations")
    _require(isinstance(names, list) and len(names) == len(set(names)), "unique temporary-view inventory required")
    for name in names:
        _name(name)
        matches = con.execute("SELECT temporary FROM duckdb_views() WHERE view_name=?", [name]).fetchall()
        tables = con.execute("SELECT 1 FROM duckdb_tables() WHERE table_name=?", [name]).fetchall()
        _require(not tables and (not matches or matches == [(True,)]),
                 "cleanup refused a non-owned temporary view: " + name)
    for name in reversed(names):
        con.execute("DROP VIEW IF EXISTS " + _name(name))
