"""Step 1: rigorous exploration of the Kalshi parquet on EC2.

Produces a JSON summary used to write `kalshi_exploration.md`. No mutations.

Sections:
  1a. Structural inventory (distinct counts, prefix taxonomy, samples)
  1b. Format inventory (date / strike / time-of-day patterns)
  1c. Multi-snapshot audit (raw_rows_for_ticker distribution, question stability)
"""
import json
import re
import random
from collections import Counter
from pathlib import Path

import duckdb

PARQUET = "/mnt/data/kalshi/kalshi_contract_questions_dates_available.parquet"
OUT_JSON = Path("/home/ubuntu/kalshi/kalshi_exploration.json")
SEED = 7

con = duckdb.connect()
con.execute("SET memory_limit='40GB'")
con.execute("SET threads=8")

result = {}

# ============================================================================
# 1a. STRUCTURAL INVENTORY
# ============================================================================
print(">>> 1a. structural inventory")
r = con.execute(f"""
SELECT
  COUNT(*)                                                AS rows,
  COUNT(DISTINCT ticker)                                  AS n_ticker,
  COUNT(DISTINCT event_ticker)                            AS n_event_ticker,
  COUNT(DISTINCT question)                                AS n_question,
  COUNT(DISTINCT regexp_extract(ticker, '^[A-Z0-9]+', 0)) AS n_prefix
FROM '{PARQUET}'
""").fetchone()
result["counts"] = {
    "rows": r[0], "n_ticker": r[1], "n_event_ticker": r[2],
    "n_question": r[3], "n_prefix_alphanumeric": r[4],
}
print(f"    rows={r[0]:,} tickers={r[1]:,} events={r[2]:,} questions={r[3]:,} prefixes={r[4]:,}")

print(">>> distinct ticker prefixes (with counts)")
prefix_df = con.execute(f"""
SELECT regexp_extract(ticker, '^[A-Z0-9]+', 0)         AS prefix,
       COUNT(DISTINCT ticker)                          AS n_tickers,
       COUNT(DISTINCT event_ticker)                    AS n_events,
       COUNT(DISTINCT question)                        AS n_questions
FROM '{PARQUET}'
GROUP BY 1
ORDER BY n_tickers DESC
""").fetchdf()
result["prefix_counts"] = {
    "n_total": len(prefix_df),
    "top_30": prefix_df.head(30).to_dict("records"),
    "bottom_30": prefix_df.tail(30).to_dict("records"),
}
print(f"    {len(prefix_df):,} distinct prefixes")

print(">>> sample questions per top-30 + bottom-30 prefix")
samples_by_prefix = {}
for rec in prefix_df.head(30).to_dict("records") + prefix_df.tail(30).to_dict("records"):
    pfx = rec["prefix"]
    if pfx in samples_by_prefix:
        continue
    qs = con.execute(f"""
    SELECT DISTINCT ticker, event_ticker, question, contract_subtitle, yes_sub_title,
                    market_type, strike_type, floor_strike, cap_strike
    FROM '{PARQUET}'
    WHERE regexp_extract(ticker, '^[A-Z0-9]+', 0) = '{pfx}'
    USING SAMPLE 10
    """).fetchdf()
    samples_by_prefix[pfx] = qs.to_dict("records")
result["samples_by_prefix"] = samples_by_prefix

print(">>> sibling-contract audit (50 random event_tickers)")
sibling_audit = con.execute(f"""
WITH events AS (
  SELECT DISTINCT event_ticker FROM '{PARQUET}' USING SAMPLE 50 (reservoir, {SEED})
)
SELECT e.event_ticker,
       COUNT(DISTINCT k.ticker)   AS n_tickers,
       COUNT(DISTINCT k.question) AS n_distinct_questions
FROM events e JOIN '{PARQUET}' k USING (event_ticker)
GROUP BY 1
ORDER BY n_tickers DESC
""").fetchdf()
result["sibling_audit"] = sibling_audit.to_dict("records")

# Pull full content for first 5 events
print(">>> sibling content for first 5 events")
sibling_detail = []
for et in sibling_audit["event_ticker"].head(5).tolist():
    rows = con.execute(f"""
    SELECT DISTINCT ticker, question, yes_sub_title, contract_subtitle,
                    floor_strike, cap_strike, strike_type
    FROM '{PARQUET}'
    WHERE event_ticker = '{et}'
    LIMIT 10
    """).fetchdf()
    sibling_detail.append({"event_ticker": et, "rows": rows.to_dict("records")})
result["sibling_detail"] = sibling_detail

# ============================================================================
# 1b. FORMAT INVENTORY (10K random questions)
# ============================================================================
print(">>> 1b. format inventory on 10K random questions")
sample_qs = con.execute(f"""
SELECT DISTINCT question FROM '{PARQUET}'
USING SAMPLE 10000 (reservoir, {SEED})
""").fetchdf()["question"].tolist()
result["format_sample_size"] = len(sample_qs)

# --- Date patterns ---
# Long-form: "Jan 14, 2025", "January 14, 2025", "Jan 14"
# Compact:   "2024-12-23T15:00:00Z", "12/14/2024"
# Quarter:   "Q1 2025", "Q4-25"
# Time-of-day attached: "at 12pm EST", "at 3pm EDT"
date_patterns = {
    r"\b[A-Z][a-z]{2,8}\s+\d{1,2},?\s*\d{4}\b":            "long_month_day_year",   # Jan 14, 2025
    r"\b[A-Z][a-z]{2,8}\s+\d{1,2}\b(?!,)":                 "month_day_no_year",     # Jan 14
    r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}":              "iso_datetime",          # 2024-12-23T15:00:00
    r"\b\d{4}-\d{2}-\d{2}\b":                              "iso_date",              # 2024-12-23
    r"\b\d{1,2}/\d{1,2}/\d{2,4}\b":                        "slash_date",            # 12/14/2024
    r"\bQ[1-4][\s\-]*\d{2,4}\b":                           "quarter",               # Q1 2025
    r"\b\d{1,2}(?:am|pm)\s*[A-Z]{2,4}\b":                  "time_with_tz",          # 12pm EST
    r"\b\d{1,2}:\d{2}\s*(?:am|pm)\b":                      "hhmm_ampm",             # 3:30 pm
    r"\bend\s+of\s+\d{4}\b":                               "end_of_year",           # end of 2024
}
date_hits = {label: 0 for label in date_patterns.values()}
for q in sample_qs:
    for rgx, label in date_patterns.items():
        if re.search(rgx, q, re.IGNORECASE):
            date_hits[label] += 1
result["date_format_hits"] = date_hits

# Questions with NO recognized date
no_date = sum(1 for q in sample_qs
              if not any(re.search(rgx, q, re.IGNORECASE) for rgx in date_patterns))
result["questions_with_no_date"] = no_date

# --- Strike/number patterns ---
strike_patterns = {
    r"\$\s*\d{1,3}(?:,\d{3})*(?:\.\d+)?":                  "dollar_amount",         # $5,525.99
    r"\b\d{1,3}(?:,\d{3})+(?:\.\d+)?\b":                   "comma_grouped",         # 5,549.99
    r"\b\d+\.\d{2,}\b":                                    "decimal_2plus",         # 1.07339
    r"\b\d+\.\d\b":                                        "decimal_1",             # 25.5
    r"\b\d+\s*(?:bps|bp)\b":                               "bps",                   # 25 bps
    r"\b\d+(?:\.\d+)?\s*[KMB]\b":                          "abbreviated_number",    # 100K
    r"\b\d+(?:\.\d+)?%\b":                                 "percentage",            # 12.5%
    r"\bbetween\b.*\band\b":                               "range_between",         # between X and Y
    r"\b(?:above|below|over|under)\b\s*\$?[\d,.]+":        "threshold_above_below", # above $5,525
    r"\b\d+\s*or\s+(?:above|more|higher|below|less|lower)\b": "or_above_below",
}
strike_hits = {label: 0 for label in strike_patterns.values()}
for q in sample_qs:
    for rgx, label in strike_patterns.items():
        if re.search(rgx, q, re.IGNORECASE):
            strike_hits[label] += 1
result["strike_format_hits"] = strike_hits

no_strike = sum(1 for q in sample_qs
                if not any(re.search(rgx, q, re.IGNORECASE) for rgx in strike_patterns))
result["questions_with_no_strike"] = no_strike

# Question shape: count digits
digit_count_dist = Counter(min(len(re.findall(r"\d", q)), 20) for q in sample_qs)
result["digit_count_distribution"] = {
    f"{k}_digits": v for k, v in sorted(digit_count_dist.items())
}

# Plain English: questions with 0 digits
result["questions_with_zero_digits"] = digit_count_dist.get(0, 0)

# 30 examples of zero-digit questions
zero_digit_examples = [q for q in sample_qs if not re.search(r"\d", q)][:30]
result["zero_digit_examples"] = zero_digit_examples

# 20 questions with > 5 numbers in them (potentially complex / multi-strike)
many_num_examples = [q for q in sample_qs if len(re.findall(r"\b\d+(?:\.\d+)?\b", q)) > 5][:20]
result["many_number_examples"] = many_num_examples

# ============================================================================
# 1c. MULTI-SNAPSHOT AUDIT
# ============================================================================
print(">>> 1c. multi-snapshot audit")
snapshot_dist = con.execute(f"""
SELECT raw_rows_for_ticker, COUNT(*) AS n_tickers
FROM (SELECT DISTINCT ticker, raw_rows_for_ticker FROM '{PARQUET}')
GROUP BY 1 ORDER BY 1
""").fetchdf()
result["snapshot_distribution"] = snapshot_dist.to_dict("records")

# Verify question text is stable per ticker
print(">>> question stability check (200 random tickers)")
qstab = con.execute(f"""
WITH t AS (SELECT DISTINCT ticker FROM '{PARQUET}' USING SAMPLE 200 (reservoir, {SEED}))
SELECT ticker, COUNT(DISTINCT question) AS n_distinct_question
FROM '{PARQUET}' k JOIN t USING (ticker)
GROUP BY 1
""").fetchdf()
qstab_dist = qstab["n_distinct_question"].value_counts().to_dict()
result["question_stability"] = {str(k): int(v) for k, v in qstab_dist.items()}

# Save
OUT_JSON.write_text(json.dumps(result, default=str, indent=2))
print(f">>> wrote {OUT_JSON} ({OUT_JSON.stat().st_size/1024:.0f} KB)")
print(">>> done")
