"""Stage 2 LLM batch for Kalshi prefix-level classification.

Reuses Polymarket `SYSTEM_A` + `FEWSHOT_A` verbatim from stage2_test.py.
One request per ticker prefix (~7,166 requests for the full batch). Each
request gets:

    event_template:  <prefix>
    market_template: <representative normalized question>
    question:        <representative original question>

The same 11-field extraction schema as Polymarket comes back.

Usage:
    python stage2_kalshi_llm.py validation   # 50-prefix smoke batch (~$0.10)
    python stage2_kalshi_llm.py full         # 7,166-prefix production batch (~$15)
"""
import os, sys, json, time, math
from pathlib import Path

import pandas as pd
import anthropic
from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
from anthropic.types.messages.batch_create_params import Request

sys.path.insert(0, "/home/ubuntu/kalshi")
from kalshi_prompt import SYSTEM_A, FEWSHOT_A, MODEL, _full_system, _parse_response

# Config
INPUT_PARQUET = Path("/mnt/data/kalshi/kalshi_prefix_pairs.parquet")
OUT_DIR       = Path("/mnt/data/kalshi")
MAX_REQS_PER_BATCH = 100_000
POLL_SEC      = 30

KEY_PATH = Path.home() / ".anthropic_api_key"
os.environ["ANTHROPIC_API_KEY"] = KEY_PATH.read_text().strip()
client = anthropic.Anthropic()

SYS = _full_system(SYSTEM_A, FEWSHOT_A)
print(f">>> system prompt: {len(SYS):,} chars (~{len(SYS)//4:,} tokens)")


_WS = __import__("re").compile(r"\s+")


def _clean_ws(s: str) -> str:
    """Collapse runs of whitespace in raw Kalshi questions (e.g., double-space
    'Bitcoin price  on ...'). Cosmetic; keeps LLM input legible."""
    return _WS.sub(" ", (s or "").strip())


def build_request(custom_id: str, ev_t: str, mk_t: str, question: str) -> Request:
    user = (f"event_template:  {ev_t}\n"
            f"market_template: {mk_t}\n"
            f"question:        {_clean_ws(question)}")
    return Request(
        custom_id=custom_id,
        params=MessageCreateParamsNonStreaming(
            model=MODEL,
            max_tokens=600,
            temperature=0.0,
            system=[{"type": "text", "text": SYS,
                     "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user}],
        ),
    )


def pick_validation_50(df: pd.DataFrame) -> pd.DataFrame:
    """Hand-pick 50 diverse prefixes for the validation batch."""
    # Top 20 by tickers (covers the big families, including all 5 MVE)
    top20 = df.head(20)
    # 10 sports / player-prop families
    sports_prefixes = ["KXNHLGOAL", "KXNHLFIRSTGOAL", "KXNFLANYTD", "KXNFLFIRSTTD",
                       "KXNCAAMBSPREAD", "KXNCAAMBTOTAL", "KXNHLPTS", "KXATPMATCH",
                       "KXWTAMATCH", "KXPGATOUR"]
    sports = df[df["event_template"].isin(sports_prefixes)]
    # 5 plain-English topical prefixes (the user wants Polymarket-comparable categories)
    topical_prefixes = ["KXOSCARWINNERS", "KXCITIESWEATHER", "KXSERIEA",
                       "KXBUNDESLIGA", "KXCANADAELECT"]
    topical = df[df["event_template"].isin(topical_prefixes)]
    # 15 random from the long tail (1-ticker prefixes, mostly custom markets)
    long_tail = df[df["prefix_n_tickers"] <= 5].sample(n=15, random_state=42)
    out = pd.concat([top20, sports, topical, long_tail]).drop_duplicates("event_template").head(50)
    return out.reset_index(drop=True)


def submit_validation():
    df = pd.read_parquet(INPUT_PARQUET)
    sample = pick_validation_50(df)
    print(f">>> validation sample: {len(sample)} prefixes")
    print(sample[["event_template", "prefix_n_tickers", "prefix_n_templates"]].to_string())
    sample.to_parquet(OUT_DIR / "kalshi_validation_input.parquet", index=False)

    reqs = [build_request(f"v-{i}", r.event_template, r.market_template, r.question)
            for i, r in enumerate(sample.itertuples(index=False))]
    print(f">>> submitting validation batch ({len(reqs)} reqs)")
    batch = client.messages.batches.create(requests=reqs)
    print(f"    -> {batch.id} ({batch.processing_status})")
    state = [{"batch_id": batch.id, "n_requests": len(reqs), "offset": 0,
              "tag": "validation", "status": batch.processing_status}]
    (OUT_DIR / "kalshi_validation_state.json").write_text(json.dumps(state, indent=2))
    return state, sample


def submit_full():
    df = pd.read_parquet(INPUT_PARQUET)
    print(f">>> full batch: {len(df)} prefixes")
    reqs = [build_request(f"f-{i}", r.event_template, r.market_template, r.question)
            for i, r in enumerate(df.itertuples(index=False))]

    sample_bytes = len(json.dumps(reqs[0]))
    print(f"    per-req JSON: ~{sample_bytes:,} bytes "
          f"(~{len(reqs)*sample_bytes/1024/1024:.1f} MB total)")

    # If too large to fit one batch, split. Polymarket precedent: 256 MB limit,
    # 100K request limit. 7K requests at ~13KB each = ~90 MB → 1 batch.
    if len(reqs) * sample_bytes > 240 * 1024 * 1024 or len(reqs) > MAX_REQS_PER_BATCH:
        raise RuntimeError("batch too large to submit as one — implement split")

    print(">>> submitting full batch")
    batch = client.messages.batches.create(requests=reqs)
    print(f"    -> {batch.id} ({batch.processing_status})")
    state = [{"batch_id": batch.id, "n_requests": len(reqs), "offset": 0,
              "tag": "full", "status": batch.processing_status}]
    (OUT_DIR / "kalshi_full_state.json").write_text(json.dumps(state, indent=2))
    return state, df


def poll_all(state, tag):
    print(f">>> polling {len(state)} batches every {POLL_SEC}s")
    done = {e["batch_id"] for e in state if e.get("status") == "ended"}
    t0 = time.time()
    while len(done) < len(state):
        for e in state:
            if e["batch_id"] in done: continue
            b = client.messages.batches.retrieve(e["batch_id"])
            e["status"] = b.processing_status
            if b.processing_status == "ended":
                done.add(e["batch_id"])
                rc = b.request_counts
                e["request_counts"] = {"succeeded": rc.succeeded, "errored": rc.errored}
                print(f"    [{int(time.time()-t0):>5}s] {e['batch_id']}: ENDED "
                      f"({rc.succeeded:,} ok, {rc.errored} err)", flush=True)
                (OUT_DIR / f"kalshi_{tag}_state.json").write_text(json.dumps(state, indent=2))
        if len(done) < len(state):
            time.sleep(POLL_SEC)
    print(f">>> all batches complete in {(time.time()-t0)/60:.1f} min")


def fetch_results(state, sample_df, tag):
    print(">>> downloading results")
    in_t = out_t = cw = cr = 0
    n_succ = n_err = 0
    rows = []
    pair_records = sample_df.to_dict("records")
    for e in state:
        bid = e["batch_id"]
        print(f"    {bid} ...", flush=True)
        for r in client.messages.batches.results(bid):
            idx = int(r.custom_id.split("-")[1])
            base = pair_records[idx]
            if r.result.type == "succeeded":
                msg = r.result.message
                u = msg.usage
                in_t  += u.input_tokens or 0
                out_t += u.output_tokens or 0
                cw    += (u.cache_creation_input_tokens or 0)
                cr    += (u.cache_read_input_tokens or 0)
                text = next((b for b in msg.content if getattr(b,"type",None)=="text"), None)
                if text is None:
                    extracted = {"_error": "no_text_content"}
                    n_err += 1
                else:
                    raw = text.text.strip()
                    try:
                        extracted = _parse_response(raw)
                        n_succ += 1
                    except Exception as e2:
                        extracted = {"_error": f"parse_failed: {type(e2).__name__}: {e2}",
                                     "_raw": raw[:500]}
                        n_err += 1
            else:
                extracted = {"_error": r.result.type}
                n_err += 1
            rows.append({
                "event_template": base["event_template"],
                "market_template": base["market_template"],
                "question":        base["question"],
                "extracted":       extracted,
            })

    out_jsonl = OUT_DIR / f"kalshi_{tag}_extracted.jsonl"
    with open(out_jsonl, "w") as f:
        for r in rows: f.write(json.dumps(r) + "\n")
    print(f">>> wrote {out_jsonl}")

    cost_raw = (in_t*3.00 + cw*3.75 + cr*0.30 + out_t*15.00) / 1e6
    cost_batched = cost_raw * 0.5
    print()
    print(f"Successful:     {n_succ:,}")
    print(f"Errored:        {n_err:,}")
    print(f"Input tokens:   {in_t:,}")
    print(f"Output tokens:  {out_t:,}")
    print(f"Cache reads:    {cr:,}")
    print(f"Cache writes:   {cw:,}")
    print(f"Cost (raw):     ${cost_raw:.3f}")
    print(f"Cost (batched): ${cost_batched:.3f}")


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "validation"
    state_path = OUT_DIR / f"kalshi_{mode}_state.json"
    input_path = OUT_DIR / f"kalshi_{mode}_input.parquet"

    if state_path.exists():
        state = json.loads(state_path.read_text())
        sample_df = pd.read_parquet(input_path) if input_path.exists() else \
                    pd.read_parquet(INPUT_PARQUET)
        print(f">>> resuming from existing state: {len(state)} batches")
    else:
        if mode == "validation":
            state, sample_df = submit_validation()
        elif mode == "full":
            state, sample_df = submit_full()
            sample_df.to_parquet(input_path, index=False)
        else:
            raise SystemExit(f"unknown mode {mode!r}")

    poll_all(state, mode)
    fetch_results(state, sample_df, mode)


if __name__ == "__main__":
    main()
