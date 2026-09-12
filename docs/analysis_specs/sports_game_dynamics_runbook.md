# NBA/NFL game dynamics: Stage 01--10 runbook

**Status:** code-approved workflow with completed, independently audited exploratory
production runs at `/mnt/data/runs/2026-09-12_nfl_game_dynamics_v3` and
`/mnt/data/runs/2026-09-12_nba_game_dynamics_v3`. Their results and artifact hashes are
recorded in
[`../runs/2026-09-12_nfl_nba_game_dynamics_v3.md`](../runs/2026-09-12_nfl_nba_game_dynamics_v3.md).
The commands below remain the required protocol for a new immutable run.

Run every command from the repository root. Follow
[`docs/workflow.md`](../workflow.md): confirm the active data vintage first,
use a new immutable run root, record the Git state and exact commands in the
run manifest, and never reuse a completed output directory. These are module
entry points; do not invoke the `.py` files directly.

The file examples use the current documented data layout. Provider caches are
run inputs too; replace the cache placeholder with a reviewed persistent cache
directory and fingerprint its contents with the run.

```sh
SPORT=nba
SPORT_RUN_ROOT=/mnt/data/runs/2026-09-11_nba_game_dynamics_replace-with-run-id
MARKETS=/mnt/data/embedding_difficulty/universe_markets.parquet
UNIVERSE_TOKENS=/mnt/data/embedding_difficulty/universe_tokens.parquet
TOKEN_MAP=/mnt/data/pipeline_data/token_map.parquet
RAW_TRADES=/mnt/data/pipeline_data/resolved_trades.parquet
EXACT_CACHE=/mnt/data/pipeline_data/block_timestamps.parquet
WALLET_FLAGS=/mnt/data/learnability/cache/wallet_flags.parquet
PROVIDER_CACHE=/mnt/data/research_cache/replace-with-reviewed-nba-provider-cache
PHASE_CONTRACT=configs/game_dynamics/nba_phase_contract_v2.json
```

For NFL, use a different fresh root and the NFL provider cache and contract:

```sh
SPORT=nfl
SPORT_RUN_ROOT=/mnt/data/runs/2026-09-11_nfl_game_dynamics_replace-with-run-id
PROVIDER_CACHE=/mnt/data/research_cache/replace-with-reviewed-nfl-provider-cache
PHASE_CONTRACT=configs/game_dynamics/nfl_phase_contract_v1.json
```

The remaining shared input variables are unchanged.

## Production execution policy

The first production run for each sport and data vintage must be executed with
the manual Stage 01--10 commands below so that each stage can be reviewed
before the next stage starts. The top-level `run_workflow` command is for
synthetic or cached dry runs, or for a fresh production rerun only after a
separate Stage 01--02 adapter/cache audit has been completed and reviewed using
the same frozen Stage-01 inputs, provider cache, and phase contract. A reviewed
audit from different inputs or cache contents does not qualify. The automated
runner always requires a fresh run ID, requires an existing cache directory,
does not accept `--refresh`, and does not replace the stage-by-stage
publication review.

## Automated immutable workflow command

The top-level runner executes the exact Stage 01--10 sequence below, including
the mandatory Stage-03 bridge, and atomically records the Git state, command
plan, environment, input fingerprints, provider-cache inventory, timestamps,
outputs, and final success, failure, or interruption status in `manifest.json`.
It refuses to reuse an existing run ID.

```sh
/home/ubuntu/venv/bin/python -m analysis.sports_game_dynamics.run_workflow \
  --sport "$SPORT" \
  --run-root /mnt/data/runs \
  --run-id "2026-09-11_${SPORT}_game_dynamics_replace-with-run-id" \
  --work-state candidate \
  --data-vintage replace-with-reviewed-vintage \
  --python-executable /home/ubuntu/venv/bin/python \
  --markets "$MARKETS" \
  --provider-cache "$PROVIDER_CACHE" \
  --universe-tokens "$UNIVERSE_TOKENS" \
  --token-map "$TOKEN_MAP" \
  --raw-trades "$RAW_TRADES" \
  --block-timestamps "$EXACT_CACHE" \
  --wallet-flags "$WALLET_FLAGS" \
  --phase-contract "$PHASE_CONTRACT"
```

The stage-by-stage commands remain below for audit and recovery planning; do
not mix outputs from different top-level run IDs.

## Stages 01--03: NBA adapter

### Stage 01: strict market candidates

```sh
python3 -m analysis.nba_game_dynamics.build_market_universe \
  --markets "$MARKETS" \
  --run-dir "$SPORT_RUN_ROOT/01_universe"
```

Inspect `candidate_diagnostics.parquet` before continuing. This is a syntactic
candidate set, not a validated game or moneyline universe.

### Stage 02: official schedule and timing audit

```sh
python3 -m analysis.nba_game_dynamics.build_game_timing \
  --candidates "$SPORT_RUN_ROOT/01_universe/candidate_markets.parquet" \
  --cache-dir "$PROVIDER_CACHE" \
  --run-dir "$SPORT_RUN_ROOT/02_timing" \
  --phase-contract "$PHASE_CONTRACT"
```

Omit `--refresh` for a reproducible cached run. Use it only for a deliberate,
reviewed provider-cache refresh. NBA historical schedules come from
`data.nba.com`; timing comes from the official NBA LiveData S3 origin because
the public CDN returns HTTP 403 from the production EC2 environment. Each
boundary must be directly observed as a parseable, absolute `timeActual` value
on the required LiveData action. Missing or invalid absolute boundaries fail
closed: scheduled tip time, game clock, neighboring actions, interpolation,
and extrapolation are not substitutes. NBA contract v2 also requires the
audited first post-start opening-tip signature, complete observed-period and
`game/end` evidence, and exact PBP-to-schedule score/winner reconciliation.
Stage 02 also requires the supplied candidate artifact and complete immutable
provider-cache tree to match the v2 audit-scope fingerprints and counts exactly;
a refreshed or substituted input requires a newly audited contract version.
The preserved v1 contract is audit history and must not be used for production.

### Stage 03: validated universe and mandatory bridge

```sh
python3 -m analysis.nba_game_dynamics.build_validated_universe \
  --candidates "$SPORT_RUN_ROOT/01_universe/candidate_markets.parquet" \
  --timing-run-dir "$SPORT_RUN_ROOT/02_timing" \
  --universe-tokens "$UNIVERSE_TOKENS" \
  --token-map "$TOKEN_MAP" \
  --run-dir "$SPORT_RUN_ROOT/03_validated"

python3 -m analysis.nba_game_dynamics.build_downstream_handoff \
  --validated-run-dir "$SPORT_RUN_ROOT/03_validated" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/03_handoff"
```

## Stages 01--03: NFL adapter

### Stage 01: strict market candidates

```sh
python3 -m analysis.nfl_game_dynamics.build_market_universe \
  --markets "$MARKETS" \
  --run-dir "$SPORT_RUN_ROOT/01_universe"
```

Inspect `candidate_diagnostics.parquet` before continuing. Duplicate valid
event candidates fail closed.

### Stage 02: ESPN schedule and timing audit

```sh
python3 -m analysis.nfl_game_dynamics.build_game_timing \
  --candidates "$SPORT_RUN_ROOT/01_universe/candidate_markets.parquet" \
  --cache-dir "$PROVIDER_CACHE" \
  --run-dir "$SPORT_RUN_ROOT/02_timing"
```

Omit `--refresh` for a reproducible cached run. Before continuing, review
`summary.json`, all match and timing exclusions, provider-cache fingerprints,
and the frozen phase-contract and ESPN-taxonomy fingerprints.

### Stage 03: validated universe and mandatory bridge

```sh
python3 -m analysis.nfl_game_dynamics.build_validated_universe \
  --candidates "$SPORT_RUN_ROOT/01_universe/candidate_markets.parquet" \
  --timing-run-dir "$SPORT_RUN_ROOT/02_timing" \
  --universe-tokens "$UNIVERSE_TOKENS" \
  --token-map "$TOKEN_MAP" \
  --run-dir "$SPORT_RUN_ROOT/03_validated"

python3 -m analysis.nfl_game_dynamics.build_downstream_handoff \
  --validated-run-dir "$SPORT_RUN_ROOT/03_validated" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/03_handoff"
```

## Mandatory Stage-03 gate

Do not point Stage 04 directly at the sport-specific
`03_validated/eligible_moneylines.parquet`. The bridge command is mandatory: it
publishes the exact shared 18-column schema in `03_handoff` and emits adapter
provenance schema v2. That provenance records provider identity and source
status, binds the phase-contract fingerprint and timing semantics, and contains
the reverified native Stage-02/03 lineage plus fingerprints for the native
manifests, summaries, provider provenance where applicable, and every cached
provider resource in `native_lineage.source_evidence`. Stages 04--10 must use
only:

- `03_handoff/eligible_moneylines.parquet`
- `03_handoff/adapter_provenance.json`

Stop if either is absent, if the handoff sport or contract differs from the
requested run, if `adapter_provenance.json` is not schema version 2, or if the
native-lineage or source-evidence fingerprints cannot be reopened and verified.

## Stages 04--10: shared pipeline

These commands are identical for NBA and NFL after setting `SPORT`,
`SPORT_RUN_ROOT`, and `PHASE_CONTRACT` consistently.

### Stage 04: exact-timestamp declaration

```sh
python3 -m analysis.sports_game_dynamics.timestamp_provenance \
  --sport "$SPORT" \
  --raw-trades "$RAW_TRADES" \
  --eligible "$SPORT_RUN_ROOT/03_handoff/eligible_moneylines.parquet" \
  --cache "$EXACT_CACHE" \
  --adapter-provenance "$SPORT_RUN_ROOT/03_handoff/adapter_provenance.json" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/04_timestamp"
```

Require `missing_blocks = 0` and `fallback_rows = 0` in
`timestamp_provenance.json`.

### Stage 05: exact sport-scoped BUY fills

```sh
python3 -m analysis.sports_game_dynamics.build_exact_trades \
  --sport "$SPORT" \
  --raw-trades "$RAW_TRADES" \
  --eligible "$SPORT_RUN_ROOT/03_handoff/eligible_moneylines.parquet" \
  --cache "$EXACT_CACHE" \
  --timestamp-declaration "$SPORT_RUN_ROOT/04_timestamp/timestamp_provenance.json" \
  --adapter-provenance "$SPORT_RUN_ROOT/03_handoff/adapter_provenance.json" \
  --phase-contract "$PHASE_CONTRACT" \
  --wallet-flags "$WALLET_FLAGS" \
  --run-dir "$SPORT_RUN_ROOT/05_exact"
```

Review replay deduplication and scoped input/output reconciliation in
`build_audit.json`.

### Stage 06: quarter-phase assignment

```sh
python3 -m analysis.sports_game_dynamics.build_phase_dataset \
  --sport "$SPORT" \
  --eligible "$SPORT_RUN_ROOT/03_handoff/eligible_moneylines.parquet" \
  --exact-trades "$SPORT_RUN_ROOT/05_exact/exact_trades.parquet" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/06_phase"
```

Review price, buyer-bot, post-final, and boundary-sensitivity counts in
`phase_summary.json`.

### Stage 07: dual pregame closes

```sh
python3 -m analysis.sports_game_dynamics.build_dual_closes \
  --sport "$SPORT" \
  --eligible "$SPORT_RUN_ROOT/03_handoff/eligible_moneylines.parquet" \
  --exact-trades "$SPORT_RUN_ROOT/05_exact/exact_trades.parquet" \
  --run-dir "$SPORT_RUN_ROOT/07_closes"
```

Review primary and sensitivity close coverage in `close_summary.json`.

### Stage 08: fixed-bin calibration

```sh
python3 -m analysis.sports_game_dynamics.estimate_calibration \
  --sport "$SPORT" \
  --game-closes "$SPORT_RUN_ROOT/07_closes/game_closes.parquet" \
  --phase-trades "$SPORT_RUN_ROOT/06_phase/phase_trades.parquet" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/08_calibration"
```

Require 22 closing rows, 11 paired-close rows, and 100 phase-profile rows.
Treat all estimates as exploratory and preserve every suppressed row.

### Stage 09: fixed D1/D10 tails

```sh
python3 -m analysis.sports_game_dynamics.estimate_flb_tails \
  --sport "$SPORT" \
  --calibration-run-dir "$SPORT_RUN_ROOT/08_calibration" \
  --game-closes "$SPORT_RUN_ROOT/07_closes/game_closes.parquet" \
  --phase-trades "$SPORT_RUN_ROOT/06_phase/phase_trades.parquet" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/09_tails"
```

Require all 12 fixed tail rows and review D1/D10 support and suppression before
interpreting signs.

### Stage 10: deterministic offline report

```sh
python3 -m analysis.sports_game_dynamics.render_flb_report \
  --sport "$SPORT" \
  --calibration-run-dir "$SPORT_RUN_ROOT/08_calibration" \
  --tail-run-dir "$SPORT_RUN_ROOT/09_tails" \
  --timestamp-declaration "$SPORT_RUN_ROOT/04_timestamp/timestamp_provenance.json" \
  --phase-contract "$PHASE_CONTRACT" \
  --run-dir "$SPORT_RUN_ROOT/10_report"
```

The final artifacts are `sports_flb_report.html` and
`report_manifest.json`. Before citing the report, apply the publication gate in
[`docs/workflow.md`](../workflow.md): verify fingerprints, schemas, cardinality,
suppression, lineage, clean-code reproducibility, and the immutable run ID.
