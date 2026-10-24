# Research handoff

**Index date: 2026-10-24.** This is navigation for the research code and accepted
artifacts, not a new estimation generation or a certificate of complete exchange
history. Finished packages use the private Dropbox parent
`Polymarket Data and Code/joseph_code/`, with an immutable dated release below it.
Actual upload/readback completion belongs in that release's publication receipt.

## Choose a study

| Workstream | Start here | Handoff status |
|---|---|---|
| Polymarket replication | [Exchange-excluded full-date run](../runs/2026-10-14_kaushik_exchange_filtered_rerun_v1.md) | Current replication. Older cutoff and unexcluded full-date releases are preserved comparisons. |
| Kalshi replication | [Native Kalshi run](../runs/2026-10-16_kaushik_kalshi_replication_v1.md) | Current native-print study. No combinations or inferred wallet/maker/taker labels. |
| Nine-sport game dynamics | [Corrected-wallet adoption](../runs/2026-10-10_sports_wallet_adoption_v1.md) | Current repaired-filtered and all-trades summaries; three older flag/coverage regimes are comparisons. |
| Tennis timing and wallet exits | [Tennis investigation](../runs/2026-10-02_tennis_wallet_investigation_v1.md) | Frozen diagnostic cohort and timing qualifications. |
| Profit taking, two observation grains | [Two-grain investigation](../runs/2026-10-02_sports_profit_taking_two_grain_v1.md) | Frozen own-action/execution accounting investigation, not a causal identification result. |
| Other terminal-pattern explanations | [Terminal diagnostics](../runs/2026-10-03_terminal_pattern_diagnostics_v1.md) | Compact diagnostic tables; no separate report was authored. |
| Data integrity and repairs | [MLB coverage rebuild](../runs/2026-10-09_mlb_unfiltered_rebuild_v1.md), [wallet repair](../runs/2026-10-09_wallet_attribution_repair_v1.md), [exchange-wallet counts](../runs/2026-10-14_kaushik_exchange_wallet_audit_v2.md) | Separate bounded audits, not blanket whole-history certification. |
| Earlier time regressions | [September filtered/all study](../runs/2026-09-20_flb_time_regressions_filtered_all_v1.md) | Historical comparison, before later MLB coverage and wallet-filter repairs. |

The machine-readable [study catalog](study_catalog.json) gives stable IDs, source
modules, specifications, run records and release locations. It is the place to add
the next version, not a reason to overwrite an old one.

## Package layout

The release has one current `code/` tree with the existing import layout. Study
folders under `releases/` hold accepted reports, figures, aggregate summaries,
bounded event/clock/exclusion metadata and receipts. `comparisons/` and `historical/`
are explicitly noncurrent. Frozen prior
source capsules preserve exact bytes where supplied; their original absolute
paths and as-of-publication wording are provenance, not portable defaults.
`records/` contains the selected inventory, checks and source/environment identity.

Raw trade tapes, per-wallet histories, FIFO records, trade-level/wallet-level
membership rows, fitted score caches,
provider response caches and credentials are not sharing payloads. Full code is
not the same as full data. [Data access](data_access.md) identifies what must be
restored or supplied before real-data reproduction.

## Use or extend it

- [Reproduce](reproduce.md): fixture validation, report consumption and production
  rebuild are three different operations.
- [Continue the project](continue.md): where new code goes and how to publish the
  next immutable version.
- [Repository map](../repository_map.md): module responsibilities and entrypoints.
- [Methods reference](../methods_reference.md): trust rules and scoped defaults;
  the chosen study's approved contract defines its explicit overrides.

No analysis modules were relocated for this handoff. No estimator, coefficient,
sample rule or completed scientific manifest was changed by packaging.
