# References inside frozen release archives

The four earlier replication capsules are preserved as deterministic
`records/original_release_capsule.zip` archives under their respective study roots:
current exchange-excluded Polymarket, native Kalshi, comparison full-date Polymarket
and comparison cutoff Polymarket. Their original documentation/source is not exposed
as current guidance beside the ready-to-read reports and summaries.

An audit found **52 unresolved relative links inside those frozen capsules**:
12 exchange-excluded, 21 Kalshi, 10 full-date and 9 cutoff. These are archived
references, not live handoff-navigation links. Their 19 documentation counterparts
are reachable below in the current `code/` tree. The capsules and their hash-bound
bodies were not rewritten and no path aliases were inserted.

## Documentation crosswalk

Paths in the left column are normalized from the old capsule root. Kalshi's frozen
source snapshot adds a `source/` prefix to its `docs/` paths. Repeated links across
capsules share one row. The exceptional `analysis_specs/` row records a misplaced
relative link in `release_notes/run_record.md`.

| Archived unresolved target | Working current documentation |
|---|---|
| `docs/runs/2026-10-14_kaushik_exchange_wallet_audit_v2.md` | [Exchange-wallet count audit](../runs/2026-10-14_kaushik_exchange_wallet_audit_v2.md) |
| `.agents/skills/data-first-reporting/SKILL.md` | [Data-first reporting instructions](../../.agents/skills/data-first-reporting/SKILL.md) |
| `docs/runs/2026-10-09_wallet_attribution_repair_v1.md` | [Wallet-attribution repair](../runs/2026-10-09_wallet_attribution_repair_v1.md) |
| `docs/runs/2026-10-10_sports_wallet_adoption_v1.md` | [Nine-sport wallet-filter adoption](../runs/2026-10-10_sports_wallet_adoption_v1.md) |
| `docs/analysis_specs/multisport_game_dynamics_v1.md` | [Multisport game-dynamics contract](../analysis_specs/multisport_game_dynamics_v1.md) |
| `docs/analysis_specs/flb_time_regressions_v3.md` | [Filtered/all time-regression contract](../analysis_specs/flb_time_regressions_v3.md) |
| `docs/analysis_specs/polymarket_data_certification_v1.json` | [Bounded data-certification contract](../analysis_specs/polymarket_data_certification_v1.json) |
| `docs/analysis_specs/tennis_timing_wallet_exits_v1.md` | [Tennis timing and wallet-sequence contract](../analysis_specs/tennis_timing_wallet_exits_v1.md) |
| `docs/analysis_specs/sports_profit_taking_v1.md` | [Profit-taking contract](../analysis_specs/sports_profit_taking_v1.md) |
| `analysis_specs/kaushik_exchange_wallet_exclusion_v1.md` | [Exchange-wallet exclusion contract](../analysis_specs/kaushik_exchange_wallet_exclusion_v1.md) |
| `docs/runs/2026-09-20_atp_swing_audit_v2.md` | [Historical ATP swing audit](../runs/2026-09-20_atp_swing_audit_v2.md) |
| `docs/runs/2026-09-20_multisport_terminal_reversal_audit_v2.md` | [Historical terminal-reversal audit](../runs/2026-09-20_multisport_terminal_reversal_audit_v2.md) |
| `docs/runs/2026-09-20_flb_time_regressions_filtered_all_v1.md` | [Historical filtered/all time-regression run](../runs/2026-09-20_flb_time_regressions_filtered_all_v1.md) |
| `docs/analysis_specs/flb_time_regressions_v1.md` | [Initial time-regression contract](../analysis_specs/flb_time_regressions_v1.md) |
| `docs/runs/2026-09-15_flb_time_regressions_v1.md` | [Initial time-regression run](../runs/2026-09-15_flb_time_regressions_v1.md) |
| `docs/runs/2026-09-15_multisport_game_dynamics_v2.md` | [Historical multisport v2 run](../runs/2026-09-15_multisport_game_dynamics_v2.md) |
| `docs/runs/2026-09-15_multisport_game_dynamics_v1.md` | [Historical multisport v1 run](../runs/2026-09-15_multisport_game_dynamics_v1.md) |
| `docs/runs/2026-09-13_sports_late_game_tail_diagnostics_v2.md` | [Historical late-game tail diagnostics](../runs/2026-09-13_sports_late_game_tail_diagnostics_v2.md) |
| `docs/runs/2026-09-12_nfl_nba_game_dynamics_v3.md` | [Historical NFL/NBA production record](../runs/2026-09-12_nfl_nba_game_dynamics_v3.md) |

This crosswalk repairs discoverability, not historical source identity. Current
documentation may contain later repairs, qualifications or completion notes and is
not a byte-identical substitute for the producer/auditor revision recorded by an old
run. Exact reproduction still requires that revision and its original admitted
private inputs. Historical findings do not become repaired-data results merely
because a current documentation counterpart exists.
