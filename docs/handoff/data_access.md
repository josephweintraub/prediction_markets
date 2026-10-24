# Data and external dependencies

This handoff shares source and aggregate evidence, not billion-row trade histories
or credentials. Storage paths below are private dependencies/provenance, not files
expected to exist in the portable code folder. Access must be granted separately.

| Input family | Location or discovery route | Important distinction |
|---|---|---|
| Polymarket native events, ROOT/CLEAN and token spine | Canonical `/mnt/data`; [methods reference](../methods_reference.md) and selected run's exact input manifest | Original CLEAN, repaired copies and native own-actions are different products. An overloaded trade `conditionId` is a token key, not a Gamma market ID. |
| Repaired wallet identity and flags | [Repair run](../runs/2026-10-09_wallet_attribution_repair_v1.md), [adoption run](../runs/2026-10-10_sports_wallet_adoption_v1.md) | Do not redirect a legacy flag path or combine repaired identity with old codes/bases silently. |
| Native Kalshi tape/metadata | Private Dropbox `Polymarket Data and Code/kalshi/`; exact archive manifests/admission plan in the [native run record](../runs/2026-10-16_kaushik_kalshi_replication_v1.md) | Public prints lack wallet identities; combo exclusion and finalized-market admission remain explicit. |
| Sport identities, results and event clocks | Selected study's saved provider index, cache and acceptance manifests under `/mnt/data/runs/` | Do not refetch a new provider vintage or replace scheduled/qualified clocks with assumed actual times. |
| FIFO, own-action and execution proofs | [Profit-taking run](../runs/2026-10-02_sports_profit_taking_two_grain_v1.md) | Full market/wallet/native receipt and opening-inventory dependencies are not included. Aggregate attribution is not causal identification. |
| Historical embedding/learnability inputs | `/mnt/data/embedding_difficulty/`, `/mnt/data/learnability/`, original historical run records | Legacy flags and positional embeddings are not repaired-generation inputs. |
| Archived trade backups and quote ticks | [Storage handling in the repair run](../runs/2026-10-09_wallet_attribution_repair_v1.md) | Restore from the verified private Dropbox archive before a dependent quote/history workflow. Do not assume old EBS paths remain populated. |

An original manifest may bind files intentionally omitted from a compact summary
export, including score arrays, market memberships or source exclusions. The handoff
inventory checks only the explicitly delivered files; it does not falsify or replace
the original manifest. Preserve that difference in further work.

## Credentials and infrastructure

No SSH key, AWS profile contents, RPC token, API secret or rclone configuration is
part of the package. Existing endpoint configuration reads private environment or
key files. The stale Mac pipeline configuration is not the exported canonical file.
Arrange access with the project owner; never paste keys into shared documentation.

Infrastructure procedures are in [EC2 setup](../EC2_SETUP.md). Production currently
requires the canonical Linux checkout and the verified data-volume mount. Only the
root orchestrator manages lifecycle, and every task that uses the instance must
verify it is stopped after all compute and transfers finish.

## What the handoff does not establish

Packaging does not establish whole-history native completeness, economically unique
fills in every archive, causal profit taking, independent actual timing for every
sport, or full clean-install reproduction. Resolution-censoring and each frozen
study's scope still apply. The purpose is to make the evidence and those boundaries
easy to locate, not to upgrade historical claims.
