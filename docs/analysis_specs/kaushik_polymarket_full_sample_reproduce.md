# Reproducing the Kaushik Polymarket full available-date sample

Status: staged reproduction guide, 11 October 2026. Full-date inputs v2 have
reopened and been accepted, the cutoff-subset audit has passed, and fresh sports
metadata v3 has reopened and been accepted. Estimation v3 and saved-score audit
v1 have completed and been accepted under
committed H14 with fresh reviewed source/metadata/preflight bindings and actual
physical optimizer-policy readbacks and unchanged caps. V3 completion is
run-specific capacity evidence, not a general guarantee. Removing the raw-cache sort did not solve the preserved
v1/v2 estimation failures. Read-only join-plan diagnostics, local/independent QA
and 145 production-runtime fixture tests passed before H14 admission. Root's
verified shutdown at 2026-10-11 15:58:09 UTC is historical; it subsequently
restarted for v3. Canonical source remained frozen through the body/audit.
Estimation v1/v2 failures and verified shutdowns remain preserved history.
Independent numerical release QA and report v1 numerical/visual QA are accepted.
Final source release/push, finished-bundle assembly/QA, shared publication and
current-run shutdown remain pending. This guide
records stage admission, reconciliation and failure evidence, not estimated
findings. Root must append later accepted bindings and execution receipts as
those stages finish.

The [frozen full-sample specification](kaushik_polymarket_full_sample_v1.md)
inherits the [cutoff scientific specification](kaushik_polymarket_replication_v1.md)
except for execution-date scope. The [cutoff reproduction guide](kaushik_polymarket_replication_reproduce.md)
continues to describe that separate accepted run; do not overwrite or relabel its
inputs, corrected estimates v5 or report v2. The
[methods reference](../methods_reference.md) remains authoritative for data and
censoring qualifications.

## Scientific and source boundary

The full-date contract explicitly sets `sample_scope: all_available_dates` and
null cutoff fields. It retains every available execution date in the admitted
source, subject to the original validity predicates; it introduces no replacement
upper bound or lower bound. Former post-cutoff records must pass every non-date
eligibility rule. Full-date `scope_rows` includes invalid source records before
eligibility exclusions. Cutoff-relative future-ending diagnostics are null/not
applicable and omitted from the full-date report, never shown as zero.

Retain equal archive-record weights, taker BUY plus verified complemented taker
SELL, unrounded normalized binary64 prices, eventual binary payouts, the native
category taxonomy, exact-price/UTC-month controls, duration proxies/inequalities,
all original models, joint event/game CR0 uncertainty and the original support
guards. No bot, up/down, lifecycle or extreme-price filter is added. The full
writeup includes every original table, pooled/sport-specific figure and archive
BUY convention. The renderer performs no estimation.

Inputs use the same repaired CLEAN generation and 44 monthly partitions,
November 2022 through June 2026, under
`/mnt/data/runs/2026-10-09_polymarket_wallet_attribution_repair_v1/clean/`, with the
same token map, token/outcome spine, native metadata and category map. EC2 archive
`conditionId` is a token identifier. Do not replace sources, refresh resolutions,
redirect shared paths or change join keys for this reproduction. The retained
resolved-source vintage, approximate execution clocks and omission of unresolved
long-horizon claims remain qualifications; full-date eligibility is not an
exchange census or evidence of a recency trend.

The retained nine-sport provider inventory is recorded in
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json`.
It has no new API collection or provider games. Sports means all available
execution dates within that audited provider-covered winner cohort, including
EPL draws. Its sport-specific historical ranges remain limited. Eight sports use
accepted play clocks; ATP retains scheduled-start/archive-duration qualification.
Fresh sports metadata must bind the full-date base; old sports observation bodies
or cutoff-base metadata receipts are not interchangeable with this new admission.

## Environment and verified stage bindings

Production uses Linux, the canonical `/home/ubuntu/prediction_markets` checkout,
mounted `/mnt/data` and `/home/ubuntu/venv/bin/python`. Entrypoints enforce the
production-host guard. Local work is limited to source review, fixture tests,
bounded summaries and report-only rendering. Follow [EC2_SETUP.md](../EC2_SETUP.md);
root alone manages instance/storage lifecycle and production execution.

Recorded original input/subset and sports/failed estimation v1 HEAD:
`2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a`, branch
`codex/kaushik-polymarket-full-sample`.
Earlier sports metadata/failed estimation v2 HEAD:
`94cd21798e9961f6cfdb173ec034df75228ed19b` on the same branch. Current sports
metadata/accepted estimation v3 committed H14:
`7c7d2badcddcb28ac9d24189ce35096bbfc78201`. Input and subset
acceptances remain unchanged. AWS reports the same `r5.8xlarge`; no instance-type
change was made. Fresh measured capacity, not an assumed host size, gates v3.
Canonical controls directory:
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1`.
Local small evidence copies:
`output/kaushik_polymarket_full_sample_2026-10-10_v1/`.

The accepted full-date base is
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2`.
Its `inputs_complete` manifest is accompanied by an
`inputs_reopened_accepted` receipt with the full-date scope, matching source/rows
and a true schema/hash/count/support reopening gate. The saved input body receipt
records exit zero; this acceptance does not establish completed estimates.

Verified small-artifact SHA256 bindings:

| Artifact | SHA256 |
| --- | --- |
| Full input contract v2 | `0ab3c86e22d3a880d321ab9df7e4913d6a51d51dc9666994b51fb0822ef2d0b3` |
| Full input reviewed preflight v2 | `88fec8c03588ac2e8444bcca49cdf749cbbc5ea426300fa97ca7e51590f5d9f6` |
| Full input manifest v2 | `72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c` |
| Full input acceptance v2 | `e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519` |
| Cutoff input manifest v2 | `a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9` |
| Cutoff input acceptance v2 | `426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a` |
| Subset reviewed preflight v1 | `f0cb9264b11200e540c9d6764b0bb8faad42dc3dd30c723a2128297f6adddfe2` |
| Accepted subset audit v1 | `19bcee0a823aaf156945699f300475afdda3ffb6210f212f25e6d1242af844e1` |
| Subset body execution receipt v1 | `ca3dbac4c1997eac2d7485f18af22eeed318574681d9482cba0b5f3ce8d61e41` |
| Retained sports binding v1, before new metadata review | `0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe` |
| Sports reviewed preflight v1 | `5895c201f8880dc858c97a0cc9e967ad71ce42e101f932e982c36c9b153c43e2` |
| Accepted sports metadata manifest v1 | `2f02ab263bfcf615ee5592331b85d9215e8ad0545c8618735f90928a13459279` |
| Sports metadata acceptance v1 | `56ad997cc92f9e6910d661a7801a15f4c8ef2128b9c1dab9e7fb3cb986569f41` |
| Sports body execution receipt v1 | `d331f8b5f75461e04252293d33b9d37e4c439b7e6d2748987f329e69cba158bd` |
| Reviewed sports binding v1, with accepted full-date metadata | `ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58` |
| Estimate reviewed preflight v1 | `f96d6d50a8ae3306e6060a65b14b20875b6f5ddae062dc788ae010b87332341c` |
| Failed estimate body execution receipt v1 | `12c000c024af95ef6fcae644efc75f9852d7c311dd516dabcc079e5b5ad96460` |
| Estimate failure record v1 | `2d9710b24f98f531b2c63e69dee25f432dd2483d5f72abc7ec6966a38c9f16de` |
| Estimate stderr v1 | `7f75bffe51c28aebea0d06f71df823db24200301bcae47c7f88592d3fc32036a` |
| Cache-fix EC2 fixture execution receipt v2 | `977ac44058e5fd88d10d7370f228375a1700b4b713e35c38f6bcf065543b16f7` |
| Root-confirmed direct preflight execution records v2 | `548ff3dfce3c3ff1e619b7c8f75a81736dd326946342076ef4435c6520377cb2` |
| Sports reviewed preflight v2 | `03ca6ddb6af72ea6b069ae5489347314a52f2885288c00b0960902dc0f848975` |
| Accepted sports metadata manifest v2 | `97a774fcad3c80d74aa1970a594c91186a6da3e19ffb974146f50389468d1a3e` |
| Sports metadata acceptance v2 | `b282e6cd15a7b40ca24cd845a7c00d97e44b3a9dbc26945f2464b2a24c5d07dd` |
| Sports body execution receipt v2 | `783085681c79f5017b1c443ff5c6dbaa39c95a8781ee081dcd1a03e112326a79` |
| Reviewed sports binding v2, with accepted metadata v2 | `ad60852b83c922cb11711d4b22d23bf5cd153aaeb26f4ea43e0481f8aa8f8abe` |
| Estimate reviewed preflight v2 | `ab266c8e31abc83baaeaf1591a4ba9ec44ce144f0b1ce095e4351040e0e56307` |
| Estimate root preflight review v2 | `339e09735f944cbf92f896eed47f7469d9221ca732c8a3d005cd37cc32fd756f` |
| Failed estimate body execution receipt v2 | `0b028c03cb8c3ba76be8e846819637295ed09f1bfe3f0037e50fde7bf8377ddb` |
| Estimate failure record v2 | `a3e78d249f27b87d9aeebd60b71ad30656fc89b904f2845d490cdb5a251de748` |
| Estimate stderr v2 | `47afebbc6e0a807873d25748086f3adf9ae8f288d6381ea421f34a00735ebdfb` |
| Estimate resource log v2 | `bc70e5852c03af2c9112c8ffd008e9d9c48390bb3a4dd707c101c91ea7996cea` |
| Original read-only production sports plan diagnostic v1 | `9f795eb5dad7d5075da58f0a5c5fb200025fa9432f5e1e5c636810c55c412ecf` |
| Production baseline/controlled join-plan diagnostic v1 | `882e91f248f8d45ac341cd60e78696e6368f889c478de719edab2c56a62eb007` |
| Verified post-estimate-v2 shutdown receipt | `90f133385939386b5a33ee7d5620d31a722ea11125e467dccdf2a498f8dee26a` |
| Failed-build recovery receipt v1 | `40b45e436bbb712ffa2dacd44fe7ce6be8c011b6e19aa9de993949813a24cf13` |
| Sports reviewed preflight v3 | `7292ef0db184b8f05d10246c676ea46f49e8cd32845e8b04c4fbf9ebb7bbb862` |
| Accepted sports metadata manifest v3 | `3b51578df6958d991151e41befe5f2f1a1ef68a12c3978065403504ee432ee6c` |
| Sports metadata acceptance v3 | `f0bd0918a18c850a89e841f55fb8b1dd450813ad3b2afdb0a65c8dbc38e2353a` |
| Sports body execution receipt v3 | `0f5c167c2f372e799be1ba17eccd0438b7b7e9632add4b95516b27c4c9161af0` |
| Reviewed sports binding v3 | `e52cefcd23d2451faccc1c2d1cc7861dc85d5cd7161913bfe6d86cdcd4ca881d` |
| Estimate reviewed preflight v3 | `ed1c444987a194f29bcb1d363a88510d21179c74b0d4d208506e033e1fd03603` |
| Root estimate preflight review v3 | `eb43fcee8450d95e2cab40cf050335ccc6ac42c91090b1b4907eb6435b34fb39` |
| Actual direct preflight execution records v3 | `6aaaddea709585a948941a41efb5288fad9510a808809af6f281efc528677573` |
| Fixed sports body wrapper v3 | `db6cd4348653f049ccdc19288a8f4d6c4f7b64469f72d542f845025961e508e3` |
| Fixed estimate body wrapper v3 | `a09b8226011c8274ccdde7f7702e7d46b9d557f7c4e0e938d9a04040ffa4cbec` |
| Accepted estimate manifest v3 | `3d4611756e0e9df9cb93d418861cd0041dd7adff087f080ace05b833a4950103` |
| Estimate acceptance v3 | `9d723fe5dfc0517033737c8be5df44c9cbba49df6b343453d0fa3773d6eb90eb` |
| Accepted estimates JSON v3 | `6c3543cb4f4065dfd80d4961c85b86e5371ce3e5ba436f8e8dff01e8597f049a` |
| Estimate body execution receipt v3 | `14f58f61135f1f59276ae91b76e47a89955fd13e45b26ad702102cbf03b5b60e` |
| Root accepted-estimate review v3 | `f4918b1d2e76fb0d8de7a75f16d7b76367af711b1888aad1fbe49ffe4c83a169` |
| Independent saved-score audit v1 | `ca0d2b9198cec65fe5022da326a8849cf8b2173f819dce59029aad14a273bf86` |
| Saved-score audit body execution receipt v1 | `c53cbebb88bbb39b5269257cf1b12ab8250f65271c256bc496137cc23b2a7cce` |

Per-file source and data inventories belong in the bound manifests, not the
reader-facing manuscript. The local accepted input copies contain small manifest
and acceptance summaries; computational reproduction still requires the admitted
EC2 Parquet bodies.

## Resource-only retry and verified failure recovery

The first full-date input build used source
`4098527caddadec5c16ca3979c3fcf768fc2adfd` and failed at final exact support
aggregation under 64GB DuckDB memory and the unchanged 16GB spill cap. It was
never an accepted input stage. The new committed producer increases only the
input memory cap from 64GB to 192GB and requires at least 240GB available RAM.
Threads, spill, read, per-file, total-output and free-space limits remain
unchanged, as do all scientific predicates and saved analytical fields. The
fresh v2 preflight and root review admit that change explicitly.

The subset auditor permits unchanged 64GB or exactly 64GB-to-192GB input memory;
all other caps, non-date contracts, definitions, source inventories and schemas
must agree. Its reviewed preflight records the sole cap delta as
`caps.memory_limit`, classified `approved_resource_only`, and preserves both
contracts. It revalidates that exception before its exact body comparison.

Separately, the user authorized moving only the two preserved failed input
builds to the same instance's system disk after per-file verification. The
recovery receipt reports `approved_recovery_verified_and_originals_removed`:

| Removed original failed-stage location | Verified retained recovery location |
| --- | --- |
| `/mnt/data/runs/.2026-10-10_kaushik_polymarket_inputs_v1.staging-94an1044` | `/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/cutoff_failed_inputs_v1` |
| `/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_inputs_v1.staging-_8i9fcvd` | `/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/full_sample_failed_inputs_v1` |

Only those failed-stage originals were removed after destination size/hash
verification. Accepted cutoff inputs/estimates and the repaired source were not
moved. Recovery preserves failure evidence; it neither accepts a failed stage
nor replaces the fresh successful v2 input build. The recovery receipt is retained
locally as `failed_build_recovery_receipt_v1.json` and on EC2 at
`/home/ubuntu/research_recovery/2026-10-11_failed_input_builds_v1/receipt.json`.
No recovery operation needs repeating to reproduce the accepted scientific run.

## Executed input and accepted subset commands

These commands record existing immutable operations, not new destinations for a
repeat run. A reproduction needs absent versioned targets and fresh separately
reviewed preflights. Preserve failures and execution receipts. Each retained
execution control ran in `/home/ubuntu/prediction_markets` and recorded command,
start/end/status, stdout/stderr and resource-log bindings.

Input preflight v2, from `input_preflight_execution_v2.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/build_kaushik_polymarket_replication.py \
  --contract /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/input_contract_v2.json \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_input_preflight_v2
```

Accepted input body v2, from `input_body_execution_v2.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/build_kaushik_polymarket_replication.py \
  --contract /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/input_contract_v2.json \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_input_preflight_v2/manifest.json \
  --reviewed-preflight-sha256 88fec8c03588ac2e8444bcca49cdf749cbbc5ea426300fa97ca7e51590f5d9f6
```

Subset preflight v1, from `subset_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_full_sample_subset.py \
  --previous-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2 \
  --previous-manifest-sha256 a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9 \
  --previous-acceptance-sha256 426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a \
  --full-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --full-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --full-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_audit_v1 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_preflight_v1
```

Accepted subset body v1, from `subset_body_execution_v1.py`; the saved
`subset_v1_execution_receipt.json` records completion with exit zero:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_full_sample_subset.py \
  --previous-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_inputs_v2 \
  --previous-manifest-sha256 a005f59a4b2da3f6eb34d97c912c5ff08aff842d3dcbac109e11c92bf43e1cc9 \
  --previous-acceptance-sha256 426ac4ba9b68b77e8b101f129a82009bc5f92a8f9f1179b3c09cd5c70589841a \
  --full-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --full-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --full-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_audit_v1 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_subset_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 f0cb9264b11200e540c9d6764b0bb8faad42dc3dd30c723a2128297f6adddfe2
```

### Accepted cutoff-subset gate

The saved `subset_audit_v1.json` reports `accepted_cutoff_subset_equal` for the
bound cutoff and full-date inputs v2. The claims map and all 44 monthly
before-cutoff subsets pass bidirectional `EXCEPT ALL` exactly. The 12 compared
fields are `source_ordinal`, `source_month`, `claim_code`, `recorded_token_code`,
`timestamp`, `recorded_price`, `P`, `Y`, `usdcSize`, `side`, `is_maker` and
`duration_eligible`. Locator uniqueness, accepted support counts, unchanged input
bindings, same canonical sources/non-date contract, retained-plus-added role
counts and the BUY partition also pass.

The primary taker population reconciles as 522,102,319 retained plus 478,868,297
newly admitted records, totaling 1,000,970,616. Across all saved roles,
1,044,204,638 retained plus 957,736,594 newly admitted records total
2,001,941,232. The auditor charged 275,681,582,243 read bytes, below the separately
reviewed 368,814,643,780-byte plan. Its completed receipt and audit bind producer
HEAD `2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a` and the reviewed preflight above.

This establishes exact preservation of the admitted before-cutoff records and
their saved analytical fields. The auditor reads accepted saved-input bodies and
bound metadata, not raw archive trades or native APIs. Added rows are admitted
builder outputs, not every old date-excluded record; raw eligibility and exclusion
assignment are not independently reconstructed. This gate does not refresh
outcomes or timing, expand provider coverage, or establish native actions or
whole-history collection completeness.

## Executed and accepted sports metadata

These commands record the completed metadata-only stage. They do not authorize
reuse or overwrite of its immutable targets. A new reproduction requires fresh
targets and separately reviewed preflights. Production bodies remain serial when
they share inputs or caches.

Sports preflight v1, from `sports_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1 \
  --sports-metadata-only \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v1
```

Accepted sports metadata body v1, from `sports_body_execution_v1.py`; the saved
`sports_v1_execution_receipt.json` records completion with exit zero:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1 \
  --sports-metadata-only \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 5895c201f8880dc858c97a0cc9e967ad71ce42e101f932e982c36c9b153c43e2
```

The new immutable
`/mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v1`
has a `sports_metadata_complete` manifest and
`sports_metadata_reopened_accepted` receipt with all outputs reopened. It binds
accepted full-date inputs v2 and retains exactly 13,909 provider games and 14,383
markets across nine sports. `trade_bodies_read` is false and
`native_pair_and_provider_result_proof` is true. This metadata-only admission
does not establish observed trade populations or completed sports estimates.

The fresh `sports_binding_reviewed_v1.json` binds this accepted metadata manifest
and receipt, the same retained provider inventory and its unchanged qualification.
No provider games, resolution refresh or API collection were added. Preserve the
sport-specific historical coverage limits, EPL draw claims and ATP timing caveat.

## Fresh estimate preflight and failed estimation v1

The new `preflight_complete` estimate admission binds accepted inputs v2, the
reviewed full-date sports metadata and producer HEAD above. Its resource plan
reserves 60,000,000,000 disk bytes: 8GB accepted output, 16GB transient file, 16GB
spill and 20GB free floor. It observed 64,685,264,896 free disk bytes and
264,257,695,744 available RAM bytes. The memory policy budgets 192GB DuckDB plus
32GB NumPy, totaling 224GB, and requires at least 240GB available RAM. Planned
maximum reads are 6,951,547,448,572 bytes under the retained 8TB read cap. These
are preflight capacity observations and limits, not actual completed-body usage.
The input retry's admission did not substitute for this fresh estimate review.

Estimate preflight v1, from `estimate_preflight_execution_v1.py`:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v1.json \
  --sports-binding-sha256 ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v1 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v1
```

Executed estimate body v1, from `estimate_body_execution_v1.py`; the completed
`estimate_v1_execution_receipt.json` records exit 1, not successful estimation:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v1.json \
  --sports-binding-sha256 ca61615860ad60fda267df2652f1e87a9fbde66718257d21ec31117a24c3fc58 \
  --expected-head 2aa260ac127c85f1a2c6efb9ab3c4ef4b8449c9a \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v1 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v1/manifest.json \
  --reviewed-preflight-sha256 f96d6d50a8ae3306e6060a65b14b20875b6f5ddae062dc788ae010b87332341c
```

### Preserved v1 failure and then-pending physical-only retry

The body started at `2026-10-11T14:57:10.006609+00:00` and ended at
`2026-10-11T15:02:33.134285+00:00`. Its `estimates_incomplete` failure record and
stderr identify `OutOfMemoryException` during `create_sports_view`'s Parquet
`COPY` of `sports_observations`, before model fitting. The unchanged 16GB spill
cap was reached (14.9 GiB/14.9 GiB in the diagnostic) under 192GB DuckDB memory.
The failure ledger charged 58,701,085,017 read bytes; it did not complete the
estimated-output or sports-cache gates.

The failed stage is preserved at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_estimates_v1.staging-0nxa_ukt`;
root reports a 0-byte sports cache. Local evidence is
`estimate_v1_execution_receipt.json`, `estimate_v1_failure.json` and
`estimate_v1_stderr.log`, bound above. Inputs v2, the accepted cutoff-subset audit
and accepted sports metadata remain unchanged. No estimates, saved-score audit
or report have been accepted.

Root verified no processes or transfers remained, stopped EC2 and verified the
stopped state. No stop timestamp is asserted here. The following records the
retry's status at that failure; the subsequent v2 admission below supersedes its
pending bindings. Local implementation is
removing only the unnecessary global raw sports-cache `ORDER BY`, with
equivalence fixtures. This physical-only hypothesis is not yet production-proven
and does not authorize any scientific predicate, model, weighting or resource-cap
change. Reviewed committed retry source, fixture results, a new retry version,
immutable targets, controls and a fresh preflight remain pending. Preserve the
failed v1 stage; do not reuse its cache or treat its receipt as acceptance.

Require reopened `estimates_complete` outputs and an
`estimates_reopened_accepted` receipt, full-date scope provenance, common duration
populations, BUY partition, complete support grids and numerical/convergence
gates. Require distinct double `sport_elapsed_seconds` and
`sport_remaining_seconds`, their saved clock-column mapping and zero post-cache
clock-equation mismatches; legacy `u/r/R` caches remain inadmissible. Estimator
source files must agree with the accepted sports metadata's source binding.
Record actual manifest, acceptance and estimates JSON digests when available.

## Reviewed physical-only retry and executed v2 commands

The frozen retry removes only the unnecessary global raw sports-cache `ORDER BY`.
Projected fields, joins, predicates, prices, clocks, source locators, multiplicity,
scientific definitions and caps remain unchanged; keyed numerical summaries and
caches retain their own sorting. The local suite passed 143 tests in 33.201
seconds. The first EC2 fixture run's host-assumption failure remains preserved;
only that test assumption was corrected, not the production guard. The retained
second EC2 fixture receipt records exit zero and 143 tests in 35.958 seconds.
These fixture gates do not yet prove full-date cache or estimation capacity.

Both v2 preflights were direct root CLI invocations, not timed body wrappers.
Their exact commands, canonical working directory, exit-zero completions and
manifest digests are retained in `preflight_execution_records_v2.json`; no
preflight timestamp is invented. The commands below record existing immutable
operations, not destinations to overwrite for another reproduction. Keep bodies
serial when they share inputs or caches, and use fresh reviewed targets for any
new run.

Executed sports preflight v2, root-confirmed in that execution record:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 94cd21798e9961f6cfdb173ec034df75228ed19b \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v2 \
  --sports-metadata-only \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v2
```

Accepted sports body v2, from `sports_body_execution_v2.py`; the saved
`sports_v2_execution_receipt.json` records exit zero:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 94cd21798e9961f6cfdb173ec034df75228ed19b \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v2 \
  --sports-metadata-only \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v2/manifest.json \
  --reviewed-preflight-sha256 03ca6ddb6af72ea6b069ae5489347314a52f2885288c00b0960902dc0f848975
```

The sports v2 body ran from `2026-10-11T15:31:01.001956+00:00` to
`2026-10-11T15:31:06.371088+00:00`, with empty stderr. Its new
`sports_metadata_complete` manifest and `sports_metadata_reopened_accepted`
receipt bind the frozen retry source and unchanged accepted inputs v2. Both
metadata outputs have exactly v1's byte counts, hashes and schemas; the provider
inventory and coverage also match v1. The retained map remains 13,909 games and
14,383 markets across nine sports. No trade bodies were read; native
pair/provider-result proof remains true. No provider games, resolution refresh
or API collection entered v2. All sport-date limits, EPL draws and ATP clock
qualification remain. The new `sports_binding_reviewed_v2.json` binds this
accepted source-matched metadata; v1 metadata and failures remain preserved.

Executed estimate preflight v2, root-confirmed in the direct execution record:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v2.json \
  --sports-binding-sha256 ad60852b83c922cb11711d4b22d23bf5cd153aaeb26f4ea43e0481f8aa8f8abe \
  --expected-head 94cd21798e9961f6cfdb173ec034df75228ed19b \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v2 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v2
```

The fresh `preflight_complete` manifest and root review bind the same accepted
inputs, accepted sports metadata v2 and source files. The root review reports
`admitted_same_sample_same_caps_physical_cache_fix`. Fresh observed RAM is
264,739,303,424 bytes and data free space is 64,682,438,656 bytes, against the
unchanged 60,000,000,000-byte reservation. Planned maximum reads remain
6,951,547,448,572 bytes under the 8TB ceiling. All v1 caps match: 192GB DuckDB
plus 32GB NumPy within 224GB, at least 240GB available RAM, four threads, 16GB
spill and the unchanged cache/output/transient/free-floor limits. These are
preflight observations and limits, not completed-body usage.

Executed estimate body v2, from `estimate_body_execution_v2.py`; the completed
`estimate_v2_execution_receipt.json` records exit 1, not estimate acceptance:

```sh
cd /home/ubuntu/prediction_markets
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v2.json \
  --sports-binding-sha256 ad60852b83c922cb11711d4b22d23bf5cd153aaeb26f4ea43e0481f8aa8f8abe \
  --expected-head 94cd21798e9961f6cfdb173ec034df75228ed19b \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v2 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v2/manifest.json \
  --reviewed-preflight-sha256 ab266c8e31abc83baaeaf1591a4ba9ec44ce144f0b1ce095e4351040e0e56307
```

Do not reuse or relabel either failed stage, cache or execution receipt. The same
output reopening, full-date provenance, duration/BUY/support/numerical and
distinct-seconds clock gates remain required before any estimate acceptance.
These guide/run status edits are local only; no canonical synchronization is
asserted, preserving the frozen producer checkout and failure evidence.

### Preserved v2 failure; sort removal unsuccessful in production

The v2 body ran from `2026-10-11T15:33:59.002652+00:00` to
`2026-10-11T15:40:51.389462+00:00`, ending with exit 1 after 6 minutes 52.387
seconds. Its `estimates_incomplete` / `OutOfMemoryException` again occurs during
the sports-observation Parquet `COPY`, before fitting. It reached the unchanged
16GB temporary-spill cap (14.9 GiB/14.9 GiB in the diagnostic) under 192GB DuckDB
memory. The resource log records 189,177,812 KiB peak resident size and no swap;
the failure ledger charged 58,701,085,017 read bytes.

Root reports the final failed stage is preserved at
`/mnt/data/runs/.2026-10-10_kaushik_polymarket_full_sample_estimates_v2.staging-xjev5f0z`,
with the raw sports cache absent. Its receipt, failure, stderr and resource log
are retained locally and bound above. Sort removal was not a successful
production-capacity fix. Fixture equivalence and unchanged metadata remain valid
evidence of those narrower gates, not evidence that a cache or model fit finished.
Inputs v2, the exact subset audit and accepted sports metadata v2 remain accepted
and unchanged. No models, saved-score audit or report have been accepted.

At the preceding failure update, the instance remained on only for root's bounded
metadata-only `EXPLAIN` diagnostic; diagnostic results and post-v2-failure
shutdown were not yet confirmed. The following entry records their completion
and independent stopped-state verification, not an inference from v1's earlier
shutdown receipt.

### Completed plan diagnostics and verified shutdown

The saved production diagnostics bind the unchanged accepted inputs and sports
metadata v2 at clean HEAD `94cd21798e9961f6cfdb173ec034df75228ed19b`, DuckDB
1.5.0. Baseline sports SELECT/COPY, Table 1 grouped-mean and duration-tail plans
put the large trade branch on the right/build side of both claims joins because
of underestimated trade cardinalities. Those cardinalities are planner estimates,
not observed body counts. The evidence identifies this build-side problem but
does not establish the full cause of the failed production runs.

The controlled follow-up temporarily set
`disabled_optimizers='join_order,build_side_probe_side'`, the exact supported
controls in the saved runtime. All three controlled query plans put the smaller
claims input on the right/build side of each claims join. The previous empty
setting was restored and the connection closed. Both diagnostics ran `EXPLAIN`
only: no `ANALYZE`, body query or COPY ran, no remote file was written, and the
probe COPY target was absent before and after. This is plan-orientation evidence,
not capacity success, completed cache output, accepted coefficients or findings.

Root then verified zero active research/transfer processes, clean canonical
source and both failures preserved. The shutdown waiter exited zero; a separate
AWS describe verified exact instance `i-0f5b31a268af53938` was `stopped`, actual
type `r5.8xlarge`, at `2026-10-11 15:58:09 UTC`. The saved
`estimate_v2_shutdown_receipt.json` binds that verified state, superseding the
earlier pending-shutdown entry above. No stopped state is inferred from waiter
completion alone.

### Locally frozen physical policy; fresh v3 admission pending

The local implementation candidate adds explicit configure-wide join policy
outside `CAPS`, without changing scientific definitions, fields, eligibility,
weights or resource limits. It saves actual scalar optimizer-setting readback
and runtime provenance in fresh preflight, checks them before body work and before
publication, and fails closed on drift for both sports metadata and estimate
stages. Core numerical estimators, `build_inputs.py` and the frozen scientific
specifications remain unchanged.

The local candidate files are frozen at these SHA256 bindings:

| Local candidate file | SHA256 |
| --- | --- |
| `analysis/kaushik_polymarket_replication/run_estimates.py` | `003fd2068681afd94a831cc19dd972ace888cabfe051131874c5917e9cac320c` |
| `tests/test_kaushik_replication_driver.py` | `1e2703b752f9df2b71c6e89aed76d86f97312ff13b448e7e9d24c95f337c786e` |

The earlier implementation-agent focused result was seven passing tests in 1.802
seconds, using DuckDB 1.5 and Python 3.10.13, not the older DuckDB 1.4.4
runtime. The fixture covariance difference remains
`1.8189894035458565e-11`, inside the unchanged `1e-10` absolute allowance. No
bitwise aggregate identity or production-capacity success is implied.

The first root 145-test suite reported three failing subtests in 32.150 seconds
only because JSON operator labels were whitespace-padded. Authorized fixture-only
`.strip()` normalization handles DuckDB 1.4.4/1.5.0 without changing production
code or its guard. The original fixture binding
`351d7b99168069ef1abb055de8dc98498cb585358a62bc6b943024277f994f0c`
is superseded by the final binding above. Root's broader suite then passed 145
tests in 30.514 seconds under Python 3.13.1 and DuckDB 1.4.4. Independent QA passed
seven tests in 1.002 seconds plus five dense oracles in 1.507 seconds, with 14
configured scalar-setting readbacks.

Reversing only the authorized policy additions in memory recovers exact H13
producer file SHA256
`8d8b24c68becb5f2f5a7de2d5c6e5a734143c95e42e6082e803ed6ce1160f687`,
preserving the prior code/scientific/cap boundary. These local tests and review
passed, but no production-capacity success follows. Root initiated restart for
reviewed production fixtures and fresh v3 admission. Root reports the same
`r5.8xlarge` running, verified UUID mounted at `/mnt/data`, clean canonical H13,
zero active research/transfer processes and a passed production-host guard. The
AWS status-ok wait completed with exit 0. Root synced only the reviewed producer/test
and fixed fixture wrapper, inspected the exact canonical diff (29 producer lines
and 90 test lines), and started the serial six-module production-runtime fixture
suite under `controls/join_policy_ec2_fixture_execution_v1`, without real-data
scans. It passed all 145 tests in 37.458 seconds; its saved receipt records
`2026-10-11T16:10:39.729823+00:00` through
`2026-10-11T16:11:18.487637+00:00`, exit 0. Root confirmed session 96378 closed
and canonical producer/test hashes match the final bindings above. Size-checked
local evidence bindings are:

| Production-runtime fixture evidence | SHA256 |
| --- | --- |
| `join_policy_ec2_fixture_v1_receipt.json` | `b24cea6d9b88794d58e86e9d6b9214b214bd6938a0547ef780ff22fbfffdedf8` |
| `join_policy_ec2_fixture_v1_stderr.log` | `d0eab82ee84e897d7ecc01e58974e57b45e4e5f82fd01cc05bf51e13232d260e` |
| `join_policy_ec2_fixture_v1_resources.log` | `bb1fca1aa8b7150fc93c078e44d38a87747ff740f48e742965cdad6363933d80` |

The fixture receipt records this command, run serially without real-data scans:

```sh
/home/ubuntu/venv/bin/python -B -m unittest tests.test_kaushik_replication_inputs tests.test_kaushik_replication_estimators tests.test_kaushik_replication_driver tests.test_kaushik_replication_report tests.test_kaushik_replication_independent_qa tests.test_kaushik_full_sample_subset_audit
```

Fixture passage is not production-capacity success. Preliminary free data space is 64,681,312,256 bytes
and available RAM 264,999,567,360 bytes; fresh v3 preflights must still bind their
own capacity observations. The
local documents at that prior fixture-admission moment were frozen-ready, not a
reviewed committed H14 or accepted
production stage. Committed H14, exact v3 source, commands, controls
and stage hashes were then pending; no v3 execution or acceptance was then asserted.

The next admitted version was planned with fresh immutable targets under the existing full-sample prefix:
`_sports_metadata_v3`, `_sports_preflight_v3`, `_estimate_preflight_v3` and
`_estimates_v3`. Reuse accepted inputs v2 and the exact subset audit only through
their existing bindings. Reopen new source-matched sports metadata and review a
fresh capacity preflight before a v3 estimate body. Preserve all accepted v1/v2
metadata and both failed estimate stages. The future audit/report templates below
now target planned estimates v3, not the failed v2 stage; their missing bindings
remain explicitly pending.

## Committed v3 admission and dispatched commands

Root committed exactly five reviewed files as H14
`7c7d2badcddcb28ac9d24189ce35096bbfc78201`; producer/test hashes remain
`003fd2068681afd94a831cc19dd972ace888cabfe051131874c5917e9cac320c` and
`1e2703b752f9df2b71c6e89aed76d86f97312ff13b448e7e9d24c95f337c786e`.
Canonical source remains frozen until the body and audits finish. The following
are actual root-executed preflights, the completed sports metadata body and the
dispatched estimate body, not pending templates. They run from the canonical
`/home/ubuntu/prediction_markets` directory; do not rerun into existing targets.
Local status-document edits do not mutate the bound production source.

`preflight_execution_records_v3.json` records both actual direct CLI preflights
with exit 0. Its root-observation start times, `2026-10-11 16:14:14 UTC` for sports
and `2026-10-11 16:17:43 UTC` for estimates, are not remote timed body timestamps.
The fixed body controls are `sports_body_execution_v3.py` and
`estimate_body_execution_v3.py`; their immutable execution directories are
`_controls_v1/sports_body_execution_v3` and `_controls_v1/estimate_body_execution_v3`.

Actual sports preflight command:

```sh
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 7c7d2badcddcb28ac9d24189ce35096bbfc78201 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v3 \
  --sports-metadata-only \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v3
```

Completed sports metadata body command, taken from its fixed wrapper/receipt:

```sh
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_v1.json \
  --sports-binding-sha256 0c207591b610f5231091fc2d6d33606e98a6ce3ca28d543e2763eae5023a5cfe \
  --expected-head 7c7d2badcddcb28ac9d24189ce35096bbfc78201 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_metadata_v3 \
  --sports-metadata-only \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_sports_preflight_v3/manifest.json \
  --reviewed-preflight-sha256 7292ef0db184b8f05d10246c676ea46f49e8cd32845e8b04c4fbf9ebb7bbb862
```

The body receipt records `2026-10-11T16:16:17.638530+00:00` through
`2026-10-11T16:16:23.178347+00:00`, exit 0; outputs reopened and accepted under
the committed H14/base/preflight bindings. Native pair/provider-result proof is
true, no trade bodies were read, and small-manifest comparison verifies output
bytes/hashes/schema, coverage and provider inventory identical to v1/v2. The map
still has 13,909 provider games and 14,383 markets across nine sports, not an
expanded historical census. Provider date limits, EPL draws, ATP clock qualification
and the unchanged source-resolution vintage remain material.

Actual estimate preflight command, using the newly reviewed metadata binding:

```sh
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v3.json \
  --sports-binding-sha256 e52cefcd23d2451faccc1c2d1cc7861dc85d5cd7161913bfe6d86cdcd4ca881d \
  --expected-head 7c7d2badcddcb28ac9d24189ce35096bbfc78201 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v3 \
  --preflight-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v3
```

Root separately admitted the estimate preflight after reopening the accepted
base and fresh metadata. It observed 264,716,660,736 bytes available RAM and
64,679,702,528 bytes free data space, against the unchanged 60,000,000,000-byte
reservation and 6,951,547,448,572-byte maximum read plan. Sports preflight separately
observed 264,754,626,560 bytes RAM and 64,681,246,720 bytes free.
Actual scalar optimizer-policy readbacks bind both fresh preflight and runtime:
`disabled_optimizers=join_order,build_side_probe_side`, outside `CAPS`, with
checks before body work and publication and fail-closed drift admission.
This is physical planning policy only. Caps remain 192GB DuckDB, 32GB NumPy,
224GB combined, 240GB available-RAM floor, four threads, 16GB spill, 3GB cache,
8GB outputs and the unchanged read/projection bounds; scientific definitions,
predicates, models, weights, source and provider cohort are unchanged.

Dispatched estimate body command, taken from the fixed wrapper:

```sh
/home/ubuntu/venv/bin/python -B scripts/estimate_kaushik_polymarket_replication.py \
  --base-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_inputs_v2 \
  --base-manifest-sha256 72405a22ee5efb8550b29d7105551a9c6286faabcec08526d932d4c27e26280c \
  --base-acceptance-sha256 e0ca7144c8766de9c68d337cef4430d2214320cc85e31c648a359673f6c08519 \
  --sports-binding /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_controls_v1/sports_binding_reviewed_v3.json \
  --sports-binding-sha256 e52cefcd23d2451faccc1c2d1cc7861dc85d5cd7161913bfe6d86cdcd4ca881d \
  --expected-head 7c7d2badcddcb28ac9d24189ce35096bbfc78201 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v3 \
  --reviewed-preflight /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimate_preflight_v3/manifest.json \
  --reviewed-preflight-sha256 ed1c444987a194f29bcb1d363a88510d21179c74b0d4d208506e033e1fd03603
```

At dispatch, root session 83963 was running this body, with execution evidence under
`_controls_v1/estimate_body_execution_v3`; no completion or acceptance was then
asserted. Accepted
inputs/subset and sports metadata v1/v2/v3 remain preserved, as do both failed
estimate stages. The following entry records subsequent actual acceptance; no
report/publication/shutdown completion is inferred from it.

## Accepted estimation and saved-score audit

The v3 body completed from `2026-10-11T16:19:14.106945+00:00` through
`2026-10-11T17:55:34.970494+00:00`, exit 0. Its manifest is `estimates_complete`,
and acceptance is `estimates_reopened_accepted`, with all outputs reopened and
`sample_scope: all_available_dates`. Root's saved acceptance review binds the
unchanged H14, command, caps, physical policy and reopened artifact identities.
Primary/all-role records reconcile at 1,000,970,616/2,001,941,232; common duration
support is N=252,319,669/G=497,313. These are support provenance, not findings.
Observed primary executions span `2022-11-21T19:50:09+00:00` through
`2026-06-21T21:41:20+00:00`. Provider coverage and resolution/clock caveats remain.

The production manifest records Python 3.12.3/GCC 13.3.0, DuckDB 1.5.0 and
NumPy 2.4.3; root additionally confirmed PyArrow 23.0.1 and Linux
7.0.0-1014-aws/x86_64/glibc 2.39. Caps and actual physical-policy readbacks match
the v3 admission above. Charged estimate reads were 598,451,354,311 bytes under
the 8,000,000,000,000-byte ceiling. Local fixture runtime is separately recorded.

Actual independent saved-score audit command, from the completed execution receipt:

```sh
/home/ubuntu/venv/bin/python -B scripts/audit_kaushik_replication_saved_scores.py \
  --estimate-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_estimates_v3 \
  --manifest-sha256 3d4611756e0e9df9cb93d418861cd0041dd7adff087f080ace05b833a4950103 \
  --acceptance-sha256 9d723fe5dfc0517033737c8be5df44c9cbba49df6b343453d0fa3773d6eb90eb \
  --expected-head 7c7d2badcddcb28ac9d24189ce35096bbfc78201 \
  --run-dir /mnt/data/runs/2026-10-10_kaushik_polymarket_full_sample_saved_score_audit_v1
```

The audit receipt records `2026-10-11T18:01:50.458272+00:00` through
`2026-10-11T18:02:19.144175+00:00`, exit 0, under unchanged H14. Accepted status
is `saved_scores_reconciled`, with 47 score files and all seven checks true:
CR0 outer products, common duration/BUY partition, influence, named contrasts/SE/CI,
saved sports-seconds contract, support/complete grids, and supplementary
`G/(G-1)` distinguished from CR1. Audit reads were 1,855,979,124 bytes under
25,000,000,000, with unchanged `2e-8` relative/`1e-10` absolute tolerances and
2,048-row batches. This auditor reconstructs covariance,
contrasts/SE/CI and influence from saved scores and reconciles saved support and
clock metadata. It does not independently refit coefficients or reconstruct raw
window membership. The two audits answer different questions; neither certifies
native actions, exact archive clocks or complete historical collection.

## Accepted numerical release and report QA

The sealed numerical receipt is `release_numerical_qa_v3.json`, status
`accepted_summary_and_saved_score_reconciled`, SHA256
`5623dd9bd969a38ddd42d359a754375f01c4648f57bbb718f0a722180896ff28`.
All 39 gates passed, with no failed/pending numerical checks and 18 JSON bindings
reopened. It reconciles all 12 model rank/convergence gates, 47 saved-score
artifacts/2,118 coefficient-and-contrast checks, 878 keyed rows, results for the
three unchanged sports populations and 22 Table 1/A2 reference rows/308 scalars.
It is not a raw coefficient refit, independent raw timing reconstruction or
whole-history completeness certificate. All 28 duration/A1 headline effective
cluster counts remain below 30; concentration warnings and nominal-interval
qualifications remain distinct from support admission. The combined absorbed
fixed-effect rank is not independently certified.

Report v1 used only byte-verified accepted `estimates.json`, `manifest.json` and
`acceptance.json`. Actual report-only rendering command from its QA receipt:

```sh
cd /Users/josephweintraub/prediction_markets
PYTHONDONTWRITEBYTECODE=1 MPLCONFIGDIR=/private/tmp/kaushik-report-v1-cache.11esHy /opt/anaconda3/bin/python -B -m analysis.kaushik_polymarket_replication.render_report \
  --estimate-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_full_sample_2026-10-10_v1/accepted_estimates_v3 \
  --run-dir /Users/josephweintraub/prediction_markets/output/kaushik_polymarket_full_sample_2026-10-10_v1/report_v1
```

Portable `source.tex`, all 21 relative vector figures and the report manifest
were reopened and bound. Rendered scope has no execution-date cutoff, uses
all-date eligibility accounting, retains provider-map versus observed-game
distinctions and actual sport-date ranges, and preserves resolved-source/clock
limitations. No expanded provider games or resolution refresh is implied.

Actual compilation and scratch preview commands, executed from `report_v1/`:

```sh
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdflatex -interaction=nonstopmode -halt-on-error -output-directory=build source.tex
pdftoppm -r 85 -png build/source.pdf qa_pages/page
```

The sealed external `report_v1_qa.json` has status `report_qa_passed`, recorded
`2026-10-11T18:11:38.535Z`. Both compile passes and preview rendering exited zero;
all 31 pages were visually inspected and passed. The report contains 36 native
tables (35 tabular/one longtable) and 21 vector PDFs. QA reconciled 1,998 PDF-text
numeric table cells and 660 plotted CR0 marks, with all 80 withheld plot cells
omitted. All 28 duration/A1 influence marks and nominal qualifications are
preserved. Points are isolated with capped whiskers/zero references; all pooled
and nine-sport panels are present. No substantive compilation/layout failures
remain. Minor underfull-box/isolated-cache notices are recorded in the receipt.
The immutable report manifest retains initial pending compile/visual fields;
the external QA receipt records their completed acceptance. Source/vector assets
are primary; the verified PDF is a preview. Scratch page PNGs are not deliverables.

| Accepted report artifact | SHA256 |
| --- | --- |
| Report v1 external QA | `6b282bd1ee61d22be8e7e7bda7bf87035ea0402bdf70c1bc668b44740f19c8cb` |
| Portable TeX source | `429cb940cde5a3b2a6e4d13f509b87bcf137112822de74eff78d7160f5dc5acb` |
| Immutable report manifest | `71fd5d7cd54e402bc34ea0fd27b25cdd4c26c2f939becaa3f10b8d100c90c47a` |
| Compiled PDF preview | `26aac815332a6ce4a6e753f8df797dedf077fbc27569e8dfd8d8bd4b47e1240b` |

## Pending finite package and completion

Root must record final source/control/estimate/audit/report/QA digests before
publishing a new finished bundle. Include this guide, both scientific contracts,
committed source/tests or retrievable revisions, reviewed controls, accepted
small summaries, both audits, portable report assets and verified preview.
Exclude raw/cache/score bodies, failure-recovery bodies, spill, credentials and
scratch page images. Do not append this run into the completed cutoff bundle.
Publication destination, finished-bundle transfer verification and any future
current v3 run's final shutdown receipt remain pending. The failed v1/v2 runs have been
verified stopped by root as recorded above. After any future restart, root must
again confirm no agent/process/transfer still needs the instance, stop it and
verify stopped. The local run index and actual receipts track current execution
state; accepted report QA does not imply publication or final shutdown. The finite
source-and-summary capsule requires separately preserved external production
input/provider and accepted input/cache/score bodies to reproduce production;
those bodies are intentionally excluded. A fresh final-candidate 145-test import
origin/summary-loader/inventory/credential QA is still required. The earlier
source-only fixture precheck is supporting evidence, not final bundle acceptance.
