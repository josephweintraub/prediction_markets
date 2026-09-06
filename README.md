# Prediction-market calibration

Research code for measuring price calibration and favorite-longshot bias (FLB) on
Polymarket. The current paper asks why calibration varies across markets, with a focus
on liquidity, market duration, semantic market families, and textual novelty.

The canonical repository is `/home/ubuntu/prediction_markets` on the project EC2
instance. Large data and generated artifacts live on the attached EBS volume under
`/mnt/data`; they are never committed to Git. A local Mac copy may be used as a viewer,
but it is not a data-compatible execution environment.

## Start here

1. Read [`docs/project_status.md`](docs/project_status.md) for the current research state,
   data vintage, and known limitations.
2. Read [`docs/methods_reference.md`](docs/methods_reference.md) before interpreting or
   changing an analysis.
3. Follow [`docs/workflow.md`](docs/workflow.md) for branches, run artifacts, validation,
   reporting, and archival rules.
4. Use [`docs/repository_map.md`](docs/repository_map.md) to distinguish active,
   supporting, and historical code.

## Repository layout

| Path | Status | Purpose |
|---|---|---|
| `analysis/embedding_difficulty/` | **Active** | Calibration-heterogeneity workstream. The directory will be renamed after its interfaces are stabilized. |
| `analysis/paper/` | Active publication support | Existing paper tables, figures, and robustness scripts. |
| `analysis/learnability/` | Mixed; mostly historical | Earlier v1-v7 learnability analyses and the older calibration engine. See its README before reuse. |
| `analysis/stage0_v2/` | Supporting | Polymarket and Kalshi contract-normalization pipelines and regression harnesses. |
| `pipeline/` | Active but infrequent | Builds and refreshes the canonical on-chain trade dataset. |
| `scripts/` | Supporting operations | Data cleaning, flags, and Telonex acquisition utilities. |
| `docs/` | Source of record | Current methods, status, workflow, decisions, and archived findings. |

Root-level legacy analysis modules remain temporarily for reproducibility. They should
not be treated as the current paper specification; their eventual archival is tracked in
the repository cleanup plan.

## Environment

Python dependencies are declared in `pyproject.toml`. On EC2, the currently provisioned
environment remains `/home/ubuntu/venv` until a clean environment has reproduced the
paper outputs.

```bash
python -m pip install -e '.[analysis,embeddings,pipeline,classification,telonex,dev]'
python -m compileall -q analysis pipeline scripts
python -m pytest
```

The compilation and test commands are safe locally, but full-data analysis must run on
EC2. Never run local joins against the local trade sample: its identifiers do not match
the canonical EC2 data convention.

## Current analysis

The current computation flow is:

```text
canonical trades + market metadata + wallet flags
                         |
                         v
             universe and compact trade bases
                         |
                         v
              market-to-slice specifications
                         |
                         v
       decile calibration + three-way clustered inference
                         |
                         v
          immutable artifacts -> rendered reports -> paper
```

No result is publication-ready unless it identifies its Git commit, data vintage,
configuration, producing command, and source artifacts.

## Secrets

Set sensitive values through environment variables or private key files. In particular,
the Polygon pipeline reads `POLYGON_RPC_URL` or `~/.polygon_rpc_url`. Never place a live
credential in source, documentation, logs, or committed configuration.

Operational EC2 instructions are in [`CLAUDE.md`](CLAUDE.md) and
[`docs/EC2_SETUP.md`](docs/EC2_SETUP.md).
