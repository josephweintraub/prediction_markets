import os, sys
sys.path.insert(0, "/home/ubuntu/prediction_markets/analysis")
os.environ.update(V5_PREFIX="v7_25_80", V5_LO="0.25", V5_HI="0.80", V5_INCLUDE_UPDOWN="0")
from pathlib import Path
from learnability import dimensions_v5 as v5
v5.V5_DIMS=["dim_dollar_volume_tier","dim_contract_horizon","dim_outcomes_per_event",
  "dim_event_slug_size","dim_prior_settlements_bin__event_slug","dim_text_novelty",
  "dim_text_neighbors_strict","dim_anchor","dim_recurrence","dim_feedback_lag","dim_prior_settlements"]
from learnability import run_phase1
run_phase1.V5_DIMS_PATH=Path("/mnt/data/learnability/output/phase1_v7_contract_dimensions.parquet")
run_phase1.main()
