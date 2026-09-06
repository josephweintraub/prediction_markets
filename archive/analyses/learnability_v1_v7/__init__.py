"""FLB-by-learnability-dimensions analysis (Phase 1) — EC2 / 1.4B-trade dataset.

Follows the results.ipynb data protocol:
  - sophisticated behavioral bot filter (bot_filter.build_wallet_flags)
  - exclude up/down markets
  - 50-80% lifecycle window
  - deciles (n_bins=10), price filter 0.01 < price < 0.99
  - 2-way clustered SE (day x trader) as primary, Fama-MacBeth as robustness
  - BUY-side returns: ret = (1 - price) if won else -price
  - join augmented per-contract classifications on conditionId
"""
