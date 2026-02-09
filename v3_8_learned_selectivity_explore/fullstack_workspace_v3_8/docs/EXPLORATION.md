# v3.8 — Automatic exploration (epsilon-greedy)

v3.8 can occasionally execute an alternative **join order** to gather better statistics.

## When exploration is available
Exploration variants are emitted when the query contains **exactly one join component** (one join root),
and the join-order DP is able to produce `topK` candidate trees.

## Selection policy
At runtime (in `executeDagBeta`), the engine chooses between:

- **best** plan (default)
- **explore:k** candidate plans

using epsilon-greedy selection:

- `AXIOMFLOW_EXPLORE_EPS` (default `0.05`) — probability of exploration
- `AXIOMFLOW_EXPLORE_MAX_FACTOR` (default `3.0`) — only explore candidates whose estimated totalCost
  is <= `bestCost * maxFactor`

## What gets learned
Even when running alternative join orders, the engine continues to update:

- `join_sig_stats` (fanout signals)
- `pred_sig_stats` and `pred_group_stats` (filter selectivity)
- `join_filter_stats` (post-join selectivity)
- `plan_variant_stats` (plan-level wall time EMA)

## Collection: `plan_variant_stats`
Tracks plan-level performance for the family of an input DAG:

- `{ familyDagHash, variantDagHash }` unique key
- `emaWallMs` — total wall time for the executed plan
- `emaTotalCost` — estimated totalCost (from optimizer) when available
