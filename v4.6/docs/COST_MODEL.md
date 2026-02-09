# Cost Model (v3.6)

This document describes the v3.6 cost model used for join ordering and physical join policy assignment.

## Outputs
The optimizer emits `cost` in the opt report:
- `cost.summary.totalCost`
- `cost.estRowsByNodeId`
- `cost.estCostByNodeId`

These are **heuristic** estimates intended for optimizer feedback loops, not absolute runtime prediction.

## Cardinality estimation rules
- `scan`: use persisted EMA rows when available, else `params.estimatedRows`, else default 10k.
- `filter`: `out = in * selectivity` where selectivity comes from EMA if present, else `params.estimatedSelectivity`, else default 0.3.
- `project/materialize/sink`: preserve row count.
- `join`: uses learned `join_sig_stats` if available:
  - `emaFanoutPerLeft`, `emaFanoutPerRight`
  - otherwise defaults to `min(left,right)`.

## Join cost rules
- `nested_loop`: proxy cost `outer * inner` (only selected for very small inner sides).
- `hash`: proxy cost `(left + right + out)` multiplied by a memory penalty if the build side exceeds `hashBuildBudgetRows`.

## Tunables (defaults)
- `joinPolicy.nestedLoopMaxInnerRows = 2000`
- `joinPolicy.nestedLoopMaxOuterRows = 80000`
- `costModel.hashBuildBudgetRows = 200000`
- `costModel.hashBuildOverBudgetPenalty = 10`
- `costModel.defaultFilterSelectivity = 0.3`
- `joinOrder.maxRelsForBushy = 7`

## v3.7: predicate selectivity learning
For `filter` nodes, selectivity is sourced from `pred_sig_stats` (by `predSig`) before falling back to node EMAs.

## v3.8: composition selectivity + join→filter segments
- Filter selectivity can be sourced from `pred_group_stats` (AND compositions) and `join_filter_stats` (post-join filters).
- This stabilizes cardinality estimates under rewrite and improves join ordering.
