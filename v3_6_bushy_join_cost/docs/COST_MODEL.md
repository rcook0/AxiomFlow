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
