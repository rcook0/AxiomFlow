# v4.1 — Multi-component exploration + Offline replayer

## Multi-component exploration (coordinate variants)
Prior to v4.1, exploration candidates were emitted only when the DAG contained **one** join component.
In real workloads, DAGs frequently have **multiple independent join roots** (multiple components).

v4.1 extends `dagOptimizeV3_8` to emit **coordinate variants** for multi-root DAGs:

- Build the best plan by rewriting each join root to its best order (existing behavior).
- Then, for each join root in the best plan, re-run top-K join ordering on the best plan and emit
  variants that change **only that component**, keeping all other components fixed.

This avoids cartesian explosion while still allowing the bandit selector to explore improvements
component-by-component over time.

Tunable:
- `joinOrder.maxTotalCandidates` (default 12) bounds total coordinate variants across components.
- `joinOrder.topK` still controls per-component enumeration (alts are `topK-1`).

## Selection quality: chosenCost propagation
`selectPlanV4` now returns `execMeta.chosenCost` and `execMeta.chosenRank` so `plan_variant_stats`
is updated with the **executed variant’s** estimated cost rather than the best-plan’s cost.

## Offline replayer CLI
`scripts/replay-optimizer-decisions.js` re-selects variants under a different policy and compares
to the executed variant.

Example:
```bash
node scripts/replay-optimizer-decisions.js mongodb://localhost:27017 axiomflow --limit 200 --policy ucb --ucbC 0.7
node scripts/replay-optimizer-decisions.js mongodb://localhost:27017 axiomflow --family <inputDagHash> --policy best
```
Output is JSON lines per run + a summary on stderr.
