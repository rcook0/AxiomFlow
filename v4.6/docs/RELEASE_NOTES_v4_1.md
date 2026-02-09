# Release Notes — v4.1

## Multi-component exploration (multi-root join DAGs)
- `dagOptimizeV3_8` now emits **coordinate variants** even when the DAG contains multiple join roots.
- Each candidate varies **one** join component while keeping others fixed.
- Bounded by `joinOrder.maxTotalCandidates` (default 12).

## Selection / learning fix
- `selectPlanV4` now returns `execMeta.chosenCost` + `execMeta.chosenRank`.
- Executor updates `plan_variant_stats` using the executed variant’s cost estimate (not the best-plan’s).

## Offline replayer
- `scripts/replay-optimizer-decisions.js` replays historic runs under a new policy and reports mismatches.
