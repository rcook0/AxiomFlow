# Release Notes — v3.9 and v4.0

## v3.9 — Bandit-ready exploration + decision log
**Goal:** turn exploration into a learnable control loop.

Changes:
- Exploration upgraded from pure epsilon-random to **UCB exploitation** over `plan_variant_stats` (EMA wall time).
- Executor now persists immutable per-run decisions to `optimizer_decisions`.
- Learned-stat documents now carry `schemaVersion: 1` for forward evolution.

Key env vars:
- `AXIOMFLOW_POLICY=ucb|epsilon|best` (default `ucb`)
- `AXIOMFLOW_UCB_C=0.5`
- `AXIOMFLOW_EXPLORE_EPS=0.05`
- `AXIOMFLOW_EXPLORE_MAX_FACTOR=3.0`

## v4.0 — Optimizer loop module
**Goal:** separate optimization/enumeration from selection policy.

New modules:
- `backend/optimizer/optimizerLoopV4.js`
- `backend/optimizer/selectPlanV4.js`

Executor changes:
- `backend/engine/executeDagBeta.js` uses `optimizeAndSelectV4(...)` for plan selection.
- UCB selection is policy-driven and consults `plan_variant_stats` automatically.

DB provisioning:
- `scripts/db-indexes-v4.js` adds indexes for:
  - `pred_group_stats`, `join_filter_stats`, `plan_variant_stats`
  - `optimizer_decisions`

Tests:
- `backend/tests/test_v4_ucb_select.js`
