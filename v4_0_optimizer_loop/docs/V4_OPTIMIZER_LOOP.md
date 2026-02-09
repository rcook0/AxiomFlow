# V3.9 → V4: Optimizer Loop

## v3.9 (behavioral upgrade)
v3.9 upgrades exploration from **pure epsilon-random** to a **bandit-ready selector**:
- Uses `plan_variant_stats` (EMA wall time, run count) when available.
- Supports UCB-style exploitation with optional epsilon exploration.
- Persists an immutable per-run **optimizer decision** record in `optimizer_decisions`.

Key environment variables:
- `AXIOMFLOW_POLICY` = `ucb` | `epsilon` | `best` (default: `ucb`)
- `AXIOMFLOW_UCB_C` = exploration strength (default: `0.5`)
- `AXIOMFLOW_EXPLORE_EPS` = epsilon for random exploration (default: `0.05`)
- `AXIOMFLOW_EXPLORE_MAX_FACTOR` = gate candidates by est cost (default: `3.0`)

## v4 (structural upgrade)
v4 introduces a dedicated optimizer-loop module (`backend/optimizer/*`) to cleanly separate:
- logical optimization + enumeration (engine)
- selection policy (optimizer loop)
- persistence + learning (artifacts)

### Module map
- `backend/optimizer/optimizerLoopV4.js`
  - Calls `optimizeDagV3_8` to get best plan + candidates
  - Selects an executable plan via `selectPlanV4`
- `backend/optimizer/selectPlanV4.js`
  - Annotates candidates with `variantDagHash`
  - Selects via UCB or epsilon strategies

### New collection: `optimizer_decisions`
Per-run decision log for replay/offline analysis:
- `runId`
- `familyDagHash`
- `executedDagHash`
- `selected` (e.g. `best`, `ucb:2`, `explore:3`)
- `explored` boolean
- `policy` (serialized policy parameters)
- `ucb` (selection diagnostics when applicable)

## Next obvious v4.x moves
- Multi-component exploration (independent per-join-component candidate sets)
- Offline “replayer” CLI that re-scores historic decisions under new policies
- Thompson sampling / Bayesian regression using `emaWallMs` + cost features
