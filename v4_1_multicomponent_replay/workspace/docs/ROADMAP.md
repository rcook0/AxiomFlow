# Roadmap (high-level)

## v2 — Rewrite-aware optimization + correctness hardening
- IR optimizer v2: rewrite-aware filter pushdown across renaming projections using `projectMap` + `exprRewrite`
- Add strict operator barriers and purity rules for computed substitutions
- Cursor-based result pagination endpoint (no skip)
- Quarantine collection + paging endpoint for DQ mode=quarantine
- Enforce/validate sort requirements for stats correlation/drawdown (or add Sort operator)

## v3 — Execution planner surfaces + pushdown + stability
- Physical planner: compile eligible segments to Mongo aggregation (pushdown)
- Operator-level schema flow (per-op schema snapshots + null-rate)
- Join + groupBy operators instrumented with multi-input lineage stats
- Cache and materialization policy (retain/reuse run_results for identical planHash+params)
- CI: unit tests for optimizer rewrite correctness + golden-corpus parity
