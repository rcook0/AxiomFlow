# AxiomFlow



## v3.1 — DAG optimize + persist (beta)
- Optimizer: linear-chain DAG rewrite (filter pushdown with computed predicate rewrite + required-columns projection).
- Persistence: stores optimizer report, run DAG lineage, and EMA node stats keyed by (dagHash,nodeId).
- Index script: scripts/db-indexes-dag-v3-1.js


## Documentation
- docs/PROJECT_SUMMARY.md
- docs/ARCHITECTURE.md


## v3.3 — Join-aware optimization
- Executor supports `join` (hash join) with explicit `left/right` ports.
- Optimizer can swap join inputs using persisted EMA stats for the input plan hash.
- Join output rows are shaped as `{ left: <row>, right: <row> }` for collision-safe lineage and projection.


## v3.4 — Advanced join policies
- Join-side predicate pushdown (left-only or right-only predicates) by rewriting `col` paths.
- `materialize` barrier inserted after fan-out joins.
- Persist join signals in EMA stats: port-specific EMA rows-in and fanout-per-side.


## v3.5 — Join ordering
- Left-deep join ordering using a bounded DP search.
- Requires (or infers) `join.params.onRef` relation-scoped predicates to remain stable under re-association.
- Learns join fanout/selectivity into `join_sig_stats` to improve ordering over time.

## v3.6 — Bushy join ordering
- Join ordering upgraded from left-deep to **bounded bushy DP**.
- Physical join policies assigned automatically (`algorithm`, `build`).
- Optimizer report now includes `cost` (estimated rows + costs per node).

### Quick demo
If you have a runner script already, run a DAG with joins and inspect `run_dag_opt_reports`.

For a synthetic smoke test:
```bash
node backend/tests/test_v3_6_join_order.js
```

## v3.7 — Predicate selectivity learning
- Learns filter selectivity by predicate signature (`pred_sig_stats`), stable across DAG rewrites.
- Optimizer cost report and join ordering use learned selectivity automatically.

