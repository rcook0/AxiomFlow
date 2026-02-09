# Roadmap


## v2.0 — Optimizer with predicate rewrite
- Rewrite-aware filter pushdown across `project` using passthrough-only `projectMap` + `exprRewrite`.
- Optimizer emits change records with predicate before/after AST.

## v2.1 — Computed predicate rewrite
- Enable rewrite-aware filter pushdown across `project` with `allowComputed=true` (pure expressions only).
- Optimizer records rewrite mode (`passthrough` or `computed`) per change.

## v2.2 — Required columns projection pass
- Barrier-delimited analysis of required columns from rewritten filter predicates.
- Inserts/extends an early `project` to keep only required columns (passthrough), reducing row width.

## v3.0-alpha — DAG IR contract + skeleton compiler
- Lock DAG IR schema (nodes/edges/ports/outputs) and invariants.
- Add validator + canonicalizer + dagHash.
- Add skeleton compiler (topological order + adjacency) for executor integration.
- Add lineage DAG collector stub (multi-input ports).

## v3.1 — DAG optimize + persist (real optimizer loop)
- Add v3.1 DAG optimizer (linear-chain beta): filter pushdown with computed predicate rewrite + required columns.
- Persist optimizer report per run; persist DAG lineage per run.
- Maintain EMA per-node cost model keyed by (dagHash,nodeId): selectivity + ms/row.

## v3.3 — Join-aware optimization
- Add join operator execution (materialized hash join) with left/right ports.
- Add join-aware optimizer: use persisted EMA stats to swap join inputs and reduce hash build costs.
- Extend optimizer loop to load dag_node_stats for a plan hash and persist join runs normally.

## v3.4 — Advanced join policies
- Push down side-only filters across join by rewriting column paths (left.* / right.*).
- Insert materialize barriers after fan-out joins.
- Extend EMA node stats with join-specific signals (port EMAs, fanout per side).
- Executor supports materialize op as a barrier/no-op under materialized execution.

## v3.5 — Join ordering
- Join-order DP (left-deep) for multi-join components when predicates can be expressed as stable `onRef` relation references.
- New learned `join_sig_stats` collection: persists EMA fanout/selectivity per join signature (stable across reorderings).
- Executor updates join signature stats on every run; optimizer consumes learned fanout for future ordering.

## v3.6 — Bushy join ordering + cost propagation
- Bounded bushy DP join ordering (`maxRelsForBushy`).
- Physical join policy selection (hash vs nested loop; build-side selection).
- Cost model + estimated cardinality propagation persisted to optimizer report.
- Join rewire supports multi-consumer roots; post-pass materialize insertion handles fan-out.

