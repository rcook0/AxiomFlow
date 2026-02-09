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


## v3.7 — Predicate selectivity learning
- Add `pred_sig_stats` collection with EMA selectivity per predicate signature.
- Cost model consumes learned selectivity to propagate cardinalities.
- Join ordering uses provisional row estimates (with learned selectivity) for more stable planning.

## v3.8 — Composition selectivity + automatic exploration
- Learn AND-predicate group selectivity (`pred_group_stats`).
- Learn join→filter segment selectivity (`join_filter_stats`).
- Emit top-K join-order variants for a single join component and explore with epsilon-greedy.
- Track plan-level EMA wall time (`plan_variant_stats`).

## v3.9 — Bandit-ready exploration + decision log
- UCB-based selection using `plan_variant_stats` (EMA wall time).
- Persist `optimizer_decisions` per run for replay/offline analysis.

## v4.0 — Optimizer loop module
- Add `backend/optimizer/*` as the dedicated loop/policy layer.
- Executor uses `optimizeAndSelectV4` for plan selection.
- Add `scripts/db-indexes-v4.js`.

## v4.1 — Multi-component exploration + replayer
- Coordinate exploration candidates for multi-root join DAGs.
- Propagate chosen variant cost via execMeta for accurate `plan_variant_stats`.
- Add offline replayer script for `optimizer_decisions`.

## v4.2 — Thompson sampling + variance tracking
- Extend `plan_variant_stats` with EMA wall-time variance proxies (`emaWallMs2`, `emaVar`).
- Enable Thompson sampling selection using variance-derived standard error.

## v4.3 — Learned wall-time feature model
- Add `runtime_models` collection storing `wall_ms_linear_v1` linear regressor.
- Extract stable plan features (op counts + cost log) and perform online SGD updates per run.
- Policy layer can predict wall-time for cold/rare variants.

## v4.4 — Plan family cache (enumeration caching)
- Add `plan_family_cache` keyed by `familyDagHash`.
- Cache best plan + candidate set to skip re-enumeration on repeated families.
- Optional refresh via `AXIOMFLOW_CACHE_REFRESH=1`.

## v4.5 — Policy lab (offline tuning)
- Extend `optimizer_decisions` to store candidate sets and chosen metadata.
- Add `scripts/policy-lab.js` to evaluate selection policies offline using recorded decisions + latest EMAs.

## v4.6 — Streaming execution fast-path
- Add `engine/executePipelineStreaming.js` and executor integration.
- Detect `scan -> (filter|project)* -> sink` pipelines and execute via Mongo cursor streaming.
- Preserve persistence (lineage, stats, decisions, feature model) in streaming mode.

### V4 environment knobs
- `AXIOMFLOW_POLICY`: `ucb | thompson | epsilon | best`
- `AXIOMFLOW_UCB_C`: exploration coefficient (ucb)
- `AXIOMFLOW_EXPLORE_EPS`: exploration probability (epsilon / hybrid)
- `AXIOMFLOW_EXPLORE_MAX_FACTOR`: candidate admissibility window
- `AXIOMFLOW_MODEL_LR`, `AXIOMFLOW_MODEL_L2`: SGD params for feature model
- `AXIOMFLOW_STREAMING`: set `0` to disable streaming fast-path
- `AXIOMFLOW_STREAMING_BATCH`: sink batch insert size
