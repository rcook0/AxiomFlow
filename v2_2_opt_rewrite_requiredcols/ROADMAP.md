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
