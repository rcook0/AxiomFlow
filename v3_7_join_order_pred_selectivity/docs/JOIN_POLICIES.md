# Join Policies (v3.4)

## Join execution model
- `join` is binary with explicit `left` and `right` ports.
- v3.x execution uses a materialized hash join.
- Output rows are shaped as:

```json
{ "left": { ... }, "right": { ... } }
```

## Predicate pushdown across join (v3.4)
Pattern:
- `join -> filter` where the predicate references **only one side** (`left.*` OR `right.*`).

Rewrite:
- Move the filter to the corresponding join input edge.
- Strip side prefixes: `col("left.symbol")` -> `col("symbol")`.

Safety (beta):
- Only applied when the filter is the **only consumer** of the join output.

## Materialize after fan-out joins (v3.4)
If a join has multiple consumers, insert:

```json
{ "op": "materialize", "params": { "reason": "join_fanout" } }
```

Under the current executor this is a semantic no-op (already materialized) but it is:
- an explicit barrier for rewrites
- a future hook for persistent caching / spill

## Persisted join signals
`dag_node_stats` now includes EMA fields:
- `emaRowsInLeft`, `emaRowsInRight`
- `emaFanoutPerLeft`, `emaFanoutPerRight`