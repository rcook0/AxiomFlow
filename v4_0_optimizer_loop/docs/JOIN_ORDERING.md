# Join Ordering (v3.5)

v3.5 introduces a **join-ordering optimizer pass** that can reorder multi-join components into a cheaper **left-deep** plan.

## Why `onRef` exists
When you re-associate joins, the output schema becomes nested (`{left,right}` of `{left,right}` ...). If join keys are expressed as concrete paths (`left.left.id`) they break under reordering.

To make join ordering stable, v3.5 allows join predicates to be expressed as **relation references**:

```json
{
  "op": "join",
  "params": {
    "onRef": [
      { "left": { "rel": "orders", "path": "customerId" },
        "right": { "rel": "customers", "path": "_id" } }
    ]
  }
}
```

The optimizer compiles `onRef` into executable `on` paths for the chosen join tree.

## What v3.5 does (beta constraints)
- Finds eligible join components (root joins where the component has a single consumer).
- Extracts all base relations (typically scan datasets) participating in the join tree.
- Runs a **bounded DP** join order search that builds a left-deep plan by repeatedly joining a growing subset with one new relation.
- Uses a conservative cost model:
  - base relation cardinalities from persisted node EMAs when available; else defaults
  - join output cardinality from learned join signature stats (if available) else `min(left,right)` (safe heuristic)
- Rewrites the plan by removing the old join nodes in the component and inserting new join nodes.

## Learned join signature stats
A join signature (`joinSig`) is a hash of the canonicalized `onRef` predicates (independent of join direction).

The executor persists EMA signals per `joinSig` into `join_sig_stats`:
- `emaFanoutPerLeft`, `emaFanoutPerRight`
- `emaLeftIn`, `emaRightIn`, `emaOut`
- `emaMs`, `emaMsPerInRow`

This makes join ordering improve **across reorderings and across runs**.

## Files
- `backend/engine/joinOrderV3_5.js` — join graph extraction, DP ordering, plan rewrite
- `backend/engine/joinKeyCompile.js` — compiles `onRef` -> executable `on` paths
- `backend/engine/bindings.js` — subtree binding maps to resolve/compile paths
- `backend/engine/dagOptimizeV3_5.js` — optimizer pipeline (v3.4 + v3.5 ordering)
- `backend/artifacts/dagPersist.js` — persists `join_sig_stats` and exposes loader functions
- `backend/engine/executeDagBeta.js` — uses v3.5 optimizer and updates `join_sig_stats`

## Operational note
If your existing plans only specify `params.on` (concrete paths), v3.5 will attempt to infer `onRef` **only when it can resolve paths to base relations** using inferred subtree bindings. If inference fails, the optimizer will skip join ordering and emit a `JOIN_ORDER_SKIP` change record.

## v3.6 — Bushy join ordering + physical join policy
- Replaces left-deep-only with a **bounded bushy DP** (defaults: max 7 relations).
- Each join candidate chooses a join **algorithm** (`hash` vs `nested_loop`) and a hash **build side**.
- Adds a cost + cardinality propagation model (`costModelV3_6`) and persists these estimates in the optimizer report.
- Supports root joins with **multiple consumers** (rewires all outgoing edges; v3.4 post-pass re-inserts materialize barriers as needed).


## v3.7 — Join ordering informed by learned predicate selectivity
- Join ordering uses provisional base-relation row estimates computed with `pred_sig_stats`.
- This makes join ordering resilient to filter pushdown/cloning (node ids change, predicate signatures do not).


## v3.8 — Automatic exploration
When a single join component exists, the join-order DP retains top-K candidates and the runtime can explore alternatives (epsilon-greedy) to improve learned statistics.
