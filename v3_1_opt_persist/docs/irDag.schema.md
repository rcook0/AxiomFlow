# DAG IR Schema (v3.0-alpha)

This document defines the **authoritative contract** between:
- planner / API submission
- optimizer
- executor
- lineage instrumentation

The intent of v3.0-alpha is to lock **structure, invariants, and canonicalization rules** before implementing full DAG execution.

---

## 1. Top-level shape

A DAG plan is a JSON object:

```json
{
  "version": "ir-dag-3.0-alpha",
  "nodes": [ { "id": "n1", "op": "scan", "params": { } }, ... ],
  "edges": [ { "from": "n1", "to": "n2", "port": "in" }, ... ],
  "outputs": ["nK"]
}
```

### Required fields
- `version` (string): must equal `"ir-dag-3.0-alpha"`
- `nodes` (array): non-empty, unique `id`
- `edges` (array): may be empty only if `nodes` has exactly one node that is also in `outputs`
- `outputs` (array): non-empty, each id must refer to an existing node

### Node
```json
{ "id": "n1", "op": "filter", "params": { ... } }
```
- `id` (string): unique within plan. Should be stable/hashable after normalization.
- `op` (string): operator type
- `params` (object): operator-specific parameters (must be JSON)

### Edge
```json
{ "from": "n1", "to": "n2", "port": "in" }
```
- `from` (string): source node id
- `to` (string): destination node id
- `port` (string, optional): input port name for multi-input operators
  - unary operators: omit or use `"in"`
  - binary operators: use `"left"` / `"right"` (recommended)

---

## 2. Invariants

### Graph
- The plan must be a **DAG**: no directed cycles.
- Every `edge.from` and `edge.to` must refer to an existing node.
- Every output id in `outputs` must refer to an existing node.
- A node may have multiple outgoing edges (fan-out).
- A node may have multiple incoming edges (fan-in), but only if its operator supports it (join, union, etc.)

### Ports
- Unary operators: may accept at most one incoming edge. If `port` is omitted, treat as `"in"`.
- Join operators: require exactly two incoming edges, one with `port:"left"` and one with `port:"right"`.

---

## 3. Operator contracts (v3.0-alpha minimal set)

### scan (unary source)
- Incoming edges: 0
- Required params: `{ "dataset": string }`

### filter (unary)
- Incoming edges: 1
- Required params: `{ "where": <exprAst> }`

### project (unary)
- Incoming edges: 1
- Required params: `{ "exprs": { <outCol>: <exprAst>, ... } }`

### join (binary)
- Incoming edges: 2 (ports: left/right)
- Required params:
  - `type`: `"inner"` | `"left"` (extend later)
  - `on`: array of pairs, e.g. `[["symbol","symbol"]]`

### groupBy (unary barrier)
- Incoming edges: 1
- Required params: `{ "keys": [string], "aggs": { <outCol>: <aggSpec>, ... } }`

### sink (unary terminal)
- Incoming edges: 1
- Required params: `{ "collection": string }` (or `{ "target": ... }`)

---

## 4. Canonicalization rules (for stable hashing)

Canonicalization produces a *normalized* plan with stable, comparable structure.

### 4.1 Stable JSON
All hashing uses a stable JSON stringify:
- object keys sorted lexicographically, recursively
- arrays preserved in order

### 4.2 Node canonical form
For each node:
- keep `id` as provided (or computed if missing; see below)
- normalize operator field to `op` (string)
- ensure `params` exists (object; default `{}`)
- canonicalize known params:
  - `project.params.exprs` key order sorted
  - `groupBy.params.keys` stringified
  - `join.params.on` preserved order; key strings normalized

### 4.3 Edge canonical form
- ensure `port` exists:
  - if absent → `"in"`
- sort edges by `(to, port, from)` for hashing and diff stability

### 4.4 ID assignment (optional)
If a producer omits ids or uses unstable ids, a normalizer may assign:
- `id = "n_" + sha1(canonicalNodeWithoutId).slice(0,10)`
However, v3.0-alpha recommends that planners provide ids already.

### 4.5 Plan hash
`dagHash = sha256(stableStringify(canonicalPlan))`

---

## 5. Lineage alignment (v3.0-alpha)
Lineage collectors MUST record per node:
- `rowsInByPort` (object): e.g. `{ "in": 1000 }` or `{ "left": 1000, "right": 1200 }`
- `rowsOut`
- `ms` (wall time)
- `timeline` events with `{ nodeId, event, ts, ms? }`

This makes lineage a first-class measurement of the DAG, enabling the optimizer feedback loop.

---
