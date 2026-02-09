# Predicate Selectivity Learning (v3.7)

v3.7 adds **predicate signature stats** so filter selectivity can be learned **independently of node ids** (which change when the optimizer rewrites the DAG).

## predSig definition
`predSig` is a SHA-1 hash of a **canonicalized** representation of `filter.params.where`.

Canonicalization rules (current, intentionally conservative):
- `and` / `or` arguments are sorted (commutative)
- `eq` (`==`) arguments are sorted for stability
- object keys are sorted recursively
- other operators preserve argument order

This yields a stable signature for the same logical predicate across rewrites.

## Collection: `pred_sig_stats`
Documents store EMA signals:
- `emaSelectivity` (rowsOut / rowsIn)
- `emaInRows`, `emaOutRows`
- `emaMs`, `emaMsPerInRow`
- `runs`, `updatedAt`
- optional `example` (one representative predicate)

## Execution-time learning
During execution, for every filter node:
- compute `predSig` from `params.where`
- compute selectivity from observed runtime stats
- update EMA document in `pred_sig_stats`

## Planning-time usage
The cost model (`costModelV3_7`) estimates filter output rows as:

1. `pred_sig_stats[predSig].emaSelectivity` (preferred)
2. per-node EMA selectivity (legacy, node-id dependent)
3. `params.estimatedSelectivity`
4. default selectivity (0.3)

This improves join ordering because base-relation row estimates include learned filter selectivity even after pushdown/cloning.
