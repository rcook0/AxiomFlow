# Join Signature Stats (`join_sig_stats`)

v3.5 introduced `join_sig_stats` to learn join fanout/selectivity across runs **independent of join ordering**.

## joinSig definition
For joins with `params.onRef`, the signature is a hash of canonicalized relation predicates:
- each predicate is normalized by sorting endpoints (`rel:path`)
- predicate list is sorted
- signature is SHA-1 of the JSON string

For joins without `onRef`, the signature falls back to the compiled `params.on` key pairs.

## Persisted EMA fields
- `emaLeftIn`, `emaRightIn`, `emaOut`
- `emaFanoutPerLeft`, `emaFanoutPerRight`
- `emaMs`, `emaMsPerInRow`
- `runs`, `updatedAt`

## Why this matters
The join order DP can estimate output sizes using learned fanout:
- `out ≈ emaFanoutPerLeft * leftRows` and/or `out ≈ emaFanoutPerRight * rightRows`

This enables a real feedback loop: execute -> learn -> reorder -> execute.
