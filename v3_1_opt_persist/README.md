

## v3.1 — DAG optimize + persist (beta)
- Optimizer: linear-chain DAG rewrite (filter pushdown with computed predicate rewrite + required-columns projection).
- Persistence: stores optimizer report, run DAG lineage, and EMA node stats keyed by (dagHash,nodeId).
- Index script: scripts/db-indexes-dag-v3-1.js
