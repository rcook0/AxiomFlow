# v4.4 — Plan family cache (enumeration caching)

## Goal
Avoid repeating expensive candidate enumeration for the same `familyDagHash`.

## Storage
Collection: `plan_family_cache`
Unique index: `{ familyDagHash: 1 }`

Stored payload:
- `bestPlan`: canonical DAG plan object
- `bestCost`: cost summary object
- `candidates[]`: `{ rank, plan, cost, estCost, chosenOrder }`
- `changes[]`: optimizer rewrite log

## Behavior
- On executor start, if `plan_family_cache` has an entry for the input `dagHash`, the optimizer loop uses it as `optOverride`.
- Set `AXIOMFLOW_CACHE_REFRESH=1` to refresh cache entries.

## Caveats
Cache correctness assumes the optimizer’s enumeration algorithm is deterministic for a given input plan.
