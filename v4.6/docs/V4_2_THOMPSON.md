# v4.2 — Thompson sampling + variance tracking

## What changed
`plan_variant_stats` now tracks a variance proxy in addition to EMA wall-time:

- `emaWallMs`: EMA wall time in ms
- `emaWallMs2`: EMA of squared wall time (ms²)
- `emaVar`: `emaWallMs2 - (emaWallMs)^2` (clamped ≥ 0)
- `runs`, `lastWallMs`

This enables a standard-error estimate: `sigma = sqrt(emaVar) / sqrt(runs+1)`.

## How selection uses it
When `AXIOMFLOW_POLICY=thompson`, the selector draws:

`sample = mean + sigma * N(0,1)`

and picks the minimum-sample candidate.

If `emaVar` is missing, the selector falls back to an assumed coefficient-of-variation (25% of mean).

## Data migration
No migration required; the new fields are written on the next execution of a plan variant.
