# v4.5 — Offline policy lab

## Goal
Evaluate selection policies offline using recorded decision traces and current EMA statistics.

## Inputs
- `optimizer_decisions` (candidate sets per run)
- `plan_variant_stats` (EMA wall times / variance)

## Script
`scripts/policy-lab.js`

Example:
```bash
node scripts/policy-lab.js mongodb://localhost:27017 axiomflow --family <dagHash> --limit 5000 --seed 1
```

### Tunables
- `--modes ucb,thompson,epsilon,best`
- `--eps 0.00,0.03,0.05`
- `--maxFactor 2.0,3.0,5.0`
- `--ucbC 0.25,0.5,1.0`
- `--thSigma 0.5,1.0,2.0`

## Output
Ranks configs by average regret (ms) vs best-mean candidate under the current stats snapshot.
