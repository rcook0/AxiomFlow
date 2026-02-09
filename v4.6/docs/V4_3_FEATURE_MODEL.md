# v4.3 — Learned wall-time feature model

## Goal
Provide a cold-start wall-time estimate for plan variants that have insufficient historical runs.

## Storage
Collection: `runtime_models`
Key: `name`
Default model: `wall_ms_linear_v1`

## Model
A lightweight linear regression trained online:

`wall_ms ≈ dot(w, x)`

### Features
Extracted from the plan and its computed cost summary:

- bias
- node count, edge count
- counts of scan/filter/project/join/materialize/sink
- `log(1+totalCost)` (stabilizes scale)

Implementation: `backend/optimizer/featureModelV4.js`

## Training
Online SGD with L2 penalty:

- `AXIOMFLOW_MODEL_LR` (default `0.001`)
- `AXIOMFLOW_MODEL_L2` (default `0.0001`)

The executor updates the model after each run.
