# v4.6 — Streaming execution fast-path

## Goal
Avoid full in-memory materialization for common linear pipelines.

## Supported shape
`scan -> (filter|project)* -> sink`

## Mechanism
The executor detects streamable plans and executes them via a Mongo cursor:
- reads documents from the scan dataset
- applies filters/projects row-by-row
- inserts results into sink collection in batches

Implementation: `backend/engine/executePipelineStreaming.js`

## Controls
- `AXIOMFLOW_STREAMING=0` disables the fast-path.
- `AXIOMFLOW_STREAMING_BATCH` sets insert batch size (default 1000).

## Lineage
Lineage is preserved at a coarse level (rows-in/out per node) without retaining all row payloads.
