# Roadmap


## v3.0-beta — Minimal DAG executor (unary ops)
- Implement executor for `scan → filter → project → sink` on DAG IR.
- Add expression evaluator for filter/project AST.
- Emit DAG-native lineage per node (`rowsInByPort`, `rowsOut`, `ms`).
- Provide `scripts/run-dag-demo.js` for smoke testing.
