# AxiomFlow Architecture (Living Document)

This document describes the current architecture of AxiomFlow and outlines how it is expected to evolve. It is intentionally forward-looking and will expand as new optimizer capabilities and execution strategies are added.

---

## 1. Core Design Principles

### Explicit Intermediate Representation
All computation is represented as a DAG IR. There is no implicit execution order, hidden pushdown, or opaque planner state. Every transformation is explicit and auditable.

### Lineage as a First-Class Signal
Execution lineage is not a by-product; it is a primary output. Lineage feeds the optimizer, supports audit and replay, and enables cost modeling.

### Rule-Based Before Cost-Based
Optimization begins with deterministic rewrite rules. Cost signals are layered in incrementally, avoiding premature global search or complex heuristics.

### Separation of Concerns
- Planner produces IR
- Optimizer rewrites IR
- Executor runs IR
- Lineage measures execution
- Persistence closes the feedback loop

---

## 2. Logical Layers

### 2.1 DAG IR Layer
Defines nodes, edges, ports, and outputs. This layer is stable and versioned. All higher layers depend on it.

### 2.2 Optimizer Layer
Consumes DAG IR and emits a rewritten DAG plus an explain artifact.

Current capabilities:
- Filter pushdown (rewrite-aware)
- Required column projection
- Materialization insertion
- Cost-aware rule gating (EMA-based)

Future:
- Join reordering
- Operator fusion
- Physical operator selection

### 2.3 Execution Layer
Executes DAGs in topological order.

Current state:
- Unary operators
- Materialized execution (arrays)

Planned:
- Streaming execution
- Parallel branches
- Backpressure-aware scheduling

### 2.4 Lineage Layer
Captures per-node timing, per-port row counts, and execution timelines. Lineage is DAG-native and persists independently of execution.

### 2.5 Persistence & Feedback Layer
Stores optimizer reports, lineage per run, and EMA cost models per node. This layer enables learning across runs without compromising determinism.

---

## 3. Optimizer Feedback Loop

1. Normalize DAG to dagHash
2. Optimize DAG
3. Execute optimized DAG
4. Collect lineage
5. Update EMA cost model
6. Reuse stats in future optimizations

---

## 4. Scope for Evolution

### Near-Term (v3.x)
- Join-aware optimization
- DAG-wide cost propagation
- Streaming execution
- Operator-level materialization policies

### Mid-Term (v4.x)
- Cost-based search
- Multi-backend execution targets
- Adaptive execution strategies
- Partial recomputation and caching

### Long-Term
- Declarative query language front-end
- Distributed execution
- Formal optimization correctness proofs
- Deterministic replay and verification modes

---

## 5. Non-Goals

- Black-box optimizers
- Opaque adaptive heuristics
- Backend-specific coupling
- Hidden execution state

AxiomFlow prioritizes correctness, explainability, and control over raw opacity-driven performance.