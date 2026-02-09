# v3.8 — Learned selectivity for compositions

v3.8 generalizes selectivity learning beyond single predicates and single joins:

## 1) AND predicate groups (`pred_group_stats`)
When a filter predicate is a conjunction (an `and` tree), v3.8 computes:

- `predSig`: signature of the whole predicate expression
- `predGroupSig`: signature of the **multiset** of **atom** signatures (flattened `and`)

This lets the optimizer reuse learned selectivity even if the expression is rewritten/reordered
or partially refactored (as long as the atom set is the same).

**Planning-time priority (filter selectivity):**
1. `join_filter_stats` (if filter consumes a join output; see below)
2. `pred_group_stats[predGroupSig].emaSelectivity`
3. `pred_sig_stats[predSig].emaSelectivity`
4. per-node EMA selectivity (legacy)
5. `params.estimatedSelectivity`
6. default (0.3)

## 2) Join→Filter segments (`join_filter_stats`)
If a filter node consumes the output of a join, selectivity often depends on *both* sides.
These predicates cannot be pushed down, so the optimizer needs a dedicated signal.

v3.8 computes a **segment signature**:

`joinFilterSig = sha1(joinSig + ":" + (predGroupSig || predSig))`

and learns selectivity (rowsOut / rowsIn) for that post-join filter.

This improves join ordering and cost estimates in workloads where the expensive reduction
happens after a join.
