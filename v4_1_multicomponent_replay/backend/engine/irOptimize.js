const { extractColsFromExpr } = require("./irFields");
const { buildProjectMap } = require("./projectMap");
const { rewriteColsThroughProject } = require("./exprRewrite");

const BARRIERS = new Set(["sort", "limit", "distinct", "groupBy", "join", "union", "intersect", "except", "unnest"]);

function isBarrier(op) {
  return BARRIERS.has(op.type);
}

function clonePlan(plan) {
  return { pipeline: plan.pipeline.map((x) => ({ ...x })) };
}

function mkOptOpId(prefix, n) {
  return `${prefix}_${String(n).padStart(3, "0")}`;
}

/**
 * Attempt to swap: project(P) -> filter(F)  ===>  filter(F') -> project(P)
 * by rewriting filter predicate through the project mapping.
 *
 * v2.1: computed substitution allowed (pure expressions only).
 * v2.0 compatibility: prefers passthrough-only rewrite first.
 */
function tryRewriteAndSwapProjectFilter(projectOp, filterOp) {
  // First try passthrough-only (strongest safety)
  {
    const pm = buildProjectMap(projectOp, { passthroughOnly: true });
    if (pm.ok) {
      const rw = rewriteColsThroughProject(filterOp.where, pm, { mode: "passthrough" });
      if (rw.ok) {
        return {
          ok: true,
          newFilter: { ...filterOp, where: rw.rewritten },
          rewrite: { mode: "passthrough", changed: rw.changed, issues: rw.issues, before: filterOp.where, after: rw.rewritten }
        };
      }
    }
  }

  // Then try computed (pure) substitution
  {
    const pm = buildProjectMap(projectOp, { allowComputed: true });
    if (!pm.ok) return { ok: false, reason: pm.issues };

    const rw = rewriteColsThroughProject(filterOp.where, pm, { mode: "computed" });
    if (!rw.ok) return { ok: false, reason: rw.issues };

    return {
      ok: true,
      newFilter: { ...filterOp, where: rw.rewritten },
      rewrite: { mode: "computed", changed: rw.changed, issues: rw.issues, before: filterOp.where, after: rw.rewritten }
    };
  }
}

/**
 * v2.2: Required columns pass (barrier-delimited).
 *
 * For each segment between barriers, compute columns referenced by filters (after any rewrites),
 * and ensure an early projection exists that keeps at least those columns.
 *
 * This is conservative: it assumes required columns exist under the same name upstream (passthrough).
 */
function applyRequiredColumnsPass(ops, changes) {
  let optCounter = 1;

  let segStart = 0;
  for (let i = 0; i <= ops.length; i++) {
    const atEnd = i === ops.length;
    const isSegBoundary = atEnd || isBarrier(ops[i]);

    if (!isSegBoundary) continue;

    const segEnd = i; // [segStart, segEnd)
    if (segEnd - segStart > 0) {
      const required = new Set();

      for (let k = segStart; k < segEnd; k++) {
        if (ops[k].type === "filter" && ops[k].where) {
          extractColsFromExpr(ops[k].where, required);
        }
      }

      if (required.size > 0) {
        const first = ops[segStart];

        if (first.type === "project") {
          first.exprs = first.exprs || {};
          let added = 0;
          for (const col of required) {
            if (!(col in first.exprs)) {
              first.exprs[col] = ["col", col];
              added++;
            }
          }
          if (added > 0) {
            changes.push({
              rule: "REQUIRED_COLS_EXTEND_PROJECT",
              at: segStart,
              detail: { opId_project: first.opId, added, cols: Array.from(required) }
            });
          }
        } else {
          const newOpId = mkOptOpId("opt_proj", optCounter++);
          const exprs = {};
          for (const col of required) exprs[col] = ["col", col];

          const newProj = { opId: newOpId, type: "project", exprs };
          ops.splice(segStart, 0, newProj);

          changes.push({
            rule: "REQUIRED_COLS_INSERT_PROJECT",
            at: segStart,
            detail: { opId_project: newOpId, cols: Array.from(required) }
          });

          i += 1;
        }
      }
    }

    segStart = i + 1;
  }
}

function optimizePipeline(plan) {
  const before = clonePlan(plan);
  const ops = before.pipeline;
  const changes = [];

  // Pass 1: merge adjacent filters into one early (helps rewrite/pushdown)
  for (let i = 0; i < ops.length - 1; ) {
    if (ops[i].type === "filter" && ops[i + 1].type === "filter") {
      const a = ops[i], b = ops[i + 1];
      ops.splice(i, 2, { ...a, where: ["and", a.where, b.where] });
      changes.push({ rule: "MERGE_FILTERS", at: i, detail: { opId_a: a.opId, opId_b: b.opId } });
      continue;
    }
    i++;
  }

  // Pass 2: rewrite-aware filter pushdown across unary ops, including computed rewrite over projects (v2.1)
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type !== "filter") continue;

    let j = i;
    while (j > 0) {
      const prev = ops[j - 1];
      const cur = ops[j];

      if (isBarrier(prev)) break;

      if (prev.type === "project") {
        const attempt = tryRewriteAndSwapProjectFilter(prev, cur);
        if (!attempt.ok) break;

        ops[j] = prev;
        ops[j - 1] = attempt.newFilter;

        changes.push({
          rule: "FILTER_PUSHDOWN_REWRITE",
          from: j,
          to: j - 1,
          detail: { swappedWith: "project", opId_filter: cur.opId, opId_project: prev.opId },
          rewrite: attempt.rewrite
        });

        j--;
        continue;
      }

      ops[j] = prev;
      ops[j - 1] = cur;
      changes.push({
        rule: "FILTER_PUSHDOWN",
        from: j,
        to: j - 1,
        detail: { swappedWith: prev.type, opId_filter: cur.opId, opId_prev: prev.opId }
      });
      j--;
    }
  }

  // Pass 3: remove empty projects
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type === "project" && ops[i].exprs && Object.keys(ops[i].exprs).length === 0) {
      const removed = ops[i];
      ops.splice(i, 1);
      changes.push({ rule: "REMOVE_EMPTY_PROJECT", at: i, detail: { opId: removed.opId } });
      i--;
    }
  }

  // Pass 4: required columns projection pass (v2.2)
  applyRequiredColumnsPass(ops, changes);

  return { plan: before, changes };
}

module.exports = { optimizePipeline };
