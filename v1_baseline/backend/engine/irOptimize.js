const { extractFilterCols, projectOutputCols } = require("./irFields");

const BARRIERS = new Set(["sort", "limit", "distinct", "groupBy", "join", "union", "intersect", "except", "unnest"]);

function isBarrier(op) {
  return BARRIERS.has(op.type);
}

function clonePlan(plan) {
  return { pipeline: plan.pipeline.map((x) => ({ ...x })) };
}

// Conservative v1: swap project->filter only if filter references only output columns of project (no rewrite).
function canSwapProjectFilter(projectOp, filterOp) {
  const filterCols = extractFilterCols(filterOp);
  const projOut = projectOutputCols(projectOp);
  for (const c of filterCols) {
    if (!projOut.has(c)) return false;
  }
  return true;
}

function optimizePipeline(plan) {
  const before = clonePlan(plan);
  const ops = before.pipeline;
  const changes = [];

  // Pass 1: bubble filters left across safe unary ops (conservative)
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type !== "filter") continue;

    let j = i;
    while (j > 0) {
      const prev = ops[j - 1];
      const cur = ops[j];

      if (isBarrier(prev)) break;

      if (prev.type === "project") {
        if (!canSwapProjectFilter(prev, cur)) break;
      }

      ops[j] = prev;
      ops[j - 1] = cur;
      changes.push({ rule: "FILTER_PUSHDOWN", from: j, to: j - 1, detail: { swappedWith: prev.type } });
      j--;
    }
  }

  // Pass 2: merge adjacent filters into one
  for (let i = 0; i < ops.length - 1; ) {
    if (ops[i].type === "filter" && ops[i + 1].type === "filter") {
      const a = ops[i], b = ops[i + 1];
      ops.splice(i, 2, { ...a, where: ["and", a.where, b.where] });
      changes.push({ rule: "MERGE_FILTERS", at: i });
      continue;
    }
    i++;
  }

  // Pass 3: remove empty projects
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type === "project" && ops[i].exprs && Object.keys(ops[i].exprs).length === 0) {
      ops.splice(i, 1);
      changes.push({ rule: "REMOVE_EMPTY_PROJECT", at: i });
      i--;
    }
  }

  return { plan: before, changes };
}

module.exports = { optimizePipeline };
