const crypto = require("crypto");

function stableStringify(x) {
  if (x === null || typeof x !== "object") return JSON.stringify(x);
  if (Array.isArray(x)) return "[" + x.map(stableStringify).join(",") + "]";

  const keys = Object.keys(x).sort();
  const entries = keys.map((k) => JSON.stringify(k) + ":" + stableStringify(x[k]));
  return "{" + entries.join(",") + "}";
}

function canonicalizeOp(op) {
  const out = { ...op };

  if (out.op && !out.type) out.type = out.op;

  if (out.type === "project" && out.exprs && typeof out.exprs === "object") {
    out.exprs = Object.fromEntries(Object.keys(out.exprs).sort().map((k) => [k, out.exprs[k]]));
  }

  if (out.type === "sort" && Array.isArray(out.keys)) {
    out.keys = out.keys.map(String);
  }

  if (out.type === "groupBy") {
    if (Array.isArray(out.keys)) out.keys = out.keys.map(String);
    if (out.aggs && typeof out.aggs === "object") {
      out.aggs = Object.fromEntries(Object.keys(out.aggs).sort().map((k) => [k, out.aggs[k]]));
    }
  }

  delete out.opId;
  delete out.debug;
  return out;
}

function opIdFor(normalizedOp, index) {
  const h = crypto.createHash("sha1");
  h.update(stableStringify(normalizedOp));
  h.update("|" + String(index));
  return "op_" + h.digest("hex").slice(0, 10);
}

function normalizePlan(plan) {
  const pipeline = plan?.pipeline || plan?.pipeline?.pipeline || plan?.ops || plan?.pipeline;
  const ops = Array.isArray(pipeline) ? pipeline : (plan?.pipeline && Array.isArray(plan.pipeline) ? plan.pipeline : []);

  const normalizedOps = ops.map(canonicalizeOp);
  const opIds = normalizedOps.map((op, i) => opIdFor(op, i));

  const nodes = normalizedOps.map((op, i) => ({
    opId: opIds[i],
    type: op.type || op.op || "unknown",
    params: (() => {
      const { type, op: opField, ...rest } = op;
      return rest;
    })()
  }));

  const edges = [];
  for (let i = 0; i < opIds.length - 1; i++) edges.push({ from: opIds[i], to: opIds[i + 1] });

  const normalizedPlan = { pipeline: normalizedOps.map((op, i) => ({ opId: opIds[i], ...op })) };
  const planHash = crypto.createHash("sha256").update(stableStringify(normalizedPlan)).digest("hex");

  return { normalizedPlan, opIds, planHash, graph: { nodes, edges } };
}

module.exports = { normalizePlan, stableStringify };
