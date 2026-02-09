/**
 * featureModelV4.js
 *
 * Lightweight online regression: wallMs ≈ dot(w, x)
 *
 * Stored in Mongo in collection `runtime_models` under name `wall_ms_linear_v1`.
 */
function extractFeatures(plan, optCost) {
  const nodes = plan.nodes || [];
  const edges = plan.edges || [];

  let joins=0, filters=0, projects=0, mats=0, scans=0, sinks=0;
  for (const n of nodes) {
    if (n.op === "join") joins++;
    else if (n.op === "filter") filters++;
    else if (n.op === "project") projects++;
    else if (n.op === "materialize") mats++;
    else if (n.op === "scan") scans++;
    else if (n.op === "sink") sinks++;
  }

  const totalCost = optCost?.summary?.totalCost ?? optCost?.totalCost ?? 0;

  // Stable ordering
  const x = [
    1,                    // bias
    nodes.length,
    edges.length,
    scans,
    filters,
    projects,
    joins,
    mats,
    sinks,
    Math.log(1 + Math.max(0, totalCost))
  ];

  return x;
}

function dot(w, x) {
  let s = 0;
  for (let i=0; i<x.length; i++) s += (w[i] || 0) * x[i];
  return s;
}

function predictWallMs(modelDoc, x) {
  if (!modelDoc || !Array.isArray(modelDoc.w)) return null;
  const y = dot(modelDoc.w, x);
  if (!Number.isFinite(y)) return null;
  return Math.max(1, y);
}

function updateModelSgd(modelDoc, x, yTrue, opts = {}) {
  const lr = opts.lr ?? 0.001;
  const l2 = opts.l2 ?? 1e-4;

  const dim = x.length;
  const w = Array.isArray(modelDoc?.w) ? modelDoc.w.slice() : new Array(dim).fill(0);
  while (w.length < dim) w.push(0);

  const yPred = dot(w, x);
  const err = yPred - yTrue;

  for (let i=0; i<dim; i++) {
    const grad = err * x[i] + l2 * w[i];
    w[i] -= lr * grad;
  }

  return {
    w,
    runs: (modelDoc?.runs || 0) + 1,
    lastErr: err,
    lastPred: yPred
  };
}

module.exports = { extractFeatures, predictWallMs, updateModelSgd };
