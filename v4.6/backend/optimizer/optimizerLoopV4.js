const { optimizeDagV3_8 } = require("../engine/dagOptimizeV3_8");
const { selectPlanVariantV4 } = require("./selectPlanV4");
const { extractFeatures, predictWallMs } = require("./featureModelV4");
const { compileDagSkeleton } = require("../engine/compileDagSkeleton");

function optimizeAndSelectV4(inputPlan, stats, opts = {}) {
  const {
    statsByNodeId = new Map(),
    joinSigStats = new Map(),
    predSigStats = new Map(),
    predGroupStats = new Map(),
    joinFilterStats = new Map(),
    planVariantStats = new Map(),
    featureModel = null,
    inputDagHash = null
  } = stats || {};

  let opt = opts.optOverride || null;
  if (!opt) {
    opt = optimizeDagV3_8(inputPlan, statsByNodeId, joinSigStats, predSigStats, predGroupStats, joinFilterStats, opts.optimize || {});
  }

  if (!opt.ok) {
    return { ok: false, opt, plan: inputPlan, meta: { selected: "input", explored: false } };
  }

  const predictedWallMsByHash = new Map();
  try {
    const candidatePlans = [];
    candidatePlans.push({ plan: opt.after, cost: opt.cost });
    for (const c of (opt.variants?.candidates || [])) candidatePlans.push({ plan: c.plan, cost: c.cost });

    for (const c of candidatePlans) {
      const compiled = compileDagSkeleton(c.plan);
      if (!compiled.ok) continue;
      const x = extractFeatures(compiled.plan, c.cost);
      const pred = predictWallMs(featureModel, x);
      if (pred != null) predictedWallMsByHash.set(compiled.dagHash, pred);
    }
  } catch (_) {}

  const policy = { ...(opts.policy || {}), predictedWallMsByHash };
  const sel = selectPlanVariantV4(inputDagHash, opt, planVariantStats, policy);
  return { ok: true, opt, plan: sel.plan, meta: sel.meta, variantDagHash: sel.variantDagHash };
}

module.exports = { optimizeAndSelectV4 };
