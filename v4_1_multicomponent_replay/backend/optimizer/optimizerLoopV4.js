const { optimizeDagV3_8 } = require("../engine/dagOptimizeV3_8");
const { selectPlanVariantV4 } = require("./selectPlanV4");

/**
 * V4 optimizer loop: optimize + select an executable plan variant.
 *
 * Inputs are already-loaded stat maps. Selection may use plan_variant_stats.
 */
function optimizeAndSelectV4(inputPlan, stats, opts = {}) {
  const {
    statsByNodeId = new Map(),
    joinSigStats = new Map(),
    predSigStats = new Map(),
    predGroupStats = new Map(),
    joinFilterStats = new Map(),
    planVariantStats = new Map(),
    inputDagHash = null
  } = stats || {};

  const opt = optimizeDagV3_8(inputPlan, statsByNodeId, joinSigStats, predSigStats, predGroupStats, joinFilterStats, opts.optimize || {});
  if (!opt.ok) {
    return { ok: false, opt, plan: inputPlan, meta: { selected: "input", explored: false } };
  }

  const sel = selectPlanVariantV4(inputDagHash, opt, planVariantStats, opts.policy || {});
  return { ok: true, opt, plan: sel.plan, meta: sel.meta, variantDagHash: sel.variantDagHash };
}

module.exports = { optimizeAndSelectV4 };
