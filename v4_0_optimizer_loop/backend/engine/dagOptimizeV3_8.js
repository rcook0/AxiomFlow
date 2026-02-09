const { optimizeDagV3_4 } = require("./dagOptimizeV3_4");
const { rewriteJoinTreeV3_8, findRootJoins, leafRelIdForNode } = require("./joinOrderV3_8");
const { estimatePlanCostV3_8 } = require("./costModelV3_8");

function buildIncoming(plan){
  const inc = new Map();
  for (const e of (plan.edges || [])) {
    if (!inc.has(e.to)) inc.set(e.to, []);
    inc.get(e.to).push(e);
  }
  return inc;
}

function chooseAlgo(leftRows, rightRows, opts){
  const minSide = Math.min(leftRows, rightRows);
  const maxSide = Math.max(leftRows, rightRows);
  const nlMaxInner = opts?.nestedLoopMaxInnerRows ?? 2000;
  const nlMaxOuter = opts?.nestedLoopMaxOuterRows ?? 80000;
  if (minSide <= nlMaxInner && maxSide <= nlMaxOuter) return "nested_loop";
  return "hash";
}

/**
 * Apply physical join policies (algorithm/build) using row estimates.
 */
function assignPhysicalJoinPolicies(plan, rowsEstimate, changes, joinPolicyOpts){
  const inc = buildIncoming(plan);
  for (const n of (plan.nodes || [])) {
    if (n.op !== "join") continue;
    const inE = inc.get(n.id) || [];
    const leftE = inE.find(e=>(e.port||"in")==="left");
    const rightE = inE.find(e=>(e.port||"in")==="right");
    if (!leftE || !rightE) continue;

    const leftRows = rowsEstimate.get(leftE.from) ?? 1000;
    const rightRows = rowsEstimate.get(rightE.from) ?? 1000;

    const algo = chooseAlgo(leftRows, rightRows, joinPolicyOpts || {});
    const build = (rightRows <= leftRows) ? "right" : "left";

    const prevAlgo = n.params?.algorithm;
    const prevBuild = n.params?.build;

    if (!n.params) n.params = {};
    n.params.algorithm = algo;
    n.params.build = build;

    if (prevAlgo !== algo || prevBuild !== build) {
      changes.push({ rule: "JOIN_PHYSICAL_POLICY", nodeId: n.id, detail: { leftRows, rightRows, algorithm: algo, build } });
    }
  }
}

/**
 * v3.8 optimizer:
 * - v3.4 logical join policies
 * - top-K bushy join ordering (bounded)
 * - learned selectivity for AND predicate groups + join->filter segments
 * - automatic exploration support (emits candidate plan variants when a single join component exists)
 */
function optimizeDagV3_8(plan, statsByNodeId, joinSigStats, predSigStats, predGroupStats, joinFilterStats, opts) {
  const s = statsByNodeId || new Map();
  const js = joinSigStats || new Map();
  const ps = predSigStats || new Map();
  const pg = predGroupStats || new Map();
  const jf = joinFilterStats || new Map();
  const o = opts || {};

  // Phase 1: v3.4
  const pre = optimizeDagV3_4(plan, s);
  if (!pre.ok) return pre;

  const changes = [...(pre.changes || [])];
  const prePlan = pre.after;

  // Pre-cost to seed base rel sizes with learned filter/join->filter selectivity
  const preCost = estimatePlanCostV3_8(prePlan, s, js, ps, pg, jf, o.costModel || {});
  const rowsPre = preCost.rowsByNode;

  const roots = findRootJoins(prePlan);

  // Helper to build baseRelSizes for a plan
  function computeBaseRelSizes(planX){
    const inc = buildIncoming(planX);
    const nodeById = new Map((planX.nodes || []).map(n=>[n.id,n]));
    const baseRelSizes = new Map();

    for (const n of (planX.nodes || [])) {
      if (n.op !== "join") continue;
      const inE = inc.get(n.id) || [];
      for (const e of inE) {
        const port = e.port || "in";
        if (port !== "left" && port !== "right") continue;
        const fromNode = nodeById.get(e.from);
        if (fromNode && fromNode.op === "join") continue;
        const rel = leafRelIdForNode(planX, e.from);
        const rows = rowsPre.get(e.from) ?? 1000;
        if (!baseRelSizes.has(rel)) baseRelSizes.set(rel, rows);
        else baseRelSizes.set(rel, Math.min(baseRelSizes.get(rel), rows));
      }
    }
    return baseRelSizes;
  }

  // Order joins (best plan)
  let cur = prePlan;
  let variants = null;

  if (roots.length === 1) {
    const r = roots[0];
    const baseRelSizes = computeBaseRelSizes(cur);
    const res = rewriteJoinTreeV3_8(cur, r.id, baseRelSizes, js, o.joinOrder || {});
    if (res.ok) {
      // The "best" rewrite becomes the main plan
      cur = res.best.plan;
      changes.push({ rule: "JOIN_ORDER_TOPK_DP", nodeId: r.id, detail: { dp: res.dp, best: { estCost: res.best.estCost, estRows: res.best.estRows, chosenOrder: res.best.chosenOrder } } });

      // Candidates for exploration (excluding best)
      const cands = (res.candidates || []).slice(1).map(c => ({
        rank: c.rank,
        reason: "JOIN_ORDER_TOPK_DP",
        estCost: c.estCost,
        estRows: c.estRows,
        chosenOrder: c.chosenOrder,
        plan: c.plan
      }));
      variants = { candidates: cands, dp: res.dp };
    } else {
      changes.push({ rule: "JOIN_ORDER_SKIP", nodeId: r.id, detail: res });
    }
  } else {
    // Multiple join roots: rewrite each to best only (no variants), in a stable order.
    for (const r of roots) {
      const baseRelSizes = computeBaseRelSizes(cur);
      const res = rewriteJoinTreeV3_8(cur, r.id, baseRelSizes, js, o.joinOrder || {});
      if (!res.ok) {
        changes.push({ rule: "JOIN_ORDER_SKIP", nodeId: r.id, detail: res });
        continue;
      }
      cur = res.best.plan;
      changes.push({ rule: "JOIN_ORDER_TOPK_DP", nodeId: r.id, detail: { dp: res.dp, best: { estCost: res.best.estCost, estRows: res.best.estRows, chosenOrder: res.best.chosenOrder } } });
    }
  }

  // Post-pass v3.4
  const post = optimizeDagV3_4(cur, s);
  if (post.ok) {
    cur = post.after;
    changes.push(...(post.changes || []));
  } else {
    changes.push({ rule: "POSTPASS_V3_4_FAILED", detail: post });
  }

  // Physical join policy assignment + cost report
  const cost = estimatePlanCostV3_8(cur, s, js, ps, pg, jf, o.costModel || {});
  assignPhysicalJoinPolicies(cur, cost.rowsByNode, changes, o.joinPolicy || {});

  // Recompute cost after policy assignment (algorithms can affect cost)
  const cost2 = estimatePlanCostV3_8(cur, s, js, ps, pg, jf, o.costModel || {});
  const estRowsObj = Object.fromEntries([...cost2.rowsByNode.entries()]);
  const estCostObj = Object.fromEntries([...cost2.costByNode.entries()]);
  const costSummary = { totalCost: cost2.totalCost };

  // Finalize variants similarly (if present)
  if (variants && variants.candidates && variants.candidates.length) {
    const finalized = [];
    for (const v of variants.candidates) {
      let p = v.plan;

      const postV = optimizeDagV3_4(p, s);
      if (postV.ok) p = postV.after;

      const c0 = estimatePlanCostV3_8(p, s, js, ps, pg, jf, o.costModel || {});
      const ch = [];
      assignPhysicalJoinPolicies(p, c0.rowsByNode, ch, o.joinPolicy || {});
      const c1 = estimatePlanCostV3_8(p, s, js, ps, pg, jf, o.costModel || {});

      finalized.push({
        rank: v.rank,
        estCost: v.estCost,
        estRows: v.estRows,
        chosenOrder: v.chosenOrder,
        plan: p,
        cost: { summary: { totalCost: c1.totalCost } }
      });
    }
    variants.candidates = finalized;
  }

  return {
    ok: true,
    before: plan,
    after: cur,
    changes,
    cost: { summary: costSummary, estRowsByNodeId: estRowsObj, estCostByNodeId: estCostObj },
    variants
  };
}

module.exports = { optimizeDagV3_8 };
