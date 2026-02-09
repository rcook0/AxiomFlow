const { optimizeDagV3_4 } = require("./dagOptimizeV3_4");
const { rewriteJoinTreeV3_6, findRootJoins, leafRelIdForNode } = require("./joinOrderV3_6");
const { estimatePlanCostV3_6 } = require("./costModelV3_6");

function statRows(st){
  if (!st) return null;
  return (st.emaRowsOut ?? st.emaRowsIn ?? null);
}

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
 * v3.6 optimizer:
 * - v3.4 logical join policies
 * - bushy join ordering (bounded) for eligible join components
 * - physical join policy assignment (algorithm + build side)
 * - cost propagation report (estimated rows/cost)
 */
function optimizeDagV3_6(plan, statsByNodeId, joinSigStats, opts) {
  const s = statsByNodeId || new Map();
  const js = joinSigStats || new Map();
  const o = opts || {};

  // Phase 1: v3.4
  const pre = optimizeDagV3_4(plan, s);
  if (!pre.ok) return pre;

  let cur = pre.after;
  const changes = [...(pre.changes || [])];

  // Phase 2: join ordering
  const roots = findRootJoins(cur);
  for (const r of roots) {
    // estimate base rel sizes from stats on non-join inputs to join nodes
    const baseRelSizes = new Map();
    const inc = buildIncoming(cur);
    const nodeById = new Map((cur.nodes || []).map(n=>[n.id,n]));

    for (const n of (cur.nodes || [])) {
      if (n.op !== "join") continue;
      const inE = inc.get(n.id) || [];
      for (const e of inE) {
        const port = e.port || "in";
        if (port !== "left" && port !== "right") continue;
        const fromNode = nodeById.get(e.from);
        if (fromNode && fromNode.op === "join") continue;
        const rel = leafRelIdForNode(cur, e.from);
        const rows = statRows(s.get(e.from)) ?? 1000;
        if (!baseRelSizes.has(rel)) baseRelSizes.set(rel, rows);
        else baseRelSizes.set(rel, Math.min(baseRelSizes.get(rel), rows));
      }
    }

    const res = rewriteJoinTreeV3_6(cur, r.id, baseRelSizes, js, o.joinOrder || {});
    if (!res.ok) {
      changes.push({ rule: "JOIN_ORDER_SKIP", nodeId: r.id, detail: res });
      continue;
    }

    cur = res.plan;
    changes.push({
      rule: "JOIN_ORDER_BUSHY_DP",
      nodeId: r.id,
      detail: {
        beforeRootJoinId: res.beforeRootJoinId,
        afterRootJoinId: res.afterRootJoinId,
        rels: res.rels,
        estCost: res.estCost,
        estRows: res.estRows,
        chosenOrder: res.explain?.chosenOrder,
        baseRelSizes: res.explain?.baseRelSizes || Object.fromEntries([...baseRelSizes.entries()])
      }
    });
  }

  // Phase 3: post logical cleanups (v3.4)
  const post = optimizeDagV3_4(cur, s);
  if (post.ok) {
    cur = post.after;
    changes.push(...(post.changes || []));
  } else {
    changes.push({ rule: "POSTPASS_V3_4_FAILED", detail: post });
  }

  // Phase 4: physical join policy assignment (algorithm/build)
  const inc = buildIncoming(cur);
  const nodeById = new Map((cur.nodes || []).map(n=>[n.id,n]));
  const rowsEstimate = estimatePlanCostV3_6(cur, s, js, o.costModel || {}).rowsByNode;

  for (const n of (cur.nodes || [])) {
    if (n.op !== "join") continue;
    const inE = inc.get(n.id) || [];
    const leftE = inE.find(e=>(e.port||"in")==="left");
    const rightE = inE.find(e=>(e.port||"in")==="right");
    if (!leftE || !rightE) continue;

    const leftRows = rowsEstimate.get(leftE.from) ?? 1000;
    const rightRows = rowsEstimate.get(rightE.from) ?? 1000;

    const algo = chooseAlgo(leftRows, rightRows, o.joinPolicy || {});
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

  // Phase 5: cost propagation report
  const cost = estimatePlanCostV3_6(cur, s, js, o.costModel || {});
  const estRowsObj = Object.fromEntries([...cost.rowsByNode.entries()]);
  const estCostObj = Object.fromEntries([...cost.costByNode.entries()]);
  const costSummary = { totalCost: cost.totalCost };

  return {
    ok: true,
    before: plan,
    after: cur,
    changes,
    cost: { summary: costSummary, estRowsByNodeId: estRowsObj, estCostByNodeId: estCostObj }
  };
}

module.exports = { optimizeDagV3_6 };
