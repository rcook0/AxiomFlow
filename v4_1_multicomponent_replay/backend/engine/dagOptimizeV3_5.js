const { optimizeDagV3_4 } = require("./dagOptimizeV3_4");
const { rewriteJoinTree, findRootJoins, collectJoinEdges, leafRelIdForNode } = require("./joinOrderV3_5");

function statRows(st){
  if (!st) return null;
  return (st.emaRowsOut ?? st.emaRowsIn ?? null);
}

/**
 * v3.5: join ordering (left-deep DP) + v3.4 rules.
 *
 * Inputs:
 * - statsByNodeId: Map(nodeId -> EMA stats doc) for the current dagHash (optional)
 * - joinSigStats: Map(joinSig -> stats doc) for learned join fanout (optional)
 */
function optimizeDagV3_5(plan, statsByNodeId, joinSigStats) {
  const s = statsByNodeId || new Map();
  const js = joinSigStats || new Map();

  // Phase 1: v3.4
  const pre = optimizeDagV3_4(plan, s);
  if (!pre.ok) return pre;

  let cur = pre.after;
  const changes = [...(pre.changes || [])];

  // Phase 2: join ordering for eligible root join components
  const roots = findRootJoins(cur);
  for (const r of roots) {
    const baseRelSizes = new Map();
    try {
      const { joins } = collectJoinEdges(cur, r.id);
      for (const j of joins) {
        // leaf inputs only (non-join)
        const leftIsJoin = false; // best-effort: rewriteJoinTree will handle joins; we size leaves here
        const rightIsJoin = false;
      }
      // Simpler: infer leaf roots by looking at join inputs where input node op != "join"
      // We don't have nodeById here; leafRelIdForNode can still infer rel id from leaf root.
      // We'll use stats keyed by leaf root node id if present.
      // We approximate by scanning all join nodes in current plan: if a join input is non-join, use it.
      for (const n of (cur.nodes || [])) {
        if (n.op !== "join") continue;
        // find incoming edges
        const inc = (cur.edges || []).filter(e=>e.to===n.id);
        for (const e of inc) {
          const port = e.port || "in";
          if (port !== "left" && port !== "right") continue;
          const fromId = e.from;
          const fromNode = (cur.nodes || []).find(x=>x.id===fromId);
          if (fromNode && fromNode.op === "join") continue;
          const rel = leafRelIdForNode(cur, fromId);
          const rows = statRows(s.get(fromId)) ?? 1000;
          if (!baseRelSizes.has(rel)) baseRelSizes.set(rel, rows);
          else baseRelSizes.set(rel, Math.min(baseRelSizes.get(rel), rows)); // be conservative
        }
      }
    } catch (_) {
      // ok: sizing remains default inside DP
    }

    const res = rewriteJoinTree(cur, r.id, baseRelSizes, js);
    if (!res.ok) {
      changes.push({ rule: "JOIN_ORDER_SKIP", nodeId: r.id, detail: res });
      continue;
    }

    cur = res.plan;
    changes.push({
      rule: "JOIN_ORDER_DP_LEFTDEEP",
      nodeId: r.id,
      detail: {
        beforeRootJoinId: res.beforeRootJoinId,
        afterRootJoinId: res.afterRootJoinId,
        rels: res.rels,
        estCost: res.estCost,
        estRows: res.estRows,
        chosenOrder: res.explain?.chosenOrder,
        baseRelSizes: Object.fromEntries([...baseRelSizes.entries()])
      }
    });
  }

  // Phase 3: v3.4 again (post-cleanup)
  const post = optimizeDagV3_4(cur, s);
  if (!post.ok) {
    return { ok: true, before: plan, after: cur, changes: [...changes, { rule: "POSTPASS_V3_4_FAILED", detail: post }] };
  }

  return { ok: true, before: plan, after: post.after, changes: [...changes, ...(post.changes || [])] };
}

module.exports = { optimizeDagV3_5 };