const { optimizeDagLinear } = require("./dagOptimizeV3_1");

/**
 * v3.3 join-aware optimization (beta):
 * - Supports plans containing one or more `join` nodes.
 * - Uses persisted EMA stats (dag_node_stats) to:
 *   - reorder join inputs (swap left/right) so the smaller side is on the `right` port
 *     (the default build side for hash join in this project).
 * - For join-free plans, delegates to v3.1 linear optimizer.
 *
 * Notes:
 * - This does NOT yet do join-order enumeration across multiple joins.
 * - This does NOT push filters across joins (v3.4+).
 */
function optimizeDagV3_3(plan, statsByNodeId /* Map<string,doc> | null */) {
  const stats = statsByNodeId || new Map();

  // Fast path: if no joins, reuse v3.1 optimizer.
  const hasJoin = (plan.nodes || []).some((n) => n.op === "join");
  if (!hasJoin) {
    const opt = optimizeDagLinear(plan);
    if (!opt.ok) return { ok: false, reason: opt.reason };
    return { ok: true, before: opt.before, after: opt.after, changes: opt.changes };
  }

  // Clone plan
  const after = JSON.parse(JSON.stringify(plan));
  const nodeById = new Map(after.nodes.map((n) => [n.id, n]));

  // Build incoming edges per join
  const edges = after.edges || [];
  const changes = [];

  for (const n of after.nodes) {
    if (n.op !== "join") continue;

    const inc = edges.filter((e) => e.to === n.id);
    const leftE = inc.find((e) => (e.port || "in") === "left");
    const rightE = inc.find((e) => (e.port || "in") === "right");

    if (!leftE || !rightE) {
      // beta: require explicit ports
      changes.push({ rule: "JOIN_INPUTS_INVALID", nodeId: n.id, detail: { reason: "join must have left/right inputs" } });
      continue;
    }

    const leftId = leftE.from;
    const rightId = rightE.from;

    const ls = stats.get(leftId);
    const rs = stats.get(rightId);

    const leftEst = (ls && (ls.emaRowsOut ?? ls.emaRowsIn)) ?? null;
    const rightEst = (rs && (rs.emaRowsOut ?? rs.emaRowsIn)) ?? null;

    // If both estimates exist and right looks larger than left, swap to put smaller on right.
    if (leftEst !== null && rightEst !== null && rightEst > leftEst) {
      // swap edge sources
      leftE.from = rightId;
      rightE.from = leftId;

      // swap join params.on pairs (leftKey <-> rightKey)
      if (n.params && Array.isArray(n.params.on)) {
        n.params.on = n.params.on.map(([lk, rk]) => [rk, lk]);
      }

      changes.push({
        rule: "JOIN_SWAP_INPUTS_BY_STATS",
        nodeId: n.id,
        detail: { before: { left: leftId, right: rightId, leftEst, rightEst }, after: { left: rightId, right: leftId } }
      });
    }
  }

  // Keep outputs as-is (single output typically).
  return { ok: true, before: plan, after, changes };
}

module.exports = { optimizeDagV3_3 };