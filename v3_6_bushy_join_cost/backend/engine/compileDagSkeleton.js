const { normalizeDagPlan } = require("./planNormalizeDag");

function topoSort(nodes, edges) {
  const indeg = new Map();
  const adj = new Map();
  for (const n of nodes) {
    indeg.set(n.id, 0);
    adj.set(n.id, []);
  }
  for (const e of edges) {
    if (!adj.has(e.from) || !indeg.has(e.to)) continue;
    adj.get(e.from).push(e.to);
    indeg.set(e.to, indeg.get(e.to) + 1);
  }
  const q = [];
  for (const [id, d] of indeg.entries()) if (d === 0) q.push(id);

  const order = [];
  while (q.length) {
    const id = q.shift();
    order.push(id);
    for (const nxt of adj.get(id) || []) {
      indeg.set(nxt, indeg.get(nxt) - 1);
      if (indeg.get(nxt) === 0) q.push(nxt);
    }
  }
  if (order.length !== nodes.length) throw new Error("DAG cycle detected during topoSort");
  return { order, adj };
}

function buildIncomingByPort(edges) {
  const incoming = new Map();
  for (const e of edges) {
    const port = e.port || "in";
    if (!incoming.has(e.to)) incoming.set(e.to, new Map());
    const m = incoming.get(e.to);
    if (!m.has(port)) m.set(port, []);
    m.get(port).push(e.from);
  }
  return incoming;
}

function compileDagSkeleton(plan, opts = {}) {
  const norm = normalizeDagPlan(plan, { assignIdsIfMissing: !!opts.assignIdsIfMissing });
  if (!norm.ok) return { ok: false, errors: norm.errors };

  const nodes = norm.plan.nodes;
  const edges = norm.plan.edges;

  const nodeById = new Map(nodes.map((n) => [n.id, n]));
  const { order, adj } = topoSort(nodes, edges);
  const incomingByPort = buildIncomingByPort(edges);

  return {
    ok: true,
    dagHash: norm.dagHash,
    plan: norm.plan,
    nodeById,
    topoOrder: order,
    adj,
    incomingByPort
  };
}

module.exports = { compileDagSkeleton };
