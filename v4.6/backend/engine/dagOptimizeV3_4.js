const crypto = require("crypto");
const { optimizeDagV3_3 } = require("./dagOptimizeV3_3");

function sha1(s){ return crypto.createHash("sha1").update(s).digest("hex"); }

function outEdges(plan){
  const m = new Map();
  for (const e of (plan.edges||[])) {
    if (!m.has(e.from)) m.set(e.from, []);
    m.get(e.from).push(e);
  }
  return m;
}

function inEdges(plan){
  const m = new Map();
  for (const e of (plan.edges||[])) {
    if (!m.has(e.to)) m.set(e.to, []);
    m.get(e.to).push(e);
  }
  return m;
}

function collectColPaths(expr, out=new Set()){
  if (expr == null) return out;
  if (Array.isArray(expr)) {
    const [op, ...args] = expr;
    if (op === "col" && typeof args[0] === "string") { out.add(args[0]); return out; }
    for (const a of args) collectColPaths(a, out);
    return out;
  }
  if (typeof expr === "object") for (const v of Object.values(expr)) collectColPaths(v, out);
  return out;
}

function predicateSide(whereExpr){
  const cols = [...collectColPaths(whereExpr)];
  if (cols.length === 0) return { side: "either", cols };
  const allLeft = cols.every((c) => c.startsWith("left."));
  const allRight = cols.every((c) => c.startsWith("right."));
  if (allLeft) return { side: "left", cols };
  if (allRight) return { side: "right", cols };
  return { side: "mixed", cols };
}

function stripSidePrefix(expr, side){
  const prefix = side + ".";
  function walk(x){
    if (x == null) return x;
    if (Array.isArray(x)) {
      const [op, ...args] = x;
      if (op === "col" && typeof args[0] === "string") {
        const p = args[0];
        if (p.startsWith(prefix)) return ["col", p.slice(prefix.length)];
        return x;
      }
      return [op, ...args.map(walk)];
    }
    if (typeof x === "object") {
      const o = {};
      for (const [k,v] of Object.entries(x)) o[k]=walk(v);
      return o;
    }
    return x;
  }
  return walk(expr);
}

function mkId(prefix, seed){
  return `${prefix}_${sha1(prefix+":"+seed).slice(0,10)}`;
}

/**
 * v3.4 advanced join policies:
 * - side-only predicate pushdown across join: join -> filter  ==> push filter to join input edge
 * - insert materialize after fan-out joins
 *
 * Safety constraints (beta):
 * - predicate pushdown only when filter is the sole consumer of join output
 */
function optimizeDagV3_4(plan, statsByNodeId){
  const stats = statsByNodeId || new Map();
  const base = optimizeDagV3_3(plan, stats);
  if (!base.ok) return base;

  const after = JSON.parse(JSON.stringify(base.after));
  const nodes = after.nodes || [];
  const edges = after.edges || [];
  const nodeById = new Map(nodes.map((n)=>[n.id,n]));
  const changes = [...(base.changes||[])];

  // helper maps
  let outM = outEdges(after);
  let inM = inEdges(after);

  // A) Materialize after fan-out joins
  for (const n of [...nodes]) {
    if (n.op !== "join") continue;
    const outs = outM.get(n.id) || [];
    if (outs.length <= 1) continue;

    const matId = mkId("opt_mat", n.id);
    if (nodeById.has(matId)) continue;

    const matNode = { id: matId, op: "materialize", params: { reason: "join_fanout" } };
    nodes.push(matNode);
    nodeById.set(matId, matNode);

    // Remove old outgoing edges from join
    for (let i=edges.length-1;i>=0;i--){
      if (edges[i].from === n.id) edges.splice(i,1);
    }

    // Connect join -> materialize
    edges.push({ from: n.id, to: matId, port: "in" });

    // Connect materialize -> old targets
    for (const e of outs) {
      edges.push({ from: matId, to: e.to, port: e.port || "in" });
    }

    changes.push({ rule:"INSERT_MATERIALIZE_AFTER_JOIN", nodeId:n.id, detail:{ materialize: matId, fanout: outs.length } });
  }

  // rebuild maps
  outM = outEdges(after);
  inM = inEdges(after);

  // B) Predicate pushdown across join (join -> filter)
  for (const f of [...nodes]) {
    if (f.op !== "filter") continue;

    const inc = inM.get(f.id) || [];
    if (inc.length !== 1) continue;
    const parentId = inc[0].from;
    const parent = nodeById.get(parentId);
    if (!parent || parent.op !== "join") continue;

    const joinOuts = outM.get(parentId) || [];
    if (joinOuts.length !== 1) continue; // must be sole consumer

    const cls = predicateSide(f.params?.where);
    if (cls.side !== "left" && cls.side !== "right") continue;

    // locate join input edge to rewrite
    const joinInc = inM.get(parentId) || [];
    const targetEdge = joinInc.find((e) => (e.port || "in") === cls.side);
    if (!targetEdge) continue;

    const newFilterId = mkId("opt_f", f.id + "_" + cls.side);
    if (nodeById.has(newFilterId)) continue;

    const newWhere = stripSidePrefix(f.params.where, cls.side);
    const newFilter = { id: newFilterId, op: "filter", params: { where: newWhere } };
    nodes.push(newFilter);
    nodeById.set(newFilterId, newFilter);

    const prevFrom = targetEdge.from;
    targetEdge.from = newFilterId;

    // connect prevFrom -> newFilter
    edges.push({ from: prevFrom, to: newFilterId, port: "in" });

    // Rewire join outputs to filter outputs, then remove original filter
    const filterOuts = outM.get(f.id) || [];

    // remove all edges in/out of original filter
    for (let i=edges.length-1;i>=0;i--){
      const e = edges[i];
      if (e.to === f.id || e.from === f.id) edges.splice(i,1);
    }

    // connect join directly to former consumers of filter
    for (const e of filterOuts) {
      edges.push({ from: parentId, to: e.to, port: e.port || "in" });
    }

    // remove filter node
    const idx = nodes.findIndex((n)=>n.id===f.id);
    if (idx >= 0) nodes.splice(idx,1);
    nodeById.delete(f.id);

    changes.push({
      rule:"PUSHDOWN_FILTER_ACROSS_JOIN",
      nodeId: parentId,
      detail: { movedFilter: f.id, newFilter: newFilterId, side: cls.side, rewrittenCols: cls.cols }
    });

    // refresh maps after destructive edit
    outM = outEdges(after);
    inM = inEdges(after);
  }

  after.nodes = nodes;
  after.edges = edges;
  return { ok:true, before: plan, after, changes };
}

module.exports = { optimizeDagV3_4 };