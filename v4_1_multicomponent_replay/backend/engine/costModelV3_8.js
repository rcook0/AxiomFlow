const crypto = require("crypto");

function sha1(s){ return crypto.createHash("sha1").update(String(s)).digest("hex"); }

function joinSigFromOnRef(onRef){
  const preds = (onRef || []).map(p => {
    const a = `${p.left.rel}:${p.left.path||""}`;
    const b = `${p.right.rel}:${p.right.path||""}`;
    if (a <= b) return { l: a, r: b };
    return { l: b, r: a };
  });
  preds.sort((x,y)=> (x.l+x.r).localeCompare(y.l+y.r));
  return sha1(JSON.stringify(preds));
}

function joinSigFromOn(on){
  const pairs = (on || []).map(([l,r])=>({l:String(l), r:String(r)}));
  pairs.sort((a,b)=> (a.l+a.r).localeCompare(b.l+b.r));
  return sha1(JSON.stringify(pairs));
}

// Predicate signature: stable hash of a canonicalized expression tree.
function predSigFromExpr(expr){
  function canon(x){
    if (x == null) return null;
    if (Array.isArray(x)) {
      const [op, ...args] = x;
      const cargs = args.map(canon);
      // sort commutative boolean ops for stability
      if (op === "and" || op === "or") {
        const ss = cargs.map((a)=>JSON.stringify(a)).sort();
        return [op, ...ss.map(s=>JSON.parse(s))];
      }
      // normalize equality by sorting operands when both are scalars or cols
      if (op === "eq" || op === "==" ) {
        if (cargs.length === 2) {
          const a = JSON.stringify(cargs[0]);
          const b = JSON.stringify(cargs[1]);
          if (a <= b) return ["eq", JSON.parse(a), JSON.parse(b)];
          return ["eq", JSON.parse(b), JSON.parse(a)];
        }
      }
      return [op, ...cargs];
    }
    if (typeof x === "object") {
      const keys = Object.keys(x).sort();
      const o = {};
      for (const k of keys) o[k] = canon(x[k]);
      return o;
    }
    return x;
  }
  const c = canon(expr);
  return sha1(JSON.stringify(c));
}

// AND-composition signature: stable hash of the multiset of atom pred signatures.
function predGroupSigFromExpr(expr){
  function flattenAnd(x, out){
    if (!Array.isArray(x)) { out.push(x); return; }
    const [op, ...args] = x;
    if (op !== "and") { out.push(x); return; }
    for (const a of args) flattenAnd(a, out);
  }

  if (!Array.isArray(expr) || expr[0] !== "and") return null;
  const atoms = [];
  flattenAnd(expr, atoms);

  // Atom signatures (canonicalized) — multiset encoded by sorting string list.
  const atomSigs = atoms.map(predSigFromExpr).filter(Boolean).sort();
  if (atomSigs.length <= 1) return null; // no benefit vs predSig

  return sha1(JSON.stringify(atomSigs));
}

// Join+Filter segment signature (selectivity after join)
function joinFilterSig(joinSig, predOrGroupSig){
  if (!joinSig || !predOrGroupSig) return null;
  return sha1(`${joinSig}:${predOrGroupSig}`);
}

function buildMaps(plan){
  const nodes = plan.nodes || [];
  const edges = plan.edges || [];
  const nodeById = new Map(nodes.map(n=>[n.id,n]));
  const out = new Map();
  const inc = new Map();
  for (const e of edges) {
    if (!out.has(e.from)) out.set(e.from, []);
    out.get(e.from).push(e);
    if (!inc.has(e.to)) inc.set(e.to, []);
    inc.get(e.to).push(e);
  }
  return { nodeById, out, inc };
}

function topoSort(plan){
  const { nodeById, out, inc } = buildMaps(plan);
  const indeg = new Map();
  for (const id of nodeById.keys()) indeg.set(id, 0);
  for (const [to, arr] of inc.entries()) indeg.set(to, (indeg.get(to)||0) + arr.length);

  const q = [];
  for (const [id, d] of indeg.entries()) if (d === 0) q.push(id);

  const order = [];
  while (q.length) {
    const id = q.shift();
    order.push(id);
    for (const e of (out.get(id) || [])) {
      const t = e.to;
      indeg.set(t, (indeg.get(t)||0) - 1);
      if (indeg.get(t) === 0) q.push(t);
    }
  }
  return order;
}

function statRows(st){
  if (!st) return null;
  return (st.emaRowsOut ?? st.emaRowsIn ?? null);
}

function estimateJoinOut(leftRows, rightRows, joinSigStatsDoc){
  if (joinSigStatsDoc) {
    const fL = joinSigStatsDoc.emaFanoutPerLeft ?? null;
    const fR = joinSigStatsDoc.emaFanoutPerRight ?? null;
    const ests = [];
    if (fL != null) ests.push(fL * leftRows);
    if (fR != null) ests.push(fR * rightRows);
    if (ests.length === 2) return Math.sqrt(ests[0] * ests[1]);
    if (ests.length === 1) return ests[0];
  }
  return Math.min(leftRows, rightRows);
}

/**
 * v3.8 cost model:
 * - uses pred_group_stats (AND compositions) and join_filter_stats (join->filter segment selectivity)
 * - falls back to pred_sig_stats, then node EMAs, then params, then defaults
 */
function estimatePlanCostV3_8(plan, statsByNodeId, joinSigStats, predSigStats, predGroupStats, joinFilterStats, opts){
  const s = statsByNodeId || new Map();
  const js = joinSigStats || new Map();
  const ps = predSigStats || new Map();
  const pg = predGroupStats || new Map();
  const jf = joinFilterStats || new Map();
  const o = opts || {};

  const { nodeById, inc } = buildMaps(plan);
  const topo = topoSort(plan);

  const rowsByNode = new Map();
  const costByNode = new Map();

  function inputEdge(nodeId, port){
    const inE = (inc.get(nodeId) || []).filter(e => (e.port || "in") === (port || "in"));
    if (inE.length === 0) return null;
    return inE[0];
  }

  function inputRows(nodeId, port) {
    const e = inputEdge(nodeId, port);
    if (!e) return 0;
    const fromId = e.from;
    return rowsByNode.get(fromId) ?? (statRows(s.get(fromId)) ?? 1000);
  }

  for (const id of topo) {
    const n = nodeById.get(id);
    if (!n) continue;

    if (n.op === "scan") {
      const est = statRows(s.get(id)) ?? n.params?.estimatedRows ?? 10000;
      rowsByNode.set(id, est);
      costByNode.set(id, est);
      continue;
    }

    if (n.op === "filter") {
      const inRows = inputRows(id, "in");

      const predSig = predSigFromExpr(n.params?.where);
      const groupSig = predGroupSigFromExpr(n.params?.where);
      const predOrGroupSig = groupSig || predSig;

      // join->filter segment signature (if input is join)
      let segSel = null;
      const inE = inputEdge(id, "in");
      if (inE) {
        const src = nodeById.get(inE.from);
        if (src && src.op === "join") {
          const joinSig = src.params?.onRef ? joinSigFromOnRef(src.params.onRef) : joinSigFromOn(src.params?.on || []);
          const segSig = joinFilterSig(joinSig, predOrGroupSig);
          segSel = segSig ? (jf.get(segSig)?.emaSelectivity ?? null) : null;
        }
      }

      const groupSel = groupSig ? (pg.get(groupSig)?.emaSelectivity ?? null) : null;
      const predSel = predSig ? (ps.get(predSig)?.emaSelectivity ?? null) : null;

      const nodeSel = (s.get(id)?.emaSelectivity ?? null);
      const paramSel = (n.params?.estimatedSelectivity ?? null);
      const defSel = (o.defaultFilterSelectivity ?? 0.3);

      const sel = segSel ?? groupSel ?? predSel ?? nodeSel ?? paramSel ?? defSel;

      const outRows = Math.max(0, Math.floor(inRows * sel));
      rowsByNode.set(id, outRows);
      costByNode.set(id, inRows);
      continue;
    }

    if (n.op === "project" || n.op === "materialize" || n.op === "sink") {
      const inRows = inputRows(id, "in");
      rowsByNode.set(id, inRows);
      costByNode.set(id, inRows);
      continue;
    }

    if (n.op === "join") {
      const leftRows = inputRows(id, "left");
      const rightRows = inputRows(id, "right");

      const joinSig = n.params?.onRef ? joinSigFromOnRef(n.params.onRef) : joinSigFromOn(n.params?.on || []);
      const sigDoc = joinSig ? js.get(joinSig) : null;
      const outRows = estimateJoinOut(leftRows, rightRows, sigDoc);
      rowsByNode.set(id, outRows);

      const algo = n.params?.algorithm || "hash";
      const build = n.params?.build || (rightRows <= leftRows ? "right" : "left");
      const memBudget = o.hashBuildBudgetRows ?? 200000;
      const penalty = o.hashBuildOverBudgetPenalty ?? 10;

      let cost = 0;
      if (algo === "nested_loop") {
        const inner = Math.min(leftRows, rightRows);
        const outer = Math.max(leftRows, rightRows);
        cost = outer * inner;
      } else {
        const buildRows = build === "left" ? leftRows : rightRows;
        const over = buildRows > memBudget ? (buildRows - memBudget) / memBudget : 0;
        const memPenalty = over > 0 ? (1 + over * penalty) : 1;
        cost = (leftRows + rightRows + outRows) * memPenalty;
      }

      costByNode.set(id, cost);
      continue;
    }

    // default pass-through cost
    const inRows = inputRows(id, "in");
    rowsByNode.set(id, inRows);
    costByNode.set(id, inRows);
  }

  let total = 0;
  for (const v of costByNode.values()) total += v;

  return { rowsByNode, costByNode, totalCost: total };
}

module.exports = {
  estimatePlanCostV3_8,
  joinSigFromOnRef,
  joinSigFromOn,
  predSigFromExpr,
  predGroupSigFromExpr,
  joinFilterSig
};
