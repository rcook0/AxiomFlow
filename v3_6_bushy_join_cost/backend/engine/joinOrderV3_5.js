const crypto = require("crypto");
const { resolvePathToRel, prefixBindings, mergeBindings } = require("./bindings");
const { compileJoinOn } = require("./joinKeyCompile");

function sha1(s){ return crypto.createHash("sha1").update(s).digest("hex"); }

function mkId(prefix, seed){
  return `${prefix}_${sha1(prefix+":"+seed).slice(0,10)}`;
}

// ----- join signature -----

function canonicalPredicate(p) {
  // Canonicalize direction inside predicate for stable signature:
  // sort endpoints by (rel, path)
  const a = `${p.left.rel}:${p.left.path||""}`;
  const b = `${p.right.rel}:${p.right.path||""}`;
  if (a <= b) return { left: p.left, right: p.right };
  return { left: p.right, right: p.left };
}

function joinSigFromOnRef(onRef) {
  const preds = (onRef || []).map(canonicalPredicate).map(p => ({
    l: `${p.left.rel}:${p.left.path||""}`,
    r: `${p.right.rel}:${p.right.path||""}`
  }));
  preds.sort((x,y)=> (x.l+x.r).localeCompare(y.l+y.r));
  return sha1(JSON.stringify(preds));
}

// ----- extraction utilities -----

function buildMaps(plan){
  const nodes = plan.nodes || [];
  const edges = plan.edges || [];
  const nodeById = new Map(nodes.map(n=>[n.id,n]));

  const out = new Map(); // from -> [edge]
  const inc = new Map(); // to -> [edge]
  for (const e of edges) {
    if (!out.has(e.from)) out.set(e.from, []);
    out.get(e.from).push(e);
    if (!inc.has(e.to)) inc.set(e.to, []);
    inc.get(e.to).push(e);
  }
  return { nodeById, out, inc };
}

function findRootJoins(plan){
  // join nodes that are not inputs to another join (i.e. top join(s))
  const { nodeById, inc } = buildMaps(plan);
  const joins = [...nodeById.values()].filter(n=>n.op==="join");
  const joinIds = new Set(joins.map(j=>j.id));
  const isInputToJoin = new Set();
  for (const j of joins) {
    const inE = inc.get(j.id) || [];
    for (const e of inE) isInputToJoin.add(e.from);
  }
  return joins.filter(j=>!isInputToJoin.has(j.id));
}

function leafRelIdForNode(plan, nodeId){
  // Prefer scan.params.rel; else scan.params.dataset; else nodeId
  const { nodeById, inc } = buildMaps(plan);

  // walk upstream until scan found
  let cur = nodeId;
  const seen = new Set();
  while (cur && !seen.has(cur)) {
    seen.add(cur);
    const n = nodeById.get(cur);
    if (!n) break;
    if (n.op === "scan") {
      return n.params.rel || n.params.dataset || cur;
    }
    const inE = inc.get(cur) || [];
    if (inE.length !== 1) break;
    cur = inE[0].from;
  }
  return nodeId;
}

function computeBindingsForJoinTree(plan, rootJoinId){
  // Compute bindings for each node in the join tree rooted at rootJoinId.
  const { nodeById, inc } = buildMaps(plan);

  const bindingsByNode = new Map(); // nodeId -> bindings map
  const relsByNode = new Map(); // nodeId -> Set(rels)

  function dfs(nodeId) {
    if (bindingsByNode.has(nodeId)) return;
    const n = nodeById.get(nodeId);
    if (!n) return;

    if (n.op !== "join") {
      const rel = leafRelIdForNode(plan, nodeId);
      bindingsByNode.set(nodeId, { [rel]: "" });
      relsByNode.set(nodeId, new Set([rel]));
      return;
    }

    const inE = inc.get(nodeId) || [];
    const leftE = inE.find(e=>(e.port||"in")==="left");
    const rightE = inE.find(e=>(e.port||"in")==="right");
    if (!leftE || !rightE) throw new Error("join node must have left/right ports");

    dfs(leftE.from);
    dfs(rightE.from);

    const lb = bindingsByNode.get(leftE.from);
    const rb = bindingsByNode.get(rightE.from);
    const outB = mergeBindings(prefixBindings(lb, "left"), prefixBindings(rb, "right"));
    bindingsByNode.set(nodeId, outB);

    const rels = new Set([...relsByNode.get(leftE.from), ...relsByNode.get(rightE.from)]);
    relsByNode.set(nodeId, rels);
  }

  dfs(rootJoinId);
  return { bindingsByNode, relsByNode };
}

function inferOnRefFromOn(joinNode, leftBindings, rightBindings){
  const on = joinNode.params?.on || [];
  const out = [];
  for (const [lk, rk] of on) {
    const lres = resolvePathToRel(leftBindings, lk);
    const rres = resolvePathToRel(rightBindings, rk);
    if (!lres || !rres) return null;
    out.push({
      left: { rel: lres.rel, path: lres.relPath },
      right: { rel: rres.rel, path: rres.relPath }
    });
  }
  return out;
}

function collectJoinEdges(plan, rootJoinId){
  const { nodeById, inc } = buildMaps(plan);
  const { bindingsByNode, relsByNode } = computeBindingsForJoinTree(plan, rootJoinId);

  const joins = [];
  const seen = new Set();

  function walkJoin(nodeId) {
    const n = nodeById.get(nodeId);
    if (!n || n.op !== "join") return;
    if (seen.has(nodeId)) return;
    seen.add(nodeId);

    const inE = inc.get(nodeId) || [];
    const leftE = inE.find(e=>(e.port||"in")==="left");
    const rightE = inE.find(e=>(e.port||"in")==="right");

    // ensure children walked
    if (leftE) walkJoin(leftE.from);
    if (rightE) walkJoin(rightE.from);

    // infer onRef if missing
    const leftBindings = bindingsByNode.get(leftE.from);
    const rightBindings = bindingsByNode.get(rightE.from);

    let onRef = n.params?.onRef;
    if (!onRef) onRef = inferOnRefFromOn(n, leftBindings, rightBindings);

    joins.push({
      id: n.id,
      leftFrom: leftE.from,
      rightFrom: rightE.from,
      leftRels: new Set([...relsByNode.get(leftE.from)]),
      rightRels: new Set([...relsByNode.get(rightE.from)]),
      onRef,
      joinSig: onRef ? joinSigFromOnRef(onRef) : null
    });
  }

  walkJoin(rootJoinId);
  return { joins, bindingsByNode, relsByNode };
}

// ----- DP join ordering -----

function subsetKey(set) {
  return [...set].sort().join("|");
}

function setUnion(a,b){ return new Set([...a, ...b]); }
function setHasAny(a, b){ for (const x of a) if (b.has(x)) return true; return false; }

function pickJoinPredicates(onRefAll, leftSet, rightSet) {
  // choose preds crossing the cut
  const preds = [];
  for (const p of onRefAll) {
    const lInL = leftSet.has(p.left.rel);
    const rInR = rightSet.has(p.right.rel);
    const lInR = rightSet.has(p.left.rel);
    const rInL = leftSet.has(p.right.rel);

    if (lInL && rInR) preds.push(p);
    else if (lInR && rInL) preds.push({ left: p.right, right: p.left });
  }
  return preds;
}

function estimateJoinOut(leftRows, rightRows, joinSigStatsDoc){
  // Prefer learned fanout if available
  if (joinSigStatsDoc) {
    const fL = joinSigStatsDoc.emaFanoutPerLeft ?? null;
    const fR = joinSigStatsDoc.emaFanoutPerRight ?? null;
    const ests = [];
    if (fL != null) ests.push(fL * leftRows);
    if (fR != null) ests.push(fR * rightRows);
    if (ests.length === 2) return Math.sqrt(ests[0] * ests[1]);
    if (ests.length === 1) return ests[0];
  }
  // Default heuristic: assume output around min(rows) (1:1-ish)
  return Math.min(leftRows, rightRows);
}

function estimateJoinCost(leftRows, rightRows, outRows) {
  // crude: build + probe + output materialization
  return leftRows + rightRows + outRows;
}

/**
 * Compute a left-deep join order for a join tree component rooted at rootJoinId.
 *
 * Inputs:
 * - baseRelSizes: Map(rel -> est rows)
 * - joinPredicates: list of all predicates in the component (onRef form)
 * - joinSigStats: Map(joinSig -> statsDoc) for learned fanout estimates (optional)
 *
 * Output:
 * - planTree: nested { rels:Set, rootNodeId, bindings, estRows, estCost, joinsUsed:[...] }
 */
function dpJoinOrder(allRels, baseRelSizes, joinPredicates, joinSigStats) {
  const relList = [...allRels];
  const best = new Map(); // subsetKey -> state

  for (const r of relList) {
    best.set(r, {
      rels: new Set([r]),
      rootNodeId: null,
      bindings: { [r]: "" },
      estRows: baseRelSizes.get(r) ?? 1000,
      estCost: 0,
      joinPreds: []
    });
  }

  // enumerate subsets by size
  for (let sz = 2; sz <= relList.length; sz++) {
    // build subsets via bitmasks for small N
    const n = relList.length;
    for (let mask = 0; mask < (1<<n); mask++) {
      if (popcount(mask) !== sz) continue;
      const S = new Set();
      for (let i=0;i<n;i++) if (mask & (1<<i)) S.add(relList[i]);

      let bestState = null;

      // partition S into A and B where B is a single relation (left-deep bias)
      for (let bIdx=0;bIdx<n;bIdx++) {
        const bRel = relList[bIdx];
        if (!S.has(bRel)) continue;
        const A = new Set(S);
        A.delete(bRel);
        const Akey = subsetKey(A);
        if (!best.has(Akey)) continue;
        const aState = best.get(Akey);
        const bState = best.get(bRel);

        // pick predicates crossing the cut
        const preds = pickJoinPredicates(joinPredicates, A, new Set([bRel]));
        if (preds.length === 0) continue;

        const sig = joinSigFromOnRef(preds.map(canonicalPredicate));
        const sigDoc = joinSigStats ? joinSigStats.get(sig) : null;

        const outRows = estimateJoinOut(aState.estRows, bState.estRows, sigDoc);
        const joinCost = estimateJoinCost(aState.estRows, bState.estRows, outRows);
        const totalCost = aState.estCost + bState.estCost + joinCost;

        if (!bestState || totalCost < bestState.estCost) {
          bestState = {
            rels: setUnion(A, new Set([bRel])),
            left: aState,
            right: bState,
            joinPreds: preds,
            joinSig: sig,
            estRows: outRows,
            estCost: totalCost
          };
        }
      }

      if (bestState) best.set(subsetKey(S), bestState);
    }
  }

  return best.get(subsetKey(allRels)) || null;
}

function popcount(x){
  let c=0;
  while (x){ x &= (x-1); c++; }
  return c;
}

// ----- plan rewrite -----

/**
 * Rewrite the join tree rooted at rootJoinId into the DP-chosen join order.
 * Keeps leaf subplans intact; replaces join nodes in the component with new join nodes.
 */
function rewriteJoinTree(plan, rootJoinId, baseRelSizes, joinSigStats){
  const { nodeById, out, inc } = buildMaps(plan);

  const { joins, relsByNode } = collectJoinEdges(plan, rootJoinId);

  // Gather all predicates across the component
  const allPreds = [];
  for (const j of joins) {
    if (!j.onRef) return { ok:false, reason:"missing onRef; unable to infer from on", rootJoinId };
    for (const p of j.onRef) allPreds.push(p);
  }

  const allRels = relsByNode.get(rootJoinId);
  if (!allRels || allRels.size < 3) return { ok:false, reason:"component too small for ordering", rootJoinId };

  const dp = dpJoinOrder(allRels, baseRelSizes, allPreds, joinSigStats);
  if (!dp) return { ok:false, reason:"no feasible join order (missing predicates?)", rootJoinId };

  // Determine old join node ids in this component
  const oldJoinIds = new Set(joins.map(j=>j.id));

  // Identify downstream consumers of rootJoinId
  const rootOut = out.get(rootJoinId) || [];
  if (rootOut.length !== 1) {
    return { ok:false, reason:"root join must have exactly one consumer in beta", rootJoinId };
  }
  const consumerEdge = rootOut[0];

  // Build new join nodes bottom-up; leaves are represented by their rel leaf root nodes.
  // Map rel -> leafRootNodeId (choose the leaf node in the old tree that carries that rel)
  // We'll pick by walking joins: any join input that is non-join yields a leaf root with its rel id.
  const relToLeafNode = new Map();
  for (const j of joins) {
    if (!nodeById.get(j.leftFrom) || nodeById.get(j.leftFrom).op !== "join") {
      const rel = leafRelIdForNode(plan, j.leftFrom);
      if (!relToLeafNode.has(rel)) relToLeafNode.set(rel, j.leftFrom);
    }
    if (!nodeById.get(j.rightFrom) || nodeById.get(j.rightFrom).op !== "join") {
      const rel = leafRelIdForNode(plan, j.rightFrom);
      if (!relToLeafNode.has(rel)) relToLeafNode.set(rel, j.rightFrom);
    }
  }
  for (const r of allRels) {
    if (!relToLeafNode.has(r)) return { ok:false, reason:"unable to map rel to leaf node", rel:r };
  }

  const newNodes = [];
  const newEdges = [];

  function materializeState(s){
    if (s.rootNodeId) return s.rootNodeId; // already built
    // leaf
    if (s.rels && s.rels.size === 1) {
      const r = [...s.rels][0];
      s.rootNodeId = relToLeafNode.get(r);
      s.bindings = { [r]: "" };
      return s.rootNodeId;
    }

    // build children
    const leftId = materializeState(s.left);
    const rightId = materializeState(s.right);

    const joinId = mkId("opt_j", subsetKey(s.rels));
    // compute subtree bindings
    const lb = s.left.bindings || { [[...s.left.rels][0]]: "" };
    const rb = s.right.bindings || { [[...s.right.rels][0]]: "" };

    const compiled = compileJoinOn(s.joinPreds, lb, rb);
    const joinNode = { id: joinId, op: "join", params: { onRef: s.joinPreds, on: compiled.on } };
    newNodes.push(joinNode);

    newEdges.push({ from: leftId, to: joinId, port: "left" });
    newEdges.push({ from: rightId, to: joinId, port: "right" });

    s.rootNodeId = joinId;
    s.bindings = compiled.bindingsOut;
    return joinId;
  }

  const newRootJoinId = materializeState(dp);

  // Now rewrite plan: remove old join nodes + their incident edges; add new join nodes/edges; connect new root to consumer.
  const plan2 = JSON.parse(JSON.stringify(plan));
  plan2.nodes = (plan2.nodes||[]).filter(n=>!oldJoinIds.has(n.id));
  plan2.edges = (plan2.edges||[]).filter(e=>!(oldJoinIds.has(e.from) || oldJoinIds.has(e.to)));

  // Add new nodes/edges
  for (const n of newNodes) plan2.nodes.push(n);
  for (const e of newEdges) plan2.edges.push(e);

  // Redirect consumer to new root join
  // Remove any edge that previously came from rootJoinId to consumer
  plan2.edges = plan2.edges.filter(e=>!(e.from===rootJoinId && e.to===consumerEdge.to));
  plan2.edges.push({ from: newRootJoinId, to: consumerEdge.to, port: consumerEdge.port || "in" });

  return {
    ok:true,
    beforeRootJoinId: rootJoinId,
    afterRootJoinId: newRootJoinId,
    estCost: dp.estCost,
    estRows: dp.estRows,
    rels: [...allRels],
    plan: plan2,
    explain: {
      chosenOrder: describeState(dp),
      baseRelSizes: Object.fromEntries([...baseRelSizes.entries()]),
    }
  };
}

function describeState(s){
  if (s.rels && s.rels.size === 1) return [...s.rels][0];
  return { join: [describeState(s.left), describeState(s.right)], preds: (s.joinPreds||[]).map(p=>`${p.left.rel}.${p.left.path}=${p.right.rel}.${p.right.path}`) };
}

module.exports = { joinSigFromOnRef, rewriteJoinTree, findRootJoins, leafRelIdForNode, collectJoinEdges };