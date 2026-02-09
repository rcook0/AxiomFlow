const crypto = require("crypto");
const { resolvePathToRel, prefixBindings, mergeBindings } = require("./bindings");
const { compileJoinOn } = require("./joinKeyCompile");

function sha1(s){ return crypto.createHash("sha1").update(s).digest("hex"); }
function mkId(prefix, seed){ return `${prefix}_${sha1(prefix+":"+seed).slice(0,10)}`; }

// ----- join signature -----
function canonicalPredicate(p) {
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

function joinSigFromOn(on) {
  const pairs = (on || []).map(([l,r])=>({l:String(l), r:String(r)}));
  pairs.sort((a,b)=> (a.l+a.r).localeCompare(b.l+b.r));
  return sha1(JSON.stringify(pairs));
}

// ----- extraction utilities -----
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

function findRootJoins(plan){
  const { nodeById, inc } = buildMaps(plan);
  const joins = [...nodeById.values()].filter(n=>n.op==="join");
  const isInputToJoin = new Set();
  for (const j of joins) {
    const inE = inc.get(j.id) || [];
    for (const e of inE) isInputToJoin.add(e.from);
  }
  return joins.filter(j=>!isInputToJoin.has(j.id));
}

function leafRelIdForNode(plan, nodeId){
  const { nodeById, inc } = buildMaps(plan);

  let cur = nodeId;
  const seen = new Set();
  while (cur && !seen.has(cur)) {
    seen.add(cur);
    const n = nodeById.get(cur);
    if (!n) break;
    if (n.op === "scan") return n.params.rel || n.params.dataset || cur;
    const inE = inc.get(cur) || [];
    if (inE.length !== 1) break;
    cur = inE[0].from;
  }
  return nodeId;
}

function computeBindingsForJoinTree(plan, rootJoinId){
  const { nodeById, inc } = buildMaps(plan);
  const bindingsByNode = new Map();
  const relsByNode = new Map();

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

    if (leftE) walkJoin(leftE.from);
    if (rightE) walkJoin(rightE.from);

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

// ----- DP utilities -----
function subsetKey(set) { return [...set].sort().join("|"); }
function setUnion(a,b){ return new Set([...a, ...b]); }

function pickJoinPredicates(onRefAll, leftSet, rightSet) {
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

function chooseJoinAlgorithm(leftRows, rightRows, opts){
  const minSide = Math.min(leftRows, rightRows);
  const maxSide = Math.max(leftRows, rightRows);
  const nlMaxInner = opts?.nestedLoopMaxInnerRows ?? 2000;
  const nlMaxOuter = opts?.nestedLoopMaxOuterRows ?? 80000;

  if (minSide <= nlMaxInner && maxSide <= nlMaxOuter) return "nested_loop";
  return "hash";
}

function chooseBuildSide(leftRows, rightRows){
  return (rightRows <= leftRows) ? "right" : "left";
}

function estimateJoinCost(leftRows, rightRows, outRows, algorithm, buildSide, opts){
  const memBudget = opts?.hashBuildBudgetRows ?? 200000; // coarse
  const penalty = opts?.hashBuildOverBudgetPenalty ?? 10;

  if (algorithm === "nested_loop") {
    // O(n*m) but only selected for tiny inner; cost proxy
    const inner = Math.min(leftRows, rightRows);
    const outer = Math.max(leftRows, rightRows);
    return outer * inner;
  }

  // hash: build + probe + output; penalize oversized build
  const buildRows = buildSide === "left" ? leftRows : rightRows;
  const over = buildRows > memBudget ? (buildRows - memBudget) / memBudget : 0;
  const memPenalty = over > 0 ? (1 + over * penalty) : 1;
  return (leftRows + rightRows + outRows) * memPenalty;
}

function popcount(x){
  let c=0;
  while (x){ x &= (x-1); c++; }
  return c;
}

// ----- DP join ordering (bushy, bounded) -----
function dpJoinOrderBushy(allRels, baseRelSizes, joinPredicates, joinSigStats, opts){
  const relList = [...allRels];
  const n = relList.length;

  // Bounded complexity
  const maxRels = opts?.maxRelsForBushy ?? 7;
  if (n > maxRels) {
    return { ok:false, reason:`too_many_rels(${n})`, maxRels };
  }

  const best = new Map(); // subsetKey -> state

  for (const r of relList) {
    best.set(r, {
      rels: new Set([r]),
      estRows: baseRelSizes.get(r) ?? 1000,
      estCost: 0,
      bindings: { [r]: "" },
      rootNodeId: null,
      algo: null,
      build: null,
      joinPreds: []
    });
  }

  // enumerate by subset size
  for (let sz = 2; sz <= n; sz++) {
    for (let mask = 0; mask < (1<<n); mask++) {
      if (popcount(mask) !== sz) continue;
      const S = new Set();
      for (let i=0;i<n;i++) if (mask & (1<<i)) S.add(relList[i]);

      const Skey = subsetKey(S);
      let bestState = null;

      // iterate partitions A (proper non-empty subset); ensure canonical Akey < Bkey to avoid duplicates
      for (let aMask = (mask - 1) & mask; aMask > 0; aMask = (aMask - 1) & mask) {
        const bMask = mask ^ aMask;
        if (bMask === 0) continue;

        const A = new Set();
        const B = new Set();
        for (let i=0;i<n;i++) {
          if (aMask & (1<<i)) A.add(relList[i]);
          if (bMask & (1<<i)) B.add(relList[i]);
        }
        const Akey = subsetKey(A);
        const Bkey = subsetKey(B);
        if (Akey >= Bkey) continue;

        const aState = best.get(Akey);
        const bState = best.get(Bkey);
        if (!aState || !bState) continue;

        const preds = pickJoinPredicates(joinPredicates, A, B);
        if (preds.length === 0) continue;

        const sig = joinSigFromOnRef(preds.map(canonicalPredicate));
        const sigDoc = joinSigStats ? joinSigStats.get(sig) : null;

        const outRows = estimateJoinOut(aState.estRows, bState.estRows, sigDoc);
        const algo = chooseJoinAlgorithm(aState.estRows, bState.estRows, opts);
        const build = chooseBuildSide(aState.estRows, bState.estRows);
        const joinCost = estimateJoinCost(aState.estRows, bState.estRows, outRows, algo, build, opts);
        const totalCost = aState.estCost + bState.estCost + joinCost;

        if (!bestState || totalCost < bestState.estCost) {
          bestState = {
            rels: setUnion(A, B),
            left: aState,
            right: bState,
            joinPreds: preds,
            joinSig: sig,
            estRows: outRows,
            estCost: totalCost,
            algo,
            build
          };
        }
      }

      if (bestState) best.set(Skey, bestState);
    }
  }

  const final = best.get(subsetKey(allRels));
  if (!final) return { ok:false, reason:"no_feasible_plan" };
  return { ok:true, state: final };
}

// ----- plan rewrite -----
function describeState(s){
  if (s.rels && s.rels.size === 1) return [...s.rels][0];
  return {
    join: [describeState(s.left), describeState(s.right)],
    algo: s.algo,
    build: s.build,
    preds: (s.joinPreds||[]).map(p=>`${p.left.rel}.${p.left.path}=${p.right.rel}.${p.right.path}`)
  };
}

/**
 * Rewrite join component rooted at rootJoinId into DP-chosen bushy plan.
 *
 * - Allows multiple consumers of the root join (rewires all outgoing edges).
 * - Keeps leaf subplans intact (scan/filter/project chains).
 */
function rewriteJoinTreeV3_6(plan, rootJoinId, baseRelSizes, joinSigStats, opts){
  const { nodeById, out, inc } = buildMaps(plan);
  const { joins, relsByNode } = collectJoinEdges(plan, rootJoinId);

  // Gather predicates across the component
  const allPreds = [];
  for (const j of joins) {
    if (!j.onRef) return { ok:false, reason:"missing onRef; unable to infer from on", rootJoinId };
    for (const p of j.onRef) allPreds.push(p);
  }

  const allRels = relsByNode.get(rootJoinId);
  if (!allRels || allRels.size < 3) return { ok:false, reason:"component too small for ordering", rootJoinId };

  const dp = dpJoinOrderBushy(allRels, baseRelSizes || new Map(), allPreds, joinSigStats || new Map(), opts);
  if (!dp.ok) return { ok:false, reason:"dp_failed", detail: dp, rootJoinId };

  const state = dp.state;

  const oldJoinIds = new Set(joins.map(j=>j.id));

  // Outgoing consumers of the root join (rewire all)
  const rootOut = out.get(rootJoinId) || [];
  if (rootOut.length === 0) return { ok:false, reason:"root join has no consumers", rootJoinId };

  // Map rel -> leafRootNodeId
  const relToLeafNode = new Map();
  for (const j of joins) {
    const ln = nodeById.get(j.leftFrom);
    const rn = nodeById.get(j.rightFrom);
    if (!ln || ln.op !== "join") {
      const rel = leafRelIdForNode(plan, j.leftFrom);
      if (!relToLeafNode.has(rel)) relToLeafNode.set(rel, j.leftFrom);
    }
    if (!rn || rn.op !== "join") {
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
    if (s.rootNodeId) return s.rootNodeId;

    if (s.rels && s.rels.size === 1) {
      const r = [...s.rels][0];
      s.rootNodeId = relToLeafNode.get(r);
      s.bindings = { [r]: "" };
      return s.rootNodeId;
    }

    const leftId = materializeState(s.left);
    const rightId = materializeState(s.right);

    const joinId = mkId("opt_j", subsetKey(s.rels));
    const lb = s.left.bindings;
    const rb = s.right.bindings;

    const compiled = compileJoinOn(s.joinPreds, lb, rb);

    const joinNode = {
      id: joinId,
      op: "join",
      params: {
        onRef: s.joinPreds,
        on: compiled.on,
        algorithm: s.algo || "hash",
        build: s.build || "right"
      }
    };
    newNodes.push(joinNode);

    newEdges.push({ from: leftId, to: joinId, port: "left" });
    newEdges.push({ from: rightId, to: joinId, port: "right" });

    s.rootNodeId = joinId;
    s.bindings = compiled.bindingsOut;
    return joinId;
  }

  const newRootJoinId = materializeState(state);

  // Rewrite plan: remove old joins + incident edges
  const plan2 = JSON.parse(JSON.stringify(plan));
  plan2.nodes = (plan2.nodes||[]).filter(n=>!oldJoinIds.has(n.id));
  plan2.edges = (plan2.edges||[]).filter(e=>!(oldJoinIds.has(e.from) || oldJoinIds.has(e.to)));

  // Add new nodes/edges
  for (const n of newNodes) plan2.nodes.push(n);
  for (const e of newEdges) plan2.edges.push(e);

  // Rewire all root consumers to new root
  plan2.edges = plan2.edges.filter(e=>!(e.from===rootJoinId && rootOut.some(ro=>ro.to===e.to)));
  for (const e of rootOut) {
    plan2.edges.push({ from: newRootJoinId, to: e.to, port: e.port || "in" });
  }

  return {
    ok:true,
    beforeRootJoinId: rootJoinId,
    afterRootJoinId: newRootJoinId,
    rels: [...allRels],
    estCost: state.estCost,
    estRows: state.estRows,
    explain: {
      chosenOrder: describeState(state),
      baseRelSizes: Object.fromEntries([...(baseRelSizes || new Map()).entries()]),
      dp: { maxRelsForBushy: opts?.maxRelsForBushy ?? 7 }
    },
    plan: plan2
  };
}

module.exports = {
  joinSigFromOnRef,
  joinSigFromOn,
  rewriteJoinTreeV3_6,
  findRootJoins,
  leafRelIdForNode,
  collectJoinEdges
};
