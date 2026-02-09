const { buildProjectMap } = require("./projectMap");
const { rewriteColsThroughProject } = require("./exprRewrite");
const { stableStringify } = require("./irDagSchema");
const crypto = require("crypto");

const BARRIERS = new Set(["sort", "limit", "distinct", "groupBy", "join", "union", "intersect", "except", "unnest"]);

function sha1(s){ return crypto.createHash("sha1").update(s).digest("hex"); }

function isBarrierOp(op){ return BARRIERS.has(op); }

function extractColsFromExpr(expr, out = new Set()){
  if (expr == null) return out;
  if (Array.isArray(expr)) {
    const [op, ...args] = expr;
    if (op === "col" && typeof args[0] === "string") { out.add(args[0]); return out; }
    for (const a of args) extractColsFromExpr(a, out);
    return out;
  }
  if (typeof expr === "object") for (const v of Object.values(expr)) extractColsFromExpr(v, out);
  return out;
}

function detectLinearChain(plan) {
  // returns ordered node ids if plan is a single chain from a single source to single output
  if (!plan || plan.version !== "ir-dag-3.0-alpha") return { ok:false, reason:"bad plan version" };
  if (!Array.isArray(plan.outputs) || plan.outputs.length !== 1) return { ok:false, reason:"outputs must be length 1 for beta optimizer" };

  const outId = plan.outputs[0];
  const indeg = new Map();
  const outdeg = new Map();
  const incoming = new Map(); // nodeId -> [{from,port}]
  for (const n of plan.nodes) { indeg.set(n.id, 0); outdeg.set(n.id, 0); incoming.set(n.id, []); }
  for (const e of plan.edges) {
    indeg.set(e.to, (indeg.get(e.to)||0)+1);
    outdeg.set(e.from, (outdeg.get(e.from)||0)+1);
    incoming.get(e.to).push({ from:e.from, port:e.port||"in" });
  }

  // linear chain constraints: each node indeg<=1, outdeg<=1, except source indeg=0 and sink outdeg=0
  for (const n of plan.nodes) {
    if ((indeg.get(n.id)||0) > 1) return { ok:false, reason:`node ${n.id} indeg>1` };
    if ((outdeg.get(n.id)||0) > 1) return { ok:false, reason:`node ${n.id} outdeg>1` };
  }

  // walk backwards from output to source
  const orderRev = [];
  let cur = outId;
  const seen = new Set();
  while (true) {
    if (seen.has(cur)) return { ok:false, reason:"cycle in walk" };
    seen.add(cur);
    orderRev.push(cur);
    const inc = incoming.get(cur);
    if (!inc || inc.length === 0) break; // reached source
    cur = inc[0].from;
  }
  const order = orderRev.reverse();
  if (order.length !== plan.nodes.length) return { ok:false, reason:"graph has disconnected nodes or multiple sources" };
  return { ok:true, order };
}

function nodeById(plan){
  const m = new Map();
  for (const n of plan.nodes) m.set(n.id, n);
  return m;
}

function mkOptNodeId(prefix, baseNode){
  // stable-ish: derive from base node id + prefix
  return `${prefix}_${sha1(prefix + ":" + baseNode).slice(0,8)}`;
}

function optimizeDagLinear(plan) {
  const linear = detectLinearChain(plan);
  if (!linear.ok) return { ok:false, reason: linear.reason };

  const ids = linear.order;
  const byId = nodeById(plan);
  const changes = [];

  // represent as array of node objects (cloned)
  const pipeline = ids.map((id) => JSON.parse(JSON.stringify(byId.get(id))));

  // Pass A: merge adjacent filters
  for (let i=0;i<pipeline.length-1;){
    if (pipeline[i].op==="filter" && pipeline[i+1].op==="filter") {
      const a=pipeline[i], b=pipeline[i+1];
      pipeline.splice(i,2,{...a, params:{...a.params, where:["and", a.params.where, b.params.where]}});
      changes.push({ rule:"MERGE_FILTERS", at:i, detail:{ a:a.id, b:b.id }});
      continue;
    }
    i++;
  }

  // Pass B: rewrite-aware filter pushdown across project (computed allowed)
  function trySwapProjectFilter(projectNode, filterNode){
    // try passthrough first
    {
      const pm = buildProjectMap(projectNode, { passthroughOnly:true });
      if (pm.ok) {
        const rw = rewriteColsThroughProject(filterNode.params.where, pm, { mode:"passthrough" });
        if (rw.ok) return { ok:true, newFilter:{...filterNode, params:{...filterNode.params, where:rw.rewritten}}, rewrite:{mode:"passthrough", changed:rw.changed, before:filterNode.params.where, after:rw.rewritten} };
      }
    }
    // computed
    const pm = buildProjectMap(projectNode, { allowComputed:true });
    if (!pm.ok) return { ok:false, reason:pm.issues };
    const rw = rewriteColsThroughProject(filterNode.params.where, pm, { mode:"computed" });
    if (!rw.ok) return { ok:false, reason:rw.issues };
    return { ok:true, newFilter:{...filterNode, params:{...filterNode.params, where:rw.rewritten}}, rewrite:{mode:"computed", changed:rw.changed, before:filterNode.params.where, after:rw.rewritten} };
  }

  for (let i=0;i<pipeline.length;i++){
    if (pipeline[i].op!=="filter") continue;
    let j=i;
    while (j>0){
      const prev=pipeline[j-1];
      const cur=pipeline[j];
      if (isBarrierOp(prev.op)) break;

      if (prev.op==="project"){
        const attempt = trySwapProjectFilter(prev, cur);
        if (!attempt.ok) break;
        // swap and replace filter
        pipeline[j]=prev;
        pipeline[j-1]=attempt.newFilter;
        changes.push({ rule:"FILTER_PUSHDOWN_REWRITE", from:j, to:j-1, detail:{ project:prev.id, filter:cur.id }, rewrite: attempt.rewrite });
        j--;
        continue;
      }

      // swap across other unary ops
      pipeline[j]=prev; pipeline[j-1]=cur;
      changes.push({ rule:"FILTER_PUSHDOWN", from:j, to:j-1, detail:{ swappedWith:prev.op, filter:cur.id }});
      j--;
    }
  }

  // Pass C: required columns (segment delimited by barriers)
  let segStart=0;
  for (let i=0;i<=pipeline.length;i++){
    const boundary = (i===pipeline.length) || isBarrierOp(pipeline[i].op);
    if (!boundary) continue;

    const segEnd=i;
    const required=new Set();
    for (let k=segStart;k<segEnd;k++){
      if (pipeline[k].op==="filter") extractColsFromExpr(pipeline[k].params.where, required);
    }
    if (required.size>0 && segEnd>segStart){
      const first=pipeline[segStart];
      if (first.op==="project"){
        first.params.exprs = first.params.exprs || {};
        let added=0;
        for (const c of required){
          if (!(c in first.params.exprs)) { first.params.exprs[c]=["col", c]; added++; }
        }
        if (added>0) changes.push({ rule:"REQUIRED_COLS_EXTEND_PROJECT", at:segStart, detail:{ project:first.id, added, cols:[...required] }});
      } else {
        const newId = mkOptNodeId("opt_proj", first.id);
        const exprs={}; for (const c of required) exprs[c]=["col", c];
        pipeline.splice(segStart,0,{ id:newId, op:"project", params:{ exprs }});
        changes.push({ rule:"REQUIRED_COLS_INSERT_PROJECT", at:segStart, detail:{ project:newId, cols:[...required] }});
        i++; // adjust
      }
    }

    segStart=i+1;
  }

  // Rebuild DAG from pipeline (linear edges)
  const nodes = pipeline;
  const edges = [];
  for (let i=0;i<nodes.length-1;i++){
    edges.push({ from:nodes[i].id, to:nodes[i+1].id, port:"in" });
  }
  const outputs=[nodes[nodes.length-1].id];

  const after = { version:"ir-dag-3.0-alpha", nodes, edges, outputs };

  return { ok:true, before: plan, after, changes };
}

module.exports = { optimizeDagLinear };
