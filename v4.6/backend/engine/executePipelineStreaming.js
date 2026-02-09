const { evalExpr, evalProject } = require("./exprEval");
const { LineageDagCollector } = require("../lineage/lineageCollectorDag");

function detectStreamingPipeline(plan){
  const nodes = plan.nodes || [];
  const edges = plan.edges || [];
  const byId = new Map(nodes.map(n=>[n.id,n]));

  const incoming = new Map();
  const outgoing = new Map();
  for (const e of edges) {
    if (!incoming.has(e.to)) incoming.set(e.to, []);
    incoming.get(e.to).push(e);
    if (!outgoing.has(e.from)) outgoing.set(e.from, []);
    outgoing.get(e.from).push(e);
  }

  const scan = nodes.find(n=>n.op==="scan");
  const sink = nodes.find(n=>n.op==="sink");
  if (!scan || !sink) return null;

  for (const n of nodes) {
    if (!["scan","filter","project","sink"].includes(n.op)) return null;
  }

  for (const n of nodes) {
    const inc = (incoming.get(n.id) || []).length;
    const out = (outgoing.get(n.id) || []).length;
    if (n.op === "scan" && inc !== 0) return null;
    if (n.op === "sink" && out !== 0) return null;
    if (n.op !== "scan" && inc !== 1) return null;
    if (n.op !== "sink" && out !== 1) return null;
  }

  const order = [scan.id];
  let cur = scan.id;
  const seen = new Set([scan.id]);

  while (cur !== sink.id) {
    const outs = outgoing.get(cur) || [];
    if (outs.length !== 1) return null;
    const nxt = outs[0].to;
    if (seen.has(nxt)) return null;
    seen.add(nxt);
    order.push(nxt);
    cur = nxt;
    if (order.length > nodes.length+1) return null;
  }

  if (order.length !== nodes.length) return null;
  return order;
}

async function tryExecuteStreamingPipeline(plan, ctx){
  const order = detectStreamingPipeline(plan);
  if (!order) return { ok: false };

  const { db, runId } = ctx || {};
  if (!db) throw new Error("streaming pipeline requires ctx.db");

  const byId = new Map((plan.nodes||[]).map(n=>[n.id,n]));
  const scan = byId.get(order[0]);
  const sink = byId.get(order[order.length-1]);

  const dataset = scan.params.dataset;
  const outColl = sink.params.collection;
  const batchSize = parseInt(process.env.AXIOMFLOW_STREAMING_BATCH || "1000", 10);

  const lineage = new LineageDagCollector({ runId, plan });

  await db.collection(outColl).deleteMany({ _runId: runId });

  const cursor = db.collection(dataset).find({});
  let outCount = 0;
  let inCount = 0;

  let materialized = new Map();
  const batch = [];

  lineage.start(scan.id);
  lineage.start(sink.id);

  for await (const row of cursor) {
    inCount++;
    let r = row;

    let alive = true;
    for (let i=1; i<order.length; i++) {
      const nodeId = order[i];
      const node = byId.get(nodeId);

      if (node.op === "filter") {
        const ok = !!evalExpr(node.params.predicate || node.params.where, r);
        if (!ok) { alive = false; break; }
      } else if (node.op === "project") {
        r = evalProject(node.params.exprs || node.params.project, r);
      } else if (node.op === "sink") {
        // handled after loop
      }
    }

    if (!alive) continue;

    batch.push({ ...r, _runId: runId });
    outCount++;

    if (batch.length >= batchSize) {
      await db.collection(outColl).insertMany(batch);
      batch.length = 0;
    }
  }

  if (batch.length) await db.collection(outColl).insertMany(batch);

  lineage.end(scan.id);
  lineage.end(sink.id);

  // Coarse lineage counters
  lineage.incRowsOut(scan.id, inCount);
  for (let i=1; i<order.length-1; i++) {
    const n = byId.get(order[i]);
    lineage.incRowsIn(n.id, "in", inCount);
    lineage.incRowsOut(n.id, outCount);
  }
  lineage.incRowsIn(sink.id, "in", outCount);
  lineage.incRowsOut(sink.id, outCount);

  materialized.set(sink.id, []); // avoid retaining rows
  return { ok: true, materialized, lineage, stats: { inCount, outCount } };
}

module.exports = { tryExecuteStreamingPipeline, detectStreamingPipeline };
