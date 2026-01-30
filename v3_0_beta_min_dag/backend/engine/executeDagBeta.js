const { compileDagSkeleton } = require("./compileDagSkeleton");
const { evalExpr } = require("./exprEval");
const { LineageDagCollector } = require("../lineage/lineageCollectorDag");

/**
 * v3.0-beta minimal DAG executor:
 * Supports: scan -> filter -> project -> sink
 * - scan reads from Mongo collection named by params.dataset
 * - sink writes to Mongo collection named by params.collection
 *
 * Notes:
 * - join/groupBy/sort/limit not supported in beta (explicit error).
 * - execution is pull-based via async iterators, stitched according to edges and ports.
 */
function ensureSupportedOp(op) {
  const ok = new Set(["scan", "filter", "project", "sink"]);
  if (!ok.has(op)) throw new Error(`Unsupported op in v3.0-beta: ${op}`);
}

async function* scanIter(db, node, lineage) {
  const col = db.collection(node.params.dataset);
  lineage.start(node.id);
  const cursor = col.find({});
  for await (const doc of cursor) {
    lineage.incRowsOut(node.id, 1);
    yield doc;
  }
  lineage.end(node.id);
}

async function* filterIter(upstream, node, lineage) {
  lineage.start(node.id);
  for await (const row of upstream) {
    lineage.incRowsIn(node.id, "in", 1);
    const ok = !!evalExpr(node.params.where, row);
    if (ok) {
      lineage.incRowsOut(node.id, 1);
      yield row;
    }
  }
  lineage.end(node.id);
}

async function* projectIter(upstream, node, lineage) {
  const exprs = node.params.exprs || {};
  lineage.start(node.id);
  for await (const row of upstream) {
    lineage.incRowsIn(node.id, "in", 1);
    const out = {};
    for (const [k, expr] of Object.entries(exprs)) {
      out[k] = evalExpr(expr, row);
    }
    lineage.incRowsOut(node.id, 1);
    yield out;
  }
  lineage.end(node.id);
}

async function sinkConsume(db, upstream, node, lineage, opts = {}) {
  const col = db.collection(node.params.collection);
  const batchSize = opts.batchSize || 500;
  const docs = [];
  lineage.start(node.id);

  for await (const row of upstream) {
    lineage.incRowsIn(node.id, "in", 1);
    docs.push(row);
    if (docs.length >= batchSize) {
      await col.insertMany(docs);
      lineage.incRowsOut(node.id, docs.length);
      docs.length = 0;
    }
  }

  if (docs.length) {
    await col.insertMany(docs);
    lineage.incRowsOut(node.id, docs.length);
  }

  lineage.end(node.id);
}

/**
 * Execute a minimal DAG.
 *
 * @param {object} plan - DAG plan (v3.0-alpha schema)
 * @param {object} ctx
 * @param {import("mongodb").Db} ctx.db - MongoDB Db instance
 * @param {string} ctx.runId - run identifier
 */
async function executeDagBeta(plan, ctx, opts = {}) {
  const compiled = compileDagSkeleton(plan);
  if (!compiled.ok) return { ok: false, errors: compiled.errors };

  const { plan: canonPlan } = compiled;
  const db = ctx.db;
  if (!db) throw new Error("executeDagBeta requires ctx.db (Mongo Db)");

  // validate ops supported
  for (const n of canonPlan.nodes) ensureSupportedOp(n.op);

  const lineage = new LineageDagCollector({ runId: ctx.runId || "run", plan: canonPlan });

  // Build node iterators in topo order
  const iters = new Map(); // nodeId -> async iterable OR special marker for sink
  const incoming = compiled.incomingByPort;

  for (const nodeId of compiled.topoOrder) {
    const node = compiled.nodeById.get(nodeId);

    if (node.op === "scan") {
      iters.set(nodeId, scanIter(db, node, lineage));
      continue;
    }

    // unary ops in beta: exactly one incoming on port "in"
    const inc = incoming.get(nodeId);
    const inArr = inc ? (inc.get("in") || []) : [];
    if (inArr.length !== 1) throw new Error(`Node ${nodeId} (${node.op}) must have exactly 1 input`);

    const upstreamId = inArr[0];
    const upstreamIter = iters.get(upstreamId);
    if (!upstreamIter) throw new Error(`Upstream iterator missing for ${upstreamId} -> ${nodeId}`);

    if (node.op === "filter") {
      iters.set(nodeId, filterIter(upstreamIter, node, lineage));
      continue;
    }

    if (node.op === "project") {
      iters.set(nodeId, projectIter(upstreamIter, node, lineage));
      continue;
    }

    if (node.op === "sink") {
      // sinks are executed, not materialized as iterators
      await sinkConsume(db, upstreamIter, node, lineage, opts.sink || {});
      iters.set(nodeId, null);
      continue;
    }
  }

  return { ok: true, dagHash: compiled.dagHash, lineage: lineage.finalize() };
}

module.exports = { executeDagBeta };
