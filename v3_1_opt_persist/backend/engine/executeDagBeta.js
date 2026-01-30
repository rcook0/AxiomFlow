const { compileDagSkeleton } = require("./compileDagSkeleton");
const { optimizeDagLinear } = require("./dagOptimizeV3_1");
const { LineageDagCollector } = require("../lineage/lineageCollectorDag");
const { writeDagLineage, updateDagNodeStats, writeDagOptReport } = require("../artifacts/dagPersist");
const { evalExpr, evalProject } = require("./exprEval");

/**
 * v3.0-beta executor (unary-only) + v3.1 optimizer/persistence glue.
 *
 * Supported ops: scan, filter, project, sink
 * Unsupported ops throw.
 *
 * If `db` is provided, this function will:
 * - persist optimizer report (before/after/changes)
 * - persist run DAG lineage
 * - update EMA node stats keyed by (dagHash,nodeId)
 *
 * @param {object} plan - DAG IR
 * @param {object} ctx - { db, runId, mongoDbName? }
 */
async function executeDagBeta(plan, ctx = {}) {
  const { db, runId = "run_" + Date.now() } = ctx;

  // 1) Compile skeleton + get dagHash
  const compiled0 = compileDagSkeleton(plan, { assignIdsIfMissing: true });
  if (!compiled0.ok) throw new Error("DAG validation failed: " + JSON.stringify(compiled0.errors));

  // 2) Optimize (linear-chain only in v3.1)
  const opt = optimizeDagLinear(compiled0.plan);
  const execPlan = opt.ok ? opt.after : compiled0.plan;

  const compiled = compileDagSkeleton(execPlan, { assignIdsIfMissing: false });
  if (!compiled.ok) throw new Error("Optimized DAG invalid: " + JSON.stringify(compiled.errors));

  const dagHash = compiled.dagHash;

  // Persist optimizer report (if available)
  if (db && opt.ok) {
    await writeDagOptReport(db, runId, dagHash, { before: opt.before, after: opt.after, changes: opt.changes });
  }

  const nodeById = compiled.nodeById;
  const incomingByPort = compiled.incomingByPort;
  const topo = compiled.topoOrder;

  // Only unary in this beta: verify indeg constraints
  for (const id of topo) {
    const node = nodeById.get(id);
    const inc = incomingByPort.get(id);
    const incCount = inc ? Array.from(inc.values()).reduce((a, arr) => a + arr.length, 0) : 0;

    if (node.op === "scan") {
      if (incCount !== 0) throw new Error(`scan node ${id} must have 0 inputs`);
    } else {
      if (incCount !== 1) throw new Error(`unary node ${id} must have exactly 1 input (got ${incCount})`);
    }

    if (!["scan", "filter", "project", "sink"].includes(node.op)) {
      throw new Error(`Unsupported op in v3.0-beta executor: ${node.op}`);
    }
  }

  const lineage = new LineageDagCollector({ runId, plan: compiled.plan });

  // Materialize each node output as an array (beta). Later: streaming.
  const materialized = new Map(); // nodeId -> rows[]

  for (const nodeId of topo) {
    const node = nodeById.get(nodeId);
    lineage.start(nodeId);

    if (node.op === "scan") {
      const dataset = node.params.dataset;
      if (!db) throw new Error("scan requires ctx.db (MongoDB handle)");
      const rows = await db.collection(dataset).find({}).toArray();
      lineage.incRowsOut(nodeId, rows.length);
      materialized.set(nodeId, rows);
      lineage.end(nodeId);
      continue;
    }

    // unary input
    const inc = incomingByPort.get(nodeId);
    const fromId = inc.get("in")[0];
    const inRows = materialized.get(fromId) || [];
    lineage.incRowsIn(nodeId, "in", inRows.length);

    if (node.op === "filter") {
      const outRows = [];
      for (const r of inRows) {
        if (evalExpr(node.params.where, r)) outRows.push(r);
      }
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "project") {
      const outRows = inRows.map((r) => evalProject(node.params.exprs, r));
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "sink") {
      const coll = node.params.collection;
      if (!db) throw new Error("sink requires ctx.db (MongoDB handle)");
      const outRows = inRows;
      // naive: replace by runId; beta behavior
      await db.collection(coll).deleteMany({ _runId: runId });
      if (outRows.length) {
        const withMeta = outRows.map((r) => ({ ...r, _runId: runId }));
        await db.collection(coll).insertMany(withMeta);
      }
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }
  }

  const lineageDoc = lineage.finalize();

  if (db) {
    await writeDagLineage(db, runId, dagHash, lineageDoc);
    // update EMA per node
    for (const [nodeId, st] of Object.entries(lineageDoc.stats || {})) {
      await updateDagNodeStats(db, dagHash, nodeId, st, { alpha: 0.2 });
    }
  }

  return { runId, dagHash, plan: compiled.plan, optimized: opt.ok, optChanges: opt.ok ? opt.changes : [], lineage: lineageDoc };
}

module.exports = { executeDagBeta };
