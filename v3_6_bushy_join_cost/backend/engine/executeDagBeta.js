const { compileDagSkeleton } = require("./compileDagSkeleton");
const { optimizeDagV3_6 } = require("./dagOptimizeV3_6");
const { LineageDagCollector } = require("../lineage/lineageCollectorDag");
const { writeDagLineage, updateDagNodeStats, writeDagOptReport, getDagNodeStats, getJoinSigStats, updateJoinSigStats, joinSigFromOnRef, joinSigFromOn } = require("../artifacts/dagPersist");
const { evalExpr, evalProject } = require("./exprEval");
const { execJoin } = require("./joinExec");

async function executeDagBeta(plan, ctx = {}) {
  const { db, runId = "run_" + Date.now() } = ctx;

  const compiled0 = compileDagSkeleton(plan, { assignIdsIfMissing: true });
  if (!compiled0.ok) throw new Error("DAG validation failed: " + JSON.stringify(compiled0.errors));
  const inputDagHash = compiled0.dagHash;

  let statsByNodeId = new Map();
  if (db) {
    try { statsByNodeId = await getDagNodeStats(db, inputDagHash); } catch (_) { statsByNodeId = new Map(); }
  }

  let joinSigStats = new Map();
  if (db) {
    try {
      const joinSigs = [];
      for (const n of (compiled0.plan.nodes || [])) {
        if (n.op !== "join") continue;
        const sig = n.params?.onRef ? joinSigFromOnRef(n.params.onRef) : joinSigFromOn(n.params?.on || []);
        if (sig) joinSigs.push(sig);
      }
      joinSigStats = await getJoinSigStats(db, joinSigs);
    } catch (_) { joinSigStats = new Map(); }
  }

  const opt = optimizeDagV3_6(compiled0.plan, statsByNodeId, joinSigStats);
  const execPlan = opt.ok ? opt.after : compiled0.plan;

  const compiled = compileDagSkeleton(execPlan, { assignIdsIfMissing: false });
  if (!compiled.ok) throw new Error("Optimized DAG invalid: " + JSON.stringify(compiled.errors));
  const dagHash = compiled.dagHash;

  if (db && opt.ok) {
    await writeDagOptReport(db, runId, dagHash, { before: opt.before, after: opt.after, changes: opt.changes , cost: opt.cost});
  }

  const nodeById = compiled.nodeById;
  const incomingByPort = compiled.incomingByPort;
  const topo = compiled.topoOrder;

  const lineage = new LineageDagCollector({ runId, plan: compiled.plan });
  const materialized = new Map();

  function countIncoming(nodeId) {
    const inc = incomingByPort.get(nodeId);
    if (!inc) return 0;
    return Array.from(inc.values()).reduce((a, arr) => a + arr.length, 0);
  }

  for (const nodeId of topo) {
    const node = nodeById.get(nodeId);
    lineage.start(nodeId);

    if (node.op === "scan") {
      if (countIncoming(nodeId) !== 0) throw new Error(`scan node ${nodeId} must have 0 inputs`);
      if (!db) throw new Error("scan requires ctx.db (MongoDB handle)");
      const rows = await db.collection(node.params.dataset).find({}).toArray();
      lineage.incRowsOut(nodeId, rows.length);
      materialized.set(nodeId, rows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "filter") {
      if (countIncoming(nodeId) !== 1) throw new Error(`filter node ${nodeId} must have exactly 1 input`);
      const fromId = incomingByPort.get(nodeId).get("in")[0];
      const inRows = materialized.get(fromId) || [];
      lineage.incRowsIn(nodeId, "in", inRows.length);
      const outRows = [];
      for (const r of inRows) if (evalExpr(node.params.where, r)) outRows.push(r);
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "project") {
      if (countIncoming(nodeId) !== 1) throw new Error(`project node ${nodeId} must have exactly 1 input`);
      const fromId = incomingByPort.get(nodeId).get("in")[0];
      const inRows = materialized.get(fromId) || [];
      lineage.incRowsIn(nodeId, "in", inRows.length);
      const outRows = inRows.map((r) => evalProject(node.params.exprs, r));
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "materialize") {
      if (countIncoming(nodeId) !== 1) throw new Error(`materialize node ${nodeId} must have exactly 1 input`);
      const fromId = incomingByPort.get(nodeId).get("in")[0];
      const inRows = materialized.get(fromId) || [];
      lineage.incRowsIn(nodeId, "in", inRows.length);
      lineage.incRowsOut(nodeId, inRows.length);
      materialized.set(nodeId, inRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "join") {
      if (countIncoming(nodeId) !== 2) throw new Error(`join node ${nodeId} must have exactly 2 inputs`);
      const inc = incomingByPort.get(nodeId);
      const leftFrom = (inc.get("left") || [])[0];
      const rightFrom = (inc.get("right") || [])[0];
      if (!leftFrom || !rightFrom) throw new Error(`join node ${nodeId} requires ports left/right`);

      const leftRows = materialized.get(leftFrom) || [];
      const rightRows = materialized.get(rightFrom) || [];
      lineage.incRowsIn(nodeId, "left", leftRows.length);
      lineage.incRowsIn(nodeId, "right", rightRows.length);

      let buildSide = node.params?.build;
      if (buildSide !== "left" && buildSide !== "right") {
        buildSide = (rightRows.length <= leftRows.length) ? "right" : "left";
      }
      const outRows = hashJoin(leftRows, rightRows, node.params, { buildSide });
      lineage.incRowsOut(nodeId, outRows.length);
      materialized.set(nodeId, outRows);
      lineage.end(nodeId);
      continue;
    }

    if (node.op === "sink") {
      if (countIncoming(nodeId) !== 1) throw new Error(`sink node ${nodeId} must have exactly 1 input`);
      if (!db) throw new Error("sink requires ctx.db (MongoDB handle)");
      const fromId = incomingByPort.get(nodeId).get("in")[0];
      const inRows = materialized.get(fromId) || [];
      lineage.incRowsIn(nodeId, "in", inRows.length);
      const coll = node.params.collection;
      await db.collection(coll).deleteMany({ _runId: runId });
      if (inRows.length) await db.collection(coll).insertMany(inRows.map((r)=>({ ...r, _runId: runId })));
      lineage.incRowsOut(nodeId, inRows.length);
      materialized.set(nodeId, inRows);
      lineage.end(nodeId);
      continue;
    }

    throw new Error(`Unsupported op: ${node.op}`);
  }

  const lineageDoc = lineage.finalize();
  if (db) {
    await writeDagLineage(db, runId, dagHash, lineageDoc);
    for (const [nid, st] of Object.entries(lineageDoc.stats || {})) {
      await updateDagNodeStats(db, dagHash, nid, st, { alpha: 0.2 });
    }

    // v3.5: learned join signature stats (stable across reorderings)
    try {
      const joinNodes = (compiled.plan.nodes || []).filter((n) => n.op === "join");
      for (const jn of joinNodes) {
        const st = (lineageDoc.stats || {})[jn.id];
        if (!st) continue;
        const sig = jn.params?.onRef ? joinSigFromOnRef(jn.params.onRef) : joinSigFromOn(jn.params?.on || []);
        if (!sig) continue;

        const meta = jn.params?.onRef ? {
          rels: Array.from(new Set((jn.params.onRef || []).flatMap(p => [p.left.rel, p.right.rel]).filter(Boolean))),
          predicates: (jn.params.onRef || []).map(p => `${p.left.rel}.${p.left.path}=${p.right.rel}.${p.right.path}`)
        } : null;

        await updateJoinSigStats(db, sig, meta, st, { alpha: 0.2 });
      }
    } catch (_) {}
  }

  return { runId, inputDagHash, dagHash, plan: compiled.plan, optimized: !!opt.ok, optChanges: opt.ok ? opt.changes : [], lineage: lineageDoc };
}

module.exports = { executeDagBeta };
