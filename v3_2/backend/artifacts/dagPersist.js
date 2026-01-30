async function writeDagLineage(db, runId, dagHash, lineageDoc) {
  const col = db.collection("run_dag_lineage");
  const doc = { runId, dagHash, createdAt: new Date(), ...lineageDoc };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_dag_lineage", runId };
}

function ema(prev, x, alpha) {
  if (prev === null || prev === undefined) return x;
  return alpha * x + (1 - alpha) * prev;
}

/**
 * Persist per-node stats keyed by (dagHash,nodeId).
 * Uses EMA to build a cheap cost model:
 * - rowsInTotal, rowsOut, selectivity, ms, msPerInRow
 */
async function updateDagNodeStats(db, dagHash, nodeId, nodeStat, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("dag_node_stats");

  const rowsInTotal = Object.values(nodeStat.rowsInByPort || {}).reduce((a,b)=>a+b,0);
  const rowsOut = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const selectivity = rowsInTotal > 0 ? (rowsOut / rowsInTotal) : null;
  const msPerInRow = rowsInTotal > 0 ? (ms / rowsInTotal) : null;

  const existing = await col.findOne({ dagHash, nodeId });

  const next = {
    dagHash,
    nodeId,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,
    emaRowsIn: ema(existing?.emaRowsIn, rowsInTotal, alpha),
    emaRowsOut: ema(existing?.emaRowsOut, rowsOut, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),
    emaSelectivity: (selectivity === null) ? existing?.emaSelectivity : ema(existing?.emaSelectivity, selectivity, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha)
  };

  await col.updateOne({ dagHash, nodeId }, { $set: next }, { upsert: true });
  return next;
}

async function writeDagOptReport(db, runId, dagHash, optReport) {
  const col = db.collection("run_dag_opt_reports");
  const doc = { runId, dagHash, createdAt: new Date(), ...optReport };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_dag_opt_reports", runId };
}

module.exports = { writeDagLineage, updateDagNodeStats, writeDagOptReport };
