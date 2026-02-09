const crypto = require("crypto");

function sha1(s){ return crypto.createHash("sha1").update(String(s)).digest("hex"); }

function canonicalPredRef(p){
  const a = `${p.left.rel}:${p.left.path||""}`;
  const b = `${p.right.rel}:${p.right.path||""}`;
  if (a <= b) return { left: p.left, right: p.right };
  return { left: p.right, right: p.left };
}

function joinSigFromOnRef(onRef){
  const preds = (onRef || []).map(canonicalPredRef).map(p => ({
    l: `${p.left.rel}:${p.left.path||""}`,
    r: `${p.right.rel}:${p.right.path||""}`,
  }));
  preds.sort((x,y)=> (x.l+x.r).localeCompare(y.l+y.r));
  return sha1(JSON.stringify(preds));
}

function joinSigFromOn(on){
  const pairs = (on || []).map(([l,r])=>({l:String(l), r:String(r)}));
  pairs.sort((a,b)=> (a.l+a.r).localeCompare(b.l+b.r));
  return sha1(JSON.stringify(pairs));
}

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

  const rowsInByPort = nodeStat.rowsInByPort || {};
  const leftIn = rowsInByPort.left ?? null;
  const rightIn = rowsInByPort.right ?? null;
  const inTotal = Object.values(rowsInByPort).reduce((a,b)=>a+b,0);

  const rowsOut = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const selectivity = inTotal > 0 ? (rowsOut / inTotal) : null;
  const msPerInRow = inTotal > 0 ? (ms / inTotal) : null;

  const fanoutPerLeft = (leftIn !== null && leftIn > 0) ? (rowsOut / leftIn) : null;
  const fanoutPerRight = (rightIn !== null && rightIn > 0) ? (rowsOut / rightIn) : null;

  const existing = await col.findOne({ dagHash, nodeId });

  const next = {
    dagHash,
    nodeId,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,

    emaRowsIn: ema(existing?.emaRowsIn, inTotal, alpha),
    emaRowsOut: ema(existing?.emaRowsOut, rowsOut, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),

    emaSelectivity: (selectivity === null) ? existing?.emaSelectivity : ema(existing?.emaSelectivity, selectivity, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha),

    emaRowsInLeft: (leftIn === null) ? existing?.emaRowsInLeft : ema(existing?.emaRowsInLeft, leftIn, alpha),
    emaRowsInRight: (rightIn === null) ? existing?.emaRowsInRight : ema(existing?.emaRowsInRight, rightIn, alpha),
    emaFanoutPerLeft: (fanoutPerLeft === null) ? existing?.emaFanoutPerLeft : ema(existing?.emaFanoutPerLeft, fanoutPerLeft, alpha),
    emaFanoutPerRight: (fanoutPerRight === null) ? existing?.emaFanoutPerRight : ema(existing?.emaFanoutPerRight, fanoutPerRight, alpha),
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


async function getDagNodeStats(db, dagHash) {
  const col = db.collection("dag_node_stats");
  const docs = await col.find({ dagHash }).toArray();
  const m = new Map();
  for (const d of docs) m.set(d.nodeId, d);
  return m;
}

module.exports.getDagNodeStats = getDagNodeStats;

async function getJoinSigStats(db, joinSigs) {
  const sigs = Array.from(new Set(joinSigs || [])).filter(Boolean);
  const m = new Map();
  if (sigs.length === 0) return m;
  const col = db.collection("join_sig_stats");
  const docs = await col.find({ joinSig: { $in: sigs } }).toArray();
  for (const d of docs) m.set(d.joinSig, d);
  return m;
}

async function updateJoinSigStats(db, joinSig, meta, nodeStat, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("join_sig_stats");

  const rowsInByPort = nodeStat.rowsInByPort || {};
  const leftIn = rowsInByPort.left ?? null;
  const rightIn = rowsInByPort.right ?? null;
  const inTotal = Object.values(rowsInByPort).reduce((a,b)=>a+b,0);
  const out = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const fanoutPerLeft = (leftIn !== null && leftIn > 0) ? (out / leftIn) : null;
  const fanoutPerRight = (rightIn !== null && rightIn > 0) ? (out / rightIn) : null;
  const msPerInRow = inTotal > 0 ? (ms / inTotal) : null;

  const existing = await col.findOne({ joinSig });
  const next = {
    schemaVersion: 1,
    joinSig,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,

    // optional meta for debugging/explainability
    rels: meta?.rels || existing?.rels,
    predicates: meta?.predicates || existing?.predicates,

    emaLeftIn: (leftIn === null) ? existing?.emaLeftIn : ema(existing?.emaLeftIn, leftIn, alpha),
    emaRightIn: (rightIn === null) ? existing?.emaRightIn : ema(existing?.emaRightIn, rightIn, alpha),
    emaOut: ema(existing?.emaOut, out, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),

    emaFanoutPerLeft: (fanoutPerLeft === null) ? existing?.emaFanoutPerLeft : ema(existing?.emaFanoutPerLeft, fanoutPerLeft, alpha),
    emaFanoutPerRight: (fanoutPerRight === null) ? existing?.emaFanoutPerRight : ema(existing?.emaFanoutPerRight, fanoutPerRight, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha),
  };

  await col.updateOne({ joinSig }, { $set: next }, { upsert: true });
  return next;
}


module.exports.getJoinSigStats = getJoinSigStats;
module.exports.updateJoinSigStats = updateJoinSigStats;
module.exports.joinSigFromOnRef = joinSigFromOnRef;

function predSigFromExpr(expr){
  function canon(x){
    if (x == null) return null;
    if (Array.isArray(x)) {
      const [op, ...args] = x;
      const cargs = args.map(canon);
      if (op === "and" || op === "or") {
        const ss = cargs.map((a)=>JSON.stringify(a)).sort();
        return [op, ...ss.map(s=>JSON.parse(s))];
      }
      if (op === "eq" || op === "==") {
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
  return sha1(JSON.stringify(canon(expr)));
}

async function getPredSigStats(db, predSigs) {
  const sigs = Array.from(new Set(predSigs || [])).filter(Boolean);
  const m = new Map();
  if (sigs.length === 0) return m;
  const col = db.collection("pred_sig_stats");
  const docs = await col.find({ predSig: { $in: sigs } }).toArray();
  for (const d of docs) m.set(d.predSig, d);
  return m;
}

async function updatePredSigStats(db, predSig, meta, nodeStat, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("pred_sig_stats");

  const rowsInByPort = nodeStat.rowsInByPort || {};
  const inRows = rowsInByPort.in ?? rowsInByPort[""] ?? rowsInByPort.input ?? null;
  const out = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const selectivity = (inRows !== null && inRows > 0) ? (out / inRows) : null;
  const msPerInRow = (inRows !== null && inRows > 0) ? (ms / inRows) : null;

  const existing = await col.findOne({ predSig });
  const next = {
    schemaVersion: 1,
    predSig,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,

    // optional meta
    example: meta?.example || existing?.example,

    emaInRows: (inRows === null) ? existing?.emaInRows : ema(existing?.emaInRows, inRows, alpha),
    emaOutRows: ema(existing?.emaOutRows, out, alpha),
    emaSelectivity: (selectivity === null) ? existing?.emaSelectivity : ema(existing?.emaSelectivity, selectivity, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha),
  };

  await col.updateOne({ predSig }, { $set: next }, { upsert: true });
  return next;
}


module.exports.predSigFromExpr = predSigFromExpr;

module.exports.getPredSigStats = getPredSigStats;

module.exports.updatePredSigStats = updatePredSigStats;

// AND-composition signature: stable hash of the multiset of atom predicate signatures.
function predGroupSigFromExpr(expr){
  function canon(x){
    if (x == null) return null;
    if (Array.isArray(x)) {
      const [op, ...args] = x;
      const cargs = args.map(canon);
      if (op === "and" || op === "or") {
        const ss = cargs.map((a)=>JSON.stringify(a)).sort();
        return [op, ...ss.map(s=>JSON.parse(s))];
      }
      if (op === "eq" || op === "==") {
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

  function predSigLocal(ex){
    return sha1(JSON.stringify(canon(ex)));
  }

  function flattenAnd(x, out){
    if (!Array.isArray(x)) { out.push(x); return; }
    const [op, ...args] = x;
    if (op !== "and") { out.push(x); return; }
    for (const a of args) flattenAnd(a, out);
  }

  if (!Array.isArray(expr) || expr[0] !== "and") return null;
  const atoms = [];
  flattenAnd(expr, atoms);

  const atomSigs = atoms.map(predSigLocal).filter(Boolean).sort();
  if (atomSigs.length <= 1) return null;
  return sha1(JSON.stringify(atomSigs));
}

function joinFilterSig(joinSig, predOrGroupSig){
  if (!joinSig || !predOrGroupSig) return null;
  return sha1(`${joinSig}:${predOrGroupSig}`);
}

async function getPredGroupStats(db, groupSigs) {
  const sigs = Array.from(new Set(groupSigs || [])).filter(Boolean);
  const m = new Map();
  if (sigs.length === 0) return m;
  const col = db.collection("pred_group_stats");
  const docs = await col.find({ predGroupSig: { $in: sigs } }).toArray();
  for (const d of docs) m.set(d.predGroupSig, d);
  return m;
}

async function updatePredGroupStats(db, predGroupSig, meta, nodeStat, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("pred_group_stats");

  const rowsInByPort = nodeStat.rowsInByPort || {};
  const inRows = rowsInByPort.in ?? rowsInByPort[""] ?? rowsInByPort.input ?? null;
  const out = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const selectivity = (inRows !== null && inRows > 0) ? (out / inRows) : null;
  const msPerInRow = (inRows !== null && inRows > 0) ? (ms / inRows) : null;

  const existing = await col.findOne({ predGroupSig });
  const next = {
    schemaVersion: 1,
    predGroupSig,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,

    example: meta?.example || existing?.example,
    atomSigs: meta?.atomSigs || existing?.atomSigs,

    emaInRows: (inRows === null) ? existing?.emaInRows : ema(existing?.emaInRows, inRows, alpha),
    emaOutRows: ema(existing?.emaOutRows, out, alpha),
    emaSelectivity: (selectivity === null) ? existing?.emaSelectivity : ema(existing?.emaSelectivity, selectivity, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha),
  };

  await col.updateOne({ predGroupSig }, { $set: next }, { upsert: true });
  return next;
}

async function getJoinFilterStats(db, segSigs) {
  const sigs = Array.from(new Set(segSigs || [])).filter(Boolean);
  const m = new Map();
  if (sigs.length === 0) return m;
  const col = db.collection("join_filter_stats");
  const docs = await col.find({ joinFilterSig: { $in: sigs } }).toArray();
  for (const d of docs) m.set(d.joinFilterSig, d);
  return m;
}

async function updateJoinFilterStats(db, joinFilterSigKey, meta, nodeStat, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("join_filter_stats");

  const rowsInByPort = nodeStat.rowsInByPort || {};
  const inRows = rowsInByPort.in ?? rowsInByPort[""] ?? rowsInByPort.input ?? null;
  const out = nodeStat.rowsOut ?? 0;
  const ms = nodeStat.ms ?? 0;

  const selectivity = (inRows !== null && inRows > 0) ? (out / inRows) : null;
  const msPerInRow = (inRows !== null && inRows > 0) ? (ms / inRows) : null;

  const existing = await col.findOne({ joinFilterSig: joinFilterSigKey });
  const next = {
    schemaVersion: 1,
    joinFilterSig: joinFilterSigKey,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,

    joinSig: meta?.joinSig || existing?.joinSig,
    predSig: meta?.predSig || existing?.predSig,
    predGroupSig: meta?.predGroupSig || existing?.predGroupSig,
    example: meta?.example || existing?.example,

    emaInRows: (inRows === null) ? existing?.emaInRows : ema(existing?.emaInRows, inRows, alpha),
    emaOutRows: ema(existing?.emaOutRows, out, alpha),
    emaSelectivity: (selectivity === null) ? existing?.emaSelectivity : ema(existing?.emaSelectivity, selectivity, alpha),
    emaMs: ema(existing?.emaMs, ms, alpha),
    emaMsPerInRow: (msPerInRow === null) ? existing?.emaMsPerInRow : ema(existing?.emaMsPerInRow, msPerInRow, alpha),
  };

  await col.updateOne({ joinFilterSig: joinFilterSigKey }, { $set: next }, { upsert: true });
  return next;
}

async function updatePlanVariantStats(db, familyDagHash, variantDagHash, wallMs, costSummary, opts = {}) {
  const alpha = opts.alpha ?? 0.2;
  const col = db.collection("plan_variant_stats");
  const existing = await col.findOne({ familyDagHash, variantDagHash });

  const next = {
    schemaVersion: 1,
    familyDagHash,
    variantDagHash,
    updatedAt: new Date(),
    runs: (existing?.runs || 0) + 1,
    emaWallMs: ema(existing?.emaWallMs, wallMs, alpha),
    emaTotalCost: costSummary?.totalCost == null ? existing?.emaTotalCost : ema(existing?.emaTotalCost, costSummary.totalCost, alpha)
  };

  await col.updateOne({ familyDagHash, variantDagHash }, { $set: next }, { upsert: true });
  return next;
}


module.exports.predGroupSigFromExpr = predGroupSigFromExpr;

module.exports.joinFilterSig = joinFilterSig;

module.exports.getPredGroupStats = getPredGroupStats;

module.exports.updatePredGroupStats = updatePredGroupStats;

module.exports.getJoinFilterStats = getJoinFilterStats;

module.exports.updateJoinFilterStats = updateJoinFilterStats;

module.exports.updatePlanVariantStats = updatePlanVariantStats;

async function getPlanVariantStats(db, familyDagHash, variantDagHashes) {
  const hashes = Array.from(new Set(variantDagHashes || [])).filter(Boolean);
  const m = new Map();
  if (!familyDagHash || hashes.length === 0) return m;
  const col = db.collection("plan_variant_stats");
  const docs = await col.find({ familyDagHash, variantDagHash: { $in: hashes } }).toArray();
  for (const d of docs) m.set(d.variantDagHash, d);
  return m;
}

module.exports.getPlanVariantStats = getPlanVariantStats;

async function writeOptimizerDecision(db, doc) {
  if (!doc) return null;
  const col = db.collection("optimizer_decisions");
  const next = {
    schemaVersion: 1,
    createdAt: new Date(),
    ...doc
  };
  await col.insertOne(next);
  return next;
}

module.exports.writeOptimizerDecision = writeOptimizerDecision;
