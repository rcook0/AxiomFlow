/**
 * replay-optimizer-decisions.js
 *
 * Offline "replayer": re-selects plan variants for historical runs under a new policy,
 * and compares to the executed variant recorded in optimizer_decisions.
 *
 * Usage:
 *   node scripts/replay-optimizer-decisions.js mongodb://localhost:27017 axiomflow --limit 50 --policy ucb --ucbC 0.7
 *   node scripts/replay-optimizer-decisions.js mongodb://... axiomflow --family <inputDagHash> --limit 200 --policy best
 */
const { MongoClient } = require("mongodb");
const { compileDagSkeleton } = require("../backend/engine/compileDagSkeleton");
const { selectPlanVariantV4 } = require("../backend/optimizer/selectPlanV4");

function arg(name, def=null) {
  const i = process.argv.indexOf(name);
  if (i === -1) return def;
  return process.argv[i+1] ?? def;
}
function flag(name) { return process.argv.includes(name); }

async function main() {
  const uri = process.argv[2] || "mongodb://localhost:27017";
  const dbName = process.argv[3] || "axiomflow";

  const family = arg("--family", null);
  const limit = parseInt(arg("--limit", "50"), 10);

  const policyMode = arg("--policy", process.env.AXIOMFLOW_POLICY || "ucb");
  const eps = parseFloat(arg("--eps", process.env.AXIOMFLOW_EXPLORE_EPS || "0.05"));
  const ucbC = parseFloat(arg("--ucbC", process.env.AXIOMFLOW_UCB_C || "0.5"));
  const maxFactor = parseFloat(arg("--maxFactor", process.env.AXIOMFLOW_EXPLORE_MAX_FACTOR || "3.0"));

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);

  const q = family ? { familyDagHash: family } : {};
  const decisions = await db.collection("optimizer_decisions")
    .find(q)
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();

  let n = 0, mism = 0;

  for (const d of decisions) {
    const runId = d.runId;
    const optReport = await db.collection("run_dag_opt_reports").findOne({ runId });
    if (!optReport) continue;

    const bestPlan = optReport.after;
    const variants = optReport.variants?.candidates || [];

    // build list of candidate hashes
    const compiledBest = compileDagSkeleton(bestPlan, { assignIdsIfMissing: false });
    if (!compiledBest.ok) continue;

    const candHashes = [compiledBest.dagHash];
    for (const v of variants) {
      const c = compileDagSkeleton(v.plan, { assignIdsIfMissing: false });
      if (c.ok) candHashes.push(c.dagHash);
    }

    // fetch stats for these candidates
    const statsDocs = await db.collection("plan_variant_stats").find({
      familyDagHash: d.familyDagHash,
      variantDagHash: { $in: candHashes }
    }).toArray();

    const statsMap = new Map(statsDocs.map(x => [x.variantDagHash, x]));

    // construct "opt-like" object expected by selectPlanVariantV4
    const optLike = {
      ok: true,
      after: bestPlan,
      cost: optReport.cost,
      variants: optReport.variants
    };

    const sel = selectPlanVariantV4(d.familyDagHash, optLike, statsMap, {
      mode: policyMode,
      eps,
      ucbC,
      maxFactor
    });

    const predicted = sel.variantDagHash;
    const executed = d.executedDagHash;

    const ok = predicted && executed && (predicted === executed);
    n += 1;
    if (!ok) mism += 1;

    const exSt = statsMap.get(executed);
    const prSt = statsMap.get(predicted);

    console.log(JSON.stringify({
      runId,
      familyDagHash: d.familyDagHash,
      executed,
      predicted,
      match: ok,
      executedWallMs: exSt?.emaWallMs ?? null,
      predictedWallMs: prSt?.emaWallMs ?? null,
      executedRuns: exSt?.runs ?? 0,
      predictedRuns: prSt?.runs ?? 0,
      selected: sel.meta?.selected,
      explored: !!sel.meta?.explored,
      policy: sel.meta?.policy
    }));
  }

  console.error(JSON.stringify({ scanned: n, mismatches: mism, mismatchRate: n ? (mism/n) : null }));
  await client.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
