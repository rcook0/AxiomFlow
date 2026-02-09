/**
 * policy-lab.js
 *
 * Offline policy tuning using recorded optimizer_decisions + plan_variant_stats.
 *
 * Goal:
 * - Compare policies (UCB / Thompson / Epsilon / Best) by simulated regret using current EMA stats.
 *
 * Usage:
 *   node scripts/policy-lab.js mongodb://localhost:27017 axiomflow --family <hash> --limit 5000 --seed 1
 *
 * Notes:
 * - This is an *offline* evaluator: it does not reconstruct per-run "stats at that time".
 * - It uses the latest `plan_variant_stats` EMAs for each candidate variant.
 * - Candidate sets come from optimizer_decisions.candidates = [{rank, variantDagHash, estCost}]
 */
const { MongoClient } = require("mongodb");

function parseArgs(argv) {
  const args = { _: [] };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith("--")) {
      const k = a.slice(2);
      const v = (i + 1 < argv.length && !argv[i + 1].startsWith("--")) ? argv[++i] : true;
      args[k] = v;
    } else {
      args._.push(a);
    }
  }
  return args;
}

function mulberry32(seed) {
  let t = seed >>> 0;
  return function() {
    t += 0x6D2B79F5;
    let x = Math.imul(t ^ (t >>> 15), 1 | t);
    x ^= x + Math.imul(x ^ (x >>> 7), 61 | x);
    return ((x ^ (x >>> 14)) >>> 0) / 4294967296;
  };
}

function randn(rng) {
  let u = 0, v = 0;
  while (u === 0) u = rng();
  while (v === 0) v = rng();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function meanFor(hash, statsByHash, fallbackCost) {
  const st = statsByHash.get(hash);
  if (st?.emaWallMs != null) return st.emaWallMs;
  if (st?.emaTotalCost != null) return st.emaTotalCost;
  if (fallbackCost != null) return fallbackCost;
  return Infinity;
}

function sigmaFor(hash, statsByHash, baseline) {
  const st = statsByHash.get(hash);
  const runs = st?.runs ?? 0;
  const var0 = st?.emaVar ?? null;
  const mean = st?.emaWallMs ?? baseline;
  const varAssumed = var0 != null ? var0 : Math.pow(0.25 * mean, 2);
  return Math.sqrt(Math.max(1e-6, varAssumed)) / Math.sqrt(runs + 1);
}

function chooseOffline(candidates, statsByHash, policy, rng) {
  const mode = policy.mode;
  const eps = policy.eps;
  const maxFactor = policy.maxFactor;
  const ucbC = policy.ucbC;
  const thScale = policy.thompsonSigmaScale ?? 1.0;

  const scored = candidates.map(c => ({
    ...c,
    mean: meanFor(c.variantDagHash, statsByHash, c.estCost ?? null),
    runs: statsByHash.get(c.variantDagHash)?.runs ?? 0
  })).sort((a,b)=>a.mean-b.mean);

  const best = scored[0];
  const bestMean = best.mean;
  const bestHash = best.variantDagHash;

  // epsilon exploration among viable non-best
  const viable = scored.slice(1).filter(c => (c.estCost ?? Infinity) <= ( (best.estCost ?? Infinity) * maxFactor ));
  const willExplore = (rng() < eps) && viable.length > 0;

  if ((mode === "epsilon" || mode === "ucb" || mode === "thompson") && willExplore) {
    const pick = viable[Math.floor(rng() * viable.length)];
    return { pick, explored: true, bestHash, bestMean };
  }

  if (mode === "best") {
    return { pick: best, explored: false, bestHash, bestMean };
  }

  if (mode === "thompson") {
    let baseline = Number.isFinite(bestMean) ? bestMean : 1000;
    let bestSample = Infinity;
    let pick = best;
    for (const c of scored) {
      const sig = sigmaFor(c.variantDagHash, statsByHash, baseline) * thScale;
      const sample = c.mean + sig * randn(rng);
      if (sample < bestSample) { bestSample = sample; pick = c; }
    }
    return { pick, explored: false, bestHash, bestMean };
  }

  // default UCB
  const baseline = Number.isFinite(bestMean) ? bestMean : 1;
  const totalRuns = scored.reduce((acc,x)=>acc+(x.runs||0),0);
  let bestScore = -Infinity;
  let pick = best;
  for (const c of scored) {
    const reward = -c.mean;
    const explore = (ucbC * baseline) * Math.sqrt(Math.log(totalRuns + 1) / (c.runs + 1));
    const score = reward + explore;
    if (score > bestScore) { bestScore = score; pick = c; }
  }
  return { pick, explored: false, bestHash, bestMean };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const uri = args._[0] || "mongodb://localhost:27017";
  const dbName = args._[1] || "axiomflow";

  const family = args.family || null;
  const limit = parseInt(args.limit || "5000", 10);
  const seed = parseInt(args.seed || "1", 10);

  const epsList = (args.eps || "0.00,0.01,0.03,0.05,0.10").split(",").map(parseFloat);
  const maxFactorList = (args.maxFactor || "2.0,3.0,5.0").split(",").map(parseFloat);
  const ucbCList = (args.ucbC || "0.25,0.5,1.0").split(",").map(parseFloat);
  const thScaleList = (args.thSigma || "0.5,1.0,2.0").split(",").map(parseFloat);
  const modes = (args.modes || "ucb,thompson,epsilon,best").split(",").map(s=>s.trim()).filter(Boolean);

  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);

  const q = {};
  if (family) q.familyDagHash = family;

  const decisions = await db.collection("optimizer_decisions")
    .find(q)
    .sort({ createdAt: -1 })
    .limit(limit)
    .toArray();

  if (!decisions.length) {
    console.log("No optimizer_decisions found for query:", q);
    await client.close();
    return;
  }

  // Collect candidate hashes
  const allHashes = new Set();
  for (const d of decisions) {
    for (const c of (d.candidates || [])) {
      if (c.variantDagHash) allHashes.add(c.variantDagHash);
    }
    if (d.bestVariantDagHash) allHashes.add(d.bestVariantDagHash);
    if (d.executedDagHash) allHashes.add(d.executedDagHash);
  }
  const hashes = Array.from(allHashes);

  const statsDocs = await db.collection("plan_variant_stats")
    .find({ variantDagHash: { $in: hashes } })
    .toArray();

  const statsByHash = new Map(statsDocs.map(d => [d.variantDagHash, d]));

  function evalConfig(cfg) {
    const rng = mulberry32(seed);
    let n = 0;
    let regretSum = 0;
    let explored = 0;
    let missing = 0;

    for (const d of decisions) {
      const cands = (d.candidates || []).filter(c => c.variantDagHash);
      if (!cands.length) { missing++; continue; }

      const best = cands.reduce((a,b)=> (meanFor(a.variantDagHash, statsByHash, a.estCost??null) <= meanFor(b.variantDagHash, statsByHash, b.estCost??null) ? a : b));
      const bestMean = meanFor(best.variantDagHash, statsByHash, best.estCost ?? null);

      const { pick, explored: ex } = chooseOffline(cands, statsByHash, cfg, rng);
      const pickedMean = meanFor(pick.variantDagHash, statsByHash, pick.estCost ?? null);

      if (!Number.isFinite(bestMean) || !Number.isFinite(pickedMean)) { missing++; continue; }

      regretSum += Math.max(0, pickedMean - bestMean);
      n++;
      if (ex) explored++;
    }

    return {
      n,
      missing,
      exploredRate: n ? explored / n : 0,
      avgRegretMs: n ? regretSum / n : Infinity,
      cfg
    };
  }

  const results = [];

  for (const mode of modes) {
    if (mode === "best") {
      results.push(evalConfig({ mode: "best", eps: 0, maxFactor: 1, ucbC: 0.5 }));
      continue;
    }
    if (mode === "epsilon") {
      for (const eps of epsList) for (const maxFactor of maxFactorList) {
        results.push(evalConfig({ mode: "epsilon", eps, maxFactor, ucbC: 0.5 }));
      }
      continue;
    }
    if (mode === "ucb") {
      for (const eps of epsList) for (const maxFactor of maxFactorList) for (const ucbC of ucbCList) {
        results.push(evalConfig({ mode: "ucb", eps, maxFactor, ucbC }));
      }
      continue;
    }
    if (mode === "thompson") {
      for (const eps of epsList) for (const maxFactor of maxFactorList) for (const thompsonSigmaScale of thScaleList) {
        results.push(evalConfig({ mode: "thompson", eps, maxFactor, ucbC: 0.5, thompsonSigmaScale }));
      }
      continue;
    }
  }

  results.sort((a,b)=>a.avgRegretMs-b.avgRegretMs);

  console.log(`Policy lab results (family=${family||"ALL"}, N=${decisions.length}, used=${results[0].n}, seed=${seed})`);
  console.log("Top 15 by avg regret (ms):");
  for (const r of results.slice(0, 15)) {
    const c = r.cfg;
    const tag = [
      c.mode,
      `eps=${c.eps}`,
      `maxF=${c.maxFactor}`,
      c.mode==="ucb" ? `ucbC=${c.ucbC}` : "",
      c.mode==="thompson" ? `thSig=${c.thompsonSigmaScale}` : ""
    ].filter(Boolean).join(" ");
    console.log(`${r.avgRegretMs.toFixed(2)} ms | explored ${(100*r.exploredRate).toFixed(1)}% | used ${r.n} | missing ${r.missing} | ${tag}`);
  }

  await client.close();
}

main().catch((e)=>{ console.error(e); process.exit(1); });
