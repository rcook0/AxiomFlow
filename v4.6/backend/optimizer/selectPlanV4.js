const { compileDagSkeleton } = require("../engine/compileDagSkeleton");

function annotateCandidatesWithHashes(candidates){
  const out = [];
  for (const c of candidates) {
    const compiled = compileDagSkeleton(c.plan);
    if (!compiled.ok) continue;
    out.push({ ...c, variantDagHash: compiled.dagHash, normalizedPlan: compiled.plan });
  }
  return out;
}

function toMap(maybe){
  if (!maybe) return new Map();
  if (maybe instanceof Map) return maybe;
  return new Map(Object.entries(maybe));
}

function meanForCandidate(c, statsByHash, predictedWallMsByHash){
  const st = statsByHash.get(c.variantDagHash);
  if (st?.emaWallMs != null) return st.emaWallMs;
  const pred = predictedWallMsByHash.get(c.variantDagHash);
  if (pred != null) return pred;
  if (st?.emaTotalCost != null) return st.emaTotalCost;
  if (c.estCost != null) return c.estCost;
  if (c.cost?.summary?.totalCost != null) return c.cost.summary.totalCost;
  return Infinity;
}

function sigmaForCandidate(c, statsByHash, baseline){
  const st = statsByHash.get(c.variantDagHash);
  const runs = st?.runs ?? 0;
  const var0 = st?.emaVar ?? null;
  const mean = st?.emaWallMs ?? baseline;
  const varAssumed = var0 != null ? var0 : Math.pow(0.25 * mean, 2);
  const se = Math.sqrt(Math.max(1e-6, varAssumed)) / Math.sqrt(runs + 1);
  return se;
}

function randn(){
  let u = 0, v = 0;
  while (u === 0) u = Math.random();
  while (v === 0) v = Math.random();
  return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v);
}

function selectUcb(candidates, statsByHash, predictedWallMsByHash, policy){
  const ucbC = policy.ucbC ?? 0.5;
  const means = [];
  for (const c of candidates) {
    const st = statsByHash.get(c.variantDagHash);
    const mean = meanForCandidate(c, statsByHash, predictedWallMsByHash);
    const runs = st?.runs ?? 0;
    means.push({ c, mean: mean ?? Infinity, runs });
  }
  means.sort((a,b)=>a.mean-b.mean);
  const best = means[0];
  const baseline = Number.isFinite(best.mean) ? best.mean : 1;
  const totalRuns = means.reduce((acc,x)=>acc+(x.runs||0),0);

  let bestScore = -Infinity;
  let pick = best.c;

  for (const x of means) {
    const reward = -x.mean;
    const explore = (ucbC * baseline) * Math.sqrt(Math.log(totalRuns + 1) / (x.runs + 1));
    const score = reward + explore;
    if (score > bestScore) {
      bestScore = score;
      pick = x.c;
    }
  }

  return { pick, reason: "UCB", ucbC, baseline, totalRuns };
}

function selectThompson(candidates, statsByHash, predictedWallMsByHash, policy){
  let bestMean = Infinity;
  for (const c of candidates) {
    const mu = meanForCandidate(c, statsByHash, predictedWallMsByHash);
    if (mu < bestMean) bestMean = mu;
  }
  const baseline = Number.isFinite(bestMean) ? bestMean : 1000;

  let bestSample = Infinity;
  let pick = candidates[0];

  for (const c of candidates) {
    const mu = meanForCandidate(c, statsByHash, predictedWallMsByHash);
    const sigma = sigmaForCandidate(c, statsByHash, baseline) * (policy.thompsonSigmaScale ?? 1.0);
    const sample = mu + sigma * randn();
    if (sample < bestSample) {
      bestSample = sample;
      pick = c;
    }
  }

  return { pick, reason: "THOMPSON", baseline };
}

function selectPlanVariantV4(inputDagHash, opt, planVariantStatsMap, policy = {}){
  const statsByHash = toMap(planVariantStatsMap);
  const predictedWallMsByHash = toMap(policy.predictedWallMsByHash);

  const eps = policy.eps ?? parseFloat(process.env.AXIOMFLOW_EXPLORE_EPS || "0.05");
  const maxFactor = policy.maxFactor ?? parseFloat(process.env.AXIOMFLOW_EXPLORE_MAX_FACTOR || "3.0");
  const mode = policy.mode ?? (process.env.AXIOMFLOW_POLICY || "ucb"); // ucb | epsilon | best | thompson
  const ucbC = policy.ucbC ?? parseFloat(process.env.AXIOMFLOW_UCB_C || "0.5");

  const best = { rank: "best", plan: opt.after, estCost: opt.cost?.summary?.totalCost ?? null, cost: opt.cost };
  const cands = opt.variants?.candidates ? opt.variants.candidates.map(c => ({
    rank: c.rank,
    plan: c.plan,
    estCost: c.cost?.summary?.totalCost ?? c.estCost ?? null,
    cost: c.cost,
    chosenOrder: c.chosenOrder
  })) : [];

  let all = annotateCandidatesWithHashes([best, ...cands]);
  if (!all.length) {
    return { plan: opt.after, variantDagHash: null, meta: { explored: false, selected: "best", policy: { mode, eps, maxFactor, ucbC } } };
  }

  const seen = new Set();
  all = all.filter((c) => {
    if (seen.has(c.variantDagHash)) return false;
    seen.add(c.variantDagHash);
    return true;
  });

  const bestHash = all[0].variantDagHash;
  const bestEst = all[0].estCost ?? Infinity;

  const candidateSet = all.map(c => ({
    rank: c.rank,
    variantDagHash: c.variantDagHash,
    estCost: c.estCost ?? null
  }));

  const viableNonBest = all.slice(1).filter((c) => {
    const est = c.estCost ?? Infinity;
    return est <= bestEst * maxFactor;
  });

  const willExplore = (Math.random() < eps) && viableNonBest.length > 0;

  if (willExplore && mode !== "ucb_only") {
    const pick = viableNonBest[Math.floor(Math.random() * viableNonBest.length)];
    return {
      plan: pick.normalizedPlan || pick.plan,
      variantDagHash: pick.variantDagHash,
      meta: {
        explored: true,
        selected: `explore:${pick.rank}`,
        bestVariantDagHash: bestHash,
        chosenOrder: pick.chosenOrder || null,
        chosenRank: pick.rank,
        chosenCost: pick.estCost ?? null,
        candidates: candidateSet,
        policy: { mode, eps, maxFactor, ucbC }
      }
    };
  }

  if (mode === "best" || mode === "epsilon") {
    const min = all.reduce((a,b)=> ( (a.estCost??Infinity) <= (b.estCost??Infinity) ? a : b ));
    return {
      plan: min.normalizedPlan || min.plan,
      variantDagHash: min.variantDagHash,
      meta: {
        explored: false,
        selected: (min.rank === "best" ? "best" : `alt:${min.rank}`),
        bestVariantDagHash: bestHash,
        chosenOrder: min.chosenOrder || null,
        chosenRank: min.rank,
        chosenCost: min.estCost ?? null,
        candidates: candidateSet,
        policy: { mode, eps, maxFactor, ucbC }
      }
    };
  }

  if (mode === "thompson") {
    const t = selectThompson(all, statsByHash, predictedWallMsByHash, policy);
    const pick = t.pick;
    return {
      plan: pick.normalizedPlan || pick.plan,
      variantDagHash: pick.variantDagHash,
      meta: {
        explored: false,
        selected: (pick.rank === "best" ? "best" : `thompson:${pick.rank}`),
        bestVariantDagHash: bestHash,
        chosenOrder: pick.chosenOrder || null,
        chosenRank: pick.rank,
        chosenCost: pick.estCost ?? null,
        candidates: candidateSet,
        policy: { mode: "thompson", eps, maxFactor, ucbC },
        thompson: { reason: t.reason, baseline: t.baseline }
      }
    };
  }

  const u = selectUcb(all, statsByHash, predictedWallMsByHash, { ucbC });
  const pick = u.pick;

  return {
    plan: pick.normalizedPlan || pick.plan,
    variantDagHash: pick.variantDagHash,
    meta: {
      explored: false,
      selected: (pick.rank === "best" ? "best" : `ucb:${pick.rank}`),
      bestVariantDagHash: bestHash,
      chosenOrder: pick.chosenOrder || null,
      chosenRank: pick.rank,
      chosenCost: pick.estCost ?? null,
      candidates: candidateSet,
      policy: { mode: "ucb", eps, maxFactor, ucbC },
      ucb: { reason: u.reason, baseline: u.baseline, totalRuns: u.totalRuns }
    }
  };
}

module.exports = { selectPlanVariantV4, annotateCandidatesWithHashes };
