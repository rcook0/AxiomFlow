const { compileDagSkeleton } = require("../engine/compileDagSkeleton");

/**
 * Compute per-candidate hashes (variantDagHash) via compileDagSkeleton normalization.
 */
function annotateCandidatesWithHashes(candidates){
  const out = [];
  for (const c of candidates) {
    const compiled = compileDagSkeleton(c.plan);
    if (!compiled.ok) continue;
    out.push({ ...c, variantDagHash: compiled.dagHash, normalizedPlan: compiled.plan });
  }
  return out;
}

/**
 * UCB selection over candidates using plan_variant_stats:
 * - objective: minimize wallMs (emaWallMs)
 * - fallback: minimize estimated cost (emaTotalCost or optimizer cost)
 *
 * Score = -mean + (ucbC * baseline) * sqrt(log(totalRuns+1)/(runs+1))
 *
 * baseline is bestMean (wallMs) when available, else bestEstCost.
 */
function selectUcb(candidates, statsByHash, policy){
  const ucbC = policy.ucbC ?? 0.5;

  // compute means
  const means = [];
  for (const c of candidates) {
    const st = statsByHash.get(c.variantDagHash);
    const mean = (st?.emaWallMs ?? st?.emaTotalCost ?? c.estCost ?? c.cost?.summary?.totalCost ?? null);
    const runs = st?.runs ?? 0;
    means.push({ c, mean: mean ?? Infinity, runs });
  }

  // best mean for baseline scaling
  means.sort((a,b)=>a.mean-b.mean);
  const best = means[0];
  const baseline = Number.isFinite(best.mean) ? best.mean : 1;

  const totalRuns = means.reduce((acc,x)=>acc+(x.runs||0),0);

  let bestScore = -Infinity;
  let pick = best.c;

  for (const x of means) {
    const reward = -x.mean; // smaller mean => larger reward
    const explore = (ucbC * baseline) * Math.sqrt(Math.log(totalRuns + 1) / (x.runs + 1));
    const score = reward + explore;
    if (score > bestScore) {
      bestScore = score;
      pick = x.c;
    }
  }

  return { pick, reason: "UCB", ucbC, baseline, totalRuns };
}

/**
 * Epsilon-greedy selector:
 * - exploit via UCB or "best-estCost"
 * - explore: random viable non-best (bounded by maxFactor on estCost)
 */
function selectPlanVariantV4(inputDagHash, opt, planVariantStatsMap, policy = {}){
  const eps = policy.eps ?? parseFloat(process.env.AXIOMFLOW_EXPLORE_EPS || "0.05");
  const maxFactor = policy.maxFactor ?? parseFloat(process.env.AXIOMFLOW_EXPLORE_MAX_FACTOR || "3.0");
  const mode = policy.mode ?? (process.env.AXIOMFLOW_POLICY || "ucb"); // ucb | epsilon
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

  // Remove duplicate hashes (keep best first)
  const seen = new Set();
  all = all.filter((c) => {
    if (seen.has(c.variantDagHash)) return false;
    seen.add(c.variantDagHash);
    return true;
  });

  const bestHash = all[0].variantDagHash;
  const bestEst = all[0].estCost ?? Infinity;

  // Apply viability gating for exploration candidates
  const viableNonBest = all.slice(1).filter((c) => {
    const est = c.estCost ?? Infinity;
    return est <= bestEst * maxFactor;
  });

  // Decide explore
  const willExplore = (Math.random() < eps) && viableNonBest.length > 0;

  if (willExplore && mode !== "ucb_only") {
    // random explore among viable
    const pick = viableNonBest[Math.floor(Math.random() * viableNonBest.length)];
    return {
      plan: pick.normalizedPlan || pick.plan,
      variantDagHash: pick.variantDagHash,
      meta: {
        explored: true,
        selected: `explore:${pick.rank}`,
        chosenRank: pick.rank,
        chosenCost: pick.estCost ?? pick.cost?.summary?.totalCost ?? null,
        bestVariantDagHash: bestHash,
        chosenOrder: pick.chosenOrder || null,
        policy: { mode, eps, maxFactor, ucbC }
      }
    };
  }

  // Exploit selection
  if (mode === "epsilon" || mode === "best") {
    // best by estCost
    const min = all.reduce((a,b)=> ( (a.estCost??Infinity) <= (b.estCost??Infinity) ? a : b ));
    return {
      plan: min.normalizedPlan || min.plan,
      variantDagHash: min.variantDagHash,
      meta: {
        explored: false,
        selected: (min.rank === "best" ? "best" : `alt:${min.rank}`),
        chosenRank: min.rank,
        chosenCost: min.estCost ?? min.cost?.summary?.totalCost ?? null,
        bestVariantDagHash: bestHash,
        chosenOrder: min.chosenOrder || null,
        policy: { mode, eps, maxFactor, ucbC }
      }
    };
  }

  // UCB exploit (v3.9 bandit)
  const statsByHash = planVariantStatsMap || new Map();
  const u = selectUcb(all, statsByHash, { ucbC });
  const pick = u.pick;

  return {
    plan: pick.normalizedPlan || pick.plan,
    variantDagHash: pick.variantDagHash,
    meta: {
      explored: false,
      selected: (pick.rank === "best" ? "best" : `ucb:${pick.rank}`),
        chosenRank: pick.rank,
        chosenCost: pick.estCost ?? pick.cost?.summary?.totalCost ?? null,
        bestVariantDagHash: bestHash,
      chosenOrder: pick.chosenOrder || null,
      policy: { mode: "ucb", eps, maxFactor, ucbC },
      ucb: { reason: u.reason, baseline: u.baseline, totalRuns: u.totalRuns }
    }
  };
}

module.exports = { selectPlanVariantV4, annotateCandidatesWithHashes };
