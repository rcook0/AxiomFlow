const assert = require("assert");
const { selectPlanVariantV4, annotateCandidatesWithHashes } = require("../optimizer/selectPlanV4");

(function main(){
  const mk = (id) => ({
    version: "ir-dag-3.0-alpha",
    nodes: [{ id, op: "scan", params: { rel: id } }],
    edges: [],
    outputs: [id]
  });

  const opt = {
    ok: true,
    after: mk("best"),
    cost: { summary: { totalCost: 100 } },
    variants: {
      candidates: [
        { rank: 2, plan: mk("v2"), cost: { summary: { totalCost: 120 } } }
      ]
    }
  };

  const all = annotateCandidatesWithHashes([
    { rank: "best", plan: opt.after, cost: opt.cost, estCost: 100 },
    ...opt.variants.candidates.map(c => ({ rank: c.rank, plan: c.plan, cost: c.cost, estCost: c.cost.summary.totalCost }))
  ]);

  const bestHash = all.find(x=>x.rank==="best").variantDagHash;
  const h2 = all.find(x=>x.rank===2).variantDagHash;

  const st = new Map();
  // best: lower mean, very low variance (confident)
  st.set(bestHash, { runs: 100, emaWallMs: 100, emaVar: 1.0 });
  // v2: higher mean, but huge uncertainty (should win under Thompson when z is negative)
  st.set(h2, { runs: 0, emaWallMs: 110, emaVar: 10000.0 });

  // deterministic random: Math.random = 0.5 => randn() negative constant
  const prev = Math.random;
  Math.random = () => 0.5;

  const sel = selectPlanVariantV4("fam", opt, st, { mode: "thompson", eps: 0.0, thompsonSigmaScale: 1.0 });
  Math.random = prev;

  assert.equal(sel.meta.policy.mode, "thompson");
  assert.ok(sel.variantDagHash === h2, "expected Thompson to pick high-uncertainty variant with negative sample");
  console.log("OK: v4 Thompson selection smoke test");
})();
