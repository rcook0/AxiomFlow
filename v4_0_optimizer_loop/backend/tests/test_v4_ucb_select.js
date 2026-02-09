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
        { rank: 2, plan: mk("v2"), cost: { summary: { totalCost: 90 } } },
        { rank: 3, plan: mk("v3"), cost: { summary: { totalCost: 80 } } }
      ]
    }
  };

  const all = annotateCandidatesWithHashes([
    { rank: "best", plan: opt.after, cost: opt.cost, estCost: 100 },
    ...opt.variants.candidates.map(c => ({ rank: c.rank, plan: c.plan, cost: c.cost, estCost: c.cost.summary.totalCost }))
  ]);

  assert.ok(all.length >= 3, "expected candidate hashes");
  const bestHash = all.find(x=>x.rank==="best").variantDagHash;
  const h2 = all.find(x=>x.rank===2).variantDagHash;
  const h3 = all.find(x=>x.rank===3).variantDagHash;

  const st = new Map();
  st.set(bestHash, { runs: 10, emaWallMs: 120 });
  st.set(h2, { runs: 10, emaWallMs: 110 });
  st.set(h3, { runs: 1, emaWallMs: 50 });

  const sel = selectPlanVariantV4("fam", opt, st, { mode: "ucb", eps: 0.0, ucbC: 0.5 });
  assert.equal(sel.meta.policy.mode, "ucb");
  assert.ok(sel.variantDagHash === h3, "expected UCB to pick fastest variant");

  console.log("OK: v4 UCB selection smoke test passed");
})();
