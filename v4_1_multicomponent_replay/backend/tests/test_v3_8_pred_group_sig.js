const assert = require("assert");
const { predGroupSigFromExpr, predSigFromExpr, estimatePlanCostV3_8 } = require("../engine/costModelV3_8");

(function main(){
  const a = ["and", ["eq", ["col","x"], 1], ["eq", ["col","y"], 2]];
  const b = ["and", ["eq", ["col","y"], 2], ["eq", 1, ["col","x"]]];

  const ga = predGroupSigFromExpr(a);
  const gb = predGroupSigFromExpr(b);
  assert.ok(ga, "predGroupSig should exist for AND of >=2 atoms");
  assert.equal(ga, gb, "predGroupSig should be stable under commutative rewrites");

  const pa = predSigFromExpr(a);
  const pb = predSigFromExpr(b);
  assert.equal(pa, pb, "predSig should be stable too");

  const plan = {
    nodes: [
      { id: "s", op: "scan", params: { rel: "t", estimatedRows: 1000 } },
      { id: "f", op: "filter", params: { where: a } },
      { id: "sink", op: "sink", params: {} }
    ],
    edges: [
      { from: "s", to: "f", port: "in" },
      { from: "f", to: "sink", port: "in" }
    ]
  };

  const predSigStats = new Map([[pa, { emaSelectivity: 0.5 }]]);
  const predGroupStats = new Map([[ga, { emaSelectivity: 0.1 }]]); // should win
  const cost = estimatePlanCostV3_8(plan, new Map(), new Map(), predSigStats, predGroupStats, new Map(), { defaultFilterSelectivity: 0.3 });

  const outRows = cost.rowsByNode.get("f");
  assert.equal(outRows, 100, "filter out rows should use pred_group_stats selectivity (0.1)");

  console.log("OK: v3.8 predGroupSig + cost model precedence test passed");
})();
