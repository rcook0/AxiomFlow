const assert = require("assert");
const { predSigFromExpr, estimatePlanCostV3_7 } = require("../engine/costModelV3_7");

(function main(){
  const a = ["and", ["eq", ["col","x"], 1], ["eq", ["col","y"], 2]];
  const b = ["and", ["eq", ["col","y"], 2], ["eq", 1, ["col","x"]]]; // reordered AND + reordered EQ

  const sa = predSigFromExpr(a);
  const sb = predSigFromExpr(b);
  assert.equal(sa, sb, "predSig should be stable under commutative rewrites");

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

  const predSigStats = new Map([[sa, { emaSelectivity: 0.1 }]]);
  const cost = estimatePlanCostV3_7(plan, new Map(), new Map(), predSigStats, { defaultFilterSelectivity: 0.3 });

  const outRows = cost.rowsByNode.get("f");
  assert.equal(outRows, 100, "filter out rows should use learned selectivity (0.1)");

  console.log("OK: v3.7 predSig + cost model smoke test passed");
})();
