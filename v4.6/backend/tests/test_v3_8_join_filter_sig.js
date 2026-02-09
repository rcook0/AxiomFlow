const assert = require("assert");
const { joinSigFromOnRef, predSigFromExpr, predGroupSigFromExpr, joinFilterSig, estimatePlanCostV3_8 } = require("../engine/costModelV3_8");

(function main(){
  const where = ["and", ["eq", ["col","status"], "ok"], ["eq", ["col","flag"], 1]];

  const plan = {
    nodes: [
      { id: "a", op: "scan", params: { rel: "A", estimatedRows: 10000 } },
      { id: "b", op: "scan", params: { rel: "B", estimatedRows: 10000 } },
      { id: "j", op: "join", params: { onRef: [{ left: { rel:"A", path:"id" }, right: { rel:"B", path:"aid" } }] } },
      { id: "f", op: "filter", params: { where } },
      { id: "sink", op: "sink", params: {} }
    ],
    edges: [
      { from: "a", to: "j", port: "left" },
      { from: "b", to: "j", port: "right" },
      { from: "j", to: "f", port: "in" },
      { from: "f", to: "sink", port: "in" }
    ]
  };

  const joinSig = joinSigFromOnRef(plan.nodes.find(n=>n.id==="j").params.onRef);
  const predSig = predSigFromExpr(where);
  const groupSig = predGroupSigFromExpr(where);
  const segSig = joinFilterSig(joinSig, (groupSig || predSig));

  // join produces ~min(10000,10000)=10000; segment selectivity 0.01 => 100
  const joinFilterStats = new Map([[segSig, { emaSelectivity: 0.01 }]]);

  // competing selectivity sources
  const predSigStats = new Map([[predSig, { emaSelectivity: 0.5 }]]);
  const predGroupStats = new Map([[groupSig, { emaSelectivity: 0.2 }]]); // segment should still win

  const cost = estimatePlanCostV3_8(plan, new Map(), new Map(), predSigStats, predGroupStats, joinFilterStats, { defaultFilterSelectivity: 0.3 });

  const outRows = cost.rowsByNode.get("f");
  assert.equal(outRows, 100, "filter out rows should use join_filter_stats selectivity (0.01)");

  console.log("OK: v3.8 join->filter segment selectivity test passed");
})();
