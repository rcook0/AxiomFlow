const assert = require("assert");
const { rewriteJoinTreeV3_6 } = require("../engine/joinOrderV3_6");

function mkPlan() {
  // scans
  const nodes = [
    { id: "scan_orders", op: "scan", params: { rel: "orders" } },
    { id: "scan_customers", op: "scan", params: { rel: "customers" } },
    { id: "scan_products", op: "scan", params: { rel: "products" } },

    // joins (left-deep baseline)
    { id: "j1", op: "join", params: { onRef: [
      { left: { rel: "orders", path: "customerId" }, right: { rel: "customers", path: "_id" } }
    ]}},
    { id: "j2", op: "join", params: { onRef: [
      { left: { rel: "orders", path: "productId" }, right: { rel: "products", path: "_id" } }
    ]}},

    { id: "sink", op: "sink", params: {} }
  ];

  const edges = [
    { from: "scan_orders", to: "j1", port: "left" },
    { from: "scan_customers", to: "j1", port: "right" },
    { from: "j1", to: "j2", port: "left" },
    { from: "scan_products", to: "j2", port: "right" },
    { from: "j2", to: "sink", port: "in" }
  ];

  return { nodes, edges };
}

(function main(){
  const plan = mkPlan();
  const baseRelSizes = new Map([["orders", 100000], ["customers", 1000], ["products", 5000]]);
  const joinSigStats = new Map(); // empty for this test

  const res = rewriteJoinTreeV3_6(plan, "j2", baseRelSizes, joinSigStats, { maxRelsForBushy: 7 });
  assert.equal(res.ok, true, "rewrite should succeed");
  const p2 = res.plan;

  // Should still have one sink edge
  const sinkEdges = p2.edges.filter(e=>e.to==="sink");
  assert.equal(sinkEdges.length, 1);

  // Should have exactly 2 joins in the component (same as baseline)
  const joins = p2.nodes.filter(n=>n.op==="join");
  assert.equal(joins.length, 2);

  // Every join should have algorithm/build set
  for (const j of joins) {
    assert.ok(j.params.algorithm, "algorithm missing");
    assert.ok(j.params.build, "build missing");
    assert.ok(j.params.on && j.params.on.length > 0, "compiled on missing");
  }

  console.log("OK: v3.6 join ordering rewrite smoke test passed");
})();
