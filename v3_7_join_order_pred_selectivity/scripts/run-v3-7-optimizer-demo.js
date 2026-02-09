/**
 * run-v3-7-optimizer-demo.js
 *
 * Pure in-memory demo showing join ordering affected by learned predicate selectivity.
 *
 * Usage:
 *   node scripts/run-v3-7-optimizer-demo.js
 */
const { optimizeDagV3_7 } = require("../backend/engine/dagOptimizeV3_7");
const { predSigFromExpr } = require("../backend/engine/costModelV3_7");

function mkPlan() {
  // orders filtered heavily, products lightly: should encourage joining orders->customers first (or similar),
  // depending on join predicates and base sizes.
  const whereOrders = ["and", ["eq", ["col","country"], "UK"]];
  return {
    nodes: [
      { id: "scan_orders", op: "scan", params: { rel: "orders", estimatedRows: 100000 } },
      { id: "filter_orders", op: "filter", params: { where: whereOrders } },

      { id: "scan_customers", op: "scan", params: { rel: "customers", estimatedRows: 1000 } },
      { id: "scan_products", op: "scan", params: { rel: "products", estimatedRows: 5000 } },

      { id: "j1", op: "join", params: { onRef: [
        { left: { rel: "orders", path: "customerId" }, right: { rel: "customers", path: "_id" } }
      ]}},
      { id: "j2", op: "join", params: { onRef: [
        { left: { rel: "orders", path: "productId" }, right: { rel: "products", path: "_id" } }
      ]}},

      { id: "sink", op: "sink", params: {} }
    ],
    edges: [
      { from: "scan_orders", to: "filter_orders", port: "in" },
      { from: "filter_orders", to: "j1", port: "left" },
      { from: "scan_customers", to: "j1", port: "right" },
      { from: "j1", to: "j2", port: "left" },
      { from: "scan_products", to: "j2", port: "right" },
      { from: "j2", to: "sink", port: "in" }
    ]
  };
}

function main() {
  const plan = mkPlan();

  // Learned selectivity: orders filter keeps 1%
  const sig = predSigFromExpr(plan.nodes.find(n=>n.id==="filter_orders").params.where);
  const predSigStats = new Map([[sig, { emaSelectivity: 0.01 }]]);

  const opt = optimizeDagV3_7(plan, new Map(), new Map(), predSigStats, {
    joinOrder: { maxRelsForBushy: 7 },
    costModel: { defaultFilterSelectivity: 0.3 }
  });

  console.log(JSON.stringify({
    ok: opt.ok,
    joinOrderChanges: opt.changes.filter(c=>String(c.rule).startsWith("JOIN_ORDER")),
    cost: opt.cost?.summary,
    estRows: opt.cost?.estRowsByNodeId
  }, null, 2));
}

main();
