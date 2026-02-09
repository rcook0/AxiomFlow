/**
 * run-v3-6-optimizer-demo.js
 *
 * Pure in-memory demo (no Mongo). Prints optimizer changes + estimated costs.
 *
 * Usage:
 *   node scripts/run-v3-6-optimizer-demo.js
 */
const { optimizeDagV3_6 } = require("../backend/engine/dagOptimizeV3_6");

function mkPlan() {
  return {
    nodes: [
      { id: "scan_orders", op: "scan", params: { rel: "orders", estimatedRows: 100000 } },
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
      { from: "scan_orders", to: "j1", port: "left" },
      { from: "scan_customers", to: "j1", port: "right" },
      { from: "j1", to: "j2", port: "left" },
      { from: "scan_products", to: "j2", port: "right" },
      { from: "j2", to: "sink", port: "in" }
    ]
  };
}

function main() {
  const plan = mkPlan();
  const opt = optimizeDagV3_6(plan, new Map(), new Map(), {
    joinOrder: { maxRelsForBushy: 7 },
    joinPolicy: { nestedLoopMaxInnerRows: 2000, nestedLoopMaxOuterRows: 80000 },
    costModel: { defaultFilterSelectivity: 0.3 }
  });

  console.log(JSON.stringify({
    ok: opt.ok,
    changes: opt.changes,
    cost: opt.cost?.summary,
    exampleNodeRows: opt.cost?.estRowsByNodeId
  }, null, 2));
}

main();
