/**
 * run-v3-8-optimizer-demo.js
 *
 * Demo:
 * - optimizer emits top-K join-order candidates when a single join component exists
 * - runtime exploration is controlled by env vars (documented in docs/EXPLORATION.md)
 *
 * Usage:
 *   node scripts/run-v3-8-optimizer-demo.js
 */
const { optimizeDagV3_8 } = require("../backend/engine/dagOptimizeV3_8");
const { predSigFromExpr, predGroupSigFromExpr, joinFilterSig, joinSigFromOnRef } = require("../backend/engine/costModelV3_8");

function mkPlan() {
  const whereOrders = ["and", ["eq", ["col","country"], "UK"], ["eq", ["col","status"], "ok"]];
  return {
    nodes: [
      { id: "scan_orders", op: "scan", params: { rel: "orders", estimatedRows: 100000 } },
      { id: "filter_orders", op: "filter", params: { where: whereOrders } },

      { id: "scan_customers", op: "scan", params: { rel: "customers", estimatedRows: 5000 } },
      { id: "scan_products", op: "scan", params: { rel: "products", estimatedRows: 50000 } },

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

  // Learned AND group selectivity (orders filter keeps 1%)
  const groupSig = predGroupSigFromExpr(plan.nodes.find(n=>n.id==="filter_orders").params.where);
  const predGroupStats = new Map([[groupSig, { emaSelectivity: 0.01 }]]);

  // Learned join->filter (not used here) example
  const predSig = predSigFromExpr(plan.nodes.find(n=>n.id==="filter_orders").params.where);
  const joinSig = joinSigFromOnRef(plan.nodes.find(n=>n.id==="j1").params.onRef);
  const segSig = joinFilterSig(joinSig, predSig);
  const joinFilterStats = new Map([[segSig, { emaSelectivity: 0.5 }]]);

  const opt = optimizeDagV3_8(plan, new Map(), new Map(), new Map(), predGroupStats, joinFilterStats, {
    joinOrder: { maxRelsForBushy: 7, topK: 3 },
    costModel: { defaultFilterSelectivity: 0.3 }
  });

  console.log(JSON.stringify({
    ok: opt.ok,
    totalCost: opt.cost?.summary?.totalCost,
    variants: (opt.variants?.candidates || []).map(v => ({
      rank: v.rank,
      totalCost: v.cost?.summary?.totalCost,
      chosenOrder: v.chosenOrder
    }))
  }, null, 2));
}

main();
