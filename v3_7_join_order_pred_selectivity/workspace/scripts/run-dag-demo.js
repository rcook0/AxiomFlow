/* eslint-disable no-console */
const { MongoClient } = require("mongodb");
const { executeDagBeta } = require("../backend/engine/executeDagBeta");

async function main() {
  const mongoUrl = process.env.MONGO_URL || "mongodb://localhost:27017";
  const dbName = process.env.MONGO_DB || "fullstackapp";

  const client = new MongoClient(mongoUrl);
  await client.connect();
  const db = client.db(dbName);

  // Minimal demo plan: scan -> filter -> project -> sink
  const plan = {
    version: "ir-dag-3.0-alpha",
    nodes: [
      { id: "scan_prices", op: "scan", params: { dataset: "prices" } },
      { id: "f1", op: "filter", params: { where: [">", ["col", "close"], 1000] } },
      { id: "p1", op: "project", params: { exprs: { symbol: ["col", "symbol"], close: ["col", "close"] } } },
      { id: "out", op: "sink", params: { collection: "prices_filtered" } }
    ],
    edges: [
      { from: "scan_prices", to: "f1", port: "in" },
      { from: "f1", to: "p1", port: "in" },
      { from: "p1", to: "out", port: "in" }
    ],
    outputs: ["out"]
  };

  const res = await executeDagBeta(plan, { db, runId: "demo" }, { sink: { batchSize: 500 } });
  console.log(JSON.stringify(res, null, 2));

  await client.close();
}

main().catch((e) => { console.error(e); process.exit(1); });
