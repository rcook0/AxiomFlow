/**
 * db-indexes-v3-5.js
 *
 * Creates MongoDB indexes for DAG lineage + optimizer artifacts, including v3.5 join signature stats.
 *
 * Usage:
 *   node scripts/db-indexes-v3-5.js mongodb://localhost:27017 axiomflow
 */
const { MongoClient } = require("mongodb");

async function main() {
  const uri = process.argv[2] || "mongodb://localhost:27017";
  const dbName = process.argv[3] || "axiomflow";
  const client = new MongoClient(uri);
  await client.connect();
  const db = client.db(dbName);

  await db.collection("run_dag_lineage").createIndex({ runId: 1 }, { unique: true });
  await db.collection("run_dag_opt_reports").createIndex({ runId: 1 }, { unique: true });

  await db.collection("dag_node_stats").createIndex({ dagHash: 1, nodeId: 1 }, { unique: true });
  await db.collection("join_sig_stats").createIndex({ joinSig: 1 }, { unique: true });

  console.log("Indexes created/ensured:", { dbName });
  await client.close();
}

main().catch((e) => { console.error(e); process.exit(1); });