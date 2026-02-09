// Mongo shell script (or run via node with mongodb driver) to create indexes for DAG optimizer loop.
//
// Collections:
// - run_dag_lineage (per runId)
// - run_dag_opt_reports (per runId)
// - dag_node_stats (EMA cost model keyed by dagHash,nodeId)

db.run_dag_lineage.createIndex({ runId: 1 }, { unique: true });
db.run_dag_lineage.createIndex({ dagHash: 1 });

db.run_dag_opt_reports.createIndex({ runId: 1 }, { unique: true });
db.run_dag_opt_reports.createIndex({ dagHash: 1 });

db.dag_node_stats.createIndex({ dagHash: 1, nodeId: 1 }, { unique: true });
db.dag_node_stats.createIndex({ updatedAt: -1 });

print("✅ DAG v3.1 indexes created");
