// Run with:
//   mongosh "mongodb://localhost:27017/fullstack_app" scripts/db-indexes.js

db = db.getSiblingDB("fullstack_app");

// Results

db.run_results.createIndex({ runId: 1, rowId: 1 }, { unique: true });
db.run_results.createIndex({ runId: 1 });
db.run_results_meta.createIndex({ runId: 1 }, { unique: true });

// Artifacts

db.run_dq_reports.createIndex({ runId: 1 }, { unique: true });
db.run_stats_reports.createIndex({ runId: 1 }, { unique: true });
db.run_lineage_ops.createIndex({ runId: 1 }, { unique: true });
db.run_opt_reports.createIndex({ runId: 1 }, { unique: true });

print("✅ Indexes created");
