async function writeStatsReport(db, runId, statsFinal) {
  const col = db.collection("run_stats_reports");
  const doc = {
    runId,
    createdAt: new Date(),
    ...statsFinal
  };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_stats_reports", runId };
}

module.exports = { writeStatsReport };
