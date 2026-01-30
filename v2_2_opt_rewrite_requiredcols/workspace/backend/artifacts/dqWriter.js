async function writeDQReport(db, runId, dqFinal) {
  const col = db.collection("run_dq_reports");
  const doc = {
    runId,
    createdAt: new Date(),
    summary: dqFinal.summary,
    samplesByRule: dqFinal.samplesByRule
  };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_dq_reports", runId };
}

module.exports = { writeDQReport };
