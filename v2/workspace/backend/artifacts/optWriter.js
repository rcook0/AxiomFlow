async function writeOptReport(db, runId, opt) {
  const col = db.collection("run_opt_reports");
  const doc = { runId, createdAt: new Date(), ...opt };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_opt_reports", runId };
}

module.exports = { writeOptReport };
