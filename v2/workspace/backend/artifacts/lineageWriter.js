async function writeLineageOps(db, runId, lineageDoc) {
  const col = db.collection("run_lineage_ops");
  await col.updateOne({ runId }, { $set: lineageDoc }, { upsert: true });
  return { collection: "run_lineage_ops", runId };
}

module.exports = { writeLineageOps };
