const express = require("express");
const router = express.Router();

router.get("/:runId/stats", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_stats_reports").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No stats report for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
