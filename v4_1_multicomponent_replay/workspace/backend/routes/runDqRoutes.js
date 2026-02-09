const express = require("express");
const router = express.Router();

router.get("/:runId/dq", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_dq_reports").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No DQ report for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
