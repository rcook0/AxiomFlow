const express = require("express");
const router = express.Router();

router.get("/:runId/lineage", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_lineage_ops").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No lineage for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
