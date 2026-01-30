const { buildRuleEvaluator } = require("./rules");

class DQEngine {
  constructor(cfg) {
    this.mode = cfg?.mode || "flag";
    this.sampleLimit = cfg?.sampleLimit ?? 50;

    this.rules = (cfg?.rules || []).map((r) => ({
      ...r,
      id: r.id || `${r.type}:${r.path || (r.keys || []).join(",")}`
    }));

    this.compiled = this.rules.map((r) => ({ rule: r, eval: buildRuleEvaluator(r) }));

    this.totalRows = 0;
    this.ruleStats = Object.fromEntries(
      this.rules.map((r) => [r.id, { checked: 0, failed: 0, samples: [] }])
    );
  }

  check(row) {
    this.totalRows++;
    const violations = [];

    for (const { rule, eval: fn } of this.compiled) {
      this.ruleStats[rule.id].checked++;
      const ok = fn(row);
      if (!ok) {
        this.ruleStats[rule.id].failed++;
        violations.push({ ruleId: rule.id, type: rule.type, path: rule.path, keys: rule.keys });

        const bucket = this.ruleStats[rule.id].samples;
        if (bucket.length < this.sampleLimit) {
          bucket.push({ rowPreview: row?.data ?? {}, meta: row?.meta ?? {} });
        }
      }
    }

    const passed = violations.length === 0;
    if (!row.meta) row.meta = {};
    row.meta.dq = { passed, violations };

    if (this.mode === "flag") return { action: "keep", row };
    if (this.mode === "reject") return { action: passed ? "keep" : "drop", row };
    if (this.mode === "quarantine") return { action: passed ? "keep" : "quarantine", row };
    return { action: "keep", row };
  }

  finalize() {
    const summary = {
      mode: this.mode,
      totalRows: this.totalRows,
      rules: this.rules.map((r) => ({
        id: r.id,
        type: r.type,
        path: r.path,
        keys: r.keys,
        checked: this.ruleStats[r.id].checked,
        failed: this.ruleStats[r.id].failed,
        sampleCount: this.ruleStats[r.id].samples.length
      }))
    };

    const samplesByRule = Object.fromEntries(this.rules.map((r) => [r.id, this.ruleStats[r.id].samples]));
    return { summary, samplesByRule };
  }
}

module.exports = { DQEngine };
