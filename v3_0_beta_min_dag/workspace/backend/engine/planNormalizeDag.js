const crypto = require("crypto");
const { validateDagPlan, canonicalizeDagPlan, stableStringify } = require("./irDagSchema");

function sha1(s) {
  return crypto.createHash("sha1").update(s).digest("hex");
}

function assignIdsIfMissing(plan) {
  const nodes = (plan.nodes || []).map((n, idx) => {
    if (n.id && typeof n.id === "string") return n;
    const base = { op: n.op, params: n.params || {}, _idx: idx };
    const id = "n_" + sha1(stableStringify(base)).slice(0, 10);
    return { ...n, id };
  });
  return { ...plan, nodes };
}

function normalizeDagPlan(plan, opts = {}) {
  const p = opts.assignIdsIfMissing ? assignIdsIfMissing(plan) : plan;

  const v = validateDagPlan(p);
  if (!v.ok) return { ok: false, errors: v.errors };

  const { canonical, dagHash } = canonicalizeDagPlan(p);
  return { ok: true, plan: canonical, dagHash };
}

module.exports = { normalizeDagPlan };
