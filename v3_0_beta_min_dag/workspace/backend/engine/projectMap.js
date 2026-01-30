function isColExpr(expr) {
  return Array.isArray(expr) && expr.length === 2 && expr[0] === "col" && typeof expr[1] === "string";
}

function isPureExpr(expr) {
  if (expr == null) return true;
  if (typeof expr === "string" || typeof expr === "number" || typeof expr === "boolean") return true;
  if (Array.isArray(expr)) {
    const [op, ...args] = expr;
    const disallowed = new Set(["rand", "now", "uuid", "time", "seq"]);
    if (disallowed.has(op)) return false;
    return args.every(isPureExpr);
  }
  if (typeof expr === "object") {
    return Object.values(expr).every(isPureExpr);
  }
  return false;
}

function buildProjectMap(projectOp, opts = {}) {
  const passthroughOnly = !!opts.passthroughOnly;
  const allowComputed = !!opts.allowComputed;

  const out = { ok: true, map: {}, passthrough: {}, issues: [] };

  if (!projectOp || projectOp.type !== "project" || !projectOp.exprs || typeof projectOp.exprs !== "object") {
    out.ok = false;
    out.issues.push({ field: "*", reason: "Invalid project operator (missing exprs)" });
    return out;
  }

  for (const [field, expr] of Object.entries(projectOp.exprs)) {
    if (isColExpr(expr)) {
      out.map[field] = expr;
      out.passthrough[field] = expr[1];
      continue;
    }

    if (passthroughOnly) {
      out.ok = false;
      out.issues.push({ field, reason: "Non-passthrough expression not allowed in passthroughOnly mode" });
      continue;
    }

    if (allowComputed) {
      if (!isPureExpr(expr)) {
        out.ok = false;
        out.issues.push({ field, reason: "Expression is not pure (contains disallowed ops)" });
        continue;
      }
      out.map[field] = expr;
      continue;
    }

    out.ok = false;
    out.issues.push({ field, reason: "Computed expression not allowed (set allowComputed=true)" });
  }

  return out;
}

module.exports = { buildProjectMap, isColExpr, isPureExpr };
