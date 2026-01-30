function extractColsFromExpr(expr, out = new Set()) {
  if (expr == null) return out;

  if (Array.isArray(expr)) {
    const [op, ...args] = expr;
    if (op === "col" && typeof args[0] === "string") {
      out.add(args[0]);
      return out;
    }
    for (const a of args) extractColsFromExpr(a, out);
    return out;
  }

  if (typeof expr === "object") {
    for (const v of Object.values(expr)) extractColsFromExpr(v, out);
  }
  return out;
}

function extractFilterCols(op) {
  if (!op || op.type !== "filter") return new Set();
  return extractColsFromExpr(op.where, new Set());
}

function extractProjectCols(op) {
  const s = new Set();
  if (!op || op.type !== "project" || !op.exprs) return s;
  for (const v of Object.values(op.exprs)) extractColsFromExpr(v, s);
  return s;
}

function projectOutputCols(op) {
  const s = new Set();
  if (!op || op.type !== "project" || !op.exprs) return s;
  for (const k of Object.keys(op.exprs)) s.add(k);
  return s;
}

module.exports = { extractFilterCols, extractProjectCols, projectOutputCols, extractColsFromExpr };
