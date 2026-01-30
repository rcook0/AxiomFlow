const { isColExpr } = require("./projectMap");

function deepClone(x) {
  if (x === null || typeof x !== "object") return x;
  if (Array.isArray(x)) return x.map(deepClone);
  const o = {};
  for (const [k, v] of Object.entries(x)) o[k] = deepClone(v);
  return o;
}

function rewriteColsThroughProject(expr, projectMapResult, opts = {}) {
  const mode = opts.mode || "passthrough";
  const map = projectMapResult?.map || {};
  const issues = [];
  let changed = false;

  function rewrite(node) {
    if (node === null || node === undefined) return node;

    if (Array.isArray(node)) {
      const [op, ...args] = node;

      if (op === "col" && typeof args[0] === "string") {
        const colName = args[0];
        const mapped = map[colName];
        if (!mapped) return node;

        if (mode === "passthrough") {
          if (!isColExpr(mapped)) {
            issues.push({ col: colName, reason: "Mapped expression is not passthrough col()" });
            return node;
          }
          changed = true;
          return deepClone(mapped);
        }

        if (mode === "computed") {
          changed = true;
          return deepClone(mapped);
        }

        issues.push({ col: colName, reason: `Unknown rewrite mode: ${mode}` });
        return node;
      }

      const newArgs = args.map(rewrite);
      return [op, ...newArgs];
    }

    if (typeof node === "object") {
      const out = {};
      for (const [k, v] of Object.entries(node)) out[k] = rewrite(v);
      return out;
    }

    return node;
  }

  const rewritten = rewrite(expr);
  return { ok: issues.length === 0, rewritten, changed, issues };
}

module.exports = { rewriteColsThroughProject };
