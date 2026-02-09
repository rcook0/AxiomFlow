function getPath(obj, path) {
  if (!path) return undefined;
  const parts = String(path).split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

function truthy(x) { return !!x; }

/**
 * Evaluate an expression AST against a row/document.
 *
 * Supported forms:
 * - ["col", "a.b"]                     -> field lookup (dot path)
 * - ["lit", 123]                       -> literal
 * - ["and", e1, e2, ...]
 * - ["or", e1, e2, ...]
 * - ["not", e]
 * - ["=", a, b], ["!=", a, b]
 * - [">", a, b], [">=", a, b], ["<", a, b], ["<=", a, b]
 * - ["in", a, [..]]                    -> membership
 * - ["exists", ["col","x"]]            -> value !== undefined && value !== null
 * - ["add"|"sub"|"mul"|"div", a, b]     -> numeric ops (div guarded)
 */
function evalExpr(expr, row) {
  if (expr === null || expr === undefined) return expr;

  if (typeof expr === "number" || typeof expr === "string" || typeof expr === "boolean") return expr;

  if (Array.isArray(expr)) {
    const [op, ...args] = expr;

    if (op === "col") return getPath(row, args[0]);
    if (op === "lit") return args[0];

    if (op === "and") return args.every((a) => truthy(evalExpr(a, row)));
    if (op === "or") return args.some((a) => truthy(evalExpr(a, row)));
    if (op === "not") return !truthy(evalExpr(args[0], row));

    if (op === "=") return evalExpr(args[0], row) === evalExpr(args[1], row);
    if (op === "!=") return evalExpr(args[0], row) !== evalExpr(args[1], row);

    if (op === ">") return evalExpr(args[0], row) > evalExpr(args[1], row);
    if (op === ">=") return evalExpr(args[0], row) >= evalExpr(args[1], row);
    if (op === "<") return evalExpr(args[0], row) < evalExpr(args[1], row);
    if (op === "<=") return evalExpr(args[0], row) <= evalExpr(args[1], row);

    if (op === "in") {
      const v = evalExpr(args[0], row);
      const arr = args[1];
      if (!Array.isArray(arr)) return false;
      return arr.includes(v);
    }

    if (op === "exists") {
      const v = evalExpr(args[0], row);
      return v !== undefined && v !== null;
    }

    if (op === "add") return Number(evalExpr(args[0], row)) + Number(evalExpr(args[1], row));
    if (op === "sub") return Number(evalExpr(args[0], row)) - Number(evalExpr(args[1], row));
    if (op === "mul") return Number(evalExpr(args[0], row)) * Number(evalExpr(args[1], row));
    if (op === "div") {
      const den = Number(evalExpr(args[1], row));
      if (den === 0) return null;
      return Number(evalExpr(args[0], row)) / den;
    }

    throw new Error(`Unsupported expr op: ${op}`);
  }

  if (typeof expr === "object") {
    // Treat objects as JSON literals
    return expr;
  }

  return expr;
}

module.exports = { evalExpr };
