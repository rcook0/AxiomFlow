const { getPath } = require("./path");

function notNull(rule, row) {
  const v = getPath(row, rule.path);
  return v !== null && v !== undefined && v !== "";
}

function range(rule, row) {
  const v = getPath(row, rule.path);
  if (v === null || v === undefined) return true;
  if (rule.min !== undefined && v < rule.min) return false;
  if (rule.max !== undefined && v > rule.max) return false;
  return true;
}

function inSet(rule, row) {
  const v = getPath(row, rule.path);
  if (v === null || v === undefined) return true;
  return Array.isArray(rule.values) && rule.values.includes(v);
}

function regex(rule, row) {
  const v = getPath(row, rule.path);
  if (v === null || v === undefined) return true;
  const re = new RegExp(rule.pattern);
  return re.test(String(v));
}

function makeUniqueEvaluator(rule) {
  const seen = new Set();
  const cap = rule.cap || 500000;
  return (row) => {
    const key = rule.keys.map((k) => getPath(row, k)).join("|");
    if (seen.has(key)) return false;
    seen.add(key);
    if (seen.size > cap) return true;
    return true;
  };
}

function buildRuleEvaluator(rule) {
  switch (rule.type) {
    case "not_null": return (row) => notNull(rule, row);
    case "range": return (row) => range(rule, row);
    case "in_set": return (row) => inSet(rule, row);
    case "regex": return (row) => regex(rule, row);
    case "unique": {
      const fn = makeUniqueEvaluator(rule);
      return (row) => fn(row);
    }
    default:
      throw new Error(`Unsupported DQ rule type: ${rule.type}`);
  }
}

module.exports = { buildRuleEvaluator };
