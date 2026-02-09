/**
 * Bindings map base relations to paths inside a row object.
 *
 * Example:
 *  - leaf scan "orders" has bindings: { orders: "" }
 *  - join(orders, customers) output bindings:
 *      { orders: "left", customers: "right" }
 *  - join(joinAB, products) output bindings:
 *      { orders: "left.left", customers: "left.right", products: "right" }
 */

function prefixBindings(bindings, prefix) {
  const out = {};
  const p = prefix ? prefix + "." : "";
  for (const [rel, path] of Object.entries(bindings)) {
    out[rel] = path ? (p + path) : (prefix || "");
  }
  return out;
}

function mergeBindings(leftBindings, rightBindings) {
  return { ...leftBindings, ...rightBindings };
}

/**
 * Resolve a concrete column path (e.g. "left.right.id") to {rel, relPath}.
 * Uses longest-prefix match over binding paths.
 */
function resolvePathToRel(bindings, keyPath) {
  const candidates = [];
  for (const [rel, bindPath] of Object.entries(bindings)) {
    const fullPrefix = bindPath ? (bindPath + ".") : "";
    if (fullPrefix === "") {
      // leaf binding matches everything but is lowest specificity
      candidates.push({ rel, bindPath, fullPrefix, relPath: keyPath });
    } else if (keyPath === bindPath || keyPath.startsWith(fullPrefix)) {
      const relPath = keyPath === bindPath ? "" : keyPath.slice(fullPrefix.length);
      candidates.push({ rel, bindPath, fullPrefix, relPath });
    }
  }
  if (candidates.length === 0) return null;
  candidates.sort((a,b)=>b.fullPrefix.length - a.fullPrefix.length);
  return { rel: candidates[0].rel, relPath: candidates[0].relPath };
}

module.exports = { prefixBindings, mergeBindings, resolvePathToRel };