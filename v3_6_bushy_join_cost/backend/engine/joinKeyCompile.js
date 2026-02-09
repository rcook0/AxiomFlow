const { prefixBindings, mergeBindings } = require("./bindings");

/**
 * Compile onRef predicates into concrete join.params.on given subtree bindings.
 *
 * onRef item shape:
 *  { left: { rel: "orders", path: "customerId" }, right: { rel: "customers", path: "_id" } }
 *
 * Returns:
 *  { on: [ [leftKeyPath, rightKeyPath], ... ], bindingsOut }
 */
function compileJoinOn(onRef, leftBindings, rightBindings) {
  const lb = leftBindings || {};
  const rb = rightBindings || {};

  function pathFor(bindings, rel, relPath) {
    const bind = bindings[rel];
    if (bind === undefined) return null;
    if (!bind) return relPath || "";
    if (!relPath) return bind;
    return bind + "." + relPath;
  }

  const on = [];
  for (const p of (onRef || [])) {
    const l = p.left, r = p.right;
    const lk = pathFor(lb, l.rel, l.path || "");
    const rk = pathFor(rb, r.rel, r.path || "");
    if (lk == null || rk == null) {
      throw new Error("compileJoinOn: predicate references rel not present in subtree");
    }
    on.push([lk, rk]);
  }

  const outBindings = mergeBindings(prefixBindings(lb, "left"), prefixBindings(rb, "right"));
  return { on, bindingsOut: outBindings };
}

module.exports = { compileJoinOn };