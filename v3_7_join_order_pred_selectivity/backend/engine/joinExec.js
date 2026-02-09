const { getPath } = require("./exprEval");

function keyTuple(row, paths) {
  if (paths.length === 1) return getPath(row, paths[0]);
  return paths.map((p) => getPath(row, p)).join("||");
}

/**
 * Hash join for materialized inputs.
 *
 * join.params.on = [ [leftKey, rightKey], ... ] (keys are paths within left/right input rows)
 * join.params.build = "left" | "right" (default: smaller side)
 *
 * Output rows:
 *   { left: <leftRow>, right: <rightRow> }
 */
function hashJoin(leftRows, rightRows, joinParams = {}) {
  const on = joinParams.on || [];
  if (on.length === 0) return [];

  const leftKeys = on.map(([l, _r]) => l);
  const rightKeys = on.map(([_l, r]) => r);

  const buildSide = joinParams.build || (rightRows.length <= leftRows.length ? "right" : "left");

  const buildRows = buildSide === "left" ? leftRows : rightRows;
  const probeRows = buildSide === "left" ? rightRows : leftRows;

  const buildKeys = buildSide === "left" ? leftKeys : rightKeys;
  const probeKeys = buildSide === "left" ? rightKeys : leftKeys;

  const ht = new Map();
  for (const r of buildRows) {
    const k = keyTuple(r, buildKeys);
    if (k === undefined || k === null) continue;
    let bucket = ht.get(k);
    if (!bucket) { bucket = []; ht.set(k, bucket); }
    bucket.push(r);
  }

  const out = [];
  for (const r of probeRows) {
    const k = keyTuple(r, probeKeys);
    if (k === undefined || k === null) continue;
    const bucket = ht.get(k);
    if (!bucket) continue;

    for (const b of bucket) {
      if (buildSide === "left") out.push({ left: b, right: r });
      else out.push({ left: r, right: b });
    }
  }
  return out;
}

/**
 * Nested-loop join for tiny inner.
 * join.params.on is the same format as hashJoin.
 *
 * This is O(n*m) and is only appropriate for very small inner sides.
 */
function nestedLoopJoin(leftRows, rightRows, joinParams = {}) {
  const on = joinParams.on || [];
  if (on.length === 0) return [];

  const leftKeys = on.map(([l,_r])=>l);
  const rightKeys = on.map(([_l,r])=>r);

  const innerSide = joinParams.build || (rightRows.length <= leftRows.length ? "right" : "left");
  const innerRows = innerSide === "left" ? leftRows : rightRows;
  const outerRows = innerSide === "left" ? rightRows : leftRows;

  const innerKeys = innerSide === "left" ? leftKeys : rightKeys;
  const outerKeys = innerSide === "left" ? rightKeys : leftKeys;

  const out = [];
  for (const o of outerRows) {
    const ok = keyTuple(o, outerKeys);
    if (ok === undefined || ok === null) continue;

    for (const i of innerRows) {
      const ik = keyTuple(i, innerKeys);
      if (ik === undefined || ik === null) continue;
      if (ik !== ok) continue;

      if (innerSide === "left") out.push({ left: i, right: o });
      else out.push({ left: o, right: i });
    }
  }
  return out;
}

function execJoin(leftRows, rightRows, joinParams = {}) {
  const algo = joinParams.algorithm || "hash";
  if (algo === "nested_loop") return nestedLoopJoin(leftRows, rightRows, joinParams);
  return hashJoin(leftRows, rightRows, joinParams);
}

module.exports = { hashJoin, nestedLoopJoin, execJoin };
