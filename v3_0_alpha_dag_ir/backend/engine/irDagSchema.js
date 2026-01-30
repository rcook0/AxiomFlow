const crypto = require("crypto");

function stableStringify(x) {
  if (x === null || typeof x !== "object") return JSON.stringify(x);
  if (Array.isArray(x)) return "[" + x.map(stableStringify).join(",") + "]";
  const keys = Object.keys(x).sort();
  const entries = keys.map((k) => JSON.stringify(k) + ":" + stableStringify(x[k]));
  return "{" + entries.join(",") + "}";
}

function sha256(s) {
  return crypto.createHash("sha256").update(s).digest("hex");
}

function validateDagPlan(plan) {
  const errors = [];

  if (!plan || typeof plan !== "object") {
    return { ok: false, errors: [{ code: "E_PLAN_TYPE", message: "Plan must be an object" }] };
  }

  if (plan.version !== "ir-dag-3.0-alpha") {
    errors.push({ code: "E_VERSION", message: "version must be 'ir-dag-3.0-alpha'" });
  }

  if (!Array.isArray(plan.nodes) || plan.nodes.length === 0) {
    errors.push({ code: "E_NODES", message: "nodes must be a non-empty array" });
  }

  if (!Array.isArray(plan.edges)) {
    errors.push({ code: "E_EDGES", message: "edges must be an array" });
  }

  if (!Array.isArray(plan.outputs) || plan.outputs.length === 0) {
    errors.push({ code: "E_OUTPUTS", message: "outputs must be a non-empty array" });
  }

  const nodeById = new Map();
  if (Array.isArray(plan.nodes)) {
    for (const n of plan.nodes) {
      if (!n || typeof n !== "object") {
        errors.push({ code: "E_NODE_TYPE", message: "node must be an object" });
        continue;
      }
      if (!n.id || typeof n.id !== "string") {
        errors.push({ code: "E_NODE_ID", message: "node.id must be a string" });
        continue;
      }
      if (nodeById.has(n.id)) {
        errors.push({ code: "E_NODE_ID_DUP", message: `duplicate node id: ${n.id}` });
        continue;
      }
      if (!n.op || typeof n.op !== "string") {
        errors.push({ code: "E_NODE_OP", message: `node.op must be a string (node ${n.id})` });
      }
      if (n.params !== undefined && (typeof n.params !== "object" || n.params === null || Array.isArray(n.params))) {
        errors.push({ code: "E_NODE_PARAMS", message: `node.params must be an object if present (node ${n.id})` });
      }
      nodeById.set(n.id, n);
    }
  }

  if (Array.isArray(plan.edges)) {
    for (const e of plan.edges) {
      if (!e || typeof e !== "object") {
        errors.push({ code: "E_EDGE_TYPE", message: "edge must be an object" });
        continue;
      }
      if (!e.from || typeof e.from !== "string" || !nodeById.has(e.from)) {
        errors.push({ code: "E_EDGE_FROM", message: `edge.from must reference an existing node (${e.from})` });
      }
      if (!e.to || typeof e.to !== "string" || !nodeById.has(e.to)) {
        errors.push({ code: "E_EDGE_TO", message: `edge.to must reference an existing node (${e.to})` });
      }
      if (e.port !== undefined && typeof e.port !== "string") {
        errors.push({ code: "E_EDGE_PORT", message: "edge.port must be a string if present" });
      }
    }
  }

  if (Array.isArray(plan.outputs)) {
    for (const o of plan.outputs) {
      if (typeof o !== "string" || !nodeById.has(o)) {
        errors.push({ code: "E_OUTPUT_REF", message: `output must reference an existing node (${o})` });
      }
    }
  }

  // DAG cycle detection (Kahn)
  if (errors.length === 0 && Array.isArray(plan.nodes) && Array.isArray(plan.edges)) {
    const indeg = new Map();
    const adj = new Map();
    for (const n of plan.nodes) {
      indeg.set(n.id, 0);
      adj.set(n.id, []);
    }
    for (const e of plan.edges) {
      const from = e.from, to = e.to;
      if (!adj.has(from) || !indeg.has(to)) continue;
      adj.get(from).push(to);
      indeg.set(to, (indeg.get(to) || 0) + 1);
    }
    const q = [];
    for (const [id, d] of indeg.entries()) if (d === 0) q.push(id);

    let visited = 0;
    while (q.length) {
      const id = q.shift();
      visited++;
      for (const nxt of adj.get(id) || []) {
        indeg.set(nxt, indeg.get(nxt) - 1);
        if (indeg.get(nxt) === 0) q.push(nxt);
      }
    }

    if (visited !== plan.nodes.length) {
      errors.push({ code: "E_CYCLE", message: "graph contains a cycle (not a DAG)" });
    }
  }

  return { ok: errors.length === 0, errors };
}

function canonicalizeDagPlan(plan) {
  const nodes = plan.nodes.map((n) => {
    const params = n.params && typeof n.params === "object" && !Array.isArray(n.params) ? n.params : {};
    let canonParams = params;

    if (n.op === "project" && params.exprs && typeof params.exprs === "object" && !Array.isArray(params.exprs)) {
      const keys = Object.keys(params.exprs).sort();
      const exprs = {};
      for (const k of keys) exprs[k] = params.exprs[k];
      canonParams = { ...params, exprs };
    }

    if (n.op === "groupBy" && Array.isArray(params.keys)) {
      canonParams = { ...params, keys: params.keys.map(String) };
    }

    return { id: n.id, op: n.op, params: canonParams };
  });

  nodes.sort((a, b) => a.id.localeCompare(b.id));

  const edges = (plan.edges || []).map((e) => ({
    from: e.from,
    to: e.to,
    port: e.port ? e.port : "in"
  }));

  edges.sort((a, b) => {
    const t = a.to.localeCompare(b.to);
    if (t) return t;
    const p = a.port.localeCompare(b.port);
    if (p) return p;
    return a.from.localeCompare(b.from);
  });

  const outputs = plan.outputs.slice().map(String).sort();

  const canonical = { version: "ir-dag-3.0-alpha", nodes, edges, outputs };
  const dagHash = sha256(stableStringify(canonical));
  return { canonical, dagHash };
}

module.exports = { validateDagPlan, canonicalizeDagPlan, stableStringify };
