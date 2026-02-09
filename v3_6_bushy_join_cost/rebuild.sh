#!/usr/bin/env bash
set -euo pipefail

# Rebuild script for the FullStack workspace (backend-focused) with:
# - Async job artifacts (results, DQ, stats, lineage)
# - IR normalization + optimizer v1
# - v2 building blocks: projectMap + exprRewrite
#
# Usage:
#   ./rebuild.sh [TARGET_DIR]
# Default target dir: ./fullstack_workspace

TARGET_DIR="${1:-fullstack_workspace}"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/${TARGET_DIR}"

if [ -e "$PROJECT_ROOT" ]; then
  echo "ERROR: Target exists: $PROJECT_ROOT"
  echo "Move/delete it or choose another target dir."
  exit 1
fi

mkdir -p "$PROJECT_ROOT"

# -----------------------------
# Directory tree
# -----------------------------
mkdir -p \
  "$PROJECT_ROOT/backend/artifacts" \
  "$PROJECT_ROOT/backend/db" \
  "$PROJECT_ROOT/backend/dq" \
  "$PROJECT_ROOT/backend/engine" \
  "$PROJECT_ROOT/backend/lineage" \
  "$PROJECT_ROOT/backend/middleware" \
  "$PROJECT_ROOT/backend/routes" \
  "$PROJECT_ROOT/backend/stats" \
  "$PROJECT_ROOT/scripts" \
  "$PROJECT_ROOT/docs"

# -----------------------------
# Backend: engine (normalization + optimizer)
# -----------------------------
cat > "$PROJECT_ROOT/backend/engine/planNormalize.js" <<'EOT'
const crypto = require("crypto");

function stableStringify(x) {
  if (x === null || typeof x !== "object") return JSON.stringify(x);
  if (Array.isArray(x)) return "[" + x.map(stableStringify).join(",") + "]";

  const keys = Object.keys(x).sort();
  const entries = keys.map((k) => JSON.stringify(k) + ":" + stableStringify(x[k]));
  return "{" + entries.join(",") + "}";
}

function canonicalizeOp(op) {
  const out = { ...op };

  if (out.op && !out.type) out.type = out.op;

  if (out.type === "project" && out.exprs && typeof out.exprs === "object") {
    out.exprs = Object.fromEntries(Object.keys(out.exprs).sort().map((k) => [k, out.exprs[k]]));
  }

  if (out.type === "sort" && Array.isArray(out.keys)) {
    out.keys = out.keys.map(String);
  }

  if (out.type === "groupBy") {
    if (Array.isArray(out.keys)) out.keys = out.keys.map(String);
    if (out.aggs && typeof out.aggs === "object") {
      out.aggs = Object.fromEntries(Object.keys(out.aggs).sort().map((k) => [k, out.aggs[k]]));
    }
  }

  delete out.opId;
  delete out.debug;
  return out;
}

function opIdFor(normalizedOp, index) {
  const h = crypto.createHash("sha1");
  h.update(stableStringify(normalizedOp));
  h.update("|" + String(index));
  return "op_" + h.digest("hex").slice(0, 10);
}

function normalizePlan(plan) {
  const pipeline = plan?.pipeline || plan?.pipeline?.pipeline || plan?.ops || plan?.pipeline;
  const ops = Array.isArray(pipeline) ? pipeline : (plan?.pipeline && Array.isArray(plan.pipeline) ? plan.pipeline : []);

  const normalizedOps = ops.map(canonicalizeOp);
  const opIds = normalizedOps.map((op, i) => opIdFor(op, i));

  const nodes = normalizedOps.map((op, i) => ({
    opId: opIds[i],
    type: op.type || op.op || "unknown",
    params: (() => {
      const { type, op: opField, ...rest } = op;
      return rest;
    })()
  }));

  const edges = [];
  for (let i = 0; i < opIds.length - 1; i++) edges.push({ from: opIds[i], to: opIds[i + 1] });

  const normalizedPlan = { pipeline: normalizedOps.map((op, i) => ({ opId: opIds[i], ...op })) };
  const planHash = crypto.createHash("sha256").update(stableStringify(normalizedPlan)).digest("hex");

  return { normalizedPlan, opIds, planHash, graph: { nodes, edges } };
}

module.exports = { normalizePlan, stableStringify };
EOT

cat > "$PROJECT_ROOT/backend/engine/irFields.js" <<'EOT'
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
EOT

cat > "$PROJECT_ROOT/backend/engine/irOptimize.js" <<'EOT'
const { extractFilterCols, projectOutputCols } = require("./irFields");

const BARRIERS = new Set(["sort", "limit", "distinct", "groupBy", "join", "union", "intersect", "except", "unnest"]);

function isBarrier(op) {
  return BARRIERS.has(op.type);
}

function clonePlan(plan) {
  return { pipeline: plan.pipeline.map((x) => ({ ...x })) };
}

function canSwapProjectFilter(projectOp, filterOp) {
  const filterCols = extractFilterCols(filterOp);
  const projOut = projectOutputCols(projectOp);
  for (const c of filterCols) {
    if (!projOut.has(c)) return false;
  }
  return true;
}

function optimizePipeline(plan) {
  const before = clonePlan(plan);
  const ops = before.pipeline;
  const changes = [];

  // Pass 1: bubble filters left across safe unary ops (conservative)
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type !== "filter") continue;

    let j = i;
    while (j > 0) {
      const prev = ops[j - 1];
      const cur = ops[j];

      if (isBarrier(prev)) break;

      if (prev.type === "project") {
        if (!canSwapProjectFilter(prev, cur)) break;
      }

      ops[j] = prev;
      ops[j - 1] = cur;
      changes.push({ rule: "FILTER_PUSHDOWN", from: j, to: j - 1, detail: { swappedWith: prev.type } });
      j--;
    }
  }

  // Pass 2: merge adjacent filters into one
  for (let i = 0; i < ops.length - 1; ) {
    if (ops[i].type === "filter" && ops[i + 1].type === "filter") {
      const a = ops[i], b = ops[i + 1];
      ops.splice(i, 2, { ...a, where: ["and", a.where, b.where] });
      changes.push({ rule: "MERGE_FILTERS", at: i });
      continue;
    }
    i++;
  }

  // Pass 3: remove empty projects
  for (let i = 0; i < ops.length; i++) {
    if (ops[i].type === "project" && ops[i].exprs && Object.keys(ops[i].exprs).length === 0) {
      ops.splice(i, 1);
      changes.push({ rule: "REMOVE_EMPTY_PROJECT", at: i });
      i--;
    }
  }

  return { plan: before, changes };
}

module.exports = { optimizePipeline };
EOT

cat > "$PROJECT_ROOT/backend/engine/projectMap.js" <<'EOT'
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
EOT

cat > "$PROJECT_ROOT/backend/engine/exprRewrite.js" <<'EOT'
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
EOT

# -----------------------------
# Backend: lineage
# -----------------------------
cat > "$PROJECT_ROOT/backend/lineage/lineageCollector.js" <<'EOT'
class LineageCollector {
  constructor({ runId, plan, engineVersion = "lineage-v1" }) {
    this.runId = runId;
    this.engineVersion = engineVersion;
    this.plan = plan;

    this.nodes = [];
    this.edges = [];
    this.opStats = {};
    this.timeline = [];
  }

  addNode(opId, type, params) {
    this.nodes.push({ opId, type, params: params || {} });
    if (!this.opStats[opId]) this.opStats[opId] = { rowsIn: 0, rowsOut: 0, ms: 0, startedAt: null };
  }

  addEdge(from, to) {
    this.edges.push({ from, to });
  }

  start(opId) {
    const st = this.opStats[opId] || (this.opStats[opId] = { rowsIn: 0, rowsOut: 0, ms: 0, startedAt: null });
    st.startedAt = Date.now();
    this.timeline.push({ opId, event: "start", ts: new Date() });
  }

  end(opId) {
    const st = this.opStats[opId];
    if (st && st.startedAt) {
      const ms = Date.now() - st.startedAt;
      st.ms += ms;
      st.startedAt = null;
      this.timeline.push({ opId, event: "end", ts: new Date(), ms });
    }
  }

  incRowsIn(opId, n = 1) {
    const st = this.opStats[opId] || (this.opStats[opId] = { rowsIn: 0, rowsOut: 0, ms: 0, startedAt: null });
    st.rowsIn += n;
  }

  incRowsOut(opId, n = 1) {
    const st = this.opStats[opId] || (this.opStats[opId] = { rowsIn: 0, rowsOut: 0, ms: 0, startedAt: null });
    st.rowsOut += n;
  }

  finalize() {
    return {
      runId: this.runId,
      createdAt: new Date(),
      engineVersion: this.engineVersion,
      plan: this.plan,
      graph: { nodes: this.nodes, edges: this.edges },
      stats: this.opStats,
      timeline: this.timeline
    };
  }
}

module.exports = { LineageCollector };
EOT

cat > "$PROJECT_ROOT/backend/lineage/instrumentStream.js" <<'EOT'
function instrumentUnaryStream({ opId, collector, upstream, mapRow }) {
  let started = false;

  return {
    async next() {
      if (!started) {
        started = true;
        collector.start(opId);
      }

      while (true) {
        const inRow = await upstream.next();
        if (!inRow) {
          collector.end(opId);
          return null;
        }

        collector.incRowsIn(opId, 1);

        const outRow = mapRow ? mapRow(inRow) : inRow;
        if (outRow == null) continue;

        collector.incRowsOut(opId, 1);
        return outRow;
      }
    }
  };
}

module.exports = { instrumentUnaryStream };
EOT

# -----------------------------
# Backend: DQ
# -----------------------------
cat > "$PROJECT_ROOT/backend/dq/path.js" <<'EOT'
function getPath(obj, path) {
  const parts = String(path).split(".");
  let cur = obj;
  for (const p of parts) {
    if (cur == null) return undefined;
    cur = cur[p];
  }
  return cur;
}

module.exports = { getPath };
EOT

cat > "$PROJECT_ROOT/backend/dq/rules.js" <<'EOT'
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
EOT

cat > "$PROJECT_ROOT/backend/dq/dqEngine.js" <<'EOT'
const { buildRuleEvaluator } = require("./rules");

class DQEngine {
  constructor(cfg) {
    this.mode = cfg?.mode || "flag";
    this.sampleLimit = cfg?.sampleLimit ?? 50;

    this.rules = (cfg?.rules || []).map((r) => ({
      ...r,
      id: r.id || `${r.type}:${r.path || (r.keys || []).join(",")}`
    }));

    this.compiled = this.rules.map((r) => ({ rule: r, eval: buildRuleEvaluator(r) }));

    this.totalRows = 0;
    this.ruleStats = Object.fromEntries(
      this.rules.map((r) => [r.id, { checked: 0, failed: 0, samples: [] }])
    );
  }

  check(row) {
    this.totalRows++;
    const violations = [];

    for (const { rule, eval: fn } of this.compiled) {
      this.ruleStats[rule.id].checked++;
      const ok = fn(row);
      if (!ok) {
        this.ruleStats[rule.id].failed++;
        violations.push({ ruleId: rule.id, type: rule.type, path: rule.path, keys: rule.keys });

        const bucket = this.ruleStats[rule.id].samples;
        if (bucket.length < this.sampleLimit) {
          bucket.push({ rowPreview: row?.data ?? {}, meta: row?.meta ?? {} });
        }
      }
    }

    const passed = violations.length === 0;
    if (!row.meta) row.meta = {};
    row.meta.dq = { passed, violations };

    if (this.mode === "flag") return { action: "keep", row };
    if (this.mode === "reject") return { action: passed ? "keep" : "drop", row };
    if (this.mode === "quarantine") return { action: passed ? "keep" : "quarantine", row };
    return { action: "keep", row };
  }

  finalize() {
    const summary = {
      mode: this.mode,
      totalRows: this.totalRows,
      rules: this.rules.map((r) => ({
        id: r.id,
        type: r.type,
        path: r.path,
        keys: r.keys,
        checked: this.ruleStats[r.id].checked,
        failed: this.ruleStats[r.id].failed,
        sampleCount: this.ruleStats[r.id].samples.length
      }))
    };

    const samplesByRule = Object.fromEntries(this.rules.map((r) => [r.id, this.ruleStats[r.id].samples]));
    return { summary, samplesByRule };
  }
}

module.exports = { DQEngine };
EOT

# -----------------------------
# Backend: stats (v1.1 with correlation)
# -----------------------------
cat > "$PROJECT_ROOT/backend/stats/online.js" <<'EOT'
class OnlineMoments {
  constructor() {
    this.n = 0;
    this.mean = 0;
    this.m2 = 0;
  }
  push(x) {
    this.n += 1;
    const delta = x - this.mean;
    this.mean += delta / this.n;
    const delta2 = x - this.mean;
    this.m2 += delta * delta2;
  }
  variance() {
    return this.n > 1 ? this.m2 / (this.n - 1) : 0;
  }
  std() {
    return Math.sqrt(this.variance());
  }
}

class OnlineDownsideMoments {
  constructor(target = 0) {
    this.target = target;
    this.m = new OnlineMoments();
  }
  push(x) {
    const d = x - this.target;
    if (d < 0) this.m.push(d);
  }
  downsideStd() {
    return this.m.std();
  }
}

class EWMA {
  constructor(lambda = 0.94) {
    this.lambda = lambda;
    this.var = null;
  }
  push(r) {
    const x2 = r * r;
    if (this.var === null) this.var = x2;
    else this.var = this.lambda * this.var + (1 - this.lambda) * x2;
  }
  std() {
    return this.var === null ? 0 : Math.sqrt(this.var);
  }
}

module.exports = { OnlineMoments, OnlineDownsideMoments, EWMA };
EOT

cat > "$PROJECT_ROOT/backend/stats/frequency.js" <<'EOT'
function secondsBetween(a, b) {
  const da = new Date(a).getTime();
  const db = new Date(b).getTime();
  if (!Number.isFinite(da) || !Number.isFinite(db)) return null;
  return Math.abs(db - da) / 1000;
}

function median(nums) {
  const a = nums.slice().sort((x, y) => x - y);
  const mid = Math.floor(a.length / 2);
  return a.length % 2 ? a[mid] : (a[mid - 1] + a[mid]) / 2;
}

function inferAnnualizationFromSeconds(medSec) {
  if (!medSec || !Number.isFinite(medSec) || medSec <= 0) return 252;

  const day = 24 * 3600;

  if (medSec >= 25 * day) return 12;
  if (medSec >= 6 * day) return 52;
  if (medSec >= 0.8 * day && medSec <= 1.2 * day) return 252;
  if (medSec >= 0.03 * day && medSec < 0.8 * day) return 252 * 24;
  if (medSec < 0.03 * day) return 252 * 24 * 60;
  return 252;
}

class FrequencyInferer {
  constructor(maxSamples = 2000) {
    this.maxSamples = maxSamples;
    this.deltas = [];
    this.lastDateBySeries = new Map();
  }

  observe(seriesKey, dateVal) {
    if (!dateVal) return;
    const last = this.lastDateBySeries.get(seriesKey);
    if (last) {
      const sec = secondsBetween(last, dateVal);
      if (sec && this.deltas.length < this.maxSamples) this.deltas.push(sec);
    }
    this.lastDateBySeries.set(seriesKey, dateVal);
  }

  finalize(defaultAnnualization = 252) {
    if (this.deltas.length < 10) return { annualization: defaultAnnualization, medianSeconds: null, inferred: false };
    const med = median(this.deltas);
    return { annualization: inferAnnualizationFromSeconds(med), medianSeconds: med, inferred: true };
  }
}

module.exports = { FrequencyInferer };
EOT

cat > "$PROJECT_ROOT/backend/stats/correlation.js" <<'EOT'
class OnlineCov {
  constructor() {
    this.n = 0;
    this.meanX = 0;
    this.meanY = 0;
    this.c = 0;
    this.m2x = 0;
    this.m2y = 0;
  }

  push(x, y) {
    this.n += 1;

    const dx = x - this.meanX;
    this.meanX += dx / this.n;
    const dy = y - this.meanY;
    this.meanY += dy / this.n;

    this.c += dx * (y - this.meanY);
    this.m2x += dx * (x - this.meanX);
    this.m2y += dy * (y - this.meanY);
  }

  corr() {
    if (this.n < 2) return 0;
    const vx = this.m2x / (this.n - 1);
    const vy = this.m2y / (this.n - 1);
    if (vx <= 0 || vy <= 0) return 0;
    const cov = this.c / (this.n - 1);
    return cov / Math.sqrt(vx * vy);
  }
}

function pairKey(a, b) {
  return a < b ? `${a}||${b}` : `${b}||${a}`;
}

class CorrelationEngine {
  constructor(cfg = {}) {
    this.cfg = cfg;
    this.pairs = new Map();
    this.seriesSeen = new Set();
  }

  _shouldIncludeSeries(seriesKey) {
    if (this.cfg.seriesAllowlist && this.cfg.seriesAllowlist.length > 0) {
      return this.cfg.seriesAllowlist.includes(seriesKey);
    }
    if (this.cfg.maxSeries && this.seriesSeen.size >= this.cfg.maxSeries && !this.seriesSeen.has(seriesKey)) {
      return false;
    }
    return true;
  }

  observeBucket(bucket) {
    const keys = Array.from(bucket.keys()).filter((k) => this._shouldIncludeSeries(k));
    for (const k of keys) this.seriesSeen.add(k);

    for (let i = 0; i < keys.length; i++) {
      for (let j = i + 1; j < keys.length; j++) {
        const a = keys[i], b = keys[j];
        const x = bucket.get(a), y = bucket.get(b);
        if (!Number.isFinite(x) || !Number.isFinite(y)) continue;

        const pk = pairKey(a, b);
        let oc = this.pairs.get(pk);
        if (!oc) { oc = new OnlineCov(); this.pairs.set(pk, oc); }
        oc.push(x, y);
      }
    }
  }

  finalize() {
    const series = Array.from(this.seriesSeen).sort();
    const matrix = {};
    for (const s of series) matrix[s] = {};

    for (const [pk, oc] of this.pairs.entries()) {
      const [a, b] = pk.split("||");
      const c = oc.corr();
      matrix[a][b] = c;
      matrix[b][a] = c;
    }
    for (const s of series) matrix[s][s] = 1;

    return { series, matrix };
  }
}

module.exports = { CorrelationEngine };
EOT

cat > "$PROJECT_ROOT/backend/stats/statsEngine.js" <<'EOT'
const { getPath } = require("../dq/path");
const { OnlineMoments, OnlineDownsideMoments, EWMA } = require("./online");
const { FrequencyInferer } = require("./frequency");
const { CorrelationEngine } = require("./correlation");

function toKey(row, keyPath) {
  if (!keyPath) return "__all__";
  const v = getPath(row, keyPath);
  return v === undefined || v === null ? "__null__" : String(v);
}

function toDateKey(dateVal) {
  const d = new Date(dateVal);
  if (!Number.isFinite(d.getTime())) return null;
  return d.toISOString();
}

class SeriesState {
  constructor(cfg) {
    this.cfg = cfg;
    this.prevPrice = null;

    this.retMoments = new OnlineMoments();
    this.downside = new OnlineDownsideMoments(cfg.sortinoTarget ?? 0);
    this.ewma = new EWMA(cfg.ewmaLambda ?? 0.94);

    this.equity = 1.0;
    this.peak = 1.0;
    this.maxDrawdown = 0;

    this.firstDate = null;
    this.lastDate = null;
  }

  pushReturn(r) {
    this.retMoments.push(r);
    this.downside.push(r);
    this.ewma.push(r);

    this.equity *= (1 + r);
    if (this.equity > this.peak) this.peak = this.equity;
    const dd = (this.peak - this.equity) / this.peak;
    if (dd > this.maxDrawdown) this.maxDrawdown = dd;
  }

  deriveReturnFromPrice(price) {
    if (this.prevPrice !== null && this.prevPrice !== 0) {
      const simple = (price / this.prevPrice) - 1;
      this.prevPrice = price;
      return simple;
    }
    this.prevPrice = price;
    return null;
  }

  push(row) {
    const cfg = this.cfg;

    const dateVal = cfg.datePath ? getPath(row, cfg.datePath) : null;
    if (dateVal) {
      const d = new Date(dateVal);
      if (Number.isFinite(d.getTime())) {
        if (!this.firstDate) this.firstDate = d;
        this.lastDate = d;
      }
    }

    let r = null;

    if (cfg.returnPath) {
      const v = Number(getPath(row, cfg.returnPath));
      if (Number.isFinite(v)) r = v;
    } else if (cfg.pricePath) {
      const p = Number(getPath(row, cfg.pricePath));
      if (Number.isFinite(p)) r = this.deriveReturnFromPrice(p);
    }

    if (r === null) return;

    if (cfg.logReturns) {
      if (r <= -1) return;
      r = Math.log1p(r);
    }

    this.pushReturn(r);
  }

  finalize(annualization = 252) {
    const n = this.retMoments.n;
    const mean = this.retMoments.mean;
    const vol = this.retMoments.std();
    const ewmaVol = this.ewma.std();
    const downsideStd = this.downside.downsideStd();

    const sharpe = vol > 0 ? (mean / vol) * Math.sqrt(annualization) : 0;
    const sortino = downsideStd > 0 ? (mean / downsideStd) * Math.sqrt(annualization) : 0;

    let cagr = null;
    if (this.firstDate && this.lastDate && this.lastDate > this.firstDate) {
      const years = (this.lastDate - this.firstDate) / (365.25 * 24 * 3600 * 1000);
      if (years > 0) cagr = Math.pow(this.equity, 1 / years) - 1;
    }

    return {
      n,
      meanReturn: mean,
      vol,
      ewmaVol,
      equityFinal: this.equity,
      maxDrawdown: this.maxDrawdown,
      sharpe,
      sortino,
      cagr
    };
  }
}

class StatsEngine {
  constructor(cfg) {
    this.cfg = cfg || {};
    this.enabled = !!this.cfg.enabled;

    this.series = new Map();
    this.totalRows = 0;

    this.freq = new FrequencyInferer(this.cfg.freqMaxSamples ?? 2000);

    this.corrEnabled = !!(this.cfg.correlation && this.cfg.correlation.enabled);
    this.corr = this.corrEnabled ? new CorrelationEngine(this.cfg.correlation) : null;

    this.currentDateKey = null;
    this.currentBucket = new Map();

    this.prevPriceBySeries = new Map();
  }

  _deriveReturn(seriesKey, row) {
    const cfg = this.cfg;

    let r = null;
    if (cfg.returnPath) {
      const v = Number(getPath(row, cfg.returnPath));
      if (Number.isFinite(v)) r = v;
    } else if (cfg.pricePath) {
      const p = Number(getPath(row, cfg.pricePath));
      if (!Number.isFinite(p)) return null;

      const prev = this.prevPriceBySeries.get(seriesKey);
      if (prev !== undefined && prev !== null && prev !== 0) {
        r = (p / prev) - 1;
      }
      this.prevPriceBySeries.set(seriesKey, p);
    }

    if (r === null) return null;

    if (cfg.logReturns) {
      if (r <= -1) return null;
      r = Math.log1p(r);
    }

    return r;
  }

  _flushCorrelationBucket() {
    if (!this.corrEnabled) return;
    if (this.currentBucket.size === 0) return;
    this.corr.observeBucket(this.currentBucket);
    this.currentBucket = new Map();
  }

  observe(row) {
    if (!this.enabled) return;
    this.totalRows++;

    const seriesKey = toKey(row, this.cfg.seriesKeyPath);

    if (this.cfg.datePath) {
      const dv = getPath(row, this.cfg.datePath);
      this.freq.observe(seriesKey, dv);
    }

    let st = this.series.get(seriesKey);
    if (!st) {
      st = new SeriesState(this.cfg);
      this.series.set(seriesKey, st);
    }
    st.push(row);

    if (this.corrEnabled) {
      if (!this.cfg.datePath) return;
      const dv = getPath(row, this.cfg.datePath);
      const dk = toDateKey(dv);
      if (!dk) return;

      if (this.currentDateKey === null) this.currentDateKey = dk;
      if (dk !== this.currentDateKey) {
        this._flushCorrelationBucket();
        this.currentDateKey = dk;
      }

      const r = this._deriveReturn(seriesKey, row);
      if (r !== null) this.currentBucket.set(seriesKey, r);
    }
  }

  finalize() {
    if (!this.enabled) return { enabled: false };

    this._flushCorrelationBucket();

    const inferred = this.freq.finalize(252);
    const annualization = this.cfg.annualization ?? inferred.annualization;

    const perSeries = {};
    for (const [k, st] of this.series.entries()) {
      perSeries[k] = st.finalize(annualization);
    }

    const correlation = this.corrEnabled ? this.corr.finalize() : null;

    return {
      enabled: true,
      createdAt: new Date(),
      totalRows: this.totalRows,
      config: {
        seriesKeyPath: this.cfg.seriesKeyPath || null,
        pricePath: this.cfg.pricePath || null,
        returnPath: this.cfg.returnPath || null,
        datePath: this.cfg.datePath || null,
        annualization,
        annualizationInferred: this.cfg.annualization == null ? inferred : { inferred: false },
        logReturns: !!this.cfg.logReturns,
        ewmaLambda: this.cfg.ewmaLambda ?? 0.94,
        correlation: this.cfg.correlation || { enabled: false }
      },
      perSeries,
      correlation
    };
  }
}

module.exports = { StatsEngine };
EOT

# -----------------------------
# Backend: artifacts (results, dq, stats, lineage, opt)
# -----------------------------
cat > "$PROJECT_ROOT/backend/artifacts/resultWriter.js" <<'EOT'
const DEFAULT_BATCH_SIZE = 1000;

class ResultWriter {
  constructor(db, opts) {
    this.db = db;
    this.runId = opts.runId;
    this.batchSize = opts.batchSize || DEFAULT_BATCH_SIZE;
    this.onProgress = opts.onProgress || (async () => {});

    this.col = this.db.collection("run_results");
    this.metaCol = this.db.collection("run_results_meta");

    this._rowId = 0;
    this._buffer = [];
    this._rowsOut = 0;
    this._bytesOut = 0;
    this._schema = {};
    this._nulls = {};
  }

  static _inferType(v) {
    if (v === null || v === undefined) return "null";
    if (Array.isArray(v)) return "array";
    if (v instanceof Date) return "date";
    const t = typeof v;
    if (t === "string") return "string";
    if (t === "number") return "number";
    if (t === "boolean") return "boolean";
    if (t === "object") return "object";
    return "unknown";
  }

  _updateSchemaAndNulls(data) {
    if (!data || typeof data !== "object") return;
    for (const [k, v] of Object.entries(data)) {
      const t = ResultWriter._inferType(v);
      if (t === "null") {
        this._nulls[k] = (this._nulls[k] || 0) + 1;
        if (!this._schema[k]) this._schema[k] = { type: "unknown" };
        continue;
      }
      if (!this._schema[k]) this._schema[k] = { type: t };
      else if (this._schema[k].type !== t && this._schema[k].type !== "mixed") this._schema[k] = { type: "mixed" };
    }
  }

  async writeRow({ data, meta }) {
    const doc = {
      runId: this.runId,
      rowId: this._rowId++,
      data: data || {},
      meta: meta || {}
    };

    try { this._bytesOut += Buffer.byteLength(JSON.stringify(doc)); } catch {}

    this._updateSchemaAndNulls(doc.data);

    this._buffer.push(doc);
    this._rowsOut++;

    if (this._buffer.length >= this.batchSize) {
      await this.flush();
    }

    if (this._rowsOut % (this.batchSize * 5) === 0) {
      await this.onProgress({ rowsOut: this._rowsOut, bytesOut: this._bytesOut });
    }
  }

  async flush() {
    if (this._buffer.length === 0) return;
    const batch = this._buffer;
    this._buffer = [];
    await this.col.insertMany(batch, { ordered: false });
  }

  async finalize() {
    await this.flush();

    const metaDoc = {
      runId: this.runId,
      schema: { fields: this._schema },
      counts: { rows: this._rowsOut, nulls: this._nulls },
      createdAt: new Date()
    };

    await this.metaCol.updateOne(
      { runId: this.runId },
      { $set: metaDoc },
      { upsert: true }
    );

    return {
      resultRef: {
        collection: "run_results",
        metaCollection: "run_results_meta",
        runId: this.runId
      },
      counts: { rowsOut: this._rowsOut, bytesOut: this._bytesOut },
      schema: metaDoc.schema
    };
  }
}

module.exports = { ResultWriter };
EOT

cat > "$PROJECT_ROOT/backend/artifacts/dqWriter.js" <<'EOT'
async function writeDQReport(db, runId, dqFinal) {
  const col = db.collection("run_dq_reports");
  const doc = {
    runId,
    createdAt: new Date(),
    summary: dqFinal.summary,
    samplesByRule: dqFinal.samplesByRule
  };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_dq_reports", runId };
}

module.exports = { writeDQReport };
EOT

cat > "$PROJECT_ROOT/backend/artifacts/statsWriter.js" <<'EOT'
async function writeStatsReport(db, runId, statsFinal) {
  const col = db.collection("run_stats_reports");
  const doc = {
    runId,
    createdAt: new Date(),
    ...statsFinal
  };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_stats_reports", runId };
}

module.exports = { writeStatsReport };
EOT

cat > "$PROJECT_ROOT/backend/artifacts/lineageWriter.js" <<'EOT'
async function writeLineageOps(db, runId, lineageDoc) {
  const col = db.collection("run_lineage_ops");
  await col.updateOne({ runId }, { $set: lineageDoc }, { upsert: true });
  return { collection: "run_lineage_ops", runId };
}

module.exports = { writeLineageOps };
EOT

cat > "$PROJECT_ROOT/backend/artifacts/optWriter.js" <<'EOT'
async function writeOptReport(db, runId, opt) {
  const col = db.collection("run_opt_reports");
  const doc = { runId, createdAt: new Date(), ...opt };
  await col.updateOne({ runId }, { $set: doc }, { upsert: true });
  return { collection: "run_opt_reports", runId };
}

module.exports = { writeOptReport };
EOT

# -----------------------------
# Backend: routes (DQ/stats/lineage)
# -----------------------------
cat > "$PROJECT_ROOT/backend/routes/runDqRoutes.js" <<'EOT'
const express = require("express");
const router = express.Router();

router.get("/:runId/dq", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_dq_reports").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No DQ report for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
EOT

cat > "$PROJECT_ROOT/backend/routes/runStatsRoutes.js" <<'EOT'
const express = require("express");
const router = express.Router();

router.get("/:runId/stats", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_stats_reports").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No stats report for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
EOT

cat > "$PROJECT_ROOT/backend/routes/runLineageRoutes.js" <<'EOT'
const express = require("express");
const router = express.Router();

router.get("/:runId/lineage", async (req, res, next) => {
  try {
    const db = req.app.locals.mongoDb;
    const doc = await db.collection("run_lineage_ops").findOne({ runId: req.params.runId });
    if (!doc) return res.status(404).json({ error: "NotFound", message: "No lineage for run" });
    res.json(doc);
  } catch (e) { next(e); }
});

module.exports = router;
EOT

# -----------------------------
# Scripts
# -----------------------------
cat > "$PROJECT_ROOT/scripts/db-indexes.js" <<'EOT'
// Run with:
//   mongosh "mongodb://localhost:27017/fullstack_app" scripts/db-indexes.js

db = db.getSiblingDB("fullstack_app");

// Results

db.run_results.createIndex({ runId: 1, rowId: 1 }, { unique: true });
db.run_results.createIndex({ runId: 1 });
db.run_results_meta.createIndex({ runId: 1 }, { unique: true });

// Artifacts

db.run_dq_reports.createIndex({ runId: 1 }, { unique: true });
db.run_stats_reports.createIndex({ runId: 1 }, { unique: true });
db.run_lineage_ops.createIndex({ runId: 1 }, { unique: true });
db.run_opt_reports.createIndex({ runId: 1 }, { unique: true });

print("✅ Indexes created");
EOT

# -----------------------------
# Docs
# -----------------------------
cat > "$PROJECT_ROOT/docs/ROADMAP.md" <<'EOT'
# Roadmap (high-level)

## v2 — Rewrite-aware optimization + correctness hardening
- IR optimizer v2: rewrite-aware filter pushdown across renaming projections using `projectMap` + `exprRewrite`
- Add strict operator barriers and purity rules for computed substitutions
- Cursor-based result pagination endpoint (no skip)
- Quarantine collection + paging endpoint for DQ mode=quarantine
- Enforce/validate sort requirements for stats correlation/drawdown (or add Sort operator)

## v3 — Execution planner surfaces + pushdown + stability
- Physical planner: compile eligible segments to Mongo aggregation (pushdown)
- Operator-level schema flow (per-op schema snapshots + null-rate)
- Join + groupBy operators instrumented with multi-input lineage stats
- Cache and materialization policy (retain/reuse run_results for identical planHash+params)
- CI: unit tests for optimizer rewrite correctness + golden-corpus parity
EOT

cat > "$PROJECT_ROOT/README.md" <<'EOT'
# FullStack Workspace (Backend Artifacts + Middleware Core)

This workspace contains the backend-core modules for:
- DQ Engine v1
- Stats Pack v1.1 (frequency inference + correlation)
- Operator-level lineage summary
- IR normalization + optimizer v1
- v2 building blocks: projectMap + exprRewrite

## Rebuild
This repository includes a rebuild script that can regenerate the workspace into a new folder.

```bash
chmod +x rebuild.sh
./rebuild.sh ./out_workspace
```

## Mongo indexes
```bash
mongosh "mongodb://localhost:27017/fullstack_app" scripts/db-indexes.js
```
EOT

chmod +x "$PROJECT_ROOT/scripts/db-indexes.js" || true

echo "✅ Rebuilt workspace at: $PROJECT_ROOT"
