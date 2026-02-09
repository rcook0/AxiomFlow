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
