class LineageDagCollector {
  constructor({ runId, plan, engineVersion = "lineage-dag-v3.0-alpha" }) {
    this.runId = runId;
    this.engineVersion = engineVersion;
    this.plan = plan;

    this.nodeStats = {};
    this.timeline = [];
  }

  _ensure(nodeId) {
    if (!this.nodeStats[nodeId]) {
      this.nodeStats[nodeId] = { rowsInByPort: {}, rowsOut: 0, ms: 0, startedAt: null };
    }
    return this.nodeStats[nodeId];
  }

  start(nodeId) {
    const st = this._ensure(nodeId);
    st.startedAt = Date.now();
    this.timeline.push({ nodeId, event: "start", ts: new Date() });
  }

  end(nodeId) {
    const st = this._ensure(nodeId);
    if (st.startedAt) {
      const ms = Date.now() - st.startedAt;
      st.ms += ms;
      st.startedAt = null;
      this.timeline.push({ nodeId, event: "end", ts: new Date(), ms });
    }
  }

  incRowsIn(nodeId, port = "in", n = 1) {
    const st = this._ensure(nodeId);
    st.rowsInByPort[port] = (st.rowsInByPort[port] || 0) + n;
  }

  incRowsOut(nodeId, n = 1) {
    const st = this._ensure(nodeId);
    st.rowsOut += n;
  }

  finalize() {
    return {
      runId: this.runId,
      createdAt: new Date(),
      engineVersion: this.engineVersion,
      plan: this.plan,
      stats: this.nodeStats,
      timeline: this.timeline
    };
  }
}

module.exports = { LineageDagCollector };
