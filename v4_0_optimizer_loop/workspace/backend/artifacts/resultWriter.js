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
